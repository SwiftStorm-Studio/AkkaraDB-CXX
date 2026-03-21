/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/src/engine/sst/SSTReader.cpp
#include "engine/sstable/SSTReader.hpp"
#include "engine/sstable/SSTFormat.hpp"
#include "core/CRC32C.hpp"
#include "core/record/AKHdr32.hpp"

#include <zstd.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <memory>

namespace akkaradb::engine::sst {

    // ============================================================================
    // Platform helpers (local to this TU)
    // ============================================================================

    namespace {

        FILE* sst_open_rb(const std::filesystem::path& p) {
#ifdef _WIN32
            return _wfopen(p.wstring().c_str(), L"rb");
#else
            return fopen(p.string().c_str(), "rb");
#endif
        }

        int sst_fseek(FILE* f, int64_t off, int whence) {
#ifdef _WIN32
            return _fseeki64(f, off, whence);
#else
            return fseeko(f, static_cast<off_t>(off), whence);
#endif
        }

        int64_t sst_ftell(FILE* f) {
            #ifdef _WIN32
            return _ftelli64(f);
            #else
            return ftello(f);
            #endif
        }

        // Read exactly n bytes into buf. Returns true on success.
        bool fread_exact(FILE* f, void* buf, size_t n) {
            return n == 0 || fread(buf, 1, n, f) == n;
        }

        // Lexicographic key comparison. Returns negative/zero/positive.
        int cmp_keys(const uint8_t* a, size_t a_len,
                     const uint8_t* b, size_t b_len) noexcept {
            const size_t n = std::min(a_len, b_len);
            if (n > 0) {
                const int c = std::memcmp(a, b, n);
                if (c != 0) return c;
            }
            if (a_len < b_len) return -1;
            if (a_len > b_len) return  1;
            return 0;
        }

        int cmp_keys(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
            return cmp_keys(a.data(), a.size(), b.data(), b.size());
        }

    } // anonymous namespace

    // ============================================================================
    // SSTReader::Iterator::Impl
    // ============================================================================

    struct SSTReader::Iterator::Impl {
        // ── File mode (uncompressed) ─────────────────────────────────────────
        FILE* file = nullptr;
        uint64_t data_end = 0; ///< absolute file offset where data section ends

        // ── Buffer mode (compressed — decomp_data_ in SSTReader) ─────────────
        const uint8_t* buf_data = nullptr; ///< pointer into SSTReader::decomp_data_
        size_t buf_size = 0; ///< total decompressed data section bytes
        size_t buf_pos = 0; ///< current read position within buf_data

        std::vector<uint8_t> end_key; ///< exclusive upper bound (empty = unbounded)
        std::optional<core::MemRecord> pending; ///< next record to return

        ~Impl() { if (file) { fclose(file); file = nullptr; } }

        bool is_buf_mode() const noexcept { return buf_data != nullptr; }

        /// Reads the next record and stores it in pending.
        /// Sets pending = nullopt if end of data / end_key exceeded.
        void advance() {
            if (is_buf_mode()) advance_from_buf();
            else advance_from_file();
        }

        void advance_from_file() {
            pending.reset();
            if (!file) return;

            const int64_t pos_i64 = sst_ftell(file);
            if (pos_i64 < 0 || static_cast<uint64_t>(pos_i64) >= data_end) return;

            // Read header
            core::AKHdr32 hdr{};
            if (!fread_exact(file, &hdr, sizeof(hdr))) return;

            // Read key
            std::vector<uint8_t> key(hdr.k_len);
            if (!fread_exact(file, key.data(), hdr.k_len)) return;

            // Check end_key (exclusive upper bound)
            if (!end_key.empty()) {
                if (cmp_keys(key.data(), key.size(), end_key.data(), end_key.size()) >= 0) return;
            }

            // Read value
            std::vector<uint8_t> val(hdr.v_len);
            if (!fread_exact(file, val.data(), hdr.v_len)) return;

            // Verify per-record CRC32C trailer
            uint32_t stored_crc = 0;
            if (!fread_exact(file, &stored_crc, sizeof(stored_crc))) return;
            {
                uint32_t expected = core::CRC32C::compute(&hdr, sizeof(hdr));
                expected = core::CRC32C::append(key.data(), key.size(), expected);
                expected = core::CRC32C::append(val.data(), val.size(), expected);
                if (stored_crc != expected) return; // CRC mismatch: stop iteration
            }

            pending = core::MemRecord::create(
                std::span<const uint8_t>(key.data(), key.size()),
                std::span<const uint8_t>(val.data(), val.size()),
                hdr.seq,
                hdr.flags,
                hdr.key_fp64
            );
        }

        void advance_from_buf() {
            pending.reset();
            if (!buf_data || buf_pos >= buf_size) return;

            // Helper lambdas for safe buffer reads
            auto buf_read = [&](void* dst, size_t n) -> bool {
                if (buf_pos + n > buf_size) return false;
                std::memcpy(dst, buf_data + buf_pos, n);
                buf_pos += n;
                return true;
            };

            // Read header
            core::AKHdr32 hdr{};
            if (!buf_read(&hdr, sizeof(hdr))) return;

            // Read key
            std::vector<uint8_t> key(hdr.k_len);
            if (!buf_read(key.data(), hdr.k_len)) return;

            // Check end_key (exclusive upper bound)
            if (!end_key.empty()) { if (cmp_keys(key.data(), key.size(), end_key.data(), end_key.size()) >= 0) return; }

            // Read value
            std::vector<uint8_t> val(hdr.v_len);
            if (!buf_read(val.data(), hdr.v_len)) return;

            // Verify per-record CRC32C trailer
            uint32_t stored_crc = 0;
            if (!buf_read(&stored_crc, sizeof(stored_crc))) return;
            {
                uint32_t expected = core::CRC32C::compute(&hdr, sizeof(hdr));
                expected = core::CRC32C::append(key.data(), key.size(), expected);
                expected = core::CRC32C::append(val.data(), val.size(), expected);
                if (stored_crc != expected) return; // CRC mismatch: stop iteration
            }

            pending = core::MemRecord::create(
                std::span<const uint8_t>(key.data(), key.size()),
                std::span<const uint8_t>(val.data(), val.size()),
                hdr.seq, hdr.flags, hdr.key_fp64
            );
        }
    };

    // ============================================================================
    // SSTReader::Iterator
    // ============================================================================

    SSTReader::Iterator::Iterator(std::unique_ptr<Impl> impl)
        : impl_(std::move(impl)) {
        if (impl_) {
            impl_->advance();
            has_pending_ = impl_->pending.has_value();
        }
    }

    SSTReader::Iterator::~Iterator() = default;

    SSTReader::Iterator::Iterator(Iterator&& o) noexcept
        : impl_(std::move(o.impl_)), has_pending_(o.has_pending_) {
        o.has_pending_ = false;
    }

    SSTReader::Iterator& SSTReader::Iterator::operator=(Iterator&& o) noexcept {
        if (this != &o) {
            impl_ = std::move(o.impl_);
            has_pending_ = o.has_pending_;
            o.has_pending_ = false;
        }
        return *this;
    }

    core::MemRecord SSTReader::Iterator::next() {
        assert(has_pending_ && impl_ && impl_->pending.has_value());
        core::MemRecord rec = std::move(*impl_->pending);
        impl_->advance();
        has_pending_ = impl_->pending.has_value();
        return rec;
    }

    // ============================================================================
    // SSTReader::open
    // ============================================================================

    std::unique_ptr<SSTReader> SSTReader::open(const std::filesystem::path& path, bool preload_data) {
        FILE* f = sst_open_rb(path);
        if (!f) return nullptr;

        // Read header
        SSTFileHeader hdr{};
        if (!fread_exact(f, &hdr, sizeof(hdr))) { fclose(f); return nullptr; }

        // Validate magic and version
        if (hdr.magic != SST_MAGIC || hdr.version != SST_VERSION) {
            fclose(f); return nullptr;
        }

        // Validate CRC32C (computed over bytes [0..59] with crc32c field zeroed)
        {
            SSTFileHeader tmp = hdr;
            tmp.crc32c = 0;
            const uint32_t expected = core::CRC32C::compute(&tmp, sizeof(tmp) - sizeof(uint32_t));
            if (expected != hdr.crc32c) { fclose(f); return nullptr; }
        }

        // ── Optional: read compressed data section bytes (lazy decompression) ──
        //
        // For Zstd-compressed files we read the compressed bytes into
        // compressed_bytes_ during open() but defer the actual ZSTD_decompress
        // call to the first get()/scan() access (ensure_decompressed()).
        // This avoids paying the CPU cost of decompression for files that are
        // opened (e.g. during recover()) but never queried.
        std::vector<uint8_t> compressed_bytes_init;
        bool compressed = false;

        if (hdr.flags == SST_CODEC_ZSTD && hdr.data_size > 0) {
            // compressed_size = index_offset - DATA_OFFSET  (no extra header field needed)
            if (hdr.index_offset <= DATA_OFFSET) {
                fclose(f);
                return nullptr;
            }
            const size_t compressed_size = static_cast<size_t>(hdr.index_offset - DATA_OFFSET);

            // Seek to data section start
            if (sst_fseek(f, static_cast<int64_t>(DATA_OFFSET), SEEK_SET) != 0) {
                fclose(f);
                return nullptr;
            }

            // Read compressed bytes — decompression is deferred to first access
            compressed_bytes_init.resize(compressed_size);
            if (!fread_exact(f, compressed_bytes_init.data(), compressed_size)) {
                fclose(f);
                return nullptr;
            }
            compressed = true;
        }

        // ── Read sparse index section ─────────────────────────────────────────
        std::vector<uint8_t> raw_index;
        if (hdr.index_count > 0 && hdr.index_offset > 0) {
            if (sst_fseek(f, static_cast<int64_t>(hdr.index_offset), SEEK_SET) != 0) {
                fclose(f); return nullptr;
            }

            uint64_t index_size;
            if (hdr.bloom_offset > 0 && hdr.bloom_offset > hdr.index_offset) {
                index_size = hdr.bloom_offset - hdr.index_offset;
            } else {
                // No bloom filter: index goes to end of file
                if (sst_fseek(f, 0, SEEK_END) != 0) { fclose(f); return nullptr; }
                const int64_t file_end = sst_ftell(f);
                if (file_end < 0) { fclose(f); return nullptr; }
                index_size = static_cast<uint64_t>(file_end) - hdr.index_offset;
                if (sst_fseek(f, static_cast<int64_t>(hdr.index_offset), SEEK_SET) != 0) {
                    fclose(f);
                    return nullptr;
                }
            }

            raw_index.resize(index_size);
            if (!fread_exact(f, raw_index.data(), index_size)) {
                fclose(f); return nullptr;
            }
        }

        // ── Read Bloom filter section ─────────────────────────────────────────
        std::vector<uint8_t> bloom_data;
        if (hdr.bloom_offset > 0 && hdr.bloom_size > 0) {
            if (sst_fseek(f, static_cast<int64_t>(hdr.bloom_offset), SEEK_SET) != 0) {
                fclose(f); return nullptr;
            }
            bloom_data.resize(hdr.bloom_size);
            if (!fread_exact(f, bloom_data.data(), hdr.bloom_size)) {
                fclose(f); return nullptr;
            }
        }

        fclose(f);
        f = nullptr;

        // ── Build SSTReader ───────────────────────────────────────────────────
        auto reader = std::unique_ptr<SSTReader>(new SSTReader());
        reader->path_ = path;
        reader->header_ = hdr;
        reader->bloom_data_ = std::move(bloom_data);
        reader->compressed_ = compressed;
        reader->compressed_bytes_ = std::move(compressed_bytes_init); // decompressed lazily
        // decomp_data_ stays empty until ensure_decompressed() is called

        // Parse sparse index
        {
            const uint8_t* p   = raw_index.data();
            const uint8_t* end = p + raw_index.size();
            while (p + INDEX_ENTRY_FIXED_SIZE <= end) {
                IndexEntry entry;
                std::memcpy(&entry.data_offset, p, 8); p += 8;
                std::memcpy(&entry.mini_key,    p, 8); p += 8;
                std::memcpy(&entry.key_fp64,    p, 8); p += 8;
                uint16_t klen = 0;
                std::memcpy(&klen, p, 2); p += 2;
                if (p + klen > end) break; // truncated entry
                entry.key.assign(p, p + klen); p += klen;
                reader->parsed_index_.push_back(std::move(entry));
            }
        }

        // Extract first/last keys from the parsed index
        if (!reader->parsed_index_.empty()) {
            reader->first_key_ = reader->parsed_index_.front().key;
            reader->last_key_  = reader->parsed_index_.back().key;
        }

        // ── Optional: preload uncompressed data section into RAM ──────────────
        //
        // When preload_data=true and the file is NOT Zstd-compressed, we eagerly
        // read the entire data section (header_.data_size bytes) into decomp_data_
        // and set compressed_=true so all dispatch logic (contains_fast, scan, get)
        // uses the in-memory buffer path instead of per-call fopen/fseek/fclose.
        //
        // ensure_decompressed() early-exits when !decomp_data_.empty(), so it will
        // never attempt a bogus ZSTD_decompress on the pre-loaded uncompressed bytes.
        //
        // For Zstd files, compressed_bytes_ is already in RAM (read above) and
        // decompression is deferred to first access — preload_data has no extra effect.
        if (preload_data && !reader->compressed_ && reader->header_.data_size > 0) {
            FILE* pf = sst_open_rb(path);
            if (pf) {
                if (sst_fseek(pf, static_cast<int64_t>(DATA_OFFSET), SEEK_SET) == 0) {
                    reader->decomp_data_.resize(static_cast<size_t>(reader->header_.data_size));
                    if (fread_exact(pf, reader->decomp_data_.data(), reader->decomp_data_.size())) {
                        // Mark as "use buffer" by reusing the compressed_ dispatch flag.
                        // decomp_data_ is already populated so ensure_decompressed() no-ops.
                        reader->compressed_ = true;
                    }
                    else {
                        reader->decomp_data_.clear(); // I/O error: fall back to file path
                    }
                }
                fclose(pf);
            }
        }

        return reader;
    }

    SSTReader::~SSTReader() = default;

    SSTReader::SSTReader(SSTReader&&) noexcept = default;
    SSTReader& SSTReader::operator=(SSTReader&&) noexcept = default;

    // ============================================================================
    // SSTReader::ensure_decompressed
    // ============================================================================

    void SSTReader::ensure_decompressed() const {
        if (!compressed_ || !decomp_data_.empty()) return; // already done or not compressed

        // compressed_bytes_ must be non-empty if compressed_ is true and we haven't
        // decompressed yet.
        if (compressed_bytes_.empty()) return; // nothing to decompress (unexpected)

        decomp_data_.resize(static_cast<size_t>(header_.data_size));
        const size_t result = ZSTD_decompress(decomp_data_.data(), decomp_data_.size(), compressed_bytes_.data(), compressed_bytes_.size());

        if (ZSTD_isError(result) || result != header_.data_size) {
            // Decompression failed: clear both to signal error to callers
            decomp_data_.clear();
            return;
        }

        // Release the compressed bytes — they are no longer needed
        compressed_bytes_.clear();
        compressed_bytes_.shrink_to_fit();
    }

    // ============================================================================
    // SSTReader::bloom_check
    // ============================================================================

    bool SSTReader::bloom_check(std::span<const uint8_t> key) const noexcept {
        if (bloom_data_.size() < sizeof(BloomHeader)) return true; // no bloom → assume present

        const BloomHeader* bh = reinterpret_cast<const BloomHeader*>(bloom_data_.data());
        if (bh->num_bits == 0 || bh->num_hashes == 0) return true;

        const uint8_t* bits = bloom_data_.data() + sizeof(BloomHeader);
        const size_t   bits_bytes = bloom_data_.size() - sizeof(BloomHeader);

        // ── Helper lambda: blocked-bloom bit test given pre-split h1, h2 ──────
        const auto blocked_test = [&](uint32_t h1, uint32_t h2) noexcept -> bool {
            const uint32_t num_blocks = bh->num_bits / 512u;
            if (num_blocks == 0 || bits_bytes < static_cast<size_t>(num_blocks) * 64u) return true; // truncated → conservative
            const uint8_t* block = bits + (h2 & (num_blocks - 1u)) * 64u;
            const uint32_t delta = (h1 >> 17u) | 1u; // must be odd
            for (uint8_t i = 0; i < bh->num_hashes; ++i) {
                const uint32_t bit = (h1 + static_cast<uint32_t>(i) * delta) & 511u;
                if (!(block[bit >> 3u] & (1u << (bit & 7u)))) return false;
            }
            return true;
        };

        if (bh->type == BLOOM_TYPE_BLOCKED_FAST) {
            // ── Fast path: bloom_fast_hash64 (~5 ns) ─────────────────────────
            const uint64_t fp = bloom_fast_hash64(key.data(), key.size());
            return blocked_test(static_cast<uint32_t>(fp), static_cast<uint32_t>(fp >> 32));
        }

        if (bh->type == BLOOM_TYPE_BLOCKED) {
            // ── Legacy blocked bloom using SipHash-2-4 ───────────────────────
            const uint64_t fp = core::AKHdr32::compute_key_fp64(key.data(), key.size());
            return blocked_test(static_cast<uint32_t>(fp), static_cast<uint32_t>(fp >> 32));
        }

        // ── Standard double-hashing (BLOOM_TYPE_STANDARD / type == 0) ────────
        if (bits_bytes < (bh->num_bits + 7u) / 8u) return true; // truncated → conservative
        const uint64_t fp = core::AKHdr32::compute_key_fp64(key.data(), key.size());
        const uint32_t h1 = static_cast<uint32_t>(fp);
        const uint32_t h2 = (static_cast<uint32_t>(fp >> 32)) | 1u;
        const uint32_t nb = bh->num_bits;
        for (uint8_t i = 0; i < bh->num_hashes; ++i) {
            const uint32_t pos = (h1 + static_cast<uint32_t>(i) * h2) % nb;
            if (!(bits[pos >> 3u] & (1u << (pos & 7u)))) return false;
        }
        return true;
    }

    // ============================================================================
    // SSTReader::bloom_check_fp  (precomputed hash variant)
    // ============================================================================

    bool SSTReader::bloom_check_fp(uint64_t fp64) const noexcept {
        if (bloom_data_.size() < sizeof(BloomHeader)) return true;

        const BloomHeader* bh = reinterpret_cast<const BloomHeader*>(bloom_data_.data());
        if (bh->num_bits == 0 || bh->num_hashes == 0) return true;

        const uint8_t* bits = bloom_data_.data() + sizeof(BloomHeader);
        const size_t bits_bytes = bloom_data_.size() - sizeof(BloomHeader);

        // ── Blocked bloom: both BLOCKED and BLOCKED_FAST use the same probe logic ──
        if (bh->type == BLOOM_TYPE_BLOCKED_FAST || bh->type == BLOOM_TYPE_BLOCKED) {
            const uint32_t num_blocks = bh->num_bits / 512u;
            if (num_blocks == 0 || bits_bytes < static_cast<size_t>(num_blocks) * 64u) return true;
            const uint32_t h1 = static_cast<uint32_t>(fp64);
            const uint32_t h2 = static_cast<uint32_t>(fp64 >> 32);
            const uint8_t* block = bits + (h2 & (num_blocks - 1u)) * 64u;
            const uint32_t delta = (h1 >> 17u) | 1u;
            for (uint8_t i = 0; i < bh->num_hashes; ++i) {
                const uint32_t bit = (h1 + static_cast<uint32_t>(i) * delta) & 511u;
                if (!(block[bit >> 3u] & (1u << (bit & 7u)))) return false;
            }
            return true;
        }

        // ── Standard double-hashing (BLOOM_TYPE_STANDARD) ────────────────────
        if (bits_bytes < (bh->num_bits + 7u) / 8u) return true;
        const uint32_t h1 = static_cast<uint32_t>(fp64);
        const uint32_t h2 = (static_cast<uint32_t>(fp64 >> 32)) | 1u;
        const uint32_t nb = bh->num_bits;
        for (uint8_t i = 0; i < bh->num_hashes; ++i) {
            const uint32_t pos = (h1 + static_cast<uint32_t>(i) * h2) % nb;
            if (!(bits[pos >> 3u] & (1u << (pos & 7u)))) return false;
        }
        return true;
    }

    // ============================================================================
    // SSTReader::index_seek
    // ============================================================================

    uint64_t SSTReader::index_seek(std::span<const uint8_t> key) const noexcept {
        if (parsed_index_.empty()) return 0;

        // Precompute target mini_key for fast prefix comparison.
        // build_mini_key stores key[i] at bit offset i*8 (LE-packed), so on
        // little-endian machines memcmp(&mini_key, &target, 8) is equivalent to
        // memcmp(key_bytes, target_bytes, 8) — i.e. a lexicographic comparison of
        // the first 8 key bytes.  When the mini comparison is non-zero we can skip
        // the full cmp_keys call entirely.
        const uint64_t target_mini = core::AKHdr32::build_mini_key(key.data(), key.size());

        // Binary search: find rightmost entry with entry.key <= key
        size_t lo = 0, hi = parsed_index_.size();
        while (lo < hi) {
            const size_t mid = lo + (hi - lo) / 2;
            const IndexEntry& e = parsed_index_[mid];

            // Fast path: compare first 8 key bytes via the precomputed mini_key.
            int c = std::memcmp(&e.mini_key, &target_mini, sizeof(uint64_t));
            if (c == 0) {
                // First 8 bytes are identical → need full-key comparison to resolve
                // keys longer than 8 bytes (or equal-length keys).
                c = cmp_keys(e.key.data(), e.key.size(), key.data(), key.size());
            }

            if (c <= 0) lo = mid + 1;  // entry.key <= key → search right
            else        hi = mid;      // entry.key  > key → search left
        }
        // lo-1 is the rightmost entry with key <= target
        if (lo == 0) return 0; // target is before first index entry → start from beginning
        return parsed_index_[lo - 1].data_offset;
    }

    // ============================================================================
    // SSTReader::scan_from  (dispatcher)
    // ============================================================================

    std::optional<core::MemRecord>
    SSTReader::scan_from(uint64_t data_section_offset,
                         std::span<const uint8_t> target_key) const {
        if (compressed_) return scan_from_buf(data_section_offset, target_key);
        return scan_from_file(data_section_offset, target_key);
    }

    // ============================================================================
    // SSTReader::scan_from_file  (uncompressed — reads directly from .aksst)
    // ============================================================================

    std::optional<core::MemRecord> SSTReader::scan_from_file(uint64_t data_section_offset, std::span<const uint8_t> target_key) const {
        FILE* f = sst_open_rb(path_);
        if (!f) return std::nullopt;

        const int64_t abs_pos = static_cast<int64_t>(DATA_OFFSET + data_section_offset);
        if (sst_fseek(f, abs_pos, SEEK_SET) != 0) { fclose(f); return std::nullopt; }

        const uint64_t data_end = DATA_OFFSET + header_.data_size;
        std::optional<core::MemRecord> result;

        while (true) {
            const int64_t cur = sst_ftell(f);
            if (cur < 0 || static_cast<uint64_t>(cur) >= data_end) break;

            core::AKHdr32 hdr{};
            if (!fread_exact(f, &hdr, sizeof(hdr))) break;

            std::vector<uint8_t> key(hdr.k_len);
            if (!fread_exact(f, key.data(), hdr.k_len)) break;

            const int c = cmp_keys(key.data(), key.size(),
                                   target_key.data(), target_key.size());
            if (c == 0) {
                std::vector<uint8_t> val(hdr.v_len);
                if (!fread_exact(f, val.data(), hdr.v_len)) break;

                uint32_t stored_crc = 0;
                if (!fread_exact(f, &stored_crc, sizeof(stored_crc))) break;
                {
                    uint32_t expected = core::CRC32C::compute(&hdr, sizeof(hdr));
                    expected = core::CRC32C::append(key.data(), key.size(), expected);
                    expected = core::CRC32C::append(val.data(), val.size(), expected);
                    if (stored_crc != expected) break;
                }
                result = core::MemRecord::create(
                    std::span<const uint8_t>(key.data(), key.size()),
                    std::span<const uint8_t>(val.data(), val.size()),
                    hdr.seq, hdr.flags, hdr.key_fp64
                );
                break;
            } else if (c > 0) {
                break; // scanned past target key
            }
            // key < target: skip value + CRC and continue
            if (sst_fseek(f, static_cast<int64_t>(hdr.v_len) + static_cast<int64_t>(RECORD_CRC_SIZE), SEEK_CUR) != 0) break;
        }

        fclose(f);
        return result;
    }

    // ============================================================================
    // SSTReader::scan_from_buf  (compressed — reads from decomp_data_)
    // ============================================================================

    std::optional<core::MemRecord> SSTReader::scan_from_buf(uint64_t data_section_offset, std::span<const uint8_t> target_key) const {
        ensure_decompressed();
        if (decomp_data_.empty()) return std::nullopt;
        if (data_section_offset >= decomp_data_.size()) return std::nullopt;

        size_t pos = static_cast<size_t>(data_section_offset);
        const size_t total = decomp_data_.size();
        const uint8_t* buf = decomp_data_.data();

        std::optional<core::MemRecord> result;

        while (pos < total) {
            // Read header
            if (pos + sizeof(core::AKHdr32) > total) break;
            core::AKHdr32 hdr{};
            std::memcpy(&hdr, buf + pos, sizeof(hdr));
            pos += sizeof(hdr);

            // Zero-copy key comparison: use a direct pointer into decomp_data_
            // (same technique as contains_from_buf — no heap allocation per record)
            if (pos + hdr.k_len > total) break;
            const uint8_t* key_ptr = buf + pos;
            pos += hdr.k_len;

            const int c = cmp_keys(key_ptr, hdr.k_len, target_key.data(), target_key.size());
            if (c == 0) {
                // Key matched — read value and CRC via direct pointer (no vector alloc)
                if (pos + hdr.v_len + RECORD_CRC_SIZE > total) break;
                const uint8_t* val_ptr = buf + pos;
                pos += hdr.v_len;

                uint32_t stored_crc = 0;
                std::memcpy(&stored_crc, buf + pos, sizeof(stored_crc));
                pos += sizeof(stored_crc);

                {
                    uint32_t expected = core::CRC32C::compute(&hdr, sizeof(hdr));
                    expected = core::CRC32C::append(key_ptr, hdr.k_len, expected);
                    expected = core::CRC32C::append(val_ptr, hdr.v_len, expected);
                    if (stored_crc != expected) break;
                }
                result = core::MemRecord::create(
                    std::span<const uint8_t>(key_ptr, hdr.k_len),
                    std::span<const uint8_t>(val_ptr, hdr.v_len),
                    hdr.seq,
                    hdr.flags,
                    hdr.key_fp64 // precomputed — skips SipHash in create()
                );
                break;
            }
            else if (c > 0) {
                break; // scanned past target key
            }
            // key < target: skip value + CRC and continue
            if (pos + hdr.v_len + RECORD_CRC_SIZE > total) break;
            pos += hdr.v_len + RECORD_CRC_SIZE;
        }

        return result;
    }

    // ============================================================================
    // SSTReader::get
    // ============================================================================

    std::optional<core::MemRecord>
    SSTReader::get(std::span<const uint8_t> key) const {
        if (key.empty()) return std::nullopt;

        // Quick range check
        if (!key_in_range(key)) return std::nullopt;

        // Bloom filter check — dispatches on BloomHeader::type internally
        if (!bloom_check(key)) return std::nullopt;

        // Index seek + sequential scan
        const uint64_t data_off = index_seek(key);
        return scan_from(data_off, key);
    }

    // ============================================================================
    // SSTReader::contains
    // ============================================================================

    std::optional<bool> SSTReader::contains(std::span<const uint8_t> key) const {
        if (key.empty()) return std::nullopt;
        if (!key_in_range(key)) return std::nullopt;
        if (!bloom_check(key)) return std::nullopt;
        const uint64_t data_off = index_seek(key);
        if (compressed_) return contains_from_buf(data_off, key);
        return contains_from_file(data_off, key);
    }

    // ── Fast path: key_in_range already verified by the caller (or not needed) ──
    std::optional<bool> SSTReader::contains_fast(std::span<const uint8_t> key) const {
        // Precondition: caller has already checked key_in_range when required.
        // Bloom hash is chosen internally based on this file's BloomHeader::type.
        if (!bloom_check(key)) return std::nullopt;
        const uint64_t data_off = index_seek(key);
        if (compressed_) return contains_from_buf(data_off, key);
        return contains_from_file(data_off, key);
    }

    // ── Precomputed hash overload: caller supplies bloom_fast_hash64(key) ─────
    std::optional<bool> SSTReader::contains_fast(std::span<const uint8_t> key, uint64_t precomputed_bloom_hash) const {
        if (!bloom_check_fp(precomputed_bloom_hash)) return std::nullopt;
        const uint64_t data_off = index_seek(key);
        if (compressed_) return contains_from_buf(data_off, key);
        return contains_from_file(data_off, key);
    }

    // ============================================================================
    // SSTReader::contains_from_file  (uncompressed — reads key+flags only)
    // ============================================================================

    std::optional<bool> SSTReader::contains_from_file(uint64_t data_section_offset, std::span<const uint8_t> target_key) const {
        FILE* f = sst_open_rb(path_);
        if (!f) return std::nullopt;

        const int64_t abs_pos = static_cast<int64_t>(DATA_OFFSET + data_section_offset);
        if (sst_fseek(f, abs_pos, SEEK_SET) != 0) {
            fclose(f);
            return std::nullopt;
        }

        const uint64_t data_end = DATA_OFFSET + header_.data_size;
        std::optional<bool> result;

        while (true) {
            const int64_t cur = sst_ftell(f);
            if (cur < 0 || static_cast<uint64_t>(cur) >= data_end) break;

            core::AKHdr32 hdr{};
            if (!fread_exact(f, &hdr, sizeof(hdr))) break;

            std::vector<uint8_t> key(hdr.k_len);
            if (!fread_exact(f, key.data(), hdr.k_len)) break;

            const int c = cmp_keys(key.data(), key.size(), target_key.data(), target_key.size());
            if (c == 0) {
                // Found: return live/tombstone status without reading value
                result = !(hdr.flags & core::AKHdr32::FLAG_TOMBSTONE);
                break;
            }
            else if (c > 0) {
                break; // scanned past target key
            }
            // key < target: skip value bytes + CRC and continue
            if (sst_fseek(f, static_cast<int64_t>(hdr.v_len) + static_cast<int64_t>(RECORD_CRC_SIZE), SEEK_CUR) != 0) break;
        }

        fclose(f);
        return result;
    }

    // ============================================================================
    // SSTReader::contains_from_buf  (compressed — reads key+flags only)
    // ============================================================================

    std::optional<bool> SSTReader::contains_from_buf(uint64_t data_section_offset, std::span<const uint8_t> target_key) const {
        ensure_decompressed();
        if (decomp_data_.empty()) return std::nullopt;
        if (data_section_offset >= decomp_data_.size()) return std::nullopt;

        size_t pos = static_cast<size_t>(data_section_offset);
        const size_t total = decomp_data_.size();
        const uint8_t* buf = decomp_data_.data();

        auto buf_read = [&](void* dst, size_t n) -> bool {
            if (pos + n > total) return false;
            std::memcpy(dst, buf + pos, n);
            pos += n;
            return true;
        };

        std::optional<bool> result;

        while (pos < total) {
            core::AKHdr32 hdr{};
            if (!buf_read(&hdr, sizeof(hdr))) break;

            if (pos + hdr.k_len > total) break;
            const uint8_t* key_ptr = buf + pos;
            pos += hdr.k_len;

            const int c = cmp_keys(key_ptr, hdr.k_len, target_key.data(), target_key.size());
            if (c == 0) {
                result = !(hdr.flags & core::AKHdr32::FLAG_TOMBSTONE);
                break;
            }
            else if (c > 0) { break; }
            // skip value + CRC
            const size_t skip = static_cast<size_t>(hdr.v_len) + RECORD_CRC_SIZE;
            if (pos + skip > total) break;
            pos += skip;
        }

        return result;
    }

    // ============================================================================
    // SSTReader::get_into_buf  (compressed — key+flags scan + value copy into out)
    // ============================================================================
    //
    // Like contains_from_buf, but on a live-record hit also copies the value bytes
    // into `out`.  Returns true=live (out filled), false=tombstone, nullopt=not found.
    // No MemRecord construction, no CRC verification — same register-sized return as
    // contains_from_buf, which avoids hidden-pointer ABI overhead up the call chain.

    std::optional<bool> SSTReader::get_into_buf(uint64_t data_section_offset, std::span<const uint8_t> target_key, std::vector<uint8_t>& out) const {
        ensure_decompressed();
        if (decomp_data_.empty()) return std::nullopt;
        if (data_section_offset >= decomp_data_.size()) return std::nullopt;

        size_t pos = static_cast<size_t>(data_section_offset);
        const size_t total = decomp_data_.size();
        const uint8_t* buf = decomp_data_.data();

        std::optional<bool> result;

        while (pos < total) {
            if (pos + sizeof(core::AKHdr32) > total) break;
            core::AKHdr32 hdr{};
            std::memcpy(&hdr, buf + pos, sizeof(hdr));
            pos += sizeof(hdr);

            if (pos + hdr.k_len > total) break;
            const uint8_t* key_ptr = buf + pos;
            pos += hdr.k_len;

            const int c = cmp_keys(key_ptr, hdr.k_len, target_key.data(), target_key.size());
            if (c == 0) {
                if (hdr.flags & core::AKHdr32::FLAG_TOMBSTONE) { result = false; }
                else {
                    // Live record — copy value bytes directly into out.
                    if (pos + hdr.v_len > total) break;
                    out.assign(buf + pos, buf + pos + hdr.v_len);
                    result = true;
                }
                break;
            }
            else if (c > 0) { break; }
            // skip value + CRC
            const size_t skip = static_cast<size_t>(hdr.v_len) + RECORD_CRC_SIZE;
            if (pos + skip > total) break;
            pos += skip;
        }

        return result;
    }

    // ============================================================================
    // SSTReader::get_into_file  (uncompressed — key+flags scan + value copy into out)
    // ============================================================================

    std::optional<bool> SSTReader::get_into_file(uint64_t data_section_offset, std::span<const uint8_t> target_key, std::vector<uint8_t>& out) const {
        FILE* f = sst_open_rb(path_);
        if (!f) return std::nullopt;

        const int64_t abs_pos = static_cast<int64_t>(DATA_OFFSET + data_section_offset);
        if (sst_fseek(f, abs_pos, SEEK_SET) != 0) {
            fclose(f);
            return std::nullopt;
        }

        const uint64_t data_end = DATA_OFFSET + header_.data_size;
        std::optional<bool> result;

        while (true) {
            const int64_t cur = sst_ftell(f);
            if (cur < 0 || static_cast<uint64_t>(cur) + sizeof(core::AKHdr32) > data_end) break;

            core::AKHdr32 hdr{};
            if (!fread_exact(f, &hdr, sizeof(hdr))) break;

            std::vector<uint8_t> key(hdr.k_len);
            if (!fread_exact(f, key.data(), hdr.k_len)) break;

            const int c = cmp_keys(key.data(), hdr.k_len, target_key.data(), target_key.size());
            if (c == 0) {
                if (hdr.flags & core::AKHdr32::FLAG_TOMBSTONE) { result = false; }
                else {
                    out.resize(hdr.v_len);
                    if (hdr.v_len > 0 && !fread_exact(f, out.data(), hdr.v_len)) {
                        out.clear();
                        break;
                    }
                    result = true;
                }
                break;
            }
            else if (c > 0) { break; }
            // skip value + CRC
            if (sst_fseek(f, static_cast<int64_t>(hdr.v_len) + static_cast<int64_t>(RECORD_CRC_SIZE), SEEK_CUR) != 0) break;
        }

        fclose(f);
        return result;
    }

    // ============================================================================
    // SSTReader::get_into_fast  (bloom → index → get_into_buf/file)
    // ============================================================================

    std::optional<bool> SSTReader::get_into_fast(std::span<const uint8_t> key, uint64_t precomputed_bloom_hash, std::vector<uint8_t>& out) const {
        if (!bloom_check_fp(precomputed_bloom_hash)) return std::nullopt;
        const uint64_t data_off = index_seek(key);
        if (compressed_) return get_into_buf(data_off, key, out);
        return get_into_file(data_off, key, out);
    }

    // ============================================================================
    // SSTReader::key_in_range
    // ============================================================================

    bool SSTReader::key_in_range(std::span<const uint8_t> key) const noexcept {
        if (first_key_.empty() && last_key_.empty()) return true;

        if (!first_key_.empty()) {
            if (cmp_keys(key, std::span<const uint8_t>(first_key_.data(), first_key_.size())) < 0)
                return false;
        }
        if (!last_key_.empty()) {
            if (cmp_keys(key, std::span<const uint8_t>(last_key_.data(), last_key_.size())) > 0)
                return false;
        }
        return true;
    }

    // ============================================================================
    // SSTReader::scan
    // ============================================================================

    SSTReader::Iterator SSTReader::scan(
        std::span<const uint8_t> start_key,
        std::span<const uint8_t> end_key) const
    {
        auto impl = std::make_unique<Iterator::Impl>();
        if (!end_key.empty()) {
            impl->end_key.assign(end_key.begin(), end_key.end());
        }

        uint64_t start_data_offset = 0;
        if (!start_key.empty()) { start_data_offset = index_seek(start_key); }

        if (compressed_) {
            // ── Buffer mode ───────────────────────────────────────────────────
            ensure_decompressed();
            if (decomp_data_.empty()) {
                return Iterator(nullptr); // decompression failed
            }
            impl->buf_data = decomp_data_.data();
            impl->buf_size = decomp_data_.size();
            impl->buf_pos = static_cast<size_t>(start_data_offset);

            // Fast-forward past records with key < start_key
            if (!start_key.empty()) {
                const size_t total = impl->buf_size;
                const uint8_t* buf = impl->buf_data;
                size_t& pos = impl->buf_pos;

                while (pos < total) {
                    const size_t record_start = pos;
                    core::AKHdr32 hdr{};
                    if (pos + sizeof(hdr) > total) break;
                    std::memcpy(&hdr, buf + pos, sizeof(hdr));
                    pos += sizeof(hdr);

                    if (pos + hdr.k_len > total) {
                        pos = record_start;
                        break;
                    }
                    const uint8_t* key_ptr = buf + pos;
                    pos += hdr.k_len;

                    const int c = cmp_keys(key_ptr, hdr.k_len, start_key.data(), start_key.size());
                    if (c >= 0) {
                        pos = record_start; // rewind to record start
                        break;
                    }
                    // key < start_key: skip value + CRC
                    if (pos + hdr.v_len + RECORD_CRC_SIZE > total) {
                        pos = total;
                        break;
                    }
                    pos += hdr.v_len + RECORD_CRC_SIZE;
                }
            }
        }
        else {
            // ── File mode ─────────────────────────────────────────────────────
            impl->data_end = DATA_OFFSET + header_.data_size;
            impl->file = sst_open_rb(path_);
            if (!impl->file) { return Iterator(nullptr); }

            const int64_t abs_start = static_cast<int64_t>(DATA_OFFSET + start_data_offset);
            if (sst_fseek(impl->file, abs_start, SEEK_SET) != 0) {
                fclose(impl->file);
                impl->file = nullptr;
                return Iterator(nullptr);
            }

            // Fast-forward past records with key < start_key
            if (!start_key.empty()) {
                while (true) {
                    const int64_t cur = sst_ftell(impl->file);
                    if (cur < 0 || static_cast<uint64_t>(cur) >= impl->data_end) break;

                    core::AKHdr32 hdr{};
                    if (!fread_exact(impl->file, &hdr, sizeof(hdr))) break;

                    std::vector<uint8_t> key(hdr.k_len);
                    if (!fread_exact(impl->file, key.data(), hdr.k_len)) break;

                    const int c = cmp_keys(key.data(), key.size(), start_key.data(), start_key.size());
                    if (c >= 0) {
                        // key >= start_key: seek back to record start and break
                        sst_fseek(impl->file, -(static_cast<int64_t>(sizeof(hdr)) + static_cast<int64_t>(hdr.k_len)), SEEK_CUR);
                        break;
                    }
                    // key < start_key: skip value + CRC trailer and continue
                    sst_fseek(impl->file, static_cast<int64_t>(hdr.v_len) + static_cast<int64_t>(RECORD_CRC_SIZE), SEEK_CUR);
                }
            }
        }

        return Iterator(std::move(impl));
    }

} // namespace akkaradb::engine::sst
