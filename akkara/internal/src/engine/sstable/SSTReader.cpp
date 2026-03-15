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
        FILE*                  file     = nullptr;
        uint64_t               data_end = 0;   ///< absolute file offset where data section ends
        std::vector<uint8_t>   end_key;         ///< exclusive upper bound (empty = unbounded)
        std::optional<core::MemRecord> pending; ///< next record to return

        ~Impl() { if (file) { fclose(file); file = nullptr; } }

        /// Reads the next record from file and stores it in pending.
        /// Sets pending = nullopt if end of data / end_key exceeded.
        void advance() {
            pending.reset();
            if (!file) return;

            // Check file position against data_end
            const int64_t pos_i64 = [this]() -> int64_t {
#ifdef _WIN32
                return _ftelli64(file);
#else
                return ftello(file);
#endif
            }();
            if (pos_i64 < 0 || static_cast<uint64_t>(pos_i64) >= data_end) return;

            // Read header
            core::AKHdr32 hdr{};
            if (!fread_exact(file, &hdr, sizeof(hdr))) return;

            // Read key
            std::vector<uint8_t> key(hdr.k_len);
            if (!fread_exact(file, key.data(), hdr.k_len)) return;

            // Check end_key (exclusive upper bound)
            if (!end_key.empty()) {
                if (cmp_keys(key.data(), key.size(),
                             end_key.data(), end_key.size()) >= 0) {
                    return; // past upper bound
                }
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

    std::unique_ptr<SSTReader> SSTReader::open(const std::filesystem::path& path) {
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

        // Read sparse index section
        std::vector<uint8_t> raw_index;
        if (hdr.index_count > 0 && hdr.index_offset > 0) {
            // Seek to index section
            if (sst_fseek(f, static_cast<int64_t>(hdr.index_offset), SEEK_SET) != 0) {
                fclose(f); return nullptr;
            }
            // Read until bloom_offset (if present) or end of file
            const uint64_t index_end = (hdr.bloom_offset > 0)
                ? hdr.bloom_offset
                : hdr.index_offset + 1024 * 1024; // safety cap; normally bounded by bloom
            // Calculate index size from positions
            uint64_t index_size;
            if (hdr.bloom_offset > 0 && hdr.bloom_offset > hdr.index_offset) {
                index_size = hdr.bloom_offset - hdr.index_offset;
            } else {
                // No bloom filter: index goes to end of file minus any bloom section.
                // Compute from entry count and approximate size.
                // Seek to file end to find size.
                if (sst_fseek(f, 0, SEEK_END) != 0) { fclose(f); return nullptr; }
#ifdef _WIN32
                const int64_t file_end = _ftelli64(f);
#else
                const int64_t file_end = ftello(f);
#endif
                if (file_end < 0) { fclose(f); return nullptr; }
                index_size = static_cast<uint64_t>(file_end) - hdr.index_offset;
                // Seek back to index
                if (sst_fseek(f, static_cast<int64_t>(hdr.index_offset), SEEK_SET) != 0) {
                    fclose(f); return nullptr;
                }
            }
            (void)index_end;

            // Re-seek to index start and read
            if (sst_fseek(f, static_cast<int64_t>(hdr.index_offset), SEEK_SET) != 0) {
                fclose(f); return nullptr;
            }
            raw_index.resize(index_size);
            if (!fread_exact(f, raw_index.data(), index_size)) {
                fclose(f); return nullptr;
            }
        }

        // Read Bloom filter section
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

        // Build SSTReader
        auto reader = std::unique_ptr<SSTReader>(new SSTReader());
        reader->path_       = path;
        reader->header_     = hdr;
        reader->bloom_data_ = std::move(bloom_data);

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

        return reader;
    }

    SSTReader::~SSTReader() = default;

    SSTReader::SSTReader(SSTReader&&) noexcept = default;
    SSTReader& SSTReader::operator=(SSTReader&&) noexcept = default;

    // ============================================================================
    // SSTReader::bloom_check
    // ============================================================================

    bool SSTReader::bloom_check(uint64_t key_fp64) const noexcept {
        if (bloom_data_.size() < sizeof(BloomHeader)) return true; // no bloom → assume present

        const BloomHeader* bh = reinterpret_cast<const BloomHeader*>(bloom_data_.data());
        if (bh->num_bits == 0 || bh->num_hashes == 0) return true;

        const uint8_t* bits = bloom_data_.data() + sizeof(BloomHeader);
        const size_t   bits_bytes = bloom_data_.size() - sizeof(BloomHeader);
        if (bits_bytes < (bh->num_bits + 7u) / 8u) return true; // truncated → be conservative

        const uint32_t h1 = static_cast<uint32_t>(key_fp64);
        const uint32_t h2 = (static_cast<uint32_t>(key_fp64 >> 32)) | 1u;
        const uint32_t nb = bh->num_bits;

        for (uint8_t i = 0; i < bh->num_hashes; ++i) {
            const uint32_t pos = (h1 + static_cast<uint32_t>(i) * h2) % nb;
            if (!(bits[pos >> 3u] & (1u << (pos & 7u)))) return false; // definite miss
        }
        return true; // might be present
    }

    // ============================================================================
    // SSTReader::index_seek
    // ============================================================================

    uint64_t SSTReader::index_seek(std::span<const uint8_t> key) const noexcept {
        if (parsed_index_.empty()) return 0;

        // Binary search: find rightmost entry with entry.key <= key
        size_t lo = 0, hi = parsed_index_.size();
        while (lo < hi) {
            const size_t mid = lo + (hi - lo) / 2;
            const IndexEntry& e = parsed_index_[mid];
            const int c = cmp_keys(e.key.data(), e.key.size(), key.data(), key.size());
            if (c <= 0) lo = mid + 1;  // entry.key <= key → search right
            else        hi = mid;      // entry.key  > key → search left
        }
        // lo-1 is the rightmost entry with key <= target
        if (lo == 0) return 0; // target is before first index entry → start from beginning
        return parsed_index_[lo - 1].data_offset;
    }

    // ============================================================================
    // SSTReader::scan_from
    // ============================================================================

    std::optional<core::MemRecord>
    SSTReader::scan_from(uint64_t data_section_offset,
                         std::span<const uint8_t> target_key) const {
        FILE* f = sst_open_rb(path_);
        if (!f) return std::nullopt;

        // Seek to absolute position: DATA_OFFSET + data_section_offset
        const int64_t abs_pos = static_cast<int64_t>(DATA_OFFSET + data_section_offset);
        if (sst_fseek(f, abs_pos, SEEK_SET) != 0) { fclose(f); return std::nullopt; }

        const uint64_t data_end = DATA_OFFSET + header_.data_size;
        std::optional<core::MemRecord> result;

        while (true) {
#ifdef _WIN32
            const int64_t cur = _ftelli64(f);
#else
            const int64_t cur = ftello(f);
#endif
            if (cur < 0 || static_cast<uint64_t>(cur) >= data_end) break;

            // Read header
            core::AKHdr32 hdr{};
            if (!fread_exact(f, &hdr, sizeof(hdr))) break;

            // Read key
            std::vector<uint8_t> key(hdr.k_len);
            if (!fread_exact(f, key.data(), hdr.k_len)) break;

            const int c = cmp_keys(key.data(), key.size(),
                                   target_key.data(), target_key.size());

            if (c == 0) {
                // Key match — read value, verify CRC, and return
                std::vector<uint8_t> val(hdr.v_len);
                if (!fread_exact(f, val.data(), hdr.v_len)) break;

                // Verify per-record CRC32C trailer
                uint32_t stored_crc = 0;
                if (!fread_exact(f, &stored_crc, sizeof(stored_crc))) break;
                {
                    uint32_t expected = core::CRC32C::compute(&hdr, sizeof(hdr));
                    expected = core::CRC32C::append(key.data(), key.size(), expected);
                    expected = core::CRC32C::append(val.data(), val.size(), expected);
                    if (stored_crc != expected) break; // CRC mismatch: treat as not found
                }

                result = core::MemRecord::create(
                    std::span<const uint8_t>(key.data(), key.size()),
                    std::span<const uint8_t>(val.data(), val.size()),
                    hdr.seq, hdr.flags, hdr.key_fp64
                );
                break;
            } else if (c > 0) {
                // Scanned past target key — not found in this scan window
                break;
            }

// key < target: skip value bytes + CRC trailer and continue
if (sst_fseek(f, static_cast<int64_t>(hdr.v_len) + static_cast<int64_t>(RECORD_CRC_SIZE), SEEK_CUR) != 0) break;
        }

        fclose(f);
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

        // Bloom filter check
        const uint64_t fp64 = core::AKHdr32::compute_key_fp64(key.data(), key.size());
        if (!bloom_check(fp64)) return std::nullopt;

        // Index seek + sequential scan
        const uint64_t data_off = index_seek(key);
        return scan_from(data_off, key);
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
        impl->data_end = DATA_OFFSET + header_.data_size;
        if (!end_key.empty()) {
            impl->end_key.assign(end_key.begin(), end_key.end());
        }

        // Open file and seek to appropriate start position
        impl->file = sst_open_rb(path_);
        if (!impl->file) {
            return Iterator(nullptr);
        }

        uint64_t start_data_offset = 0;
        if (!start_key.empty()) {
            start_data_offset = index_seek(start_key);
        }

        const int64_t abs_start = static_cast<int64_t>(DATA_OFFSET + start_data_offset);
        if (sst_fseek(impl->file, abs_start, SEEK_SET) != 0) {
            fclose(impl->file);
            impl->file = nullptr;
            return Iterator(nullptr);
        }

        // If start_key is non-empty, fast-forward past records with key < start_key
        if (!start_key.empty()) {
            while (true) {
#ifdef _WIN32
                const int64_t cur = _ftelli64(impl->file);
#else
                const int64_t cur = ftello(impl->file);
#endif
                if (cur < 0 || static_cast<uint64_t>(cur) >= impl->data_end) break;

                // Peek at the next record's key
                core::AKHdr32 hdr{};
                if (!fread_exact(impl->file, &hdr, sizeof(hdr))) break;

                std::vector<uint8_t> key(hdr.k_len);
                if (!fread_exact(impl->file, key.data(), hdr.k_len)) break;

                const int c = cmp_keys(key.data(), key.size(),
                                       start_key.data(), start_key.size());
                if (c >= 0) {
                    // key >= start_key: seek back to record start and break
                    sst_fseek(impl->file,
                              -(static_cast<int64_t>(sizeof(hdr)) + static_cast<int64_t>(hdr.k_len)),
                              SEEK_CUR);
                    break;
                }
// key < start_key: skip value + CRC trailer and continue
sst_fseek(impl->file, static_cast<int64_t>(hdr.v_len) + static_cast<int64_t>(RECORD_CRC_SIZE), SEEK_CUR);
            }
        }

        return Iterator(std::move(impl));
    }

} // namespace akkaradb::engine::sst
