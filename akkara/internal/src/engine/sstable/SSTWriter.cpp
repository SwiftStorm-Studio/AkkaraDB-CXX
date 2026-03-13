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

// internal/src/engine/sst/SSTWriter.cpp
#include "engine/sstable/SSTWriter.hpp"
#include "engine/sstable/SSTFormat.hpp"
#include "core/CRC32C.hpp"
#include "core/record/AKHdr32.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <memory>
#include <stdexcept>

namespace akkaradb::engine::sst {

    // ============================================================================
    // Platform helpers
    // ============================================================================

    namespace {

        // RAII file handle
        struct FileGuard {
            FILE* f = nullptr;
            explicit FileGuard(FILE* fp) : f(fp) {}
            ~FileGuard() { if (f) { fclose(f); f = nullptr; } }
            FileGuard(const FileGuard&) = delete;
            FileGuard& operator=(const FileGuard&) = delete;
            void release() { f = nullptr; }
        };

        FILE* sst_fopen(const std::filesystem::path& p, bool write) {
#ifdef _WIN32
            return _wfopen(p.wstring().c_str(), write ? L"wb" : L"r+b");
#else
            return fopen(p.string().c_str(), write ? "wb" : "r+b");
#endif
        }

        int64_t sst_ftell(FILE* f) {
#ifdef _WIN32
            return _ftelli64(f);
#else
            return ftello(f);
#endif
        }

        int sst_fseek(FILE* f, int64_t off, int whence) {
#ifdef _WIN32
            return _fseeki64(f, off, whence);
#else
            return fseeko(f, static_cast<off_t>(off), whence);
#endif
        }

        bool fwrite_exact(FILE* f, const void* buf, size_t n) {
            return n == 0 || fwrite(buf, 1, n, f) == n;
        }

        template <typename T>
        bool fwrite_pod(FILE* f, const T& v) {
            return fwrite(&v, sizeof(T), 1, f) == 1;
        }

        // ── Bloom filter ──────────────────────────────────────────────────────

        uint32_t bloom_next_pow2(uint32_t n) {
            if (n == 0) return 8u;
            n--; n |= n>>1; n |= n>>2; n |= n>>4; n |= n>>8; n |= n>>16; n++;
            return std::max(n, 8u);
        }

        uint8_t bloom_k_from_bpk(size_t bits_per_key) {
            const double k = std::round(0.693147 * static_cast<double>(bits_per_key));
            return static_cast<uint8_t>(std::max(1.0, k));
        }

        void bloom_add(std::vector<uint8_t>& bits, uint64_t key_fp64,
                       uint8_t num_k, uint32_t num_bits) {
            const uint32_t h1 = static_cast<uint32_t>(key_fp64);
            const uint32_t h2 = (static_cast<uint32_t>(key_fp64 >> 32)) | 1u;
            for (uint8_t i = 0; i < num_k; ++i) {
                const uint32_t pos = (h1 + static_cast<uint32_t>(i) * h2) % num_bits;
                bits[pos >> 3u] |= static_cast<uint8_t>(1u << (pos & 7u));
            }
        }

        // ── Index entry ───────────────────────────────────────────────────────

        // Append one sparse index entry to index_buf.
        // Format: data_offset[8] mini_key[8] key_fp64[8] key_len[2] key[key_len]
        void index_emit(std::vector<uint8_t>& buf, uint64_t data_off,
                        uint64_t mini_key, uint64_t key_fp64,
                        std::span<const uint8_t> key) {
            const size_t prev = buf.size();
            buf.resize(prev + INDEX_ENTRY_FIXED_SIZE + key.size());
            uint8_t* p = buf.data() + prev;
            std::memcpy(p, &data_off,  8); p += 8;
            std::memcpy(p, &mini_key,  8); p += 8;
            std::memcpy(p, &key_fp64,  8); p += 8;
            const uint16_t kl = static_cast<uint16_t>(key.size());
            std::memcpy(p, &kl, 2); p += 2;
            if (!key.empty()) std::memcpy(p, key.data(), key.size());
        }

    } // anonymous namespace

    // ============================================================================
    // SSTWriter::write
    // ============================================================================

    SSTWriter::Result SSTWriter::write(
        const std::filesystem::path&        sst_path,
        const std::vector<core::MemRecord>& sorted_records,
        const Options&                      opts)
    {
        if (sorted_records.empty()) {
            throw std::invalid_argument("SSTWriter::write: sorted_records must be non-empty");
        }

        // ── Open for writing ─────────────────────────────────────────────────
        FileGuard fg(sst_fopen(sst_path, /*write=*/true));
        if (!fg.f) {
            throw std::runtime_error("SSTWriter: cannot open for writing: " + sst_path.string());
        }

        // ── Write placeholder header (64 zero bytes) ─────────────────────────
        {
            SSTFileHeader placeholder{};
            if (!fwrite_pod(fg.f, placeholder)) {
                throw std::runtime_error("SSTWriter: I/O error writing header placeholder");
            }
        }

        // ── Write data section ───────────────────────────────────────────────
        Result result;
        result.path    = sst_path;
        result.min_seq = UINT64_MAX;
        result.max_seq = 0;

        std::vector<uint8_t> index_buf;
        uint32_t index_count = 0;

        // Bloom filter setup
        uint32_t bloom_num_bits = 0;
        uint8_t  bloom_k        = 0;
        std::vector<uint8_t> bloom_bits;

        if (opts.bloom_bits_per_key > 0) {
            const uint32_t raw = static_cast<uint32_t>(sorted_records.size() * opts.bloom_bits_per_key);
            bloom_num_bits = bloom_next_pow2(raw);
            bloom_k        = bloom_k_from_bpk(opts.bloom_bits_per_key);
            bloom_bits.assign((bloom_num_bits + 7u) / 8u, 0u);
        }

        uint64_t data_bytes = 0; // running byte offset within data section

        for (size_t i = 0; i < sorted_records.size(); ++i) {
            const core::MemRecord& rec     = sorted_records[i];
            const core::AKHdr32&   hdr     = rec.header();
            const auto             key_sp  = rec.key();
            const auto             val_sp  = rec.value();
            const bool             is_first = (i == 0);
            const bool             is_last  = (i + 1 == sorted_records.size());
            const bool             at_stride = (i % opts.index_stride == 0);

            // Update metadata
            result.entry_count++;
            if (hdr.seq < result.min_seq) result.min_seq = hdr.seq;
            if (hdr.seq > result.max_seq) result.max_seq = hdr.seq;
            if (is_first) result.first_key.assign(key_sp.begin(), key_sp.end());
            if (is_last)  result.last_key .assign(key_sp.begin(), key_sp.end());

            // Emit index entry for: first record, every INDEX_STRIDE records (excl. first),
            // and the last record if it does not fall on a stride boundary.
            // This guarantees no duplicates and exactly covers the data section.
            if (is_first || (at_stride && !is_first) || (is_last && !at_stride)) {
                index_emit(index_buf, data_bytes, hdr.mini_key, hdr.key_fp64, key_sp);
                index_count++;
            }

            // Update bloom filter
            if (bloom_num_bits > 0) {
                bloom_add(bloom_bits, hdr.key_fp64, bloom_k, bloom_num_bits);
            }

            // Write: AKHdr32 + key bytes + value bytes
            if (!fwrite_pod(fg.f, hdr)) {
                throw std::runtime_error("SSTWriter: I/O error writing AKHdr32");
            }
            if (!fwrite_exact(fg.f, key_sp.data(), key_sp.size())) {
                throw std::runtime_error("SSTWriter: I/O error writing key bytes");
            }
            if (!fwrite_exact(fg.f, val_sp.data(), val_sp.size())) {
                throw std::runtime_error("SSTWriter: I/O error writing value bytes");
            }

            data_bytes += sizeof(core::AKHdr32) + key_sp.size() + val_sp.size();
        }

        // ── Write sparse index section ────────────────────────────────────────
        const int64_t index_abs = sst_ftell(fg.f);
        if (index_abs < 0) throw std::runtime_error("SSTWriter: ftell failed at index");

        if (!fwrite_exact(fg.f, index_buf.data(), index_buf.size())) {
            throw std::runtime_error("SSTWriter: I/O error writing index section");
        }

        // ── Write Bloom filter section ────────────────────────────────────────
        uint32_t bloom_abs    = 0;
        uint32_t bloom_total  = 0;

        if (bloom_num_bits > 0) {
            const int64_t pos = sst_ftell(fg.f);
            if (pos < 0) throw std::runtime_error("SSTWriter: ftell failed at bloom");
            bloom_abs = static_cast<uint32_t>(pos);

            BloomHeader bh{};
            bh.num_bits   = bloom_num_bits;
            bh.num_hashes = bloom_k;
            if (!fwrite_pod(fg.f, bh)) {
                throw std::runtime_error("SSTWriter: I/O error writing BloomHeader");
            }
            if (!fwrite_exact(fg.f, bloom_bits.data(), bloom_bits.size())) {
                throw std::runtime_error("SSTWriter: I/O error writing bloom bits");
            }
            bloom_total = static_cast<uint32_t>(sizeof(BloomHeader) + bloom_bits.size());
        }

        // ── Seek back and write real header ───────────────────────────────────
        if (sst_fseek(fg.f, 0, SEEK_SET) != 0) {
            throw std::runtime_error("SSTWriter: fseek to header failed");
        }

        SSTFileHeader real_hdr{};
        real_hdr.magic        = SST_MAGIC;
        real_hdr.version      = SST_VERSION;
        real_hdr.level        = static_cast<uint8_t>(opts.level);
        real_hdr.flags        = 0;
        real_hdr.entry_count  = result.entry_count;
        real_hdr.data_size    = data_bytes;
        real_hdr.index_offset = static_cast<uint64_t>(index_abs);
        real_hdr.index_count  = index_count;
        real_hdr.bloom_offset = bloom_abs;
        real_hdr.bloom_size   = bloom_total;
        real_hdr.min_seq      = result.min_seq;
        real_hdr.max_seq      = result.max_seq;
        real_hdr.crc32c       = 0;
        // CRC covers all bytes except the crc32c field itself (last 4 bytes)
        real_hdr.crc32c = core::CRC32C::compute(&real_hdr, sizeof(real_hdr) - sizeof(uint32_t));

        if (!fwrite_pod(fg.f, real_hdr)) {
            throw std::runtime_error("SSTWriter: I/O error writing final header");
        }

        // fclose happens in FileGuard destructor
        return result;
    }

} // namespace akkaradb::engine::sst
