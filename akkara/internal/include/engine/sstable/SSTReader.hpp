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

// internal/include/engine/sst/SSTReader.hpp
#pragma once

#include "engine/sstable/SSTFormat.hpp"
#include "core/record/MemRecord.hpp"
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace akkaradb::engine::sst {

    /**
     * SSTReader — Reads records from a single .aksst file.
     *
     * On open():
     *   - Reads and validates SSTFileHeader (magic, version, CRC32C).
     *   - Loads the entire sparse index into memory (index_data_).
     *   - Loads the Bloom filter into memory (bloom_data_).
     *   - Parses the sparse index into a vector of (data_offset, key) entries
     *     for fast binary search.
     *   - Reads first_key and last_key from the index.
     *
     * get(key):
     *   1. Bloom filter check   — returns nullopt immediately on negative result.
     *   2. index_seek(key)      — binary search → nearest data_offset.
     *   3. Scan ≤ INDEX_STRIDE records from that offset (sequential I/O).
     *
     * scan(start, end):
     *   Returns a forward Iterator over [start_key, end_key).
     *   Snapshot semantics (file content at open time).
     *
     * Thread-safety: SSTReader is NOT thread-safe. For concurrent access,
     *   use one SSTReader per thread or guard externally.
     *   SSTManager uses shared_mutex to protect its reader list.
     */
    class SSTReader {
        public:
            // ── Iterator ──────────────────────────────────────────────────────

            /**
             * Forward iterator over a key range in an SST file.
             * Sequentially reads records from the file; each call to next()
             * returns the next record in ascending key order.
             *
             * The iterator pre-reads one record ahead (peek-based pattern):
             *   has_next() checks whether a record has been pre-read.
             *   next() returns the pre-read record and advances to the next.
             */
            class Iterator {
                public:
                    ~Iterator();
                    Iterator(Iterator&&) noexcept;
                    Iterator& operator=(Iterator&&) noexcept;
                    Iterator(const Iterator&)            = delete;
                    Iterator& operator=(const Iterator&) = delete;

                    [[nodiscard]] bool has_next() const noexcept { return has_pending_; }

                    /**
                     * Returns the current pre-read record and advances internally.
                     * Precondition: has_next() == true.
                     */
                    [[nodiscard]] core::MemRecord next();

                private:
                    friend class SSTReader;
                    struct Impl;
                    explicit Iterator(std::unique_ptr<Impl> impl);
                    std::unique_ptr<Impl> impl_;
                    bool has_pending_ = false;
            };

            // ── Factory ───────────────────────────────────────────────────────

            /**
             * Opens an SST file for reading.
             *
             * @param path         Path to the .aksst file.
             * @param preload_data When true, the uncompressed data section is loaded into
             *                     memory at open time (decomp_data_).  All subsequent
             *                     get/contains/scan calls use the in-memory buffer, eliminating
             *                     per-FP fopen/fseek/fclose overhead.  No-op for Zstd files.
             * @return      Unique_ptr<SSTReader>, or nullptr on failure
             *              (bad magic, version mismatch, CRC error, I/O failure).
             */
            [[nodiscard]] static std::unique_ptr<SSTReader> open(const std::filesystem::path& path, bool preload_data = false);

            ~SSTReader();
            SSTReader(const SSTReader&)            = delete;
            SSTReader& operator=(const SSTReader&) = delete;
            SSTReader(SSTReader&&) noexcept;
            SSTReader& operator=(SSTReader&&) noexcept;

            // ── Point lookup ──────────────────────────────────────────────────

            /**
             * Point lookup.
             *
             * Algorithm:
             *   1. Bloom filter (if present): returns nullopt on definite miss.
             *   2. Binary search sparse index → data_offset.
             *   3. Sequential scan ≤ INDEX_STRIDE records.
             *
             * @return MemRecord (possibly a tombstone), or nullopt if not found.
             */
            [[nodiscard]] std::optional<core::MemRecord>
                get(std::span<const uint8_t> key) const;

            /**
             * Existence check — no value copy, no blob I/O.
             *
             * Same bloom → index → data-block path as get(), but reads only the
             * key and flags from each candidate record.  Value bytes and blob
             * content are never loaded, making this significantly faster for
             * large or blob-stored values.
             *
             * @return
             *   nullopt → not found in this file.
             *   false   → tombstone found.
             *   true    → live record found.
             */
            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key) const;

            /**
             * Fast existence check for callers that have already verified
             * key_in_range(key) == true (or don't need it, e.g. L0 with bloom).
             *
             * Skips the internal key_in_range() call.  The bloom hash is computed
             * internally using the hash function matching this file's bloom type
             * (BLOOM_TYPE_BLOCKED_FAST → bloom_fast_hash64; others → SipHash).
             *
             * Called by SSTManager for all levels.
             */
            [[nodiscard]] std::optional<bool> contains_fast(std::span<const uint8_t> key) const;

            /**
             * Fast existence check with a precomputed bloom hash.
             *
             * Identical to contains_fast(key) but accepts a hash that was already
             * computed by the caller (e.g. in SSTManager before iterating L0 files).
             * Avoids recomputing bloom_fast_hash64 for every file in the level loop.
             *
             * @param precomputed_bloom_hash  Result of bloom_fast_hash64(key) — must match
             *                                the hash function used when the file was written
             *                                (BLOOM_TYPE_BLOCKED_FAST).
             */
            [[nodiscard]] std::optional<bool> contains_fast(std::span<const uint8_t> key, uint64_t precomputed_bloom_hash) const;

            // ── Range scan ────────────────────────────────────────────────────

            /**
             * Returns a forward iterator over [start_key, end_key).
             *
             * @param start_key  Inclusive lower bound (empty = beginning of file).
             * @param end_key    Exclusive upper bound (empty = end of file).
             */
            [[nodiscard]] Iterator scan(
                std::span<const uint8_t> start_key = {},
                std::span<const uint8_t> end_key   = {}
            ) const;

            // ── Metadata ──────────────────────────────────────────────────────

            [[nodiscard]] const SSTFileHeader&     header()    const noexcept { return header_; }
            [[nodiscard]] std::span<const uint8_t> first_key() const noexcept { return first_key_; }
            [[nodiscard]] std::span<const uint8_t> last_key()  const noexcept { return last_key_;  }
            [[nodiscard]] const std::filesystem::path& path()  const noexcept { return path_;      }

            /**
             * Returns true if key is within [first_key, last_key] of this file.
             * Used by SSTManager to skip files that cannot contain a key.
             */
            [[nodiscard]] bool key_in_range(std::span<const uint8_t> key) const noexcept;

        private:
            SSTReader() = default;

            // ── Parsed sparse index entry ─────────────────────────────────────
            struct IndexEntry {
                uint64_t             data_offset; ///< byte offset from start of data section
                uint64_t             mini_key;
                uint64_t             key_fp64;
                std::vector<uint8_t> key;
            };

            std::filesystem::path       path_;
            SSTFileHeader               header_{};
            std::vector<uint8_t>        bloom_data_;     ///< raw bloom filter bytes (header + bits)
            std::vector<IndexEntry>     parsed_index_;   ///< parsed sparse index
            std::vector<uint8_t>        first_key_;
            std::vector<uint8_t>        last_key_;

            // ── Compression ───────────────────────────────────────────────────

            /// true when the data section was Zstd-compressed on disk.
            bool compressed_ = false;

            /// Compressed bytes read from disk during open().
            /// Cleared after the first call to ensure_decompressed().
            mutable std::vector<uint8_t> compressed_bytes_;

            /// Decompressed data section.  Populated lazily on first get()/scan()
            /// call via ensure_decompressed().  Once populated it is never resized,
            /// so raw pointers handed to Iterator::Impl remain stable for the
            /// lifetime of this SSTReader.
            mutable std::vector<uint8_t> decomp_data_;

            // ── Internals ─────────────────────────────────────────────────────

            /// Returns true if key might be present according to the Bloom filter.
            /// Dispatches on BloomHeader::type to select the appropriate hash function.
            [[nodiscard]] bool bloom_check(std::span<const uint8_t> key) const noexcept;

            /// Bloom filter check with a precomputed hash value.
            /// Uses the provided fp64 directly (skips bloom_fast_hash64 / SipHash call).
            /// Callers must ensure fp64 was produced by the same hash function that was
            /// used when the bloom filter was written (BLOOM_TYPE_BLOCKED_FAST → bloom_fast_hash64).
            [[nodiscard]] bool bloom_check_fp(uint64_t fp64) const noexcept;

            /// Binary search the sparse index for the largest entry whose key <= target.
            /// Returns the data_offset to seek to (from start of data section).
            [[nodiscard]] uint64_t index_seek(std::span<const uint8_t> key) const noexcept;

            /// Ensures decomp_data_ is populated (decompresses compressed_bytes_ on
            /// first call, then clears compressed_bytes_ to free memory).
            /// No-op for uncompressed files.  Called from const accessors.
            void ensure_decompressed() const;

            /// Dispatches to scan_from_file() or scan_from_buf() based on compressed_.
            [[nodiscard]] std::optional<core::MemRecord>
                scan_from(uint64_t data_section_offset,
                          std::span<const uint8_t> target_key) const;

            /// File-based point scan (uncompressed SST).
            [[nodiscard]] std::optional<core::MemRecord> scan_from_file(uint64_t data_section_offset, std::span<const uint8_t> target_key) const;

            /// Buffer-based point scan (compressed SST — reads from decomp_data_).
            [[nodiscard]] std::optional<core::MemRecord> scan_from_buf(uint64_t data_section_offset, std::span<const uint8_t> target_key) const;

            /// File-based existence check (uncompressed SST): reads key+flags only.
            [[nodiscard]] std::optional<bool> contains_from_file(uint64_t data_section_offset, std::span<const uint8_t> target_key) const;

            /// Buffer-based existence check (compressed SST): reads key+flags only.
            [[nodiscard]] std::optional<bool> contains_from_buf(uint64_t data_section_offset, std::span<const uint8_t> target_key) const;
    };

} // namespace akkaradb::engine::sst
