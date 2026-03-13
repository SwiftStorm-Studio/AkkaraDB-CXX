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

// internal/include/engine/sst/SSTWriter.hpp
#pragma once

#include "engine/sstable/SSTFormat.hpp"
#include "core/record/MemRecord.hpp"
#include <cstdint>
#include <filesystem>
#include <vector>

namespace akkaradb::engine::sst {

    /**
     * SSTWriter — Writes a sorted sequence of MemRecords to a single .aksst file.
     *
     * All records must be sorted ascending by key before calling write().
     * Typically called from the MemTable flush callback (per-shard sorted batch).
     *
     * File layout:
     *   [SSTFileHeader: 64B]
     *   [Data section: AKHdr32 + key + value per record, tightly packed]
     *   [Sparse index: variable-length entries, one per INDEX_STRIDE records
     *                  plus always one for the first and one for the last record]
     *   [Bloom filter: BloomHeader + bit array]
     *
     * Thread-safety: SSTWriter::write() is stateless and thread-safe.
     */
    class SSTWriter {
        public:
            // ── Options ───────────────────────────────────────────────────────

            struct Options {
                /// SST level (0 = L0, 1 = L1, ...).
                int level = 0;

                /// Bloom filter bits per key. 0 = no Bloom filter.
                size_t bloom_bits_per_key = BLOOM_BITS_PER_KEY;

                /// Sparse index density: one entry every index_stride records
                /// (plus always the first and last record).
                size_t index_stride = INDEX_STRIDE;
            };

            // ── Result ────────────────────────────────────────────────────────

            struct Result {
                std::filesystem::path path;         ///< Path of the written file
                uint64_t entry_count = 0;           ///< Total records written
                uint64_t min_seq = UINT64_MAX;      ///< Minimum seq in file
                uint64_t max_seq = 0;               ///< Maximum seq in file
                std::vector<uint8_t> first_key;     ///< Key of first record
                std::vector<uint8_t> last_key;      ///< Key of last record
            };

            // ── Static write API ──────────────────────────────────────────────

            /**
             * Writes sorted_records to sst_path in .aksst format.
             *
             * @param sst_path       Destination file path (must not exist, or will be overwritten).
             * @param sorted_records Records sorted ascending by key. Must be non-empty.
             * @param opts           Tuning options.
             * @return               Result struct with metadata about the written file.
             * @throws std::runtime_error on I/O failure.
             * @throws std::invalid_argument if sorted_records is empty.
             */
            [[nodiscard]] static Result write(
                const std::filesystem::path&         sst_path,
                const std::vector<core::MemRecord>&  sorted_records,
                const Options&                       opts = {}
            );

        private:
            SSTWriter() = delete;
    };

} // namespace akkaradb::engine::sst
