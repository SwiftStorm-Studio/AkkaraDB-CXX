/*
* AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/include/engine/sstable/SSTableWriter.hpp
#pragma once

#include "core/record/MemRecord.hpp"
#include "core/buffer/BufferPool.hpp"
#include "IndexBlock.hpp"
#include "BloomFilter.hpp"
#include "format-akk/AkkBlockPacker.hpp"
#include <filesystem>
#include <memory>
#include <vector>
#include <fstream>

namespace akkaradb::engine::sstable {
    /**
     * SSTableWriter - Writes sorted MemRecords to SSTable file.
     *
     * Layout (Little-Endian):
     *   [ Data Blocks (32 KiB each) ]
     *   [ IndexBlock ('AKIX') ]
     *   [ BloomFilter ('AKBL') ]
     *   [ Footer ('AKSS', 32 bytes) ]
     *
     * Workflow:
     *   1. Create writer with expected entry count
     *   2. Call write() for each MemRecord (MUST be in sorted order)
     *   3. Call seal() to finalize
     *   4. Close writer
     *
     * Example:
     * ```cpp
     * auto writer = SSTableWriter::create(
     *     "./data/L0/sst_001.sst",
     *     buffer_pool,
     *     10000  // expected entries
     * );
     *
     * for (const auto& record : sorted_records) {
     *     writer->write(record);
     * }
     *
     * writer->seal();
     * writer->close();
     * ```
     *
     * Thread-safety: NOT thread-safe. Single writer only.
     */
    class SSTableWriter {
    public:
        /**
         * Creates SSTableWriter.
         *
         * @param file_path Output file path
         * @param buffer_pool Buffer pool for blocks
         * @param expected_entries Expected number of entries (for Bloom sizing)
         * @param bloom_fp_rate Bloom filter false positive rate (default: 0.01)
         * @return Unique pointer to writer
         * @throws std::runtime_error if file cannot be created
         */
        [[nodiscard]] static std::unique_ptr<SSTableWriter> create(
            std::filesystem::path& file_path,
            std::shared_ptr<core::BufferPool> buffer_pool,
            uint64_t expected_entries,
            double bloom_fp_rate = 0.01
        );

        ~SSTableWriter();

        /**
         * Writes a record.
         *
         * Records MUST be written in ascending key order.
         *
         * @param record Record to write
         * @throws std::runtime_error if record too large for block
         */
        void write(const core::MemRecord& record);

        /**
         * Writes multiple records.
         *
         * @param records Records to write (must be sorted)
         */
        void write_all(const std::vector<core::MemRecord>& records);

        /**
         * Finalizes SSTable.
         *
         * Writes Index, Bloom, Footer, and computes file CRC.
         *
         * @return Seal result (offsets and entry count)
         */
        struct SealResult {
            uint64_t index_off;
            uint64_t bloom_off;
            uint64_t entries;
        };

        [[nodiscard]] SealResult seal();

        /**
         * Closes file.
         */
        void close();

        /**
         * Returns total entries written.
         */
        [[nodiscard]] uint64_t entries_written() const noexcept { return total_entries_; }

    private:
        SSTableWriter(
            std::filesystem::path& file_path,
            std::shared_ptr<core::BufferPool> buffer_pool,
            uint64_t expected_entries,
            double bloom_fp_rate
        );

        void on_block_ready(core::OwnedBuffer block);

        class Impl;
        std::unique_ptr<Impl> impl_;

        std::filesystem::path file_path_;
        std::shared_ptr<core::BufferPool> buffer_pool_;
        std::ofstream file_;
        uint64_t total_entries_;

        std::unique_ptr<format::akk::AkkBlockPacker> packer_;
        IndexBlock::Builder index_builder_;
        BloomFilter::Builder bloom_builder_;

        std::vector<uint8_t> pending_first_key_;
    };
} // namespace akkaradb::engine::sstable
