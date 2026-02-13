/*
* AkkEngine
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkEngine.
 *
 * AkkEngine is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkEngine is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkEngine.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/include/engine/sstable/SSTCompactor.hpp
#pragma once

#include "SSTableReader.hpp"
#include "SSTableWriter.hpp"
#include "engine/manifest/Manifest.hpp"
#include "core/record/MemRecord.hpp"
#include "core/buffer/BufferPool.hpp"
#include <filesystem>
#include <vector>
#include <memory>
#include <optional>
#include <functional>

namespace akkaradb::engine::sstable {
    /**
 * SSTCompactor - Leveled compaction for SSTables.
 *
 * Responsibilities:
 *   - Merge overlapping SSTables across levels
 *   - Remove duplicate keys (keep highest seq)
 *   - Garbage collect tombstones at bottom level
 *   - Maintain Manifest consistency
 *
 * Leveled Structure:
 *   L0: Unsorted, overlapping (flush target)
 *   L1+: Sorted, non-overlapping
 *
 * Trigger: When file count at level exceeds threshold
 *
 * Thread-safety: NOT thread-safe. External synchronization required.
 */
    class SSTCompactor {
        public:
            using SeqClockFn = std::function<std::optional<uint64_t>(uint64_t seq)>;

            /**
     * Creates a compactor.
     *
     * @param base_dir SSTable directory (contains L0/, L1/, ...)
     * @param manifest Manifest for event logging
     * @param buffer_pool Buffer pool for I/O
     * @param max_per_level Max files per level before compaction (default: 4)
     * @param ttl_millis Tombstone TTL in milliseconds (default: 24h)
     * @param seq_clock Optional: seq → timestamp mapping for tombstone GC
     * @return Unique pointer to compactor
     */
            [[nodiscard]] static std::unique_ptr<SSTCompactor> create(
                const std::filesystem::path& base_dir,
                std::shared_ptr<manifest::Manifest> manifest,
                std::shared_ptr<core::BufferPool> buffer_pool,
                size_t max_per_level = 4,
                uint64_t ttl_millis = 24 * 60 * 60 * 1000,
                SeqClockFn seq_clock = nullptr
            );

            ~SSTCompactor();

            /**
     * Runs compaction on all levels.
     *
     * Iterates through levels 0..N and compacts any level
     * exceeding max_per_level threshold.
     */
            void compact();
            bool compact_one(); // Compact one level; returns true if work was done.

            /**
     * Compacts a specific level.
     *
     * @param level Level to compact (0-based)
     */
            void compact_level(int level);

            /**
     * Returns existing level directories.
     */
            [[nodiscard]] std::vector<int> existing_levels() const;

            /**
     * Lists SST files in a level.
     */
            [[nodiscard]] std::vector<std::filesystem::path> list_sst_files(int level) const;

        private:
            SSTCompactor(
                std::filesystem::path base_dir,
                std::shared_ptr<manifest::Manifest> manifest,
                std::shared_ptr<core::BufferPool> buffer_pool,
                size_t max_per_level,
                uint64_t ttl_millis,
                SeqClockFn seq_clock
            );

            struct CompactResult {
                uint64_t entries{};
                std::optional<std::string> first_key_hex;
                std::optional<std::string> last_key_hex;
            };

            CompactResult compact_into(const std::vector<std::filesystem::path>& inputs, const std::filesystem::path& output, bool is_bottom_level);

            std::vector<core::MemRecord> merge(std::vector<std::unique_ptr<SSTableReader>>& readers, bool is_bottom_level);

            bool should_drop_tombstone(const core::MemRecord& record, uint64_t now_millis, bool is_bottom_level) const;

            static std::string first_key_hex(std::span<const uint8_t> key);
            static std::string new_file_name(int level);
            std::filesystem::path level_path(int level) const;

            std::filesystem::path base_dir_;
            std::shared_ptr<manifest::Manifest> manifest_;
            std::shared_ptr<core::BufferPool> buffer_pool_;
            size_t max_per_level_;
            uint64_t ttl_millis_;
            SeqClockFn seq_clock_;
    };
} // namespace akkaradb::engine::sstable