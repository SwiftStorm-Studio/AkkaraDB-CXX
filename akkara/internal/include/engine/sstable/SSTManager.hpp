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

// internal/include/engine/sstable/SSTManager.hpp
#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "core/record/RecordView.hpp"
#include "engine/manifest/Manifest.hpp"
#include "engine/sstable/SSTReader.hpp"
#include "engine/sstable/SSTWriter.hpp"

namespace akkaradb::engine::sst {

    class SSTManager {
        public:
            struct Options {
                std::filesystem::path sst_dir;
                int max_levels = 7;
                int max_l0_files = 4;
                uint64_t l1_max_bytes = 64ULL * 1024ULL * 1024ULL;
                double level_size_multiplier = 10.0;
                uint64_t target_file_size = SST_DEFAULT_TARGET_FILE_SIZE;
                uint32_t block_size = SST_DEFAULT_BLOCK_SIZE;
                uint32_t bloom_bits_per_key = SST_DEFAULT_BLOOM_BITS_PER_KEY;
                uint64_t block_cache_bytes = 64ULL * 1024ULL * 1024ULL;
                int compact_threads = 2;
                SSTWriter::Codec codec = SSTWriter::Codec::Zstd;
            };

            struct LevelStats {
                int level = 0;
                size_t file_count = 0;
                uint64_t bytes = 0;
                uint64_t budget_bytes = 0;
            };

            struct CompactionSnapshot {
                uint64_t compactions_completed = 0;
                uint64_t files_compacted = 0;
                uint64_t bytes_compacted_in = 0;
                uint64_t bytes_compacted_out = 0;
            };

            class Iterator {
                public:
                    Iterator();
                    explicit Iterator(std::vector<SSTRecord> records);
                    [[nodiscard]] bool has_next() const noexcept;
                    [[nodiscard]] std::optional<SSTRecord> next();

                private:
                    std::vector<SSTRecord> records_;
                    size_t index_{0};
            };

            [[nodiscard]] static std::unique_ptr<SSTManager> create(
                Options options,
                manifest::Manifest* manifest = nullptr
            );

            ~SSTManager();
            SSTManager(const SSTManager&) = delete;
            SSTManager& operator=(const SSTManager&) = delete;

            void recover();
            void shutdown();
            uint64_t flush(std::span<const core::RecordView> records);

            [[nodiscard]] std::optional<SSTRecord> get(std::span<const uint8_t> key) const;
            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key) const;
            [[nodiscard]] std::optional<bool> get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const;
            [[nodiscard]] Iterator scan_iter(std::span<const uint8_t> start_key = {}, std::span<const uint8_t> end_key = {}) const;

            [[nodiscard]] std::vector<LevelStats> level_stats() const;
            [[nodiscard]] bool compaction_pending() const noexcept;
            [[nodiscard]] CompactionSnapshot compaction_snapshot() const noexcept;

        private:
            SSTManager(Options options, manifest::Manifest* manifest);
            class Impl;
            std::unique_ptr<Impl> impl_;
    };

} // namespace akkaradb::engine::sst
