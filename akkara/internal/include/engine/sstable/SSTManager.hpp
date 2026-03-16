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

#include "engine/sstable/SSTReader.hpp"
#include "engine/sstable/SSTWriter.hpp"
#include "engine/manifest/Manifest.hpp"
#include "core/record/MemRecord.hpp"
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
#include <thread>
#include <vector>

namespace akkaradb::engine::sst {

    /**
     * SSTManager — Multi-level leveled compaction (RocksDB-style).
     *
     * Level semantics:
     *   L0: Output of MemTable shard flushes. Files are internally sorted but
     *       may have overlapping key ranges across L0 files (hash-sharded MemTable).
     *       Read order: newest file first (highest ID).
     *
     *   L1+: Output of compaction. Each level holds multiple non-overlapping,
     *        first_key-sorted files. Each level has a byte budget that grows
     *        exponentially: level_budget(n) = l1_max_bytes * multiplier^(n-1).
     *
     * Compaction policy:
     *   - L0→L1: triggered when L0 file count >= max_l0_files.
     *             All L0 files + all L1 files are merged (L0 can cover full key range).
     *             Output is split into files of at most target_file_size_bytes.
     *   - Ln→L(n+1) (n>=1): triggered when total bytes in Ln > level_budget(n).
     *             One file from Ln (oldest by max_seq) is selected, all overlapping
     *             L(n+1) files are found, they are merged, and output replaces the
     *             compacted L(n+1) files.
     *   - Tombstones are dropped only when writing to the bottom level (max_levels-1).
     *   - Compaction runs in a dedicated background thread (non-blocking for flush()).
     *
     * Thread-safety:
     *   - flush() is called from MemTable shard flusher threads (multiple threads).
     *   - get() may be called from any thread concurrently.
     *   - Compaction is serialized in a single background thread.
     *   - sst_mu_ (shared_mutex) guards levels_.
     */
    class SSTManager {
        public:
            // ── Options ───────────────────────────────────────────────────────

            struct Options {
                std::filesystem::path sst_dir;

                /// Max L0 file count before triggering L0 → L1 compaction.
                int max_l0_files = 4;

                /// Maximum number of levels (L0 + up to max_levels-1 sorted levels).
                int max_levels = 7;

                /// Total byte budget for L1. L(n) budget = l1_max_bytes * multiplier^(n-1).
                uint64_t l1_max_bytes = 64ULL * 1024 * 1024;

                /// Size multiplier between consecutive levels. Default 10x (L2 = 640MB, etc.)
                double level_size_multiplier = 10.0;

                /// Maximum size of a single output SST file produced during compaction.
                /// Merged output is split into files of at most this size.
                uint64_t target_file_size_bytes = 64ULL * 1024 * 1024;

                size_t bloom_bits_per_key = BLOOM_BITS_PER_KEY;
                size_t index_stride       = INDEX_STRIDE;
            };

            // ── Per-file metadata ─────────────────────────────────────────────

            struct SSTMeta {
                std::filesystem::path          path;
                std::string filename; ///< filename only (for Manifest)
                int level = 0;
                uint64_t entry_count = 0;
                uint64_t file_size_bytes = 0; ///< from fs::file_size()
                uint64_t min_seq = 0;
                uint64_t max_seq = 0;
                std::vector<uint8_t>           first_key;
                std::vector<uint8_t>           last_key;
                std::shared_ptr<SSTReader> reader; ///< cached open reader
            };

            // ── Factory ───────────────────────────────────────────────────────

            /**
             * Creates an SSTManager and starts the background compaction thread.
             *
             * @param opts      Configuration (sst_dir, compaction thresholds, etc.)
             * @param manifest  Optional Manifest for SST lifecycle logging. May be null.
             */
            [[nodiscard]] static std::unique_ptr<SSTManager> create(
                Options                    opts,
                manifest::Manifest*        manifest = nullptr
            );

            ~SSTManager();
            SSTManager(const SSTManager&)            = delete;
            SSTManager& operator=(const SSTManager&) = delete;

            // ── Lifecycle ─────────────────────────────────────────────────────

            /**
             * Recovers existing SST files from the Manifest's live_sst() list.
             * Must be called once after create(), before flush() or get().
             */
            void recover();

            /**
             * Stops the background compaction thread gracefully.
             * Must be called before destruction (AkkEngine::close() calls this).
             */
            void shutdown();

            // ── Write path ────────────────────────────────────────────────────

            /**
             * Writes sorted_records as a new L0 SST file, then wakes the background
             * compaction thread (non-blocking — compaction runs asynchronously).
             *
             * @param sorted_records  Key-sorted records from one MemTable shard flush.
             * @return                max_seq of the written SST (for checkpoint tracking).
             */
            uint64_t flush(std::vector<core::MemRecord> sorted_records);

            // ── Read path ─────────────────────────────────────────────────────

            /**
             * Point lookup across all SST levels.
             *
             * Search order:
             *   L0: newest-first linear scan (files may overlap).
             *   L1+: binary search on non-overlapping sorted files (O(log N) per level).
             *
             * Returns the first match found (highest-priority / newest write wins).
             * Returns nullopt if the key is not found in any SST file.
             */
            [[nodiscard]] std::optional<core::MemRecord>
                get(std::span<const uint8_t> key) const;

            // ── Stats ─────────────────────────────────────────────────────────

            /// Number of files at a given level.
            [[nodiscard]] size_t level_file_count(int level) const;

            /// Total bytes across all files at a given level.
            [[nodiscard]] uint64_t level_bytes(int level) const;

            /// Byte budget for level n (n>=1). Returns 0 for level 0 (count-based trigger).
            [[nodiscard]] uint64_t level_budget(int n) const;

        private:
            explicit SSTManager(Options opts, manifest::Manifest* manifest);

            Options                  opts_;
            manifest::Manifest*      manifest_; ///< not owned

            // ── SST file state ─────────────────────────────────────────────
            //  levels_[0] = L0: newest-first, may overlap.
            //  levels_[n] (n>=1): first_key ascending, non-overlapping.
            mutable std::shared_mutex sst_mu_;
            std::vector<std::vector<SSTMeta>> levels_; ///< size = max_levels

            // ── Background compaction thread ───────────────────────────────
            std::thread compact_thread_;
            std::mutex compact_notify_mu_;
            std::condition_variable compact_cv_;
            bool compact_requested_ = false;
            std::atomic<bool> shutting_down_{false};

            std::atomic<uint64_t> next_sst_id_{0};

            // ── Compaction work descriptor ─────────────────────────────────

            struct CompactionWork {
                int src_level; ///< level from which inputs come (L0→L1: src=0)
                int dst_level; ///< level receiving output
                std::vector<SSTMeta> inputs; ///< snapshot of files to compact
                bool is_bottom; ///< dst_level == max_levels-1 → drop tombstones
            };

            // ── Internal helpers ───────────────────────────────────────────

            /// Returns absolute path for a new SST file: sst_dir/L{level}_{id:016x}.aksst
            std::filesystem::path make_sst_path(int level);

            /// Build SSTMeta from a SSTWriter::Result + level + filename.
            static SSTMeta make_meta(const SSTWriter::Result& res, int level, const std::string& filename, uint64_t file_size_bytes);

            /// Background compaction thread main loop.
            void compaction_loop();

            /// Selects the highest-priority compaction work. Returns nullopt if none needed.
            std::optional<CompactionWork> pick_compaction_work() const;

            /// Executes one compaction cycle described by `work`.
            void run_compaction(CompactionWork work);

            /// Splits merged records into output files of at most target_file_size_bytes.
            /// Returns list of written SSTMeta (open readers included).
            std::vector<SSTMeta> write_output_files(std::vector<core::MemRecord> merged, int dst_level);
    };

} // namespace akkaradb::engine::sst
