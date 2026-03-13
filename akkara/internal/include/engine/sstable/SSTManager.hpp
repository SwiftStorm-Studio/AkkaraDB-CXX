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

// internal/include/engine/sst/SSTManager.hpp
#pragma once

#include "engine/sstable/SSTReader.hpp"
#include "engine/sstable/SSTWriter.hpp"
#include "engine/manifest/Manifest.hpp"
#include "core/record/MemRecord.hpp"
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
#include <vector>

namespace akkaradb::engine::sst {

    /**
     * SSTManager — Manages L0 + L1 SST files and background compaction.
     *
     * Level semantics:
     *   L0: Output of MemTable shard flushes.  Files are internally sorted but
     *       may have overlapping key ranges across L0 files (MemTable shards are
     *       hash-routed, not range-partitioned).
     *       Read order: newest file first (highest ID).
     *
     *   L1: Output of compaction (merge of all L0 + all L1).  Stored as a single
     *       fully-sorted, non-overlapping file per compaction cycle.
     *       (Phase 5: at most one L1 file at a time.)
     *
     * Compaction policy: when L0 reaches max_l0_files, compact all L0 + all L1
     * into a single new L1 file.  Tombstones are dropped at L1 (bottom level).
     *
     * SST file naming: L{level}_{id:016x}.aksst  (stored in sst_dir/).
     *
     * Thread-safety:
     *   - flush() is called from MemTable shard flusher threads (multiple threads).
     *   - get() may be called from any thread concurrently.
     *   - compact_l0_to_l1() serialized by compact_mu_.
     *   - sst_mu_ (shared_mutex) guards l0_files_ and l1_files_.
     */
    class SSTManager {
        public:
            // ── Options ───────────────────────────────────────────────────────

            struct Options {
                std::filesystem::path sst_dir;
                int    max_l0_files       = 4;
                size_t bloom_bits_per_key = BLOOM_BITS_PER_KEY;
                size_t index_stride       = INDEX_STRIDE;
            };

            // ── Per-file metadata ─────────────────────────────────────────────

            struct SSTMeta {
                std::filesystem::path          path;
                std::string                    filename;   ///< filename only (for Manifest)
                int                            level = 0;
                uint64_t                       entry_count = 0;
                uint64_t                       min_seq = 0;
                uint64_t                       max_seq = 0;
                std::vector<uint8_t>           first_key;
                std::vector<uint8_t>           last_key;
                std::shared_ptr<SSTReader>     reader;     ///< cached open reader
            };

            // ── Factory ───────────────────────────────────────────────────────

            /**
             * Creates an SSTManager.
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

            // ── Write path ────────────────────────────────────────────────────

            /**
             * Writes sorted_records as a new L0 SST file.
             * Called from the MemTable flush callback (per-shard flusher thread).
             *
             * After writing, triggers maybe_compact() if L0 has reached the limit.
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
             *   L0 files newest-first (may overlap with each other).
             *   L1 files (at most one in Phase 5).
             *
             * Returns the first match found (highest-priority / newest write wins).
             * Returns nullopt if the key is not found in any SST file.
             */
            [[nodiscard]] std::optional<core::MemRecord>
                get(std::span<const uint8_t> key) const;

            // ── Compaction ────────────────────────────────────────────────────

            /**
             * Compacts L0 → L1 if L0 has reached max_l0_files.
             * No-op if already at or below the threshold.
             *
             * At most one compaction runs at a time (guarded by compact_mu_).
             * Compaction result: all L0 + all L1 merged into a single new L1 file.
             * Tombstones are dropped at L1 (bottom level).
             */
            void maybe_compact();

            // ── Stats ─────────────────────────────────────────────────────────

            [[nodiscard]] size_t l0_count() const;
            [[nodiscard]] size_t l1_count() const;

        private:
            explicit SSTManager(Options opts, manifest::Manifest* manifest);

            Options                  opts_;
            manifest::Manifest*      manifest_; ///< not owned

            mutable std::shared_mutex sst_mu_;
            std::vector<SSTMeta>     l0_files_; ///< newest first (highest id first)
            std::vector<SSTMeta>     l1_files_; ///< Phase 5: at most one file

            std::mutex               compact_mu_;
            std::atomic<uint64_t>    next_sst_id_{0};

            // ── Internals ─────────────────────────────────────────────────────

            /// Returns an absolute path for a new SST file: sst_dir/L{level}_{id:016x}.aksst
            std::filesystem::path make_sst_path(int level);

            /// Core compaction: merge all L0 + all L1 → new L1 SST.
            /// Caller must hold compact_mu_.
            void compact_l0_to_l1_locked();

            /// Build SSTMeta from a SSTWriter::Result + file path.
            static SSTMeta make_meta(const SSTWriter::Result& res, int level,
                                     const std::string& filename);
    };

} // namespace akkaradb::engine::sst
