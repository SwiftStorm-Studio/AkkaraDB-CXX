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
#include "engine/Compression.hpp"
#include "core/record/MemRecord.hpp"
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <set>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
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

                /// Compression codec for SST data sections.
                /// Codec::None = uncompressed (default); Codec::Zstd = Zstandard.
                akkaradb::engine::Codec codec = akkaradb::engine::Codec::None;

                /// Number of background compaction threads.
                /// Multiple threads allow non-conflicting level pairs (e.g. L1→L2
                /// and L2→L3) to compact simultaneously.  Default 2.
                int compact_threads = 2;

                /// L0 write-stall threshold: put() blocks when L0 file count >= l0_stall_files.
                /// 0 = auto (max_l0_files * 2). Set to INT_MAX to disable stalling.
                int l0_stall_files = 0;

                /// Total SST block cache capacity (MemRecord entries across all shards).
                /// 0 = disable cache. Default: 16384.
                size_t sst_cache_capacity = 16384;

                /// When true, each SST file's uncompressed data section is loaded into RAM
                /// at open time.  False positives from the bloom filter then hit an in-memory
                /// scan instead of fopen/fseek/fread, eliminating ~75 µs per FP lookup.
                /// Trade-off: uses ~data_size bytes of RAM per open SST file.
                /// Default: false
                bool preload_sst_data = false;
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

            /// Immutable snapshot of levels_, published atomically on every write.
            /// Used by contains() for lock-free reads on the hot lookup path.
            using LevelsData = std::vector<std::vector<SSTMeta>>;

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
             *                        Accepted as a span — no copy required from caller.
             * @return                max_seq of the written SST (for checkpoint tracking).
             */
            uint64_t flush(std::span<const core::MemRecord> sorted_records);

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

            /**
             * Existence check across all SST levels — no value copy, no blob I/O.
             *
             * Same traversal order as get() but delegates to SSTReader::contains().
             * Avoids value allocation and BlobManager::read() calls entirely.
             *
             * @return
             *   nullopt → not found in any SST file.
             *   false   → tombstone found (key is definitively deleted).
             *   true    → live record found.
             */
            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key) const;

            /**
             * Value fetch — same lock-free atomic-snapshot path as contains() but
             * also writes value bytes into `out` for live records.
             *
             * Avoids the intermediate MemRecord construction that get() requires.
             * For non-blob values this is the fastest SST read path.
             *
             * @return
             *   nullopt → not found in any SST file.
             *   false   → tombstone found (key is definitively deleted).
             *   true    → live record found, `out` contains the value bytes.
             */
            [[nodiscard]] std::optional<bool> get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const;

            // ── Stats ─────────────────────────────────────────────────────────

            /// Number of files at a given level.
            [[nodiscard]] size_t level_file_count(int level) const;

            /// Total bytes across all files at a given level.
            [[nodiscard]] uint64_t level_bytes(int level) const;

            /// Byte budget for level n (n>=1). Returns 0 for level 0 (count-based trigger).
            [[nodiscard]] uint64_t level_budget(int n) const;

            /// Per-level snapshot. Reads the lock-free levels_snap_ atomic — no mutex.
            struct LevelStats {
                int      level        = 0;
                size_t   file_count   = 0;
                uint64_t bytes        = 0;
                uint64_t budget_bytes = 0; ///< 0 for L0 (count-based)
            };
            [[nodiscard]] std::vector<LevelStats> level_stats() const;

            /// Returns true if a compaction was requested and a pool thread is
            /// still working on it (or waiting to start).
            [[nodiscard]] bool compaction_pending() const noexcept;

            /**
             * Point-in-time snapshot of compaction counters.
             * All reads are relaxed atomic loads — no lock acquired, no allocation.
             */
            struct CompactionSnapshot {
                /// Total compaction cycles completed since create().
                uint64_t compactions_completed = 0;
                /// Total input SST files consumed by all compaction cycles.
                uint64_t files_compacted       = 0;
                /// Total input bytes read by all compaction cycles.
                uint64_t bytes_compacted_in    = 0;
                /// Total output bytes written by all compaction cycles.
                uint64_t bytes_compacted_out   = 0;
                /// Times stall_if_l0_full() actually blocked a writer (L0 was full).
                uint64_t l0_stalls             = 0;
            };
            [[nodiscard]] CompactionSnapshot compaction_snapshot() const noexcept;

            // ── Write backpressure ────────────────────────────────────────────

            /**
             * Blocks until L0 file count drops below the stall threshold.
             * No-op when l0_stall_files == INT_MAX or L0 count is already safe.
             * Called by AkkEngine::put() to prevent L0 unbounded growth.
             */
            void stall_if_l0_full();

            // ── Range scan ────────────────────────────────────────────────────

            /**
             * Lazy k-way merge iterator over all relevant SST readers.
             *
             * Yields MemRecords in ascending key order; equal keys deduplicated
             * by highest-seq-wins.  Tombstones are NOT filtered — the MemTable/SST
             * merge layer in AkkEngine::ScanIterator::Impl handles that.
             *
             * The iterator holds shared_ptrs to all contributing readers so they
             * remain valid even if a concurrent compaction replaces them.
             */
            class Iterator {
                public:
                    Iterator() = default;
                    Iterator(Iterator&&) noexcept = default;
                    Iterator& operator=(Iterator&&) noexcept = default;
                    ~Iterator() = default;
                    Iterator(const Iterator&) = delete;
                    Iterator& operator=(const Iterator&) = delete;

                    [[nodiscard]] bool has_next() const noexcept { return pending_.has_value(); }
                    [[nodiscard]] core::MemRecord next();

                private:
                    friend class SSTManager;

                    struct HeapEntry {
                        core::MemRecord rec;
                        size_t source_idx;
                    };

                    struct HeapGreater {
                        bool operator()(const HeapEntry& a, const HeapEntry& b) const noexcept;
                    };

                    std::vector<std::shared_ptr<SSTReader>> snapshot_;
                    std::vector<SSTReader::Iterator> iters_;
                    std::priority_queue<HeapEntry, std::vector<HeapEntry>, HeapGreater> heap_;
                    std::vector<uint8_t> prev_key_;
                    std::optional<core::MemRecord> pending_;

                    void advance();
                    void init(std::span<const uint8_t> start_key, std::span<const uint8_t> end_key);
            };

            /**
             * Returns a lazy streaming iterator via k-way merge — no intermediate
             * vector allocation.  Prefer this for AkkEngine::scan() / count().
             *
             * @param start_key  Inclusive lower bound (empty = beginning of all SST).
             * @param end_key    Exclusive upper bound (empty = end of all SST).
             */
            [[nodiscard]] Iterator scan_iter(std::span<const uint8_t> start_key = {}, std::span<const uint8_t> end_key = {}) const;

            /**
             * Eagerly returns all records in [start_key, end_key) as a vector.
             * Used by compaction and blob-orphan scans where bulk materialization
             * is acceptable.  Prefer scan_iter() for user-facing scans.
             *
             * @param start_key  Inclusive lower bound (empty = beginning of all SST).
             * @param end_key    Exclusive upper bound (empty = end of all SST).
             */
            [[nodiscard]] std::vector<core::MemRecord> scan(std::span<const uint8_t> start_key = {}, std::span<const uint8_t> end_key = {}) const;

        private:
            explicit SSTManager(Options opts, manifest::Manifest* manifest);

            Options                  opts_;
            manifest::Manifest*      manifest_; ///< not owned

            // ── SST file state ─────────────────────────────────────────────
            //  levels_[0] = L0: newest-first, may overlap.
            //  levels_[n] (n>=1): first_key ascending, non-overlapping.
            mutable std::shared_mutex sst_mu_;
            std::vector<std::vector<SSTMeta>> levels_; ///< size = max_levels

            /// Lock-free read snapshot of levels_.
            /// Published under unique_lock(sst_mu_) whenever levels_ changes.
            /// contains() loads this atomically — no SRWLOCK acquire on the hot
            /// read path (~70 ns saved per lookup vs shared_lock).
            ///
            /// Correctness note: for compressed SSTs all data is in-memory so the
            /// snapshot keeps readers alive safely.  For uncompressed SSTs there is
            /// a theoretical (microsecond-scale) race between snapshot load and the
            /// file open inside contains_from_file(); in practice compaction takes
            /// milliseconds, making the race impossible to hit.
            mutable std::atomic<std::shared_ptr<const LevelsData>> levels_snap_;

            // ── Background compaction thread pool ──────────────────────────
            std::vector<std::thread> compact_pool_; ///< N worker threads
            std::mutex compact_notify_mu_;
            std::condition_variable compact_cv_;
            bool compact_requested_ = false;
            std::atomic<bool> shutting_down_{false};

            // ── L0 write-stall ─────────────────────────────────────────────
            std::mutex l0_stall_mu_;
            std::condition_variable l0_stall_cv_;

            // ── Block cache (sharded LRU) ───────────────────────────────────
            static constexpr size_t kCacheShards = 16;

            struct CacheShard {
                mutable std::mutex mu;
                using LruList = std::list<std::string>;
                LruList lru; ///< front=MRU, back=LRU

                /// Transparent hasher: accepts both std::string and std::string_view.
                /// Enables heterogeneous lookup in unordered_map — no std::string
                /// construction required on the cache-hit hot path.
                struct StringViewHash {
                    using is_transparent = void;
                    size_t operator()(std::string_view sv) const noexcept { return std::hash<std::string_view>{}(sv); }
                    size_t operator()(const std::string& s) const noexcept { return std::hash<std::string_view>{}(s); }
                };

                std::unordered_map<std::string, std::pair<core::MemRecord, LruList::iterator>, StringViewHash, std::equal_to<>> map;
                size_t capacity = 0; ///< max entries; 0 = disabled

                void cache_put(std::string key, core::MemRecord rec) {
                    if (capacity == 0) return;
                    auto it = map.find(key);
                    if (it != map.end()) {
                        lru.splice(lru.begin(), lru, it->second.second);
                        it->second.first = std::move(rec);
                        return;
                    }
                    if (map.size() >= capacity) {
                        map.erase(lru.back());
                        lru.pop_back();
                    }
                    lru.push_front(key); // copy for lru list
                    map.emplace(std::move(key), std::make_pair(std::move(rec), lru.begin()));
                }

                /// Zero-allocation lookup: accepts string_view, no heap alloc required.
                std::optional<core::MemRecord> cache_lookup(std::string_view key) {
                    auto it = map.find(key);
                    if (it == map.end()) return std::nullopt;
                    lru.splice(lru.begin(), lru, it->second.second); // move to MRU
                    return it->second.first;
                }

                void clear() {
                    lru.clear();
                    map.clear();
                }
            };

            mutable std::array<CacheShard, kCacheShards> cache_shards_;

            static size_t cache_shard_idx(std::span<const uint8_t> key) noexcept {
                const std::string_view sv(reinterpret_cast<const char*>(key.data()), key.size());
                return std::hash<std::string_view>{}(sv) % kCacheShards;
            }

            void cache_clear_all() {
                for (auto& shard : cache_shards_) {
                    std::lock_guard lk(shard.mu);
                    shard.clear();
                }
            }

            /// Tracks which src_levels are currently being compacted.
            /// A new task is only started for src_level if it is NOT in this set.
            /// Guarded by compact_busy_mu_.
            mutable std::mutex compact_busy_mu_;
            std::set<int> compact_busy_srcs_;

            std::atomic<uint64_t> next_sst_id_{0};

            // ── Compaction counters (relaxed atomics — zero overhead on write path) ──
            std::atomic<uint64_t> compactions_completed_{0};
            std::atomic<uint64_t> files_compacted_{0};
            std::atomic<uint64_t> bytes_compacted_in_{0};
            std::atomic<uint64_t> bytes_compacted_out_{0};
            std::atomic<uint64_t> l0_stalls_{0};

            // ── Compaction work descriptor ─────────────────────────────────

            struct CompactionWork {
                int src_level; ///< level from which inputs come (L0→L1: src=0)
                int dst_level; ///< level receiving output
                std::vector<SSTMeta> inputs; ///< snapshot of files to compact
                bool is_bottom; ///< dst_level == max_levels-1 → drop tombstones
            };

            // ── Internal helpers ───────────────────────────────────────────

            /// Publishes a fresh copy of levels_ as the new atomic snapshot.
            /// MUST be called under unique_lock(sst_mu_) whenever levels_ changes.
            void publish_snapshot_locked() noexcept;

            /// Returns absolute path for a new SST file: sst_dir/L{level}_{id:016x}.aksst
            std::filesystem::path make_sst_path(int level);

            /// Build SSTMeta from a SSTWriter::Result + level + filename.
            static SSTMeta make_meta(const SSTWriter::Result& res, int level, const std::string& filename, uint64_t file_size_bytes);

            /// Background compaction thread main loop.
            void compaction_loop();

            /// Selects the highest-priority compaction work that does NOT conflict with
            /// any src_level already in busy_srcs.  Returns nullopt if nothing to do.
            /// Caller must hold sst_mu_ (shared) and compact_busy_mu_.
            std::optional<CompactionWork> pick_compaction_work(const std::set<int>& busy_srcs) const;

            /// Atomically picks work + marks its src_level as busy.
            /// Returns nullopt if no non-conflicting work is available right now.
            std::optional<CompactionWork> pick_and_claim_work();

            /// Executes one compaction cycle described by `work`.
            void run_compaction(CompactionWork work);

            /// Splits merged records into output files of at most target_file_size_bytes.
            /// Returns list of written SSTMeta (open readers included).
            std::vector<SSTMeta> write_output_files(std::vector<core::MemRecord> merged, int dst_level);
    };

} // namespace akkaradb::engine::sst
