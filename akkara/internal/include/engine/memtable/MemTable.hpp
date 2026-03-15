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

// internal/include/engine/memtable/MemTable.hpp
#pragma once

#include "core/record/MemRecord.hpp"
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace akkaradb::engine::memtable {
    /**
     * MemTable - Multi-shard in-memory write buffer with background flush.
     *
     * Architecture:
     * - Options::shard_count shards (power-of-2, 2..256), each with its own shared_mutex
     * - Shard selection: key_fp64 & (shard_count - 1)  (~1 ns, no division)
     *   key_fp64 = AKHdr32::compute_key_fp64(), same hash as WAL routing
     * - Per-shard Flusher thread: flushes independently, no cross-shard blocking
     * - Active map: IMemMap (BPTree or SkipList, selected via Options::backend)
     * - Immutable snapshots: sealed sorted vectors awaiting flush
     * - seq_gen_: atomic monotonic counter; advanced to max(current, observed+1)
     *   on every write so WAL recovery leaves it correctly positioned
     *
     * Flush lifecycle:
     *   put/remove exceeds threshold
     *   → seal_active(): collect_sorted() → shared_ptr<const vector<MemRecord>>
     *   → Flusher::enqueue(): receives shared_ptr (records already sorted)
     *   → FlushCallback invoked with sorted records
     *   → on_flushed(id): immutable removed from shard
     *
     * Thread-safety: All public methods are thread-safe.
     */
    class MemTable {
        public:
            // ── Types ─────────────────────────────────────────────────────────

            /**
             * Called by the background flusher with a sorted, sealed batch.
             * Records are moved in; the callback takes ownership.
             * Called from a per-shard flusher thread (not the writer thread).
             */
            using FlushCallback = std::function<void(std::vector<core::MemRecord>)>;

            // ── Backend selection ─────────────────────────────────────────────

            /**
             * Selects the ordered-map implementation used inside each shard.
             *
             *   BPTree   — B+ tree with MonotonicArena node allocation.
             *              Best point-lookup and scan throughput; default.
             *
             *   SkipList — Probabilistic skip list (p = 0.25, max height 12).
             *              Industry-standard LSM MemTable structure (LevelDB / RocksDB).
             *              Per-node heap allocation; simpler to adapt for lock-free writes.
             */
            enum class Backend : uint8_t {
                BPTree = 0,
                ///< B+ tree (default)
                SkipList = 1,
                ///< Skip list
            };

            /**
             * Options - Configuration for MemTable creation.
             *
             * Nested inside MemTable so FlushCallback is already in scope.
             */
            struct Options {
                /**
                 * Number of shards.
                 * Rounded up to next power-of-2, clamped to [2, 16].
                 * 0 = auto (uses std::thread::hardware_concurrency(), min 2).
                 * Default: 0 (auto)
                 */
                size_t shard_count = 0;

                /**
                 * Approximate byte threshold per shard before automatic flush is triggered.
                 * Higher values reduce flush frequency (better throughput batching) at the
                 * cost of higher peak memory usage per shard.
                 * Default: 64 MiB
                 */
                size_t threshold_bytes_per_shard = 64ULL * 1024 * 1024;

                /**
                 * Ordered-map implementation used inside each shard.
                 * Default: BPTree
                 */
                Backend backend = Backend::BPTree;

                /**
                 * Optional flush callback. Can be set or replaced later via
                 * set_flush_callback(). Pass nullptr to disable background flushing.
                 * Default: nullptr
                 */
                FlushCallback on_flush = nullptr;
            };

            /**
             * Key range for range scans: [start, end).
             * Empty end means "unbounded upper limit".
             */
            struct KeyRange {
                std::vector<uint8_t> start; ///< Inclusive lower bound
                std::vector<uint8_t> end; ///< Exclusive upper bound (empty = unbounded)
            };

            /**
             * Forward iterator over [start, end) records in lexicographic order.
             * Performs k-way merge across all shards and immutables at construction time.
             * Deduplicates by key, yielding only the highest-seq record per key.
             */
            class RangeIterator {
                public:
                    ~RangeIterator();
                    RangeIterator(RangeIterator&&) noexcept;
                    RangeIterator& operator=(RangeIterator&&) noexcept;

                    RangeIterator(const RangeIterator&) = delete;
                    RangeIterator& operator=(const RangeIterator&) = delete;

                    [[nodiscard]] bool has_next() const noexcept;
                    [[nodiscard]] std::optional<core::MemRecord> next();

                private:
                    friend class MemTable;
                    class Impl;
                    explicit RangeIterator(std::unique_ptr<Impl> impl);
                    std::unique_ptr<Impl> impl_;
            };

            // ── Factory ───────────────────────────────────────────────────────

            /**
             * Creates a MemTable from an Options struct.
             *
             * @param options  See MemTable::Options for field descriptions.
             * @throws std::invalid_argument if options.shard_count results in 0 shards
             */
            [[nodiscard]] static std::unique_ptr<MemTable> create(const Options& options = {});

            ~MemTable();

            MemTable(const MemTable&) = delete;
            MemTable& operator=(const MemTable&) = delete;
            MemTable(MemTable&&) = delete;
            MemTable& operator=(MemTable&&) = delete;

            // ── Write path ────────────────────────────────────────────────────

            /**
             * Inserts or updates a key-value pair.
             *
             * Shard: key_fp64 & (shard_count - 1)
             * key_fp64 is computed internally via AKHdr32::compute_key_fp64().
             *
             * @param key   Key bytes
             * @param value Value bytes
             * @param seq   Sequence number (WAL sequence or next_seq())
             */
            void put(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags = core::AKHdr32::FLAG_NORMAL,
                uint64_t precomputed_fp64 = 0
            );

            /**
             * Inserts a tombstone (deletion marker).
             *
             * @param key Key bytes
             * @param seq Sequence number
             */
            void remove(std::span<const uint8_t> key, uint64_t seq);

            // ── Read path ─────────────────────────────────────────────────────

            /**
             * Point lookup. Searches active map then immutables (newest first).
             *
             * @return The record by value if found, std::nullopt otherwise.
             */
            [[nodiscard]] std::optional<core::MemRecord> get(std::span<const uint8_t> key) const;

            /**
             * Zero-copy point lookup: copies value bytes directly into out.
             *
             * Eliminates the intermediate MemRecord copy that get() produces.
             * When out has sufficient capacity (e.g. pre-reserved), assign() reuses
             * the existing allocation — no malloc on the hot path.
             *
             * Return convention:
             *   nullopt → key not found; caller may check SST
             *   false   → tombstone; caller must NOT check SST (delete is authoritative)
             *   true    → found; value written to out
             */
            [[nodiscard]] std::optional<bool> get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const;

            /**
             * Returns a range iterator over [range.start, range.end).
             * Snapshot semantics: reflects state at the moment of the call.
             */
            [[nodiscard]] RangeIterator iterator(const KeyRange& range) const;

            // ── Sequence number ───────────────────────────────────────────────

            /** Allocates and returns the next sequence number. */
            [[nodiscard]] uint64_t next_seq() noexcept;

            /** Returns the current sequence counter (last allocated + 1). */
            [[nodiscard]] uint64_t last_seq() const noexcept;

            // ── Flush control ─────────────────────────────────────────────────

            /**
             * Triggers flush for any shard currently over its threshold.
             * Non-blocking: flush happens asynchronously in flusher threads.
             */
            void flush_hint();

            /**
             * Seals and enqueues ALL non-empty shards for flush, then blocks
             * until all flusher threads have completed their queues.
             */
            void force_flush();

            /**
             * Replaces the flush callback.
             * Drains the existing flusher (blocks) before switching.
             * Pass nullptr to disable flushing.
             */
            void set_flush_callback(const FlushCallback& cb);

            // ── Metrics ───────────────────────────────────────────────────────

            /** Approximate total bytes across all shards (active + immutables). */
            [[nodiscard]] size_t approx_size() const noexcept;

        private:
            class Impl;
            explicit MemTable(const Options& options);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::memtable
