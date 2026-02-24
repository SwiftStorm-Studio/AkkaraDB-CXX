/*
 * AkkaraDB - Low-latency, crash-safe JVM KV store with WAL & stripe parity
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
     * - shard_count shards (power-of-2, 2..16), each with its own shared_mutex
     * - Shard selection: key_fp64 & (shard_count - 1)  (~1 ns, no division)
     *   key_fp64 = AKHdr32::compute_key_fp64(), same hash as WAL routing
     * - Per-shard Flusher thread: flushes independently, no cross-shard blocking
     * - Active map: accepts new writes
     * - Immutable maps: sealed maps awaiting flush, held as shared_ptr<const Map>
     *   so readers and the flusher share the same allocation without copying
     * - seq_gen_: atomic monotonic counter; advanced to max(current, observed+1)
     *   on every write so WAL recovery leaves it correctly positioned
     *
     * Flush lifecycle:
     *   put/remove exceeds threshold
     *   → seal_active(): active_ → shared_ptr<const Map>, pushed to immutables_
     *   → Flusher::enqueue(): receives shared_ptr (no copy of records)
     *   → FlushCallback invoked with sorted records (sorted inside flusher thread)
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
             * Creates a MemTable.
             *
             * @param shard_count           Number of shards.
             *                              Rounded up to next power-of-2, clamped to [2, 16].
             * @param threshold_bytes_per_shard
             *                              Approximate byte threshold per shard before
             *                              automatic flush is triggered.
             * @param on_flush              Optional flush callback. Can be set later via
             *                              set_flush_callback().
             * @throws std::invalid_argument if shard_count == 0
             */
            [[nodiscard]] static std::unique_ptr<MemTable> create(size_t shard_count, size_t threshold_bytes_per_shard, FlushCallback on_flush = nullptr);

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
            void put(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq);

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
             * @return Shared pointer to the record, or nullptr if not found.
             *         Caller may hold the pointer beyond the next write.
             */
            [[nodiscard]] std::shared_ptr<const core::MemRecord> get(std::span<const uint8_t> key) const;

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
            void set_flush_callback(FlushCallback cb);

            // ── Metrics ───────────────────────────────────────────────────────

            /** Approximate total bytes across all shards (active + immutables). */
            [[nodiscard]] size_t approx_size() const noexcept;

        private:
            class Impl;
            explicit MemTable(size_t shard_count, size_t threshold_bytes_per_shard, FlushCallback on_flush);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::memtable