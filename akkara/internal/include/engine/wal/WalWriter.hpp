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

// internal/include/engine/wal/WalWriter.hpp
#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>

namespace akkaradb::wal {
    // ============================================================================
    // WalOptions - startup configuration for WalWriter
    // ============================================================================

    /**
     * WalOptions - Configuration passed to WalWriter::create().
     *
     * All fields have sensible defaults suitable for production use.
     * Only wal_dir is required.
     */
    struct WalOptions {
        /**
         * Directory where .akwal shard segment files are stored.
         * Created automatically if it does not exist.
         */
        std::filesystem::path wal_dir;

        /**
         * Number of WAL shards.
         *   0 = auto (compute_shard_count(); always power-of-2 in [2, 16])
         *   1 = single shard (no parallelism)
         *   N = rounded up to next power-of-2, capped at 16
         *
         * More shards reduce fsync contention under high write concurrency.
         */
        uint32_t shard_count = 0;

        /**
         * Maximum entries per write batch (soft limit).
         * When a flusher wakes up with more than group_n entries queued,
         * it flushes them in group_n-sized chunks, each with its own fsync.
         *
         * Higher values increase throughput at the cost of per-entry latency.
         * Default: 256
         */
        size_t group_n = 256;

        /**
         * Flusher wait timeout in microseconds.
         * The flusher wakes up after at most group_micros even if the queue
         * did not reach group_n entries, ensuring bounded latency.
         *
         * Lower values reduce tail latency; higher values improve batching.
         * Default: 500 µs
         */
        size_t group_micros = 500;

        /**
         * If true, append_put/append_delete return immediately after
         * enqueuing the entry without waiting for fdatasync().
         * Durability is only guaranteed after the next force_sync() call.
         *
         * Use for bulk-load scenarios where throughput matters more than
         * per-write durability.
         * Default: false (durable writes)
         */
        bool fast_mode = false;

        /**
         * Maximum segment file size in bytes before the shard rotates to a
         * new segment file.
         *   0 = unlimited (no rotation)
         *
         * Rotation happens at batch boundaries: once the current segment
         * reaches or exceeds this threshold, the next batch is written to
         * a new file named shard_{id:04d}_seg{seg:04d}.akwal.
         *
         * Smaller segments speed up individual segment reads during recovery
         * and allow old segments to be archived or deleted independently.
         * Default: 0 (unlimited)
         */
        uint64_t max_segment_bytes = 0;
    };

    // ============================================================================
    // WalWriter
    // ============================================================================

    /**
     * WalWriter - Multi-shard Write-Ahead Log writer.
     *
     * Architecture:
     * - Shard count: configurable (0 = auto via compute_shard_count(), 1 = effectively disabled)
     * - Per-shard flusher thread: eliminates cross-shard fsync contention
     * - Global batch_seq: enables cross-shard ordering during recovery
     * - fast_mode=false: blocks until fdatasync() completes (durability guarantee)
     * - fast_mode=true:  returns immediately after enqueue (~300-500ns latency)
     *
     * On-disk layout (per segment file):
     *   [WalSegmentHeader:32B]   written once at file start
     *   [WalBatchHeader:24B]     per fsync group
     *   [WalEntry x N]
     *   [WalBatchHeader:24B]     next fsync group
     *   ...
     *
     * Segment files are named shard_{id:04d}_seg{seg:04d}.akwal.
     * When max_segment_bytes > 0, the writer rotates to a new segment file
     * once the current file reaches the threshold.
     *
     * Thread-safety: All public methods are thread-safe.
     */
    class WalWriter {
        public:
            /**
             * Creates a new WalWriter from a WalOptions struct.
             *
             * @throws std::runtime_error if wal_dir cannot be created or files cannot be opened
             */
            [[nodiscard]] static std::unique_ptr<WalWriter> create(WalOptions options);

            ~WalWriter();

            WalWriter(const WalWriter&) = delete;
            WalWriter& operator=(const WalWriter&) = delete;
            WalWriter(WalWriter&&) = delete;
            WalWriter& operator=(WalWriter&&) = delete;

            /**
             * Appends a Put entry to the appropriate shard.
             *
             * Shard selection: shard_for(key_fp64, shard_count)
             *
             * fast_mode=false: Blocks until fdatasync() completes.
             * fast_mode=true:  Returns after enqueue. No durability guarantee until force_sync().
             *
             * @throws std::runtime_error if WAL is closed or write fails
             */
            void append_put(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq, uint64_t key_fp64, uint64_t mini_key);

            /**
             * Appends a Delete (tombstone) entry to the appropriate shard.
             *
             * @throws std::runtime_error if WAL is closed or write fails
             */
            void append_delete(std::span<const uint8_t> key, uint64_t seq, uint64_t key_fp64, uint64_t mini_key);

            /**
             * Forces all pending entries across all shards to be flushed and fdatasync'd.
             * Blocks until complete regardless of fast_mode setting.
             *
             * @throws std::runtime_error on fsync failure
             */
            void force_sync();

            /**
             * Truncates all shard WAL files (removes all data).
             * The next write will re-write the WalSegmentHeader.
             * Blocks until complete.
             *
             * @throws std::runtime_error on truncate failure
             */
            void truncate();

            /**
             * Stops all flusher threads and closes all file handles.
             * Idempotent - safe to call multiple times.
             */
            void close();

        private:
            WalWriter(); // use create()

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::wal