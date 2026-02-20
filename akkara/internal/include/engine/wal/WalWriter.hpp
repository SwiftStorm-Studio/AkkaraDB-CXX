/*
 * AkkaraDB - Low-latency, crash-safe JVM KV store with WAL & stripe parity
 * Copyright (C) 2026 RiriFa
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
     * On-disk layout (per shard file):
     *   [WalSegmentHeader:32B]        ← written once at file start
     *   [WalBatchHeader:24B]          ← per fsync group
     *   [WalEntry × N]
     *   [WalBatchHeader:24B]          ← next fsync group
     *   ...
     *
     * Thread-safety: All public methods are thread-safe.
     */
    class WalWriter {
        public:
            /**
             * Creates a new WalWriter.
             *
             * @param wal_dir      Directory for .akwal shard files
             * @param group_n      Max entries per batch (soft limit; triggers early flush)
             * @param group_micros Flusher wait timeout in microseconds
             * @param fast_mode    If true, append returns without waiting for fsync
             * @param shard_count  Number of shards.
             *                     0 = auto (compute_shard_count(), always power-of-2 in [2,16])
             *                     1 = effectively single-shard (no parallelism)
             *                     N = rounded up to next power-of-2, capped at 16
             * @throws std::runtime_error if wal_dir cannot be created or files cannot be opened
             */
            [[nodiscard]] static std::unique_ptr<WalWriter> create(
                std::filesystem::path wal_dir,
                size_t group_n,
                size_t group_micros,
                bool fast_mode,
                uint32_t shard_count = 0
            );

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
             * Idempotent — safe to call multiple times.
             */
            void close();

        private:
            WalWriter(); // use create()

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::wal