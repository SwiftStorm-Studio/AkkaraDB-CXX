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

// internal/include/engine/wal/WalRecovery.hpp
#pragma once

#include "engine/wal/WalOp.hpp"
#include "engine/wal/WalReader.hpp"
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <span>

namespace akkaradb::wal {
    /**
     * WalRecovery - Multi-shard WAL replay with cross-shard global ordering.
     *
     * Algorithm:
     *   1. Enumerate all shard_NNNN_segNNNN.akwal files in wal_dir.
     *      Group by shard_id, sort each group by segment_id ascending.
     *   2. For each shard, create a segment chain that transparently advances
     *      through segment files in order (ShardSegmentChain).
     *   3. Seed a min-heap with the first batch from each shard chain.
     *   4. Drain the heap in batch_seq order, re-seeding from the same shard
     *      chain after each batch is consumed.
     *   5. For each batch, iterate entries and dispatch to the appropriate handler.
     *
     * This ensures that entries are replayed in the same global write order
     * that WalWriter guaranteed via its monotonic global_batch_seq counter.
     *
     * Performance notes:
     * - Sequential I/O per shard: all segment files for a shard are read in order.
     * - Heap operations are O(S log S) where S = shard count (max 16).
     * - No heap allocation per entry; WalIterator is zero-copy.
     * - Handlers are called inline; no intermediate storage of entries.
     *
     * Thread-safety: NOT thread-safe. Intended to run single-threaded
     * during engine startup, before any writers are active.
     */
    class WalRecovery {
        public:
            // ── Handler types ─────────────────────────────────────────────────

            /**
             * Called for every Record entry (Add or Delete/tombstone).
             *
             * @param ref   Zero-copy view of the record (key, value, seq, …)
             *              Valid only for the duration of this call.
             */
            using RecordHandler = std::function<void(const WalRecordOpRef & ref)>;

            /**
             * Called for every Commit entry.
             *
             * @param seq       Sequence number from the commit entry
             * @param timestamp Timestamp in microseconds
             */
            using CommitHandler = std::function<void(uint64_t seq, uint64_t timestamp)>;

            /**
             * Called for every Checkpoint marker.
             * Optional: pass nullptr to ignore checkpoints.
             *
             * @param seq Sequence number at the checkpoint
             */
            using CheckpointHandler = std::function<void(uint64_t seq)>;

            // ── Result ────────────────────────────────────────────────────────

            struct ShardError {
                uint32_t shard_id;
                WalReader::ErrorType error_type;
                uint64_t error_position;
            };

            struct Result {
                bool success; ///< true if all shards read cleanly to EOF
                uint64_t batches_replayed; ///< Total batches processed
                uint64_t records_replayed; ///< Total Record entries dispatched
                uint64_t commits_replayed; ///< Total Commit entries dispatched
                uint64_t checkpoints_replayed; ///< Total Checkpoint entries dispatched

                /**
                 * Errors from individual shards (empty if success == true).
                 * Recovery continues past shard errors: all valid data before
                 * the corruption is replayed, then the shard is abandoned.
                 */
                std::vector<ShardError> shard_errors;
            };

            // ── API ───────────────────────────────────────────────────────────

            [[nodiscard]] static std::unique_ptr<WalRecovery> create();

            ~WalRecovery();

            WalRecovery(const WalRecovery&) = delete;
            WalRecovery& operator=(const WalRecovery&) = delete;

            /**
             * Replays the WAL in global write order.
             *
             * @param wal_dir         Directory containing shard_NNNN_segNNNN.akwal files
             * @param on_record       Required. Called for every Record entry.
             * @param on_commit       Required. Called for every Commit entry.
             * @param on_checkpoint   Optional. Called for every Checkpoint entry.
             *
             * @throws std::invalid_argument if on_record or on_commit is null
             */
            [[nodiscard]] Result replay(
                const std::filesystem::path& wal_dir,
                const RecordHandler& on_record,
                const CommitHandler& on_commit,
                const CheckpointHandler& on_checkpoint = nullptr
            );

        private:
            WalRecovery() = default;
    };
} // namespace akkaradb::wal