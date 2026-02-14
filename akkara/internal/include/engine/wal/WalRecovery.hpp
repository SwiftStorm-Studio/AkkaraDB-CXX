/*
* AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
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

// internal/include/engine/wal/WalRecovery.hpp
#pragma once

#include "WalReader.hpp"
#include <memory>
#include <filesystem>
#include <cstdint>
#include <functional>

namespace akkaradb::engine::wal {
    /**
 * WalRecovery - Recovers database state from WAL file.
 *
 * Reads WAL operations and applies them to rebuild MemTable state.
 * Handles truncated/corrupted WAL gracefully, stopping at the first error.
 *
 * Design principles:
 * - Callback-based: Caller provides handlers for PUT/DELETE/CHECKPOINT
 * - Error resilient: Stops at corruption, reports statistics
 * - Idempotent: Safe to replay multiple times (last-write-wins)
 *
 * Typical usage:
 * ```cpp
 * auto recovery = WalRecovery::create();
 *
 * auto result = recovery->replay(
 *     "./data/wal/wal-12345.dat",
 *     [&](auto key, auto value, uint64_t seq) {
 *         memtable->put(key, value, seq);  // PUT handler
 *     },
 *     [&](auto key, uint64_t seq) {
 *         memtable->delete_key(key, seq);  // DELETE handler
 *     },
 *     [&](uint64_t seq) {
 *         // CHECKPOINT handler (optional)
 *     }
 * );
 *
 * if (!result.success) {
 *     std::cout << "Recovery stopped at: " << result.error_position << std::endl;
 * }
 * ```
 *
 * Thread-safety: NOT thread-safe. Single-threaded recovery only.
 */
    class WalRecovery {
        public:
            /**
     * Recovery result statistics.
     */
            struct Result {
                bool success; ///< True if replay completed without errors
                uint64_t operations_applied; ///< Number of operations successfully applied
                uint64_t put_count; ///< Number of PUT operations
                uint64_t delete_count; ///< Number of DELETE operations
                uint64_t checkpoint_count; ///< Number of CHECKPOINT operations
                uint64_t error_position; ///< File position where error occurred (0 if success)
                WalReader::ErrorType error_type; ///< Type of error encountered
            };

            /**
     * PUT operation handler.
     *
     * @param key Key bytes
     * @param value Value bytes
     * @param seq Sequence number
     */
            using PutHandler = std::function<void(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq)>;

            /**
     * DELETE operation handler.
     *
     * @param key Key bytes
     * @param seq Sequence number
     */
            using DeleteHandler = std::function<void(std::span<const uint8_t> key, uint64_t seq)>;

            /**
     * CHECKPOINT operation handler (optional).
     *
     * @param seq Sequence number at checkpoint
     */
            using CheckpointHandler = std::function<void(uint64_t seq)>;

            /**
     * Creates a WalRecovery instance.
     */
            [[nodiscard]] static std::unique_ptr<WalRecovery> create();

            ~WalRecovery();

            /**
     * Replays a WAL file and applies operations via callbacks.
     *
     * Stops at first error (truncated/corrupted frame) and returns statistics.
     *
     * @param wal_file Path to WAL file
     * @param on_put PUT operation handler
     * @param on_delete DELETE operation handler
     * @param on_checkpoint CHECKPOINT operation handler (optional, can be nullptr)
     * @return Recovery result with statistics
     * @throws std::runtime_error if file cannot be opened
     */
            static Result replay(
                const std::filesystem::path& wal_file,
                const PutHandler& on_put,
                const DeleteHandler& on_delete,
                const CheckpointHandler& on_checkpoint = nullptr
            );

        private:
            WalRecovery() = default;
    };
} // namespace akkaradb::engine::wal