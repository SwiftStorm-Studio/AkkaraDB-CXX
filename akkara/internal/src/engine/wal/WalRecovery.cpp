/*
* AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/src/engine/wal/WalRecovery.cpp
#include "engine/wal/WalRecovery.hpp"
#include <stdexcept>

namespace akkaradb::engine::wal {
    std::unique_ptr<WalRecovery> WalRecovery::create() { return std::unique_ptr<WalRecovery>(new WalRecovery()); }

    WalRecovery::~WalRecovery() = default;

    WalRecovery::Result WalRecovery::replay(
        const std::filesystem::path& wal_file,
        const PutHandler& on_put,
        const DeleteHandler& on_delete,
        const CheckpointHandler& on_checkpoint
    ) {
        if (!on_put) { throw std::invalid_argument("WalRecovery::replay: on_put handler is required"); }

        if (!on_delete) { throw std::invalid_argument("WalRecovery::replay: on_delete handler is required"); }

        // Check if file exists
        if (!std::filesystem::exists(wal_file)) {
            return Result{
                .success = true,
                .operations_applied = 0,
                .put_count = 0,
                .delete_count = 0,
                .checkpoint_count = 0,
                .error_position = 0,
                .error_type = WalReader::ErrorType::NONE
            };
        }

        // Check if file is empty
        if (std::filesystem::file_size(wal_file) == 0) {
            return Result{
                .success = true,
                .operations_applied = 0,
                .put_count = 0,
                .delete_count = 0,
                .checkpoint_count = 0,
                .error_position = 0,
                .error_type = WalReader::ErrorType::NONE
            };
        }

        // Open WAL for reading
        auto reader = WalReader::open(wal_file);

        uint64_t operations_applied = 0;
        uint64_t put_count = 0;
        uint64_t delete_count = 0;
        uint64_t checkpoint_count = 0;

        // Replay operations
        while (auto op_opt = reader->next()) {
            const auto& op = *op_opt;

            if (op.is_put()) {
                on_put(op.key(), op.value(), op.seq());
                ++put_count;
                ++operations_applied;
            }
            else if (op.is_delete()) {
                on_delete(op.key(), op.seq());
                ++delete_count;
                ++operations_applied;
            }
            else if (op.is_checkpoint()) {
                if (on_checkpoint) { on_checkpoint(op.seq()); }
                ++checkpoint_count;
                ++operations_applied;
            }
        }

        // Check if stopped due to error
        const bool success = !reader->has_error();
        const uint64_t error_position = reader->has_error() ? reader->error_position() : 0;
        const auto error_type = reader->error_type();

        return Result{
            .success = success,
            .operations_applied = operations_applied,
            .put_count = put_count,
            .delete_count = delete_count,
            .checkpoint_count = checkpoint_count,
            .error_position = error_position,
            .error_type = error_type
        };
    }
} // namespace akkaradb::engine::wal