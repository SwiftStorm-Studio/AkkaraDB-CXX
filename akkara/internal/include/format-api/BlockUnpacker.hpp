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

// internal/include/format-api/BlockUnpacker.hpp
#pragma once

#include "RecordCursor.hpp"
#include "core/buffer/BufferView.hpp"
#include "core/record/RecordView.hpp"
#include <memory>
#include <vector>

namespace akkaradb::format {
    /**
     * BlockUnpacker - Abstract interface for unpacking records from blocks.
     *
     * A block unpacker validates and extracts records from a fixed-size block.
     * It provides two access patterns:
     * - Cursor-based: Forward-only iteration (zero-copy)
     * - Vector-based: Materialize all records into a vector
     *
     * Design principles:
     * - Validation: Checks block integrity (CRC, bounds)
     * - Zero-copy: Cursor returns views, no data duplication
     * - Stateless: Each operation is independent
     * - Abstract interface: Allows different unpacking strategies
     *
     * Typical usage:
     * ```cpp
     * auto unpacker = AkkBlockUnpacker::create();
     *
     * // Option 1: Cursor (zero-copy iteration)
     * auto cursor = unpacker->cursor(block_view);
     * while (cursor->has_next()) {
     *     auto record = cursor->next();
     *     // Process record...
     * }
     *
     * // Option 2: Materialize to vector
     * std::vector<RecordView> records;
     * unpacker->unpack_into(block_view, records);
     * ```
     *
     * Thread-safety: Stateless, so thread-safe if called with different buffers.
     * Cursors are NOT thread-safe.
     */
    class BlockUnpacker {
    public:
        virtual ~BlockUnpacker() = default;

        /**
         * Creates a forward-only cursor over records in a block.
         *
         * The cursor is valid until the underlying buffer is released.
         * Validates block integrity (CRC, bounds) before returning.
         *
         * @param block Block buffer (must be valid during cursor lifetime)
         * @return Unique pointer to RecordCursor
         * @throws std::runtime_error if CRC validation fails
         * @throws std::out_of_range if block is malformed
         */
        [[nodiscard]] virtual std::unique_ptr<RecordCursor> cursor(core::BufferView block) const = 0;

        /**
         * Unpacks all records from a block into a vector.
         *
         * Records are appended to the provided vector.
         *
         * @param block Block buffer
         * @param out Output vector (appended to)
         * @throws std::runtime_error if CRC validation fails
         * @throws std::out_of_range if block is malformed
         */
        virtual void unpack_into(core::BufferView block, std::vector<core::RecordView>& out) const = 0;

        /**
         * Unpacks all records and returns a new vector.
         *
         * @param block Block buffer
         * @return Vector of RecordViews
         * @throws std::runtime_error if CRC validation fails
         * @throws std::out_of_range if block is malformed
         */
        [[nodiscard]] std::vector<core::RecordView> unpack(core::BufferView block) const {
            std::vector<core::RecordView> records;
            unpack_into(block, records);
            return records;
        }

        /**
         * Validates a block without unpacking.
         *
         * Checks CRC and basic structure.
         *
         * @param block Block buffer
         * @return true if valid, false otherwise
         */
        [[nodiscard]] virtual bool validate(core::BufferView block) const noexcept = 0;

        /**
         * Returns the block size this unpacker expects.
         */
        [[nodiscard]] virtual size_t block_size() const noexcept = 0;
    };
} // namespace akkaradb::format