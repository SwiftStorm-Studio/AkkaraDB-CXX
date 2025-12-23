/*
* AkkEngine
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkEngine.
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

// internal/include/engine/wal/WalOp.hpp
#pragma once

#include "core/buffer/BufferView.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include <cstdint>
#include <span>
#include <string_view>
#include <optional>
#include <vector>

namespace akkaradb::engine::wal {
    /**
 * OpType - WAL operation types.
 *
 * Each operation represents a mutation or control event in the database.
 */
    enum class OpType : uint8_t {
        PUT = 0x01, ///< Insert or update a key-value pair
        DELETE = 0x02, ///< Delete a key (tombstone)
        CHECKPOINT = 0x03, ///< Checkpoint marker (MemTable flushed)
    };

    /**
 * WalOp - Write-Ahead Log operation.
 *
 * Represents a single operation to be logged. Operations are serialized
 * into frames and written to the WAL.
 *
 * Design principles:
 * - Immutable: Once created, cannot be modified
 * - Lightweight: Small overhead for serialization
 * - Self-describing: Contains all necessary metadata
 *
 * Binary format (variable length):
 * [opType:u8][seq:u64][keyLen:u16][key][valueLen:u32][value]
 *
 * - PUT: opType=0x01, seq, keyLen, key, valueLen, value
 * - DELETE: opType=0x02, seq, keyLen, key, valueLen=0
 * - CHECKPOINT: opType=0x03, seq, keyLen=0, valueLen=0
 *
 * Thread-safety: Immutable, fully thread-safe.
 */
    class WalOp {
    public:
        /**
     * Creates a PUT operation.
     *
     * @param key Key bytes
     * @param value Value bytes
     * @param seq Sequence number
     * @return PUT operation
     */
        [[nodiscard]] static WalOp put(
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint64_t seq
        );

        /**
     * Creates a PUT operation from strings.
     *
     * @param key Key string
     * @param value Value string
     * @param seq Sequence number
     * @return PUT operation
     */
        [[nodiscard]] static WalOp put(
            std::string_view key,
            std::string_view value,
            uint64_t seq
        );

        /**
     * Creates a DELETE operation.
     *
     * @param key Key bytes
     * @param seq Sequence number
     * @return DELETE operation
     */
        [[nodiscard]] static WalOp del(
            std::span<const uint8_t> key,
            uint64_t seq
        );

        /**
     * Creates a DELETE operation from string.
     *
     * @param key Key string
     * @param seq Sequence number
     * @return DELETE operation
     */
        [[nodiscard]] static WalOp del(
            std::string_view key,
            uint64_t seq
        );

        /**
     * Creates a CHECKPOINT operation.
     *
     * @param seq Sequence number at checkpoint
     * @return CHECKPOINT operation
     */
        [[nodiscard]] static WalOp checkpoint(uint64_t seq);

        // ==================== Accessors ====================

        /**
     * Returns the operation type.
     */
        [[nodiscard]] OpType op_type() const noexcept { return op_type_; }

        /**
     * Returns the sequence number.
     */
        [[nodiscard]] uint64_t seq() const noexcept { return seq_; }

        /**
     * Returns the key (empty for CHECKPOINT).
     */
        [[nodiscard]] std::span<const uint8_t> key() const noexcept { return {key_.data(), key_.size()}; }

        /**
     * Returns the value (empty for DELETE/CHECKPOINT).
     */
        [[nodiscard]] std::span<const uint8_t> value() const noexcept { return {value_.data(), value_.size()}; }

        /**
     * Returns the key as string_view.
     */
        [[nodiscard]] std::string_view key_string() const noexcept { return {reinterpret_cast<const char*>(key_.data()), key_.size()}; }

        /**
     * Returns the value as string_view.
     */
        [[nodiscard]] std::string_view value_string() const noexcept { return {reinterpret_cast<const char*>(value_.data()), value_.size()}; }

        /**
     * Checks if this is a PUT operation.
     */
        [[nodiscard]] bool is_put() const noexcept { return op_type_ == OpType::PUT; }

        /**
     * Checks if this is a DELETE operation.
     */
        [[nodiscard]] bool is_delete() const noexcept { return op_type_ == OpType::DELETE; }

        /**
     * Checks if this is a CHECKPOINT operation.
     */
        [[nodiscard]] bool is_checkpoint() const noexcept { return op_type_ == OpType::CHECKPOINT; }

        // ==================== Serialization ====================

        /**
     * Computes the serialized size of this operation.
     *
     * Format: [opType:u8][seq:u64][keyLen:u16][key][valueLen:u32][value]
     *
     * @return Size in bytes
     */
        [[nodiscard]] size_t serialized_size() const noexcept;

        /**
     * Serializes this operation into a buffer.
     *
     * @param dst Destination buffer (must have at least serialized_size() bytes)
     * @return Number of bytes written
     * @throws std::out_of_range if buffer is too small
     */
        size_t serialize_into(core::BufferView dst) const;

        /**
     * Serializes this operation into a new OwnedBuffer.
     *
     * @return Owned buffer containing serialized operation
     */
        [[nodiscard]] core::OwnedBuffer serialize() const;

        /**
     * Deserializes an operation from a buffer.
     *
     * @param src Source buffer
     * @return Deserialized operation, or std::nullopt if malformed
     */
        [[nodiscard]] static std::optional<WalOp> deserialize(core::BufferView src);

    private:
        WalOp(OpType op_type, uint64_t seq, std::vector<uint8_t> key, std::vector<uint8_t> value) : op_type_{op_type}, seq_{seq}, key_{std::move(key)},
                                                                                                    value_{std::move(value)} {}

        OpType op_type_;
        uint64_t seq_;
        std::vector<uint8_t> key_;
        std::vector<uint8_t> value_;
    };
} // namespace akkaradb::engine::wal