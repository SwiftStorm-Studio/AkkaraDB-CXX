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

// internal/include/engine/wal/WalFraming.hpp
#pragma once

#include "WalOp.hpp"
#include "core/buffer/BufferView.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include <optional>
#include <cstdint>

namespace akkaradb::engine::wal {
    /**
     * WalFrame - A single framed WAL entry.
     *
     * Frame layout (variable length):
     * [payloadLen:u32][payload][crc32c:u32]
     *
     * - payloadLen: Length of payload in bytes
     * - payload: Serialized WalOp (AKHdr32 + key + value)
     * - crc32c: CRC32C checksum over [payload] only
     *
     * Design principles:
     * - Integrity: CRC32C protects against corruption
     * - Recovery-friendly: Can detect truncated frames
     *
     * Thread-safety: Frame encoding/decoding is stateless and thread-safe.
     */
    class WalFraming {
        public:
            /**
             * Frame header size: [payloadLen:u32]
             */
            static constexpr size_t FRAME_HEADER_SIZE = sizeof(uint32_t);

            /**
             * Frame footer size: [crc32c:u32].
             */
            static constexpr size_t FRAME_FOOTER_SIZE = sizeof(uint32_t);

            /**
             * Total overhead per frame (header + footer).
             */
            static constexpr size_t FRAME_OVERHEAD = FRAME_HEADER_SIZE + FRAME_FOOTER_SIZE;

            /**
             * Computes the total frame size for a given payload length.
             *
             * @param payload_len Payload length in bytes
             * @return Total frame size (header + payload + footer)
             */
            [[nodiscard]] static constexpr size_t frame_size(size_t payload_len) noexcept { return FRAME_OVERHEAD + payload_len; }

            /**
             * Encodes a WalOp into a framed buffer.
             *
             * Layout: [magic][payloadLen][payload][crc32c]
             *
             * @param op Operation to encode
             * @return Owned buffer containing the complete frame
             */
            [[nodiscard]] static core::OwnedBuffer encode(const WalOp& op);

            /**
             * Encodes a WalOp using a thread-local staging buffer, avoiding per-call malloc.
             * The returned OwnedBuffer owns a fresh allocation copied from the TLS buffer.
             * Safe to call from multiple threads concurrently.
             *
             * @param op Operation to encode
             * @return Owned buffer containing the complete frame
             */
            [[nodiscard]] static core::OwnedBuffer encode_tls(const WalOp& op);

            /**
             * Encodes a WalOp into an existing buffer.
             *
             * @param op Operation to encode
             * @param dst Destination buffer (must have at least frame_size(op.serialized_size()) bytes)
             * @return Number of bytes written
             * @throws std::out_of_range if buffer is too small
             */
            static size_t encode_into(const WalOp& op, core::BufferView dst);

            /**
             * Decodes a frame from a buffer.
             *
             * Validates magic, CRC, and deserializes the payload.
             *
             * @param src Source buffer containing a complete frame
             * @return Decoded WalOp, or std::nullopt if invalid/corrupted
             */
            [[nodiscard]] static std::optional<WalOp> decode(core::BufferView src);

            /**
             * Validates a frame without deserializing the payload.
             *
             * Checks magic and CRC32C.
             *
             * @param src Source buffer
             * @return true if frame is valid
             */
            [[nodiscard]] static bool validate(core::BufferView src) noexcept;

            /**
             * Attempts to read the frame length from a buffer.
             *
             * This is useful for determining if a full frame is available
             * before attempting to decode.
             *
             * @param src Source buffer (must have at least FRAME_HEADER_SIZE bytes)
             * @return Total frame size (header + payload + footer), or std::nullopt if header is invalid
             */
            [[nodiscard]] static std::optional<size_t> try_read_frame_length(core::BufferView src) noexcept;
    };
} // namespace akkaradb::engine::wal