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

// internal/src/engine/wal/WalFraming.cpp
#include "engine/wal/WalFraming.hpp"
#include <stdexcept>

namespace akkaradb::engine::wal {
    // ==================== Encoding ====================

    core::OwnedBuffer WalFraming::encode(const WalOp& op) {
        const size_t payload_len = op.serialized_size();
        const size_t total_size = frame_size(payload_len);

        auto buffer = core::OwnedBuffer::allocate(total_size, 4096);
        encode_into(op, buffer.view());
        return buffer;
    }

    size_t WalFraming::encode_into(const WalOp& op, core::BufferView dst) {
        const size_t payload_len = op.serialized_size();
        const size_t total_size = frame_size(payload_len);

        if (dst.size() < total_size) { throw std::out_of_range("WalFraming::encode_into: buffer too small"); }

        size_t offset = 0;

        // Write magic
        dst.write_u32_le(offset, FRAME_MAGIC);
        offset += sizeof(uint32_t);

        // Write payloadLen
        dst.write_u32_le(offset, static_cast<uint32_t>(payload_len));
        offset += sizeof(uint32_t);

        // Write payload (serialize WalOp)
        const size_t payload_written = op.serialize_into(dst.slice(offset));
        offset += payload_written;

        // Compute CRC over [magic][payloadLen][payload]
        const size_t crc_range_len = FRAME_HEADER_SIZE + payload_len;
        const uint32_t crc = compute_frame_crc(dst, crc_range_len);

        // Write CRC
        dst.write_u32_le(offset, crc);
        offset += sizeof(uint32_t);

        return offset;
    }

    // ==================== Decoding ====================

    std::optional<WalOp> WalFraming::decode(core::BufferView src) {
        // Validate minimum size (header + footer)
        if (src.size() < FRAME_OVERHEAD) { return std::nullopt; }

        size_t offset = 0;

        // Read and validate magic
        const uint32_t magic = src.read_u32_le(offset);
        if (magic != FRAME_MAGIC) { return std::nullopt; }
        offset += sizeof(uint32_t);

        // Read payloadLen
        const uint32_t payload_len = src.read_u32_le(offset);
        offset += sizeof(uint32_t);

        // Validate total frame size
        const size_t expected_size = frame_size(payload_len);
        if (src.size() < expected_size) {
            return std::nullopt; // Incomplete frame
        }

        // Read stored CRC (at end of frame)
        const size_t crc_offset = FRAME_HEADER_SIZE + payload_len;
        const uint32_t stored_crc = src.read_u32_le(crc_offset);

        // Compute CRC over [magic][payloadLen][payload]
        const uint32_t computed_crc = compute_frame_crc(src, FRAME_HEADER_SIZE + payload_len);

        // Validate CRC
        if (stored_crc != computed_crc) {
            return std::nullopt; // Corrupted frame
        }

        // Extract payload
        const auto payload_view = src.slice(FRAME_HEADER_SIZE, payload_len);

        // Deserialize WalOp
        return WalOp::deserialize(payload_view);
    }

    bool WalFraming::validate(core::BufferView src) noexcept {
        try {
            // Check minimum size
            if (src.size() < FRAME_OVERHEAD) { return false; }

            // Read magic
            const uint32_t magic = src.read_u32_le(0);
            if (magic != FRAME_MAGIC) { return false; }

            // Read payloadLen
            const uint32_t payload_len = src.read_u32_le(sizeof(uint32_t));

            // Validate frame size
            const size_t expected_size = frame_size(payload_len);
            if (src.size() < expected_size) {
                return false; // Incomplete
            }

            // Read stored CRC
            const size_t crc_offset = FRAME_HEADER_SIZE + payload_len;
            const uint32_t stored_crc = src.read_u32_le(crc_offset);

            // Compute CRC
            const uint32_t computed_crc = compute_frame_crc(src, FRAME_HEADER_SIZE + payload_len);

            return stored_crc == computed_crc;
        }
        catch (...) { return false; }
    }

    std::optional<size_t> WalFraming::try_read_frame_length(core::BufferView src) noexcept {
        try {
            // Need at least header to read length
            if (src.size() < FRAME_HEADER_SIZE) { return std::nullopt; }

            // Read magic
            const uint32_t magic = src.read_u32_le(0);
            if (magic != FRAME_MAGIC) {
                return std::nullopt; // Invalid magic
            }

            // Read payloadLen
            const uint32_t payload_len = src.read_u32_le(sizeof(uint32_t));

            // Return total frame size
            return frame_size(payload_len);
        }
        catch (...) { return std::nullopt; }
    }

    // ==================== CRC Computation ====================

    uint32_t WalFraming::compute_frame_crc(core::BufferView frame_view, size_t total_len) noexcept {
        try { return frame_view.crc32c(0, total_len); }
        catch (...) { return 0; }
    }
} // namespace akkaradb::engine::wal