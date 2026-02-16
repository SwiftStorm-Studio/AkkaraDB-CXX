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
#include <vector>
#include <cstring>

namespace akkaradb::engine::wal {
    // ==================== Encoding ====================

    core::OwnedBuffer WalFraming::encode(const WalOp& op) {
        const size_t payload_len = op.serialized_size();
        const size_t total_size = frame_size(payload_len);

        auto buffer = core::OwnedBuffer::allocate(total_size, 16);
        encode_into(op, buffer.view());
        return buffer;
    }

    core::OwnedBuffer WalFraming::encode_tls(const WalOp& op) {
        // Thread-local staging buffer: grows to fit, never shrinks.
        // Eliminates per-call malloc on the hot write path.
        thread_local std::vector<uint8_t> tls_buf;

        const size_t payload_len = op.serialized_size();
        const size_t total_size = frame_size(payload_len);

        if (tls_buf.size() < total_size) { tls_buf.resize(total_size); }

        core::BufferView view{reinterpret_cast<std::byte*>(tls_buf.data()), total_size};
        encode_into(op, view);

        // Copy into an owned allocation for the WAL queue.
        auto owned = core::OwnedBuffer::allocate(total_size, 16);
        std::memcpy(owned.view().data(), reinterpret_cast<const std::byte*>(tls_buf.data()), total_size);
        return owned;
    }

    size_t WalFraming::encode_into(const WalOp& op, core::BufferView dst) {
        const size_t payload_len = op.serialized_size();
        const size_t total_size = frame_size(payload_len);

        if (dst.size() < total_size) { throw std::out_of_range("WalFraming::encode_into: buffer too small"); }

        dst.write_u32_le(0, static_cast<uint32_t>(payload_len));

        op.serialize_into(dst.slice(sizeof(uint32_t)));

        const uint32_t crc = dst.crc32c(sizeof(uint32_t), payload_len);
        dst.write_u32_le(sizeof(uint32_t) + payload_len, crc);

        return total_size;
    }

    // ==================== Decoding ====================

    std::optional<WalOp> WalFraming::decode(core::BufferView src) {
        if (src.size() < FRAME_OVERHEAD) { return std::nullopt; }

        const uint32_t payload_len = src.read_u32_le(0);
        if (src.size() < frame_size(payload_len)) { return std::nullopt; }

        const uint32_t stored_crc = src.read_u32_le(sizeof(uint32_t) + payload_len);
        const uint32_t computed_crc = src.crc32c(sizeof(uint32_t), payload_len);
        if (stored_crc != computed_crc) { return std::nullopt; }

        return WalOp::deserialize(src.slice(sizeof(uint32_t), payload_len));
    }

    bool WalFraming::validate(core::BufferView src) noexcept {
        try {
            if (src.size() < FRAME_OVERHEAD) { return false; }

            const uint32_t payload_len = src.read_u32_le(0);
            if (src.size() < frame_size(payload_len)) { return false; }

            const uint32_t stored_crc = src.read_u32_le(sizeof(uint32_t) + payload_len);
            const uint32_t computed_crc = src.crc32c(sizeof(uint32_t), payload_len);
            return stored_crc == computed_crc;
        } catch (...) { return false; }
    }

    std::optional<size_t> WalFraming::try_read_frame_length(core::BufferView src) noexcept {
        try {
            if (src.size() < FRAME_HEADER_SIZE) { return std::nullopt; }
            const uint32_t payload_len = src.read_u32_le(0);
            return frame_size(payload_len);
        } catch (...) { return std::nullopt; }
    }
} // namespace akkaradb::engine::wal
