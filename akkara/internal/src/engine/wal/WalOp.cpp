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

// internal/src/engine/wal/WalOp.cpp
#include "engine/wal/WalOp.hpp"
#include <stdexcept>
#include <cstring>

#include "core/record/AKHdr32.hpp"

namespace akkaradb::engine::wal {
    // ==================== Serialization ====================

    size_t WalOp::serialized_size() const noexcept { return sizeof(core::AKHdr32) + key_.size() + value_.size(); }

    size_t WalOp::serialize_into(core::BufferView dst) const {
        if (const size_t required = serialized_size(); dst.size() < required) { throw std::out_of_range("WalOp::serialize_into: buffer too small"); }

        uint8_t flags = 0;
        if (op_type_ == OpType::DELETE) flags = core::AKHdr32::FLAG_TOMBSTONE; // 0x01
        else if (op_type_ == OpType::CHECKPOINT) flags = 0x02;

        const core::AKHdr32 header{
            .k_len = static_cast<uint16_t>(key_.size()),
            .v_len = static_cast<uint32_t>(value_.size()),
            .seq = seq_,
            .flags = flags,
            .pad0 = 0,
            .key_fp64 = core::AKHdr32::compute_key_fp64(key_.data(), key_.size()),
            .mini_key = core::AKHdr32::build_mini_key(key_.data(), key_.size())
        };

        size_t offset = 0;
        std::memcpy(dst.data() + offset, &header, sizeof(header));
        offset += sizeof(header);

        if (!key_.empty()) {
            std::memcpy(dst.data() + offset, key_.data(), key_.size());
            offset += key_.size();
        }

        if (!value_.empty()) {
            std::memcpy(dst.data() + offset, value_.data(), value_.size());
            offset += value_.size();
        }

        return offset;
    }

    core::OwnedBuffer WalOp::serialize() const {
        const size_t size = serialized_size();
        auto buffer = core::OwnedBuffer::allocate(size, 16);
        serialize_into(buffer.view());
        return buffer;
    }

    std::optional<WalOp> WalOp::deserialize(core::BufferView src) {
        try {
            if (src.size() < sizeof(core::AKHdr32)) { return std::nullopt; }

            const auto* hdr = reinterpret_cast<const core::AKHdr32*>(src.data());

            OpType op_type;
            if (hdr->flags == 0x02) op_type = OpType::CHECKPOINT;
            else if (hdr->flags & core::AKHdr32::FLAG_TOMBSTONE) op_type = OpType::DELETE;
            else op_type = OpType::PUT;

            const size_t expected = sizeof(core::AKHdr32) + hdr->k_len + hdr->v_len;
            if (src.size() < expected) { return std::nullopt; }

            constexpr size_t key_offset = sizeof(core::AKHdr32);
            const size_t val_offset = key_offset + hdr->k_len;

            std::vector key_vec(
                reinterpret_cast<const uint8_t*>(src.data()) + key_offset,
                reinterpret_cast<const uint8_t*>(src.data()) + key_offset + hdr->k_len
            );
            std::vector value_vec(
                reinterpret_cast<const uint8_t*>(src.data()) + val_offset,
                reinterpret_cast<const uint8_t*>(src.data()) + val_offset + hdr->v_len
            );

            return WalOp{op_type, hdr->seq, std::move(key_vec), std::move(value_vec)};
        }
        catch (...) { return std::nullopt; }
    }
} // namespace akkaradb::engine::wal
