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

namespace akkaradb::engine::wal {
    // ==================== Serialization ====================

    size_t WalOp::serialized_size() const noexcept {
        // [opType:u8][seq:u64][keyLen:u16][key][valueLen:u32][value]
        return sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint16_t) + key_.size() + sizeof(uint32_t) + value_.size();
    }

    size_t WalOp::serialize_into(core::BufferView dst) const {
        if (const size_t required = serialized_size(); dst.size() < required) { throw std::out_of_range("WalOp::serialize_into: buffer too small"); }

        size_t offset = 0;

        dst.write_u8(offset, static_cast<uint8_t>(op_type_));
        offset += sizeof(uint8_t);

        dst.write_u64_le(offset, seq_);
        offset += sizeof(uint64_t);

        if (key_.size() > UINT16_MAX) { throw std::invalid_argument("WalOp::serialize_into: key too large"); }
        dst.write_u16_le(offset, static_cast<uint16_t>(key_.size()));
        offset += sizeof(uint16_t);

        if (!key_.empty()) {
            std::memcpy(dst.data() + offset, key_.data(), key_.size());
            offset += key_.size();
        }

        dst.write_u32_le(offset, static_cast<uint32_t>(value_.size()));
        offset += sizeof(uint32_t);

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
            size_t offset = 0;

            if (offset + sizeof(uint8_t) > src.size()) return std::nullopt;
            const auto op_type = static_cast<OpType>(src.read_u8(offset));
            offset += sizeof(uint8_t);

            if (op_type != OpType::PUT && op_type != OpType::DELETE && op_type != OpType::CHECKPOINT) { return std::nullopt; }

            if (offset + sizeof(uint64_t) > src.size()) return std::nullopt;
            const uint64_t seq = src.read_u64_le(offset);
            offset += sizeof(uint64_t);

            if (offset + sizeof(uint16_t) > src.size()) return std::nullopt;
            const uint16_t key_len = src.read_u16_le(offset);
            offset += sizeof(uint16_t);

            if (offset + key_len > src.size()) return std::nullopt;

            const auto key_span = src.slice(offset, key_len).as_span<uint8_t>();
            offset += key_len;

            if (offset + sizeof(uint32_t) > src.size()) return std::nullopt;
            const uint32_t value_len = src.read_u32_le(offset);
            offset += sizeof(uint32_t);

            if (offset + value_len > src.size()) return std::nullopt;
            const auto value_span = src.slice(offset, value_len).as_span<uint8_t>();

            return WalOp{op_type, seq, key_span, value_span};
        }
        catch (...) { return std::nullopt; }
    }
} // namespace akkaradb::engine::wal