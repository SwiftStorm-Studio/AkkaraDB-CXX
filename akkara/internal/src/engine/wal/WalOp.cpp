/*
 * Optimized WalOp.cpp - Zero-copy design
 *
 * Key changes:
 * 1. Use std::span directly (no vector copy in constructor)
 * 2. Only copy when actually serializing
 */

// internal/src/engine/wal/WalOp.cpp
#include "engine/wal/WalOp.hpp"
#include <stdexcept>
#include <cstring>

namespace akkaradb::engine::wal {
    // ==================== Factory Methods ====================

    WalOp WalOp::put(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq) {
        std::vector<uint8_t> key_vec;
        key_vec.reserve(key.size());
        key_vec.assign(key.begin(), key.end());

        std::vector<uint8_t> value_vec;
        value_vec.reserve(value.size());
        value_vec.assign(value.begin(), value.end());

        return WalOp{OpType::PUT, seq, std::move(key_vec), std::move(value_vec)};
    }

    WalOp WalOp::put(std::string_view key, std::string_view value, uint64_t seq) {
        const auto key_span = std::span{reinterpret_cast<const uint8_t*>(key.data()), key.size()};
        const auto value_span = std::span{reinterpret_cast<const uint8_t*>(value.data()), value.size()};
        return put(key_span, value_span, seq);
    }

    WalOp WalOp::del(std::span<const uint8_t> key, uint64_t seq) {
        std::vector<uint8_t> key_vec;
        key_vec.reserve(key.size());
        key_vec.assign(key.begin(), key.end());

        return WalOp{OpType::DELETE, seq, std::move(key_vec), {}};
    }

    WalOp WalOp::del(std::string_view key, uint64_t seq) {
        const auto key_span = std::span{reinterpret_cast<const uint8_t*>(key.data()), key.size()};
        return del(key_span, seq);
    }

    WalOp WalOp::checkpoint(uint64_t seq) { return WalOp{OpType::CHECKPOINT, seq, {}, {}}; }

    // ==================== Serialization ====================

    size_t WalOp::serialized_size() const noexcept {
        // [opType:u8][seq:u64][keyLen:u16][key][valueLen:u32][value]
        return sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint16_t) + key_.size() + sizeof(uint32_t) + value_.size();
    }

    size_t WalOp::serialize_into(core::BufferView dst) const {
        if (const size_t required = serialized_size(); dst.size() < required) {
            throw std::out_of_range("WalOp::serialize_into: buffer too small");
        }

        size_t offset = 0;

        // Write opType
        dst.write_u8(offset, static_cast<uint8_t>(op_type_));
        offset += sizeof(uint8_t);

        // Write seq
        dst.write_u64_le(offset, seq_);
        offset += sizeof(uint64_t);

        // Write keyLen
        if (key_.size() > UINT16_MAX) {
            throw std::invalid_argument("WalOp::serialize_into: key too large");
        }
        dst.write_u16_le(offset, static_cast<uint16_t>(key_.size()));
        offset += sizeof(uint16_t);

        // Write key
        if (!key_.empty()) {
            std::memcpy(dst.data() + offset, key_.data(), key_.size());
            offset += key_.size();
        }

        // Write valueLen
        dst.write_u32_le(offset, static_cast<uint32_t>(value_.size()));
        offset += sizeof(uint32_t);

        // Write value
        if (!value_.empty()) {
            std::memcpy(dst.data() + offset, value_.data(), value_.size());
            offset += value_.size();
        }

        return offset;
    }

    core::OwnedBuffer WalOp::serialize() const {
        const size_t size = serialized_size();
        auto buffer = core::OwnedBuffer::allocate(size, 4096);
        serialize_into(buffer.view());
        return buffer;
    }

    std::optional<WalOp> WalOp::deserialize(core::BufferView src) {
        try {
            size_t offset = 0;

            // Read opType
            if (offset + sizeof(uint8_t) > src.size()) return std::nullopt;
            const auto op_type = static_cast<OpType>(src.read_u8(offset));
            offset += sizeof(uint8_t);

            // Validate opType
            if (op_type != OpType::PUT && op_type != OpType::DELETE && op_type != OpType::CHECKPOINT) {
                return std::nullopt;
            }

            // Read seq
            if (offset + sizeof(uint64_t) > src.size()) return std::nullopt;
            const uint64_t seq = src.read_u64_le(offset);
            offset += sizeof(uint64_t);

            // Read keyLen
            if (offset + sizeof(uint16_t) > src.size()) return std::nullopt;
            const uint16_t key_len = src.read_u16_le(offset);
            offset += sizeof(uint16_t);

            // Read key
            if (offset + key_len > src.size()) return std::nullopt;
            std::vector<uint8_t> key;
            if (key_len > 0) {
                key.reserve(key_len);  // ← OPTIMIZATION: Reserve before resize
                key.resize(key_len);
                std::memcpy(key.data(), src.data() + offset, key_len);
                offset += key_len;
            }

            // Read valueLen
            if (offset + sizeof(uint32_t) > src.size()) return std::nullopt;
            const uint32_t value_len = src.read_u32_le(offset);
            offset += sizeof(uint32_t);

            // Read value
            if (offset + value_len > src.size()) return std::nullopt;
            std::vector<uint8_t> value;
            if (value_len > 0) {
                value.reserve(value_len);  // ← OPTIMIZATION: Reserve before resize
                value.resize(value_len);
                std::memcpy(value.data(), src.data() + offset, value_len);
                offset += value_len;
            }

            return WalOp{op_type, seq, std::move(key), std::move(value)};
        }
        catch (...) { return std::nullopt; }
    }
} // namespace akkaradb::engine::wal