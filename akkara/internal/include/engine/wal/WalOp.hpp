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

// internal/include/engine/wal/WalOp.hpp
#pragma once

#include "core/buffer/BufferView.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include <cstdint>
#include <span>
#include <string_view>
#include <optional>

namespace akkaradb::engine::wal {
    enum class OpType : uint8_t {
        PUT = 0x01, DELETE = 0x02, CHECKPOINT = 0x03,
    };

    class WalOp {
        public:
            [[nodiscard]] static WalOp put(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq) noexcept {
                return WalOp{OpType::PUT, seq, key, value};
            }

            [[nodiscard]] static WalOp put(std::string_view key, std::string_view value, uint64_t seq) noexcept {
                return WalOp{
                    OpType::PUT,
                    seq,
                    std::span{reinterpret_cast<const uint8_t*>(key.data()), key.size()},
                    std::span{reinterpret_cast<const uint8_t*>(value.data()), value.size()}
                };
            }

            [[nodiscard]] static WalOp del(std::span<const uint8_t> key, uint64_t seq) noexcept { return WalOp{OpType::DELETE, seq, key, {}}; }

            [[nodiscard]] static WalOp del(std::string_view key, uint64_t seq) noexcept {
                return WalOp{OpType::DELETE, seq, std::span{reinterpret_cast<const uint8_t*>(key.data()), key.size()}, {}};
            }

            [[nodiscard]] static WalOp checkpoint(uint64_t seq) noexcept { return WalOp{OpType::CHECKPOINT, seq, {}, {}}; }

            // アクセサ（変更なし）
            [[nodiscard]] OpType op_type() const noexcept { return op_type_; }
            [[nodiscard]] uint64_t seq() const noexcept { return seq_; }

            [[nodiscard]] std::span<const uint8_t> key() const noexcept { return key_; }
            [[nodiscard]] std::span<const uint8_t> value() const noexcept { return value_; }

            [[nodiscard]] std::string_view key_string() const noexcept { return {reinterpret_cast<const char*>(key_.data()), key_.size()}; }
            [[nodiscard]] std::string_view value_string() const noexcept { return {reinterpret_cast<const char*>(value_.data()), value_.size()}; }

            [[nodiscard]] bool is_put() const noexcept { return op_type_ == OpType::PUT; }
            [[nodiscard]] bool is_delete() const noexcept { return op_type_ == OpType::DELETE; }
            [[nodiscard]] bool is_checkpoint() const noexcept { return op_type_ == OpType::CHECKPOINT; }

            [[nodiscard]] size_t serialized_size() const noexcept;
            size_t serialize_into(core::BufferView dst) const;
            [[nodiscard]] core::OwnedBuffer serialize() const;
            [[nodiscard]] static std::optional<WalOp> deserialize(core::BufferView src);

        private:
            WalOp(OpType op_type, uint64_t seq, std::span<const uint8_t> key, std::span<const uint8_t> value) noexcept
                : op_type_{op_type}, seq_{seq}, key_{key}, value_{value} {}

            OpType op_type_;
            uint64_t seq_;
            std::span<const uint8_t> key_;
            std::span<const uint8_t> value_;
    };
} // namespace akkaradb::engine::wal