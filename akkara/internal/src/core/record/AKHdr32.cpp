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

// internal/src/core/record/AKHdr32.cpp
#include "core/record/AKHdr32.hpp"
#include <cstring>
#include <algorithm>

namespace akkaradb::core {
    namespace {
        /**
 * SipHash-2-4 implementation.
 *
 * Reference: https://131002.net/siphash/
 * Seed: 0x5AD6DCD676D23C25 (default AkkaraDB seed)
 */
        class SipHash24 {
        public:
            explicit SipHash24(uint64_t seed = 0x5AD6DCD676D23C25) noexcept {
                v0_ = 0x736f6d6570736575ULL ^ seed;
                v1_ = 0x646f72616e646f6dULL ^ seed;
                v2_ = 0x6c7967656e657261ULL ^ seed;
                v3_ = 0x7465646279746573ULL ^ seed;
            }

            void update(const uint8_t* data, size_t len) noexcept {
                const auto* ptr = data;
                const auto* end = data + len;

                // Process 8-byte chunks
                while (ptr + 8 <= end) {
                    uint64_t m;
                    std::memcpy(&m, ptr, 8);
                    compress(m);
                    ptr += 8;
                }

                // Handle remaining bytes (0..7)
                tail_len_ = end - ptr;
                std::memcpy(tail_.data(), ptr, tail_len_);
            }

            [[nodiscard]] uint64_t finalize(size_t total_len) noexcept {
                // Pad tail to 8 bytes with total length in last byte
                uint64_t b = static_cast<uint64_t>(total_len) << 56;
                for (size_t i = 0; i < tail_len_; ++i) { b |= static_cast<uint64_t>(tail_[i]) << (i * 8); }
                compress(b);

                v2_ ^= 0xff;
                for (int i = 0; i < 4; ++i) { round(); }

                return v0_ ^ v1_ ^ v2_ ^ v3_;
            }

        private:
            void compress(uint64_t m) noexcept {
                v3_ ^= m;
                for (int i = 0; i < 2; ++i) { round(); }
                v0_ ^= m;
            }

            void round() noexcept {
                v0_ += v1_;
                v1_ = rotl(v1_, 13);
                v1_ ^= v0_;
                v0_ = rotl(v0_, 32);

                v2_ += v3_;
                v3_ = rotl(v3_, 16);
                v3_ ^= v2_;

                v0_ += v3_;
                v3_ = rotl(v3_, 21);
                v3_ ^= v0_;

                v2_ += v1_;
                v1_ = rotl(v1_, 17);
                v1_ ^= v2_;
                v2_ = rotl(v2_, 32);
            }

            static constexpr uint64_t rotl(uint64_t x, int b) noexcept { return (x << b) | (x >> (64 - b)); }

            uint64_t v0_, v1_, v2_, v3_;
            std::array<uint8_t, 8> tail_{};
            size_t tail_len_{0};
        };
    } // anonymous namespace

    // ==================== AKHdr32 Implementation ====================

    uint64_t AKHdr32::compute_key_fp64(const uint8_t* key, size_t key_len) noexcept {
        SipHash24 hasher;
        hasher.update(key, key_len);
        return hasher.finalize(key_len);
    }

    uint64_t AKHdr32::build_mini_key(const uint8_t* key, size_t key_len) noexcept {
        uint64_t mini = 0;
        const size_t copy_len = std::min<size_t>(key_len, 8);
        std::memcpy(&mini, key, copy_len);
        return mini;
    }

    AKHdr32 AKHdr32::create(
        const uint8_t* key,
        size_t key_len,
        size_t value_len,
        uint64_t seq,
        uint8_t flags
    ) noexcept {
        return AKHdr32{
            .k_len = static_cast<uint16_t>(key_len),
            .v_len = static_cast<uint32_t>(value_len),
            .seq = seq,
            .flags = flags,
            .pad0 = 0,
            .key_fp64 = compute_key_fp64(key, key_len),
            .mini_key = build_mini_key(key, key_len)
        };
    }
} // namespace akkaradb::core