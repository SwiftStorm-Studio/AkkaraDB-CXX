/*
* AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/include/core/record/MemRecord.hpp
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

#include "MemHdr16.hpp"
#include "core/buffer/BufferArena.hpp"
#include "core/memory/SmallBuffer.hpp"

namespace akkaradb::core {

    /**
     * MemRecord — 64B fixed-size owning in-memory record
     *
     * Layout (exactly 64 bytes):
     *
     *   [0..15]  MemHdr16     hdr
     *   [16..23] uint64_t     key_fp64
     *   [24..31] uint64_t     mini_key
     *   [32..63] SmallBuffer  data
     *
     * Design goals:
     *   - Single cache-line footprint (64B)
     *   - Zero heap allocation for small records
     *   - Fast key comparison (mini_key fast path)
     *
     * Notes:
     *   - data = [key][value]
     *   - NOT thread-safe
     */
    struct MemRecord {
        MemHdr16 hdr{};
        uint64_t key_fp64{};
        uint64_t mini_key{};
        SmallBuffer data;

        // ==================== Factory (implemented in .cpp) ====================

        [[nodiscard]] static MemRecord create(
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint64_t seq,
            uint8_t flags,
            BufferArena& arena,
            uint64_t fp64 = 0,
            uint64_t mk = 0
        );

        [[nodiscard]] static MemRecord create(
            std::string_view key,
            std::string_view value,
            uint64_t seq,
            uint8_t flags,
            BufferArena& arena
        );

        [[nodiscard]] static MemRecord tombstone(
            std::span<const uint8_t> key,
            uint64_t seq,
            BufferArena& arena,
            uint64_t fp64 = 0
        );

        // ==================== Accessors (hot path) ====================

        [[nodiscard]] std::span<const uint8_t> key() const noexcept {
            return {data.data(), hdr.k_len};
        }

        [[nodiscard]] std::span<const uint8_t> value() const noexcept {
            return {data.data() + hdr.k_len, hdr.v_len};
        }

        [[nodiscard]] std::string_view key_string() const noexcept {
            return {
                reinterpret_cast<const char*>(data.data()),
                hdr.k_len
            };
        }

        [[nodiscard]] std::string_view value_string() const noexcept {
            return {
                reinterpret_cast<const char*>(data.data() + hdr.k_len),
                hdr.v_len
            };
        }

        [[nodiscard]] uint64_t seq() const noexcept {
            return hdr.seq;
        }

        [[nodiscard]] bool is_tombstone() const noexcept {
            return hdr.is_tombstone();
        }

        // ==================== Comparison (hot path) ====================

        [[nodiscard]] int compare_key(const MemRecord& other) const noexcept {
            const size_t prefix = std::min<size_t>(
                std::min(hdr.k_len, other.hdr.k_len), 8u
            );

            if (prefix > 0) {
                if (int c = std::memcmp(&mini_key, &other.mini_key, prefix); c != 0)
                    return c < 0 ? -1 : 1;
            }

            const size_t min_len = std::min(hdr.k_len, other.hdr.k_len);

            if (min_len > 8) {
                if (int c = std::memcmp(
                        data.data() + 8,
                        other.data.data() + 8,
                        min_len - 8
                    ); c != 0)
                    return c < 0 ? -1 : 1;
            }

            if (hdr.k_len < other.hdr.k_len) return -1;
            if (hdr.k_len > other.hdr.k_len) return 1;
            return 0;
        }

        [[nodiscard]] int compare_key(std::span<const uint8_t> other) const noexcept {
            const size_t min_len = std::min<size_t>(hdr.k_len, other.size());
            const size_t prefix = std::min<size_t>(min_len, 8u);

            if (prefix > 0) {
                if (int c = std::memcmp(&mini_key, other.data(), prefix); c != 0)
                    return c < 0 ? -1 : 1;
            }

            if (min_len > 8) {
                if (int c = std::memcmp(
                        data.data() + 8,
                        other.data() + 8,
                        min_len - 8
                    ); c != 0)
                    return c < 0 ? -1 : 1;
            }

            if (hdr.k_len < other.size()) return -1;
            if (hdr.k_len > other.size()) return 1;
            return 0;
        }

        [[nodiscard]] bool key_equals(const MemRecord& other) const noexcept {
            if (hdr.k_len != other.hdr.k_len) return false;
            if (hdr.k_len == 0) return true;

            if (mini_key != other.mini_key) return false;
            if (hdr.k_len <= 8) return true;

            return std::memcmp(
                data.data() + 8,
                other.data.data() + 8,
                hdr.k_len - 8
            ) == 0;
        }

        [[nodiscard]] bool operator<(const MemRecord& o) const noexcept {
            return compare_key(o) < 0;
        }

        [[nodiscard]] bool operator==(const MemRecord& o) const noexcept {
            return key_equals(o);
        }
    };

    static_assert(sizeof(MemRecord) == 64, "MemRecord must be exactly 64 bytes");

} // namespace akkaradb::core