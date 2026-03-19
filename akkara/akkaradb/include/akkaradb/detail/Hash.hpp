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

// akkaradb/detail/Hash.hpp
#pragma once

#include <cstdint>
#include <string_view>

namespace akkaradb::detail {

    /**
     * FNV-1a 64-bit hash of a string.
     *
     * Used to derive the 8-byte namespace prefix for PackedTable keys.
     * Two different table names will almost certainly produce different prefixes,
     * providing namespace isolation without any runtime bookkeeping.
     *
     * Output is deterministic and platform-independent (constant-evaluated).
     */
    [[nodiscard]] constexpr uint64_t fnv1a_64(std::string_view s) noexcept {
        uint64_t h = 14695981039346656037ULL; // FNV offset basis
        for (const unsigned char c : s) {
            h ^= static_cast<uint64_t>(c);
            h *= 1099511628211ULL; // FNV prime
        }
        return h;
    }

    /**
     * Writes a uint64_t as 8 big-endian bytes into dst.
     * dst must have at least 8 bytes of space.
     */
    inline void write_be64(uint64_t v, uint8_t* dst) noexcept {
        dst[0] = static_cast<uint8_t>(v >> 56);
        dst[1] = static_cast<uint8_t>(v >> 48);
        dst[2] = static_cast<uint8_t>(v >> 40);
        dst[3] = static_cast<uint8_t>(v >> 32);
        dst[4] = static_cast<uint8_t>(v >> 24);
        dst[5] = static_cast<uint8_t>(v >> 16);
        dst[6] = static_cast<uint8_t>(v >>  8);
        dst[7] = static_cast<uint8_t>(v >>  0);
    }

    /**
     * Reads 8 big-endian bytes from src into a uint64_t.
     */
    [[nodiscard]] inline uint64_t read_be64(const uint8_t* src) noexcept {
        return (static_cast<uint64_t>(src[0]) << 56)
             | (static_cast<uint64_t>(src[1]) << 48)
             | (static_cast<uint64_t>(src[2]) << 40)
             | (static_cast<uint64_t>(src[3]) << 32)
             | (static_cast<uint64_t>(src[4]) << 24)
             | (static_cast<uint64_t>(src[5]) << 16)
             | (static_cast<uint64_t>(src[6]) <<  8)
             | (static_cast<uint64_t>(src[7]) <<  0);
    }

    /**
     * Increments an 8-byte big-endian value by 1 (for namespace end-key computation).
     * Returns false on overflow (all bytes 0xFF — astronomically unlikely for hash values).
     */
    [[nodiscard]] inline bool increment_be64(uint8_t* buf) noexcept {
        for (int i = 7; i >= 0; --i) {
            if (++buf[i] != 0) return true; // no carry
        }
        return false; // overflow
    }

} // namespace akkaradb::detail
