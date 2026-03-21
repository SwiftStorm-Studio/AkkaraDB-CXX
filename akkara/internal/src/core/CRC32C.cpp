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

// internal/src/core/CRC32C.cpp
#include "core/CRC32C.hpp"
#include <cstring>

// SSE4.2 hardware CRC32C:
//   GCC/Clang: -msse4.2  → defines __SSE4_2__
//   MSVC /arch:AVX+      → defines __AVX__ (implies SSE4.2 instruction set)
#if defined(__SSE4_2__) || (defined(_MSC_VER) && defined(__AVX__))
#  include <nmmintrin.h>
#  define AKK_HAS_CRC32C_HW 1
#endif

namespace akkaradb::core {
namespace {
    #ifdef AKK_HAS_CRC32C_HW
    // Processes `size` bytes with SSE4.2 hardware CRC32C starting from `crc`.
    // Returns the new running CRC (final XOR not applied).
    [[nodiscard]] inline uint32_t hw_step(uint32_t crc, const uint8_t* data, size_t size) noexcept {
        uint64_t c = crc; // _mm_crc32_u64 takes/returns u64
        const uint8_t* p = data;
        const uint8_t* end = data + size;

        // 8 bytes per instruction — the dominant loop
        for (; p + 8 <= end; p += 8) {
            uint64_t v;
            std::memcpy(&v, p, 8); // unaligned-safe load
            c = _mm_crc32_u64(c, v);
        }
        // 4-byte tail
        if (p + 4 <= end) {
            uint32_t v;
            std::memcpy(&v, p, 4);
            c = _mm_crc32_u32(static_cast<uint32_t>(c), v);
            p += 4;
        }
        // 2-byte tail
        if (p + 2 <= end) {
            uint16_t v;
            std::memcpy(&v, p, 2);
            c = _mm_crc32_u16(static_cast<uint32_t>(c), v);
            p += 2;
        }
        // 1-byte tail
        if (p < end) { c = _mm_crc32_u8(static_cast<uint32_t>(c), *p); }
        return static_cast<uint32_t>(c);
    }
    #else
    // Byte-at-a-time software fallback (Castagnoli, 0x82F63B78).
    [[nodiscard]] inline uint32_t sw_step(uint32_t crc, const uint8_t* data, size_t size) noexcept {
        for (size_t i = 0; i < size; ++i) {
            crc ^= static_cast<uint32_t>(data[i]);
            for (int j = 0; j < 8; ++j) {
                constexpr uint32_t POLY = 0x82F63B78u;
                crc = (crc >> 1) ^ ((crc & 1u) ? POLY : 0u);
            }
        }
        return crc;
    }
    #endif
} // namespace

uint32_t CRC32C::compute(const uint8_t* data, size_t size) noexcept {
    #ifdef AKK_HAS_CRC32C_HW
    return ~hw_step(0xFFFFFFFFu, data, size);
    #else
    return ~sw_step(0xFFFFFFFFu, data, size);
    #endif
}

    uint32_t CRC32C::append(const uint8_t* data, size_t size, uint32_t prev_crc) noexcept {
        if (size == 0) [[unlikely]] return prev_crc;
        #ifdef AKK_HAS_CRC32C_HW
        return ~hw_step(~prev_crc, data, size);
        #else
        return ~sw_step(~prev_crc, data, size);
        #endif
    }
} // namespace akkaradb::core
