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

// internal/include/cpu/crc32c/CRC32CX86SSE42.cpp
#if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <nmmintrin.h> // _mm_crc32_*

namespace akkaradb::cpu {
    uint32_t CRC32C_X86_SSE42(const std::byte* data, size_t length) noexcept {
        uint64_t crc = 0xFFFFFFFFu;

        const uint8_t* p = reinterpret_cast<const uint8_t*>(data);

        // 8-byte chunks
        while (length >= 8) {
            uint64_t chunk;
            std::memcpy(&chunk, p, sizeof(chunk));
            crc = _mm_crc32_u64(crc, chunk);
            p += 8;
            length -= 8;
        }

        // remaining 4 bytes
        if (length >= 4) {
            uint32_t chunk;
            std::memcpy(&chunk, p, sizeof(chunk));
            crc = _mm_crc32_u32(static_cast<uint32_t>(crc), chunk);
            p += 4;
            length -= 4;
        }

        // remaining bytes
        while (length > 0) {
            crc = _mm_crc32_u8(static_cast<uint32_t>(crc), *p);
            ++p;
            --length;
        }

        return static_cast<uint32_t>(~crc);
    }
} // namespace akkaradb::cpu

#endif
