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

// internal/include/core/CRC32C.hpp
#pragma once

#include <cstdint>
#include <cstddef>

namespace akkaradb::core {
    /**
     * CRC32C (Castagnoli) computation utility.
     *
     * Consolidated implementation used across the codebase.
     * Uses software polynomial 0x82F63B78 (Castagnoli).
     *
     * Thread-safety: Fully thread-safe (stateless).
     */
    class CRC32C {
        public:
            /**
             * Computes CRC32C checksum.
             *
             * @param data Pointer to data
             * @param size Size in bytes
             * @return CRC32C checksum
             */
            [[nodiscard]] static uint32_t compute(const uint8_t* data, size_t size) noexcept;

            /**
             * Computes CRC32C checksum (convenience overload).
             *
             * @param data Pointer to data
             * @param size Size in bytes
             * @return CRC32C checksum
             */
            [[nodiscard]] static uint32_t compute(const void* data, size_t size) noexcept { return compute(static_cast<const uint8_t*>(data), size); }

            /**
             * Continues a CRC32C computation across multiple non-contiguous buffers.
             * Equivalent to compute() over the concatenation of all parts.
             *
             * Usage:
             *   uint32_t crc = CRC32C::compute(part1, len1);
             *   crc = CRC32C::append(part2, len2, crc);
             *   crc = CRC32C::append(part3, len3, crc);
             *
             * @param data     Pointer to next chunk
             * @param size     Size of next chunk (may be 0 — no-op)
             * @param prev_crc Result of the previous compute() or append() call
             * @return         New running CRC32C
             */
            [[nodiscard]] static uint32_t append(const uint8_t* data, size_t size, uint32_t prev_crc) noexcept;

            [[nodiscard]] static uint32_t append(const void* data, size_t size, uint32_t prev_crc) noexcept {
                return append(static_cast<const uint8_t*>(data), size, prev_crc);
            }
    };
} // namespace akkaradb::core