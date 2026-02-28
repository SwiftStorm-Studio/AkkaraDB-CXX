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
    };
} // namespace akkaradb::core