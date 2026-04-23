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

// internal/include/cpu/CRC32C.hpp
#pragma once

#include <cstddef>
#include <cstdint>

namespace akkaradb::cpu {
    /**
     * @brief Compute CRC32C (Castagnoli) checksum.
     *
     * This function selects the fastest available implementation at runtime
     * based on CPU capabilities (e.g., SSE4.2 on x86, CRC instructions on ARM).
     *
     * @param data   Pointer to input data
     * @param length Size of input data in bytes
     * @return CRC32C checksum
     */
    [[nodiscard]] uint32_t CRC32C(const std::byte* data, size_t length) noexcept;
}
