/*
 * AkkaraDB - Low-latency, crash-safe JVM KV store with WAL & stripe parity
 * Copyright (C) 2026 RiriFa
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

namespace akkaradb::core {
    uint32_t CRC32C::compute(const uint8_t* data, size_t size) noexcept {
        auto crc = 0xFFFFFFFF;

        for (size_t i = 0; i < size; ++i) {
            crc ^= static_cast<uint32_t>(data[i]);
            for (auto j = 0; j < 8; ++j) {
                constexpr auto POLY = 0x82F63B78;
                const uint32_t mask = (crc & 1)
                                          ? 0xFFFFFFFF
                                          : 0;
                crc = (crc >> 1) ^ (POLY & mask);
            }
        }

        return ~crc;
    }
} // namespace akkaradb::core