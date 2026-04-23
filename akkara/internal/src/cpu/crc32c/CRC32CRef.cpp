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

// internal/include/cpu/crc32c/CRC32CRef.cpp
#include <array>

namespace akkaradb::cpu {
    namespace {
        constexpr uint32_t Polynomial = 0x82F63B78u;

        constexpr std::array<uint32_t, 256> GenerateTable() {
            std::array<uint32_t, 256> table{};

            for (uint32_t i = 0; i < 256; ++i) {
                uint32_t crc = i;
                for (int j = 0; j < 8; ++j) { crc = (crc & 1) ? (crc >> 1) ^ Polynomial : (crc >> 1); }
                table[i] = crc;
            }

            return table;
        }

        constinit const auto Table = GenerateTable();
    } // namespace

    // fallback implementation
    uint32_t CRC32C_Ref(const std::byte* data, size_t length) noexcept {
        uint32_t crc = 0xFFFFFFFFu;

        const auto* p = reinterpret_cast<const uint8_t*>(data);

        for (size_t i = 0; i < length; ++i) {
            uint8_t idx = static_cast<uint8_t>(crc ^ p[i]);
            crc = (crc >> 8) ^ Table[idx];
        }

        return ~crc;
    }
} // namespace akkaradb::cpu
