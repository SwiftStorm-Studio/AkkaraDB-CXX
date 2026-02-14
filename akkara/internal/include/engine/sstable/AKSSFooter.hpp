/*
* AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkEngine is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkEngine is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkEngine.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/include/engine/sstable/AKSSFooter.hpp
#pragma once

#include "core/buffer/BufferView.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include <cstdint>
#include <filesystem>

namespace akkaradb::engine::sstable {
    /**
 * AKSSFooter - SSTable footer (32 bytes, Little-Endian).
 *
 * Layout:
 *   [0..3]   magic      u32  0x414B5353 ('AKSS')
 *   [4]      version    u8   1
 *   [5..7]   padding    u24  0
 *   [8..15]  indexOff   u64  Index Block offset
 *   [16..23] bloomOff   u64  Bloom Filter offset (0 if none)
 *   [24..27] entries    u32  Total record count
 *   [28..31] crc32c     u32  CRC32C over [0..fileSize-4), or 0
 *
 * Thread-safety: All functions are thread-safe (no internal state).
 */
    class AKSSFooter {
        public:
            static constexpr size_t SIZE = 32;
            static constexpr uint32_t MAGIC = 0x414B5353; // 'AKSS'
            static constexpr uint8_t VERSION = 1;

            /**
     * Footer data.
     */
            struct Footer {
                uint64_t index_off; ///< Index Block offset
                uint64_t bloom_off; ///< Bloom Filter offset (0 if none)
                uint32_t entries; ///< Total record count
                uint8_t version; ///< Format version

                Footer(uint64_t idx_off, uint64_t blm_off, uint32_t cnt, uint8_t ver = VERSION)
                    : index_off{idx_off}, bloom_off{blm_off}, entries{cnt}, version{ver} {}
            };

            /**
     * Writes footer to buffer.
     *
     * @param buffer Output buffer (at least 32 bytes)
     * @param footer Footer data
     * @param crc32c File-level CRC (0 to omit)
     * @throws std::invalid_argument if buffer too small
     */
            static void write_to(core::BufferView buffer, const Footer& footer, uint32_t crc32c = 0);

            /**
     * Writes footer to owned buffer.
     *
     * Creates a new 32-byte buffer and writes footer.
     *
     * @param footer Footer data
     * @param crc32c File-level CRC (0 to omit)
     * @return Owned buffer containing footer
     */
            [[nodiscard]] static core::OwnedBuffer write_to_buffer(const Footer& footer, uint32_t crc32c = 0);

            /**
     * Reads footer from buffer.
     *
     * @param buffer Input buffer (at least 32 bytes)
     * @return Footer data
     * @throws std::runtime_error if magic/version invalid
     */
            [[nodiscard]] static Footer read_from(core::BufferView buffer);

            /**
     * Reads footer with optional CRC verification.
     *
     * If verify_crc is true and stored CRC != 0, computes CRC32C
     * over file [0..file_size-4) and validates.
     *
     * @param buffer Footer buffer (32 bytes)
     * @param file_path Path to SSTable file
     * @param verify_crc If true, verify file-level CRC
     * @return Footer data
     * @throws std::runtime_error if CRC mismatch or invalid format
     */
            [[nodiscard]] static Footer read_from_file(core::BufferView buffer, const std::filesystem::path& file_path, bool verify_crc = false);

        private:
            static uint32_t compute_file_crc(const std::filesystem::path& file_path, uint64_t file_size);
    };
} // namespace akkaradb::engine::sstable