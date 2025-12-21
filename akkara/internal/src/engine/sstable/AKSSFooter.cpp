/*
* AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/src/engine/sstable/AKSSFooter.cpp
#include "engine/sstable/AKSSFooter.hpp"
#include <stdexcept>
#include <fstream>
#include <algorithm>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#endif

namespace akkaradb::engine::sstable {
    namespace {
        /**
 * CRC32C computation (Castagnoli polynomial).
 */
        uint32_t crc32c(const uint8_t* data, size_t size) {
            uint32_t crc = 0xFFFFFFFF;
            for (size_t i = 0; i < size; ++i) {
                crc ^= data[i];
                for (int j = 0; j < 8; ++j) { crc = (crc >> 1) ^ (0x82F63B78 & -(crc & 1)); }
            }
            return ~crc;
        }
    } // anonymous namespace

    void AKSSFooter::write_to(
        core::BufferView buffer,
        const Footer& footer,
        uint32_t crc32c_value
    ) {
        if (buffer.size() < SIZE) { throw std::invalid_argument("AKSSFooter::write_to: buffer too small"); }

        // std::byte* → uint8_t* キャスト
        auto* data = reinterpret_cast<uint8_t*>(const_cast<std::byte*>(buffer.data()));

        // [0..3] magic
        std::memcpy(data, &MAGIC, 4);

        // [4] version
        data[4] = footer.version;

        // [5..7] padding
        data[5] = 0;
        data[6] = 0;
        data[7] = 0;

        // [8..15] indexOff (LE)
        std::memcpy(data + 8, &footer.index_off, 8);

        // [16..23] bloomOff (LE)
        std::memcpy(data + 16, &footer.bloom_off, 8);

        // [24..27] entries (LE)
        std::memcpy(data + 24, &footer.entries, 4);

        // [28..31] crc32c (LE)
        std::memcpy(data + 28, &crc32c_value, 4);
    }

    core::OwnedBuffer AKSSFooter::write_to_buffer(
        const Footer& footer,
        uint32_t crc32c_value
    ) {
        auto buffer = core::OwnedBuffer::allocate(SIZE);
        write_to(buffer.view(), footer, crc32c_value);
        return buffer;
    }

    AKSSFooter::Footer AKSSFooter::read_from(core::BufferView buffer) {
        if (buffer.size() < SIZE) { throw std::runtime_error("AKSSFooter::read_from: buffer too small"); }

        // std::byte* → const uint8_t* キャスト
        const auto* data = reinterpret_cast<const uint8_t*>(buffer.data());

        // Verify magic
        uint32_t magic;
        std::memcpy(&magic, data, 4);
        if (magic != MAGIC) { throw std::runtime_error("AKSSFooter: invalid magic"); }

        // Verify version
        const uint8_t version = data[4];
        if (version != VERSION) { throw std::runtime_error("AKSSFooter: unsupported version"); }

        // Read fields
        uint64_t index_off;
        uint64_t bloom_off;
        uint32_t entries;

        std::memcpy(&index_off, data + 8, 8);
        std::memcpy(&bloom_off, data + 16, 8);
        std::memcpy(&entries, data + 24, 4);

        return Footer{index_off, bloom_off, entries, version};
    }

    AKSSFooter::Footer AKSSFooter::read_from_file(
        core::BufferView buffer,
        const std::filesystem::path& file_path,
        bool verify_crc
    ) {
        auto footer = read_from(buffer);

        if (verify_crc) {
            const auto* data = reinterpret_cast<const uint8_t*>(buffer.data());
            uint32_t stored_crc;
            std::memcpy(&stored_crc, data + 28, 4);

            if (stored_crc != 0) {
                const auto file_size = std::filesystem::file_size(file_path);
                const uint32_t computed_crc = compute_file_crc(file_path, file_size);

                if (stored_crc != computed_crc) { throw std::runtime_error("AKSSFooter: CRC mismatch"); }
            }
        }

        return footer;
    }

    uint32_t AKSSFooter::compute_file_crc(
        const std::filesystem::path& file_path,
        uint64_t file_size
    ) {
        constexpr size_t BUFFER_SIZE = 1 << 20; // 1 MiB
        std::vector<uint8_t> buffer(BUFFER_SIZE);

        std::ifstream file(file_path, std::ios::binary);
        if (!file) { throw std::runtime_error("AKSSFooter: failed to open file for CRC"); }

        uint32_t crc = 0xFFFFFFFF;
        uint64_t total_read = 0;
        const uint64_t limit = file_size - 4; // Exclude last 4 bytes (CRC field)

        while (total_read < limit) {
            const size_t to_read = (std::min)(
                BUFFER_SIZE,
                limit - total_read
            );

            file.read(reinterpret_cast<char*>(buffer.data()), to_read);
            const size_t actually_read = file.gcount();

            if (actually_read == 0) { break; }

            for (size_t i = 0; i < actually_read; ++i) {
                crc ^= buffer[i];
                for (int j = 0; j < 8; ++j) { crc = (crc >> 1) ^ (0x82F63B78 & -(crc & 1)); }
            }

            total_read += actually_read;
        }

        return ~crc;
    }
} // namespace akkaradb::engine::sstable