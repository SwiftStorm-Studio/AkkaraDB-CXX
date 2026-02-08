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

// internal/src/engine/sstable/IndexBlock.cpp
#include "engine/sstable/IndexBlock.hpp"
#include <algorithm>
#include <stdexcept>
#include <cstring>

namespace akkaradb::engine::sstable {
    namespace {
        /**
         * FNV-1a hash (32-bit).
         */
        uint32_t fnv1a32(std::span<const uint8_t> data) {
            uint32_t hash = 0x811c9dc5;
            for (uint8_t byte : data) {
                hash ^= byte;
                hash *= 0x01000193;
            }
            return hash;
        }

        /**
         * CRC32C computation.
         */
        uint32_t crc32c(const uint8_t* data, size_t size) {
            auto crc = 0xFFFFFFFF;
            for (size_t i = 0; i < size; ++i) {
                crc ^= static_cast<uint32_t>(data[i]);
                for (auto j = 0; j < 8; ++j) {
                    const uint32_t mask = (crc & 1)
                                              ? 0xFFFFFFFF
                                              : 0;
                    crc = (crc >> 1) ^ (0x82F63B78 & mask);
                }
            }
            return ~crc;
        }
    } // anonymous namespace

    // ==================== IndexBlock ====================

    IndexBlock::IndexBlock(std::vector<Entry> entries) : entries_{std::move(entries)} {}

    IndexBlock IndexBlock::read_from(core::BufferView buffer) {
        if (buffer.size() < HEADER_SIZE) { throw std::runtime_error("IndexBlock: buffer too small for header"); }

        const auto* data = reinterpret_cast<const uint8_t*>(buffer.data());

        // Verify magic
        uint32_t magic;
        std::memcpy(&magic, data, 4);
        if (magic != MAGIC) { throw std::runtime_error("IndexBlock: invalid magic"); }

        // Verify version
        const uint8_t version = data[4];
        if (version != VERSION) { throw std::runtime_error("IndexBlock: unsupported version"); }

        // Verify key size
        const uint8_t key_size = data[5];
        if (key_size != KEY_SIZE) { throw std::runtime_error("IndexBlock: unsupported key size"); }

        // Read count
        uint32_t count;
        std::memcpy(&count, data + 8, 4);

        // Verify buffer size
        const size_t entries_size = count * ENTRY_SIZE;
        const size_t expected_size = HEADER_SIZE + entries_size + 4; // +4 for CRC
        if (buffer.size() < expected_size) { throw std::runtime_error("IndexBlock: buffer too small for entries"); }

        // Verify CRC
        const uint32_t stored_crc = *reinterpret_cast<const uint32_t*>(data + HEADER_SIZE + entries_size);
        const uint32_t computed_crc = crc32c(data, HEADER_SIZE + entries_size);
        if (stored_crc != computed_crc) { throw std::runtime_error("IndexBlock: CRC mismatch"); }

        // Parse entries
        std::vector<Entry> entries;
        entries.reserve(count);

        const uint8_t* entry_data = data + HEADER_SIZE;
        for (uint32_t i = 0; i < count; ++i) {
            uint64_t offset;
            std::memcpy(&offset, entry_data, 8);

            std::array<uint8_t, KEY_SIZE> key32{};
            std::memcpy(key32.data(), entry_data + 8, KEY_SIZE);

            entries.emplace_back(offset, key32);
            entry_data += ENTRY_SIZE;
        }

        return IndexBlock{std::move(entries)};
    }

    int64_t IndexBlock::lookup(std::span<const uint8_t> key) const {
        if (entries_.empty()) { return -1; }

        // Normalize search key
        auto key32 = normalize_key(key);

        // Binary search (lower_bound)
        auto it = std::lower_bound(
            entries_.begin(),
            entries_.end(),
            key32,
            [](const Entry& entry, const std::array<uint8_t, KEY_SIZE>& search_key) {
                return std::ranges::lexicographical_compare(entry.first_key32, search_key);
            }
        );

        // If exact match or key > last entry, return that block
        if (it != entries_.end()) { return static_cast<int64_t>(it->block_offset); }

        // Key is greater than all entries, check last block
        if (!entries_.empty()) { return static_cast<int64_t>(entries_.back().block_offset); }

        return -1;
    }

    std::array<uint8_t, IndexBlock::KEY_SIZE> IndexBlock::normalize_key(std::span<const uint8_t> key) {
        std::array<uint8_t, KEY_SIZE> result{};

        // [0..23] First 24 bytes (zero-padded if shorter)
        const size_t prefix_len = (std::min)(static_cast<size_t>(24), key.size());
        std::memcpy(result.data(), key.data(), prefix_len);

        // [24..27] FNV-1a hash
        const uint32_t hash = fnv1a32(key);
        result[24] = (hash >> 24) & 0xFF;
        result[25] = (hash >> 16) & 0xFF;
        result[26] = (hash >> 8) & 0xFF;
        result[27] = hash & 0xFF;

        // [28..31] Reserved (already zero-initialized)

        return result;
    }

    // ==================== IndexBlock::Builder ====================

    IndexBlock::Builder::Builder(size_t initial_capacity) { entries_.reserve(initial_capacity); }

    IndexBlock::Builder& IndexBlock::Builder::add(std::span<const uint8_t> first_key, uint64_t block_offset) {
        auto key32 = normalize_key(first_key);
        return add_key32(key32, block_offset);
    }

    IndexBlock::Builder& IndexBlock::Builder::add_key32(
        const std::array<uint8_t, KEY_SIZE>& first_key32,
        uint64_t block_offset
    ) {
        entries_.emplace_back(block_offset, first_key32);
        return *this;
    }

    core::OwnedBuffer IndexBlock::Builder::build() {
        const size_t entries_size = entries_.size() * ENTRY_SIZE;
        const size_t total_size = HEADER_SIZE + entries_size + 4; // +4 for CRC

        auto buffer = core::OwnedBuffer::allocate(total_size);
        auto* data = reinterpret_cast<uint8_t*>(const_cast<std::byte*>(buffer.view().data()));

        // Write header
        std::memcpy(data, &MAGIC, 4);
        data[4] = VERSION;
        data[5] = KEY_SIZE;
        data[6] = 0; // padding
        data[7] = 0; // padding
        const auto count = static_cast<uint32_t>(entries_.size());
        std::memcpy(data + 8, &count, 4);

        // Write entries
        uint8_t* entry_data = data + HEADER_SIZE;
        for (const auto& entry : entries_) {
            std::memcpy(entry_data, &entry.block_offset, 8);
            std::memcpy(entry_data + 8, entry.first_key32.data(), KEY_SIZE);
            entry_data += ENTRY_SIZE;
        }

        // Write CRC
        const uint32_t crc = crc32c(data, HEADER_SIZE + entries_size);
        std::memcpy(data + HEADER_SIZE + entries_size, &crc, 4);

        return buffer;
    }
} // namespace akkaradb::engine::sstable