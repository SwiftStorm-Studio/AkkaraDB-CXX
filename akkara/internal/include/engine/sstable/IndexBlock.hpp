/*
* AkkEngine
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkEngine.
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

// internal/include/engine/sstable/IndexBlock.hpp
#pragma once

#include "core/buffer/BufferView.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include <vector>
#include <cstdint>
#include <span>
#include <array>

namespace akkaradb::engine::sstable {
    /**
 * IndexBlock - SSTable index for fast block lookup.
 *
 * Format ('AKIX'):
 *   [magic:u32 'AKIX']
 *   [version:u8]
 *   [key_size:u8 = 32]
 *   [padding:u16]
 *   [count:u32]
 *   [entries: (blockOffset:u64, firstKey32:32 bytes) × count]
 *   [crc32c:u32]
 *
 * Usage:
 *   Writer: builder.add(key, offset) → build() → write to file
 *   Reader: read_from(buffer) → lookup(key) → blockOffset
 *
 * Thread-safety: IndexBlock is immutable after construction.
 *               Builder is NOT thread-safe.
 */
    class IndexBlock {
    public:
        static constexpr uint32_t MAGIC = 0x41484B49; // 'AKIX' (reversed due to LE)
        static constexpr uint8_t VERSION = 1;
        static constexpr uint8_t KEY_SIZE = 32;
        static constexpr size_t HEADER_SIZE = 12; // magic + ver + key_size + pad + count
        static constexpr size_t ENTRY_SIZE = 8 + 32; // offset + key32

        /**
     * Index entry.
     */
        struct Entry {
            uint64_t block_offset;
            std::array<uint8_t, KEY_SIZE> first_key32;

            Entry(uint64_t offset, const std::array<uint8_t, KEY_SIZE>& key) : block_offset{offset}, first_key32{key} {}
        };

        /**
     * Reads IndexBlock from buffer.
     *
     * @param buffer Input buffer
     * @return IndexBlock instance
     * @throws std::runtime_error if invalid format
     */
        [[nodiscard]] static IndexBlock read_from(core::BufferView buffer);

        /**
     * Looks up block offset for given key.
     *
     * Returns the offset of the block that MAY contain the key
     * (uses lower_bound semantics).
     *
     * @param key Search key
     * @return Block offset, or -1 if key < first entry
     */
        [[nodiscard]] int64_t lookup(std::span<const uint8_t> key) const;

        /**
     * Returns number of entries.
     */
        [[nodiscard]] size_t count() const noexcept { return entries_.size(); }

        /**
     * Returns all entries.
     */
        [[nodiscard]] const std::vector<Entry>& entries() const noexcept { return entries_; }

        /**
     * Builder for creating IndexBlock.
     */
        class Builder {
        public:
            explicit Builder(size_t initial_capacity = 256);

            /**
         * Adds entry with variable-length key.
         *
         * Key is normalized to 32 bytes.
         */
            Builder& add(std::span<const uint8_t> first_key, uint64_t block_offset);

            /**
         * Adds entry with pre-normalized 32-byte key.
         */
            Builder& add_key32(const std::array<uint8_t, KEY_SIZE>& first_key32, uint64_t block_offset);

            /**
         * Builds IndexBlock buffer.
         *
         * @return OwnedBuffer containing serialized index
         */
            [[nodiscard]] core::OwnedBuffer build();

        private:
            std::vector<Entry> entries_;
        };

        /**
     * Normalizes variable-length key to 32 bytes.
     *
     * Layout:
     *   [0..23]  First 24 bytes of key (zero-padded if shorter)
     *   [24..27] FNV-1a hash (4 bytes)
     *   [28..31] Reserved (zero)
     */
        [[nodiscard]] static std::array<uint8_t, KEY_SIZE> normalize_key(std::span<const uint8_t> key);

    private:
        IndexBlock(std::vector<Entry> entries);

        std::vector<Entry> entries_;
    };
} // namespace akkaradb::engine::sstable