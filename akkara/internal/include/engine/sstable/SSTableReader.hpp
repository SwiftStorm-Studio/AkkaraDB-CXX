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

// internal/include/engine/sstable/SSTableReader.hpp
#pragma once

#include "AKSSFooter.hpp"
#include "IndexBlock.hpp"
#include "BloomFilter.hpp"
#include "format-akk/AkkBlockUnpacker.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include "core/record/RecordView.hpp"
#include <filesystem>
#include <memory>
#include <optional>
#include <fstream>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

namespace akkaradb::engine::sstable {
    /**
     * BlockCache - O(1) LRU cache with intrusive list.
     *
     * Uses unordered_map + doubly-linked list for true O(1) operations.
     */
    class BlockCache {
        public:
            explicit BlockCache(size_t max_blocks = 512);

            std::shared_ptr<core::OwnedBuffer> get(uint64_t offset);
            void put(uint64_t offset, std::shared_ptr<core::OwnedBuffer> buffer);
            void clear();

        private:
            struct Entry {
                std::shared_ptr<core::OwnedBuffer> buffer;
                std::list<uint64_t>::iterator lru_it;
            };

            size_t max_blocks_;
            std::unordered_map<uint64_t, Entry> cache_;
            std::list<uint64_t> lru_list_; // Most recent at front
            mutable std::mutex mutex_;
    };

    /**
     * SSTableReader - Reads records from immutable SSTable files.
     *
     * Layout:
     *   [ Data Blocks (32 KiB each) ]
     *   [ IndexBlock ('AKIX') ]
     *   [ BloomFilter ('AKBL') ]
     *   [ Footer ('AKSS', 32 bytes) ]
     *
     * Responsibilities:
     *   - Parse footer, load index/bloom into memory
     *   - Point lookup: Bloom → Index → Block → Record
     *   - Range query: Sequential block iteration
     *   - Block caching with LRU eviction (zero-copy via shared_ptr)
     *   - CRC verification on first load
     *
     * Thread-safety: Read-only operations are thread-safe.
     *                Cache uses internal locking.
     */
    class SSTableReader {
        public:
            /**
         * Range query.
         *
         * Returns all records in [start_key, end_key).
         *
         * @param start_key Inclusive start
         * @param end_key Exclusive end (nullopt = EOF)
         * @return Vector of (key, value, header) tuples
         */
            struct RangeRecord {
                std::vector<uint8_t> key;
                std::vector<uint8_t> value;
                core::AKHdr32 header;
            };

            /**
         * RangeIterator - Zero-copy forward iterator over range.
         *
         * Returns RecordView pointing into cached blocks.
         */
            class RangeIterator {
                public:
                    [[nodiscard]] bool has_next() const;
                    [[nodiscard]] std::optional<core::RecordView> next();

                private:
                    friend class SSTableReader;

                    RangeIterator(SSTableReader* reader, std::span<const uint8_t> start_key, std::optional<std::span<const uint8_t>> end_key);

                    SSTableReader* reader_;
                    std::vector<uint8_t> start_key_;
                    std::optional<std::vector<uint8_t>> end_key_;

                    size_t current_entry_idx_;
                    std::shared_ptr<core::OwnedBuffer> current_block_;
                    std::unique_ptr<format::RecordCursor> current_cursor_;
            };

            /**
         * Opens an SSTable file.
         *
         * @param file_path Path to .sst file
         * @param verify_footer_crc If true, verify file-level CRC
         * @return Unique pointer to reader
         * @throws std::runtime_error if file invalid
         */
            [[nodiscard]] static std::unique_ptr<SSTableReader> open(const std::filesystem::path& file_path, bool verify_footer_crc = false);

            ~SSTableReader();

            /**
         * Point lookup (zero-copy).
         *
         * Returns RecordView pointing into cached block.
         * RecordView is valid until block is evicted from cache.
         *
         * @param key Key to find
         * @return RecordView if found, nullopt otherwise
         */
            [[nodiscard]] std::optional<core::RecordView> get(std::span<const uint8_t> key);

            /**
         * Point lookup (owning copy).
         *
         * Returns owned copy of value for cases where lifetime management is needed.
         *
         * @param key Key to find
         * @return Value vector if found, nullopt otherwise
         */
            [[nodiscard]] std::optional<std::vector<uint8_t>> get_copy(std::span<const uint8_t> key);

            /**
         * Range query (zero-copy iterator).
         *
         * @param start_key Inclusive start
         * @param end_key Exclusive end (nullopt = EOF)
         * @return Iterator over records
         */
            [[nodiscard]] RangeIterator range_iter(std::span<const uint8_t> start_key, const std::optional<std::span<const uint8_t>>& end_key = std::nullopt);

            /**
         * Range query (materialized vector).
         *
         * Use this when you need owned copies of all records.
         *
         * @param start_key Inclusive start
         * @param end_key Exclusive end (nullopt = EOF)
         * @return Vector of (key, value, header) tuples
         */
            [[nodiscard]] std::vector<RangeRecord> range(
                std::span<const uint8_t> start_key,
                const std::optional<std::span<const uint8_t>>& end_key = std::nullopt
            );

            /**
         * Closes file.
         */
            void close();

            /**
         * Returns footer info.
         */
            [[nodiscard]] const AKSSFooter::Footer& footer() const noexcept { return footer_; }

            /**
         * Returns entry count.
         */
            [[nodiscard]] uint32_t entries() const noexcept { return footer_.entries; }

        private:
            SSTableReader(std::filesystem::path file_path, const AKSSFooter::Footer& footer, IndexBlock index, std::optional<BloomFilter> bloom);

            std::shared_ptr<core::OwnedBuffer> load_block(uint64_t offset);
            std::optional<core::RecordView> find_in_block(core::BufferView block, std::span<const uint8_t> key);

            std::filesystem::path file_path_;
            mutable std::mutex file_mutex_; // Guards file_ seekg/read (ifstream is not thread-safe)
            std::ifstream file_;
            uint64_t file_size_;

            AKSSFooter::Footer footer_;
            IndexBlock index_;
            std::optional<BloomFilter> bloom_;

            std::unique_ptr<format::akk::AkkBlockUnpacker> unpacker_;
            BlockCache cache_;
    };
} // namespace akkaradb::engine::sstable