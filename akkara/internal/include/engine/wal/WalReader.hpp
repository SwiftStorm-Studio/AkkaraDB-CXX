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

// internal/include/engine/wal/WalReader.hpp
#pragma once

#include "engine/wal/WalFraming.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>

namespace akkaradb::wal {
    /**
     * WalReader - Sequential reader for a single .akwal shard file.
     *
     * Reads SPECv4 format:
     *   [WalSegmentHeader:32B]
     *   [WalBatchHeader:24B][entries...]
     *   [WalBatchHeader:24B][entries...]
     *   ...
     *
     * Usage:
     *   auto reader = WalReader::open(path);
     *   while (auto batch = reader->next_batch()) {
     *       WalIterator it = batch->iterator();
     *       while (it.next()) {
     *           switch (it.entry_type()) { ... }
     *       }
     *   }
     *   if (reader->has_error()) { ... reader->error_type() ... }
     *
     * Performance notes:
     * - Reads in large chunks (read_buffer) to minimise syscall count
     * - WalIterator is zero-copy over the in-memory batch buffer
     * - Batch buffer is reused across next_batch() calls (no per-batch alloc)
     * - Segment header is verified once at open(); no per-batch overhead
     *
     * Thread-safety: NOT thread-safe.
     */
    class WalReader {
        public:
            enum class ErrorType : uint8_t {
                NONE = 0,
                TRUNCATED = 1,
                ///< Unexpected EOF mid-header or mid-batch
                INVALID_MAGIC = 2,
                ///< Bad segment or batch magic
                INVALID_VERSION = 3,
                ///< Unsupported segment version
                CRC_MISMATCH = 4,
                ///< Batch CRC32C failed
            };

            /**
             * BatchView - Snapshot of a decoded batch.
             *
             * Holds a non-owning view into the reader's internal batch buffer.
             * Valid only until the next call to next_batch() or until the
             * WalReader is destroyed.
             *
             * batch_seq: Global monotonic sequence number (cross-shard ordering)
             * entry_count: Number of WalEntries in this batch
             * iterator(): Returns a fresh WalIterator over the entry payload
             */
            struct BatchView {
                uint64_t batch_seq;
                uint32_t entry_count;
                core::BufferView entries_buf; ///< Slice after WalBatchHeader

                [[nodiscard]] WalIterator iterator() const noexcept { return WalIterator{entries_buf}; }
            };

            /**
             * Opens a shard file for reading.
             * Validates WalSegmentHeader (magic, version, CRC).
             *
             * @throws std::runtime_error if the file cannot be opened
             */
            [[nodiscard]] static std::unique_ptr<WalReader> open(const std::filesystem::path& path);

            ~WalReader();

            WalReader(const WalReader&) = delete;
            WalReader& operator=(const WalReader&) = delete;
            WalReader(WalReader&&) = delete;
            WalReader& operator=(WalReader&&) = delete;

            /**
             * Advances to the next batch.
             *
             * Returns nullopt when:
             *   - Clean EOF (has_error() == false)
             *   - A read or validation error occurred (has_error() == true)
             *
             * The returned BatchView is valid until the next call to next_batch().
             *
             * Performance: ~1 syscall per read_buffer worth of batches (~256 KB)
             */
            [[nodiscard]] std::optional<BatchView> next_batch();

            [[nodiscard]] bool has_error() const noexcept;
            [[nodiscard]] ErrorType error_type() const noexcept;
            [[nodiscard]] uint64_t error_position() const noexcept;
            [[nodiscard]] uint64_t file_size() const noexcept;
            [[nodiscard]] uint64_t bytes_read() const noexcept;

            // Segment metadata (available after open())
            [[nodiscard]] uint16_t shard_id() const noexcept;
            [[nodiscard]] uint64_t segment_id() const noexcept;

        private:
            explicit WalReader(const std::filesystem::path& path);

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::wal