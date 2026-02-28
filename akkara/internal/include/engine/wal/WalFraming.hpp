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

// internal/include/engine/wal/WalFraming.hpp
#pragma once

#include "engine/wal/WalOp.hpp"
#include "core/buffer/BufferView.hpp"
#include <cstdint>
#include <optional>
#include <utility>

namespace akkaradb::wal {
    // ============================================================================
    // Shard count
    // ============================================================================

    /**
 * Returns the optimal WAL shard count for this machine.
 *
 * Rules:
 * - Always a power of 2: {2, 4, 8, 16}
 * - min = 2  (parallelism is always on)
 * - max = min(hardware_concurrency, 16)
 * - Rounds up to nearest power of 2
 *
 * Power-of-2 constraint enables fast shard selection:
 *   shard_id = key_hash & (shard_count - 1)  // ~1ns, no division
 */
    [[nodiscard]] uint32_t compute_shard_count() noexcept;

    /**
 * Returns shard index for a given hash.
 * shard_count must be a power of 2.
 */
    [[nodiscard]] inline uint32_t shard_for(uint64_t hash, uint32_t shard_count) noexcept { return static_cast<uint32_t>(hash & (shard_count - 1)); }

    // ============================================================================
    // WalSegmentHeader - File header (written once at segment start)
    // ============================================================================

    /**
 * WalSegmentHeader - Fixed header at the start of every .akwal segment file.
 *
 * On-disk layout (32 bytes):
 * [magic:u32][version:u16][shard_id:u16][segment_id:u64][created_at:u64][crc32c:u32][reserved:u32]
 *
 * Design:
 * - magic: Corruption / format detection
 * - shard_id: Which shard this segment belongs to (0-based)
 * - segment_id: Monotonic per-shard counter
 * - created_at: Microseconds since epoch
 * - crc32c: CRC of this header (with crc32c field = 0)
 */
    #pragma pack(push, 1)
    struct WalSegmentHeader {
        static constexpr uint32_t MAGIC = 0x414B5741; // "AKWA"
        static constexpr uint16_t VERSION = 0x0004; // SPECv4

        uint32_t magic; ///< Format magic
        uint16_t version; ///< Format version
        uint16_t shard_id; ///< Shard this segment belongs to
        uint64_t segment_id; ///< Monotonic per-shard segment counter
        uint64_t created_at; ///< Creation timestamp (μs since epoch)
        uint32_t crc32c; ///< CRC32C of this header (crc32c field = 0 when computing)
        uint32_t reserved; ///< Future use

        static constexpr size_t SIZE = 32;

        [[nodiscard]] bool verify_magic() const noexcept { return magic == MAGIC; }
        [[nodiscard]] bool verify_version() const noexcept { return version == VERSION; }
        [[nodiscard]] bool verify_checksum(core::BufferView buffer) const noexcept;

        /**
     * Builds a valid header and writes it to buffer.
     * Computes and fills crc32c automatically.
     *
     * @return Number of bytes written (always SIZE)
     */
        static size_t write(core::BufferView buffer, uint16_t shard_id, uint64_t segment_id) noexcept;
    };
    #pragma pack(pop)

    static_assert(sizeof(WalSegmentHeader) == 32, "WalSegmentHeader must be 32 bytes");

    // ============================================================================
    // WalBatchHeader - fsync group boundary
    // ============================================================================

    /**
 * WalBatchHeader - Marks the start of an fsync group (batch of entries).
 *
 * On-disk layout (24 bytes):
 * [magic:u32][batch_seq:u64][entry_count:u32][batch_size:u32][crc32c:u32]
 *
 * Design:
 * - magic: Quick corruption detection, allows scanning for next valid batch
 * - batch_seq: Global monotonic counter across all shards
 *              Enables cross-shard ordering during recovery
 * - entry_count: Number of WalEntries that follow
 * - batch_size: Total size of (this header + all entries)
 *               Allows jumping to next batch even on partial corruption
 * - crc32c: CRC32C of (this header with crc32c=0) + all entry bytes
 *
 * Cross-shard ordering during recovery:
 *   Sort batches by batch_seq to reconstruct global write order.
 */
    #pragma pack(push, 1)
    struct WalBatchHeader {
        static constexpr uint32_t MAGIC = 0x414B4254; // "AKBT"

        uint32_t magic; ///< Batch magic for corruption detection
        uint64_t batch_seq; ///< Global monotonic batch sequence number
        uint32_t entry_count; ///< Number of entries in this batch
        uint32_t batch_size; ///< Total size (header + all entries)
        uint32_t crc32c; ///< CRC32C of header + entries (crc32c field = 0)

        static constexpr size_t SIZE = 24;

        [[nodiscard]] bool verify_magic() const noexcept { return magic == MAGIC; }

        /**
     * Verifies CRC32C of (header + entries).
     * buffer must start at this header and span batch_size bytes.
     */
        [[nodiscard]] bool verify_checksum(core::BufferView buffer) const noexcept;

        /**
     * Computes CRC32C of (header + entries).
     * buffer must start at this header and span total_size bytes.
     */
        [[nodiscard]] static uint32_t compute_checksum(core::BufferView buffer, size_t total_size) noexcept;

        /**
     * Writes header to buffer with crc32c = 0 (call finalize_checksum after writing entries).
     *
     * @return Number of bytes written (always SIZE)
     */
        static size_t write(core::BufferView buffer, uint64_t batch_seq, uint32_t entry_count, uint32_t batch_size) noexcept;

        /**
     * Finalizes by computing and writing the crc32c field.
     * Call after all entries have been written.
     * buffer must start at this header and span batch_size bytes.
     */
        static void finalize_checksum(core::BufferView buffer, size_t total_size) noexcept;
    };
    #pragma pack(pop)

    static_assert(sizeof(WalBatchHeader) == 24, "WalBatchHeader must be 24 bytes");

    // ============================================================================
    // WalIterator - Zero-copy forward iterator over entries in a batch
    // ============================================================================

    /**
 * WalIterator - Iterates over WalEntry records within a single batch payload.
 *
 * Usage:
 *   WalIterator it(entries_buffer);
 *   while (it.next()) {
 *       switch (it.entry_type()) {
 *           case WalEntryType::Record: {
 *               WalRecordOpRef ref = it.as_record();
 *               // ...
 *           }
 *           case WalEntryType::Commit: {
 *               auto [seq, ts] = it.as_commit();
 *               // ...
 *           }
 *       }
 *   }
 *
 * Performance: ~10-20ns per next() call
 * - Zero allocation
 * - Single bounds check per entry
 * - No checksum per entry (batch-level CRC already verified)
 *
 * Thread-safety: NOT thread-safe.
 */
    class WalIterator {
        public:
            /**
     * Constructs iterator over a batch's entry payload.
     * buffer should NOT include the WalBatchHeader - pass only the entries portion.
     */
            explicit WalIterator(core::BufferView buffer) noexcept : buffer_{buffer}, offset_{0}, current_header_{} {}

            /**
     * Advances to the next entry.
     *
     * @return true if a valid entry was found, false at end of buffer
     */
            [[nodiscard]] bool next() noexcept;

            // ==================== Current entry accessors ====================

            [[nodiscard]] WalEntryType entry_type() const noexcept { return current_header_.entry_type; }

            [[nodiscard]] size_t entry_size() const noexcept { return current_header_.total_len; }

            /**
     * Returns a BufferView over the full current entry (header + payload).
     * Valid until next() is called.
     */
            [[nodiscard]] core::BufferView entry_buffer() const noexcept {
                return buffer_.slice(offset_ - current_header_.total_len, current_header_.total_len);
            }

            /**
     * Returns zero-copy view of the current Record entry.
     * Caller must check entry_type() == WalEntryType::Record first.
     */
            [[nodiscard]] WalRecordOpRef as_record() const { return WalRecordOpRef{entry_buffer()}; }

            /**
     * Returns (seq, timestamp) of the current Commit entry.
     * Caller must check entry_type() == WalEntryType::Commit first.
     */
            [[nodiscard]] std::pair<uint64_t, uint64_t> as_commit() const noexcept;

        private:
            core::BufferView buffer_;
            size_t offset_;
            WalEntryHeader current_header_;
    };
} // namespace akkaradb::wal