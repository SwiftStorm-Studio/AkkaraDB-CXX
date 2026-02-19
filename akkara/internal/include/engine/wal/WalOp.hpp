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

// internal/include/core/wal/WalOp.hpp
#pragma once

#include "core/buffer/BufferView.hpp"
#include "core/record/AKHdr32.hpp"
#include <memory>
#include <span>
#include <cstdint>

namespace akkaradb::wal {
    /**
     * WalEntryType - Type of WAL entry.
     */
    enum class WalEntryType : uint8_t {
        Record = 0x01,
        ///< Record operation (Add/Delete)
        Commit = 0x02,
        ///< Transaction commit boundary
        Checkpoint = 0x03,
        ///< Checkpoint marker
        Metadata = 0x04,
        ///< Metadata entry
    };

    /**
     * WalEntryHeader - Fixed-size header for all WAL entries.
     *
     * On-disk layout (8 bytes):
     * [total_len:u32][entry_type:u8][reserved:u8×3]
     *
     * Design:
     * - total_len: Total entry size including this header
     * - entry_type: Discriminator for entry type
     * - reserved: Padding for alignment + future extensions
     * - CRC is handled at BatchHeader level (not per-entry)
     */
    #pragma pack(push, 1)
    struct WalEntryHeader {
        uint32_t total_len; ///< Total entry size (header + payload)
        WalEntryType entry_type; ///< Entry type discriminator
        uint8_t reserved[3]; ///< Reserved for alignment/future use

        static constexpr size_t SIZE = 8;
    };
    #pragma pack(pop)

    static_assert(sizeof(WalEntryHeader) == 8, "WalEntryHeader must be 8 bytes");

    // ============================================================================
    // Internal helpers (used by serialize functions)
    // ============================================================================

    namespace internal {
        /**
         * Writes WalEntryHeader to buffer and returns offset after header.
         */
        [[nodiscard]] size_t write_header(core::BufferView buffer, uint32_t total_len, WalEntryType entry_type) noexcept;
    } // namespace internal

    // ============================================================================
    // Zero-allocation Write Path (for maximum performance)
    // ============================================================================

    /**
     * Serializes Add operation directly to buffer (zero-allocation).
     *
     * Performance: ~100-260ns (vs ~350-650ns for create_add + serialize)
     * - No heap allocations
     * - No intermediate object creation
     * - Direct memcpy to target buffer
     *
     * Layout: [WalEntryHeader:12B][AKHdr32:32B][key][value]
     *
     * @param buffer Target buffer (must have enough space)
     * @param key Key bytes
     * @param value Value bytes
     * @param seq Sequence number
     * @param key_fp64 Key fingerprint (SipHash-2-4)
     * @param mini_key Mini key (first 8 bytes of key, LE-packed)
     * @return Number of bytes written
     * @throws std::out_of_range if buffer is too small
     */
    [[nodiscard]] size_t serialize_add_direct(
        core::BufferView buffer,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint64_t key_fp64 = 0,
        uint64_t mini_key = 0
    );

    /**
     * Serializes Delete operation directly to buffer (zero-allocation).
     *
     * Performance: ~80-180ns (vs ~350-650ns for create_delete + serialize)
     * - No heap allocations
     * - No intermediate object creation
     * - Tombstone with v_len = 0
     *
     * Layout: [WalEntryHeader:12B][AKHdr32:32B][key]
     *
     * @param buffer Target buffer (must have enough space)
     * @param key Key bytes
     * @param seq Sequence number
     * @param key_fp64 Key fingerprint
     * @param mini_key Mini key
     * @return Number of bytes written
     * @throws std::out_of_range if buffer is too small
     */
    [[nodiscard]] size_t serialize_delete_direct(
        core::BufferView buffer,
        std::span<const uint8_t> key,
        uint64_t seq,
        uint64_t key_fp64 = 0,
        uint64_t mini_key = 0
    );

    /**
     * Serializes Commit operation directly to buffer (zero-allocation).
     *
     * Performance: ~50-100ns
     *
     * Layout: [WalEntryHeader:12B][seq:u64][timestamp:u64]
     *
     * @param buffer Target buffer (must have at least 28 bytes)
     * @param seq Sequence number
     * @param timestamp Timestamp in microseconds (0 = current time)
     * @return Number of bytes written (always 28)
     * @throws std::out_of_range if buffer is too small
     */
    [[nodiscard]] size_t serialize_commit_direct(core::BufferView buffer, uint64_t seq, uint64_t timestamp = 0);

    // ============================================================================
    // Zero-copy Read Path (for recovery)
    // ============================================================================

    /**
     * WalRecordOpRef - Zero-copy view of a WalRecordOp in buffer.
     *
     * Performance: Construction ~75-175ns (vs ~300-650ns for deserialize)
     * - No heap allocations
     * - No memcpy of key/value data
     * - Direct span references into buffer
     *
     * Design:
     * - Non-owning view into existing buffer
     * - Caller must ensure buffer lifetime exceeds this object
     * - Ideal for recovery where data is read-only
     *
     * Thread-safety: NOT thread-safe. External synchronization required.
     */
    class WalRecordOpRef {
        public:
            /**
             * Constructs a zero-copy view from buffer.
             *
             * Buffer layout: [WalEntryHeader:12B][AKHdr32:32B][key][value]
             *
             * @param buffer Buffer containing serialized WalRecordOp
             * @throws std::runtime_error if buffer is malformed or checksum fails
             */
            explicit WalRecordOpRef(core::BufferView buffer);

            // ==================== Accessors ====================

            [[nodiscard]] const core::AKHdr32& header() const noexcept { return header_; }

            [[nodiscard]] std::span<const uint8_t> key() const noexcept {
                return {reinterpret_cast<const uint8_t*>(buffer_.data() + key_offset_), header_.k_len};
            }

            [[nodiscard]] std::span<const uint8_t> value() const noexcept {
                return {reinterpret_cast<const uint8_t*>(buffer_.data() + value_offset_), header_.v_len};
            }

            [[nodiscard]] bool is_tombstone() const noexcept { return header_.is_tombstone(); }
            [[nodiscard]] uint64_t seq() const noexcept { return header_.seq; }
            [[nodiscard]] uint64_t key_fp64() const noexcept { return header_.key_fp64; }
            [[nodiscard]] uint64_t mini_key() const noexcept { return header_.mini_key; }
            [[nodiscard]] uint16_t k_len() const noexcept { return header_.k_len; }
            [[nodiscard]] uint32_t v_len() const noexcept { return header_.v_len; }

            /**
             * Returns total entry size including WalEntryHeader.
             */
            [[nodiscard]] size_t total_size() const noexcept { return WalEntryHeader::SIZE + sizeof(core::AKHdr32) + header_.k_len + header_.v_len; }

        private:
            core::BufferView buffer_; ///< Non-owning view of serialized data
            core::AKHdr32 header_; ///< Parsed AKHdr32
            size_t key_offset_; ///< Offset to key data in buffer
            size_t value_offset_; ///< Offset to value data in buffer
    };
} // namespace akkaradb::wal