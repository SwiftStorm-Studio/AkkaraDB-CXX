/*
 * AkkaraDB - Low-latency, crash-safe JVM KV store with WAL & stripe parity
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

// internal/include/core/record/AKHdr32.hpp
#pragma once

#include <cstdint>
#include <array>

namespace akkaradb::core {
    #pragma pack(push, 1)
    /**
     * AKHdr32 - 32-byte fixed-size record header for AkkEngine format.
     *
     * This structure is binary-compatible with the JVM version and is used
     * as the header for every record stored in blocks.
     *
     * Binary layout (Little-Endian, 32 bytes total):
     * [0..1]   k_len (u16)      - Key length (0..65535)
     * [2..5]   v_len (u32)      - Value length
     * [6..13]  seq (u64)        - Global sequence number (monotonic)
     * [14]     flags (u8)       - Flags (0x01 = TOMBSTONE)
     * [15]     pad0 (u8)        - Reserved (must be 0)
     * [16..23] key_fp64 (u64)   - SipHash-2-4 fingerprint of key
     * [24..31] mini_key (u64)   - First 8 bytes of key (LE-packed)
     *
     * Design principles:
     * - Fixed size: Always exactly 32 bytes (enforced by #pragma pack(1))
     * - Little-Endian only: No runtime endianness checks
     * - Binary compatible: Identical layout with JVM version
     * - POD type: Trivially copyable, standard layout
     *
     * Usage:
     * ```cpp
     * AKHdr32 hdr{
     *     .k_len = 5,
     *     .v_len = 10,
     *     .seq = 42,
     *     .flags = AKHdr32::FLAG_NORMAL,
     *     .key_fp64 = siphash24(key),
     *     .mini_key = pack_mini_key(key)
     * };
     * ```
     */
    struct AKHdr32 {
        // ==================== Fields ====================

        uint16_t k_len; ///< Key length (0..65535)
        uint32_t v_len; ///< Value length
        uint64_t seq; ///< Global sequence number
        uint8_t flags; ///< Flags (see FLAG_* constants)
        uint8_t pad0; ///< Reserved (must be 0)
        uint64_t key_fp64; ///< SipHash-2-4 fingerprint
        uint64_t mini_key; ///< First 8 bytes of key (LE)

        // ==================== Flag Constants ====================

        static constexpr uint8_t FLAG_NORMAL = 0x00; ///< Normal record
        static constexpr uint8_t FLAG_TOMBSTONE = 0x01; ///< Deleted record

        // ==================== Methods ====================

        /**
         * Checks if this record is a tombstone (deleted).
         */
        [[nodiscard]] constexpr bool is_tombstone() const noexcept { return (flags & FLAG_TOMBSTONE) != 0; }

        /**
         * Returns the total size of the record (header + key + value).
         */
        [[nodiscard]] constexpr size_t total_size() const noexcept { return sizeof(AKHdr32) + k_len + v_len; }

        /**
         * Computes SipHash-2-4 fingerprint of a key.
         *
         * Uses the default seed: 0x5AD6DCD676D23C25
         *
         * @param key Key data
         * @param key_len Key length
         * @return 64-bit fingerprint
         */
        [[nodiscard]] static uint64_t compute_key_fp64(const uint8_t* key, size_t key_len) noexcept;

        /**
         * Builds mini_key from the first 8 bytes of the key (Little-Endian packed).
         *
         * If key_len < 8, remaining bytes are zero.
         *
         * @param key Key data
         * @param key_len Key length
         * @return 64-bit mini_key value
         */
        [[nodiscard]] static uint64_t build_mini_key(const uint8_t* key, size_t key_len) noexcept;

        /**
         * Creates an AKHdr32 from key/value metadata.
         *
         * @param key Key data
         * @param key_len Key length
         * @param value_len Value length
         * @param seq Sequence number
         * @param flags Flags (FLAG_NORMAL or FLAG_TOMBSTONE)
         * @return Initialized header
         */
        [[nodiscard]] static AKHdr32 create(const uint8_t* key, size_t key_len, size_t value_len, uint64_t seq, uint8_t flags = FLAG_NORMAL) noexcept;
    };
    #pragma pack(pop)

    // Static assertions outside the struct (after pack(pop))
    static_assert(sizeof(AKHdr32) == 32, "AKHdr32 must be exactly 32 bytes");
    static_assert(std::is_standard_layout_v<AKHdr32>, "AKHdr32 must be standard layout");
    static_assert(std::is_trivially_copyable_v<AKHdr32>, "AKHdr32 must be trivially copyable");
} // namespace akkaradb::core