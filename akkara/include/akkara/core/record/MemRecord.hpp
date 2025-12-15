// MemRecord.hpp
#pragma once

#include <string_view>
#include <cstdint>

namespace akkaradb::core
{
    // Enforce Little-Endian architecture
    static_assert(std::endian::native == std::endian::little, "AkkaraDB requires Little-Endian (x86-64/AArch64)");

    /**
 * MemRecord - In-memory representation of a key-value record.
 *
 * This struct represents a record stored in the MemTable. It corresponds
 * to JVM's MemRecord but uses string_view for zero-copy key/value access.
 *
 * Design principles:
 * - No ownership: key/value are views into an Arena allocator
 * - Compact: 48 bytes (3 cache lines on most CPUs)
 * - Hot path: optimized for put/get operations
 *
 * Memory layout (48 bytes):
 * - key:            std::string_view (16 bytes)
 * - value:          std::string_view (16 bytes)
 * - seq:            uint64_t (8 bytes)
 * - key_hash:       uint32_t (4 bytes)
 * - approx_size:    uint32_t (4 bytes)
 * - flags:          uint8_t (1 byte)
 * - padding:        7 bytes
 *
 * Thread-safety: Immutable after construction. Safe for concurrent reads.
 */
    struct MemRecord
    {
        std::string_view key; ///< Key data (non-owning view)
        std::string_view value; ///< Value data (empty if tombstone)
        uint64_t seq; ///< Global sequence number
        uint32_t key_hash; ///< FNV-1a hash of key (for map bucketing)
        uint32_t approx_size; ///< Approximate memory footprint
        uint8_t flags; ///< Bit flags (see RecordFlags)

        /**
     * Record flag bits.
     */
        struct RecordFlags
        {
            static constexpr uint8_t TOMBSTONE = 0x01; ///< Deletion marker
            // Reserved for future: SECONDARY = 0x02, BLOB_CHUNK = 0x04
        };

        /**
     * Returns true if this record is a tombstone (deletion marker).
     */
        [[nodiscard]] constexpr bool is_tombstone() const noexcept
        {
            return (flags & RecordFlags::TOMBSTONE) != 0;
        }

        /**
     * Lexicographic comparison of two keys (bytewise).
     *
     * @param a First key
     * @param b Second key
     * @return Negative if a < b, 0 if a == b, positive if a > b
     */
        [[nodiscard]] static int lex_compare(std::string_view a,
                                             std::string_view b) noexcept;

        /**
     * Determines if a candidate record should replace an existing record.
     *
     * Replacement rules (matching JVM version):
     * 1. Higher seq always wins
     * 2. Equal seq: tombstone wins over non-tombstone
     * 3. Equal seq + both tombstone: keep existing (no churn)
     *
     * @param existing Existing record (may be nullptr)
     * @param candidate Candidate record
     * @return true if candidate should replace existing
     */
        [[nodiscard]] static bool should_replace(
            const MemRecord* existing,
            const MemRecord& candidate) noexcept;

        /**
     * Computes FNV-1a 32-bit hash of a key.
     *
     * @param key Key to hash
     * @return 32-bit hash value
     */
        [[nodiscard]] static uint32_t hash_key(std::string_view key) noexcept;

        /**
     * Estimates the memory footprint of a record.
     *
     * @param key Key
     * @param value Value
     * @return Approximate size in bytes
     */
        [[nodiscard]] static uint32_t estimate_size(
            std::string_view key,
            std::string_view value) noexcept;
    };
} // namespace akkaradb::core