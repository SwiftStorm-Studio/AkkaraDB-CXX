// AKHdr32.hpp
#pragma once
#include "akkara/core/buf/BufferView.hpp"
#include <cstdint>

namespace akkaradb::core
{
    /**
 * AKHdr32 - 32-byte fixed-size record header.
 *
 * This header prefixes every on-disk record and WAL entry, providing
 * complete binary compatibility with the JVM version.
 *
 * Layout (32 bytes, Little-Endian):
 * ┌──────────────────────────────────────────────┐
 * │ Offset │ Size │ Field    │ Type │ Description│
 * ├──────────────────────────────────────────────┤
 * │ 0      │ 2    │ k_len    │ u16  │ Key length │
 * │ 2      │ 4    │ v_len    │ u32  │ Value len  │
 * │ 6      │ 8    │ seq      │ u64  │ Sequence # │
 * │ 14     │ 1    │ flags    │ u8   │ Flags      │
 * │ 15     │ 1    │ pad0     │ u8   │ Reserved   │
 * │ 16     │ 8    │ key_fp64 │ u64  │ SipHash    │
 * │ 24     │ 8    │ mini_key │ u64  │ Key prefix │
 * └──────────────────────────────────────────────┘
 *
 * Flags (u8):
 * - 0x01: TOMBSTONE (deletion marker)
 * - 0x02-0x80: Reserved
 *
 * Design principles:
 * - Binary compatible with JVM version
 * - POD type: trivially copyable
 * - Little-Endian only (enforced by static_assert)
 */
#pragma pack(push, 1)
    struct AKHdr32
    {
        static constexpr size_t SIZE = 32;
        static constexpr uint64_t DEFAULT_SIPHASH_SEED = 0x5AD6DCD676D23C25ULL;

        uint16_t k_len; ///< Key length in bytes (0..65535)
        uint32_t v_len; ///< Value length in bytes
        uint64_t seq; ///< Global sequence number
        uint8_t flags; ///< Record flags
        uint8_t pad0; ///< Reserved (must be 0)
        uint64_t key_fp64; ///< SipHash-2-4 fingerprint of key
        uint64_t mini_key; ///< First 8 bytes of key (LE-packed)

        /**
     * Reads an AKHdr32 from a buffer at the specified offset.
     *
     * @param buf Buffer to read from
     * @param offset Offset in bytes
     * @return Parsed header
     * @throws std::out_of_range if buffer is too small
     */
        [[nodiscard]] static AKHdr32 read_from(BufferView buf, size_t offset);

        /**
     * Writes this header to a buffer at the specified offset.
     *
     * @param buf Buffer to write to
     * @param offset Offset in bytes
     * @throws std::out_of_range if buffer is too small
     */
        void write_to(BufferView buf, size_t offset) const;

        /**
     * Computes SipHash-2-4 fingerprint of a key.
     *
     * Uses the default seed (DEFAULT_SIPHASH_SEED).
     *
     * @param key Key to hash
     * @return 64-bit fingerprint
     */
        [[nodiscard]] static uint64_t sip_hash_24(std::string_view key) noexcept;

        /**
     * Computes SipHash-2-4 with a custom seed.
     *
     * @param key Key to hash
     * @param seed Seed value (k0)
     * @return 64-bit fingerprint
     */
        [[nodiscard]] static uint64_t sip_hash_24(std::string_view key,
                                                  uint64_t seed) noexcept;

        /**
     * Builds the mini_key field from a key.
     *
     * Packs the first min(8, key.size()) bytes into a uint64_t (LE).
     * Remaining high bytes are zero-padded.
     *
     * Example:
     * - key = "hello" -> mini_key = 0x6F6C6C6568 (little-endian)
     *
     * @param key Key to pack
     * @return 64-bit mini_key value
     */
        [[nodiscard]] static uint64_t build_mini_key(std::string_view key) noexcept;
    };
#pragma pack(pop)

    // Ensure struct is POD and exactly 32 bytes
    static_assert(std::is_trivially_copyable_v<AKHdr32>,
                  "AKHdr32 must be trivially copyable");
    static_assert(sizeof(AKHdr32) == 32, "AKHdr32 must be exactly 32 bytes");
} // namespace akkaradb::core