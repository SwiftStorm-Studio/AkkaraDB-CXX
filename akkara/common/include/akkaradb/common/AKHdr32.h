#pragma once
#include <akkaradb/common/ByteBufferL.h>
#include <cstdint>
#include <span>
#include <array>
#include <stdexcept>

namespace akkaradb::common
{
    /**
     * @brief 32-byte record header for AkkaraDB's block format.
     *
     * AKHdr32 contains metadata for a single key-value record:
     * - Key and value lengths
     * - Global sequence number (for MVCC)
     * - Flags (tombstone, etc.)
     * - Key fingerprint (SipHash-2-4 for fast lookups)
     * - Mini-key (first 8 bytes of key for comparison)
     *
     * Layout (little-endian):
     * ```
     * [0..4)   u32 key_len
     * [4..8)   u32 value_len
     * [8..16)  u64 seq
     * [16..20) u32 flags
     * [20..28) u64 key_fp64 (SipHash-2-4)
     * [28..32) u8[8] mini_key
     * ```
     *
     * Thread safety: Immutable after construction, safe for concurrent reads.
     */
    struct AKHdr32
    {
        static constexpr size_t SIZE = 32;
        static constexpr uint64_t DEFAULT_SIPHASH_SEED = 0x0123456789ABCDEFULL;

        // Flags
        static constexpr uint32_t FLAG_TOMBSTONE = 0x01;

        uint16_t key_len; ///< Key length in bytes
        uint32_t value_len; ///< Value length in bytes
        uint64_t seq; ///< Global sequence number (monotonic)
        uint8_t flags; ///< Flags (tombstone, etc.)
        uint64_t key_fp64; ///< SipHash-2-4 fingerprint of key
        std::array<uint8_t, 8> mini_key; ///< First 8 bytes of key (for fast comparison)

        /**
         * @brief Default constructor (zero-initialized).
         */
        AKHdr32() = default;

        /**
         * @brief Construct header from components.
         */
        AKHdr32(const uint32_t k_len, const uint32_t v_len, const uint64_t sequence, const uint32_t flgs, const uint64_t fp, const std::array<uint8_t, 8>& mk)
            : key_len(k_len), value_len(v_len), seq(sequence), flags(flgs),
              key_fp64(fp), mini_key(mk)
        {
        }

        /**
         * @brief Check if this is a tombstone (delete marker).
         */
        [[nodiscard]]
        bool is_tombstone() const noexcept
        {
            return (flags & FLAG_TOMBSTONE) != 0;
        }

        /**
         * @brief Serialize header to 32-byte buffer (little-endian).
         * @param dest Destination buffer (must be exactly 32 bytes).
         */
        void serialize_to(std::span<uint8_t, SIZE> dest) const noexcept;

        /**
         * @brief Serialize header to ByteBufferL.
         * @param buf Buffer to write to (position will advance by 32 bytes).
         */
        void serialize_to(ByteBufferL& buf) const;

        /**
         * @brief Deserialize header from 32-byte buffer (little-endian).
         * @param src Source buffer (must be exactly 32 bytes).
         * @return Parsed header.
         * @throws std::invalid_argument if header is malformed.
         */
        [[nodiscard]]
        static AKHdr32 deserialize_from(std::span<const uint8_t, SIZE> src);

        /**
         * @brief Deserialize header from ByteBufferL.
         * @param buf Buffer to read from (position will advance by 32 bytes).
         * @return Parsed header.
         * @throws std::out_of_range if not enough bytes.
         * @throws std::invalid_argument if header is malformed.
         */
        [[nodiscard]]
        static AKHdr32 deserialize_from(ByteBufferL& buf);

        /**
         * @brief Compute SipHash-2-4 fingerprint of data.
         *
         * SipHash-2-4 is a fast, cryptographically strong PRF optimized for
         * short inputs. Used for key fingerprinting to accelerate lookups.
         *
         * @param data Input data.
         * @param seed Hash seed (use DEFAULT_SIPHASH_SEED for consistency).
         * @return 64-bit hash value.
         */
        [[nodiscard]]
        static uint64_t sip_hash_24(std::span<const uint8_t> data, uint64_t seed = DEFAULT_SIPHASH_SEED) noexcept;

        /**
         * @brief Build mini-key from key bytes (first 8 bytes, little-endian).
         *
         * The mini-key is used for fast lexicographic comparisons without
         * accessing the full key. If key is shorter than 8 bytes, remainder
         * is zero-padded.
         *
         * @param key Key bytes.
         * @return 8-byte mini-key.
         */
        [[nodiscard]]
        static std::array<uint8_t, 8> build_mini_key_le(std::span<const uint8_t> key) noexcept;

        /**
         * @brief Create header from key-value pair.
         *
         * Convenience method that computes fingerprint and mini-key automatically.
         *
         * @param key Key bytes.
         * @param value Value bytes.
         * @param sequence Global sequence number.
         * @param is_tombstone True if this is a delete marker.
         * @return Constructed header.
         */
        [[nodiscard]]
        static AKHdr32 create(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t sequence, bool is_tombstone = false) noexcept;

        /**
         * @brief Validate header for consistency.
         * @throws std::invalid_argument if invalid.
         */
        void validate() const;

    private:
        // Little-endian helpers (inlined for performance)
        static void write_le_u32(uint8_t* p, uint32_t v) noexcept;
        static void write_le_u64(uint8_t* p, uint64_t v) noexcept;
        static uint32_t read_le_u32(const uint8_t* p) noexcept;
        static uint64_t read_le_u64(const uint8_t* p) noexcept;
    };
} // namespace akkaradb::common
