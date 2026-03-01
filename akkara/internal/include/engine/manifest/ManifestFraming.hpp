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

// internal/include/engine/manifest/ManifestFraming.hpp
#pragma once

#include "core/CRC32C.hpp"
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

namespace akkaradb::engine::manifest {

    // ============================================================================
    // ManifestRecordType
    // ============================================================================

    /**
     * ManifestRecordType - Discriminator for manifest record payload.
     */
    enum class ManifestRecordType : uint8_t {
        StripeCommit    = 0x01, ///< Stripe counter advance
        SSTSeal         = 0x02, ///< New SST file sealed
        SSTDelete       = 0x03, ///< SST file deleted
        CompactionStart = 0x04, ///< Compaction began
        CompactionEnd   = 0x05, ///< Compaction completed
        Checkpoint      = 0x06, ///< Checkpoint marker
        Truncate        = 0x07, ///< Manifest truncate marker
    };

    // ============================================================================
    // ManifestFileHeader - Written once at the start of each .akmf file
    // ============================================================================

    /**
     * ManifestFileHeader - Fixed 32-byte header at the start of every .akmf file.
     *
     * On-disk layout (32 bytes, all fields LE):
     * [magic:u32][version:u16][flags:u16][file_seq:u32][created_at_us:u64][crc32c:u32][reserved:u8×8]
     *
     * Design:
     * - magic: Format / corruption detection
     * - version: Forward-compatibility
     * - file_seq: Rotation counter (0 = base file, N = Nth rotated file)
     * - created_at_us: Microseconds since epoch
     * - crc32c: CRC32C of this header with crc32c field zeroed
     */
    #pragma pack(push, 1)
    struct ManifestFileHeader {
        static constexpr uint32_t MAGIC   = 0x414B4D46; ///< "AKMF"
        static constexpr uint16_t VERSION = 0x0001;

        uint32_t magic;
        uint16_t version;
        uint16_t flags;
        uint32_t file_seq;        ///< Rotation counter
        uint64_t created_at_us;   ///< Creation timestamp (μs since epoch)
        uint32_t crc32c;
        uint8_t  reserved[8];

        static constexpr size_t SIZE = 32;

        [[nodiscard]] bool verify_magic()   const noexcept { return magic == MAGIC; }
        [[nodiscard]] bool verify_version() const noexcept { return version == VERSION; }

        /**
         * Verifies the header checksum.
         * Recomputes CRC with crc32c field zeroed and compares.
         */
        [[nodiscard]] bool verify_checksum() const noexcept;

        /**
         * Builds and returns a valid header as a byte array.
         * Computes and fills crc32c automatically.
         */
        [[nodiscard]] static ManifestFileHeader build(uint32_t file_seq) noexcept;

        /**
         * Serializes this header to a 32-byte output buffer.
         */
        void serialize(uint8_t out[SIZE]) const noexcept;
    };
    #pragma pack(pop)

    static_assert(sizeof(ManifestFileHeader) == 32, "ManifestFileHeader must be 32 bytes");

    // ============================================================================
    // ManifestRecordHeader - Prefix for every manifest record
    // ============================================================================

    /**
     * ManifestRecordHeader - 8-byte header preceding each manifest record payload.
     *
     * On-disk layout (8 bytes, all fields LE):
     * [type:u8][flags:u8][payload_len:u16][crc32c:u32]
     *
     * Design:
     * - type: ManifestRecordType discriminator
     * - flags: Reserved (0 for now)
     * - payload_len: Byte length of the payload that follows
     * - crc32c: CRC32C of payload bytes only
     */
    #pragma pack(push, 1)
    struct ManifestRecordHeader {
        uint8_t  type;
        uint8_t  flags;
        uint16_t payload_len;
        uint32_t crc32c;

        static constexpr size_t SIZE = 8;

        /**
         * Builds a header for a given payload.
         */
        [[nodiscard]] static ManifestRecordHeader build(
            ManifestRecordType type,
            const uint8_t* payload,
            uint16_t payload_len
        ) noexcept;

        /**
         * Serializes this header to an 8-byte output buffer.
         */
        void serialize(uint8_t out[SIZE]) const noexcept;

        /**
         * Deserializes a header from 8 bytes.
         */
        [[nodiscard]] static ManifestRecordHeader deserialize(const uint8_t in[SIZE]) noexcept;

        /**
         * Verifies the payload CRC.
         */
        [[nodiscard]] bool verify_payload(const uint8_t* payload, uint16_t len) const noexcept {
            return crc32c == core::CRC32C::compute(payload, len);
        }
    };
    #pragma pack(pop)

    static_assert(sizeof(ManifestRecordHeader) == 8, "ManifestRecordHeader must be 8 bytes");

    // ============================================================================
    // Sentinel values
    // ============================================================================

    static constexpr uint64_t MANIFEST_ABSENT_U64 = UINT64_MAX; ///< Optional u64 absent sentinel

    // ============================================================================
    // Encode functions — produce raw payload bytes for each record type
    // ============================================================================

    /**
     * Encodes a StripeCommit payload.
     * Payload (16 bytes): [ts_us:u64][stripe_count:u64]
     */
    [[nodiscard]] std::vector<uint8_t> encode_stripe_commit(uint64_t ts_us, uint64_t stripe_count);

    /**
     * Encodes an SSTSeal payload.
     *
     * Payload fixed (24 bytes):
     *   [ts_us:u64][entries:u64][level:u8][key_flags:u8][name_len:u16][fk_len:u16][lk_len:u16]
     * Variable: name bytes, first_key bytes (if key_flags bit0), last_key bytes (if key_flags bit1)
     */
    [[nodiscard]] std::vector<uint8_t> encode_sst_seal(
        uint64_t ts_us,
        int level,
        const std::string& name,
        uint64_t entries,
        const std::optional<std::string>& first_key_hex,
        const std::optional<std::string>& last_key_hex
    );

    /**
     * Encodes an SSTDelete payload.
     * Payload fixed (10 bytes): [ts_us:u64][name_len:u16]
     * Variable: name bytes
     */
    [[nodiscard]] std::vector<uint8_t> encode_sst_delete(uint64_t ts_us, const std::string& name);

    /**
     * Encodes a CompactionStart payload.
     * Payload fixed (12 bytes): [ts_us:u64][level:u8][input_count:u8][reserved:u16]
     * Variable: [len:u16][bytes] × input_count
     */
    [[nodiscard]] std::vector<uint8_t> encode_compaction_start(
        uint64_t ts_us,
        int level,
        const std::vector<std::string>& inputs
    );

    /**
     * Encodes a CompactionEnd payload.
     *
     * Payload fixed (28 bytes):
     *   [ts_us:u64][entries:u64][level:u8][key_flags:u8][input_count:u8][reserved:u8]
     *   [out_len:u16][fk_len:u16][lk_len:u16][reserved:u16]
     * Variable: output bytes, first_key bytes, last_key bytes, [len:u16 + bytes] × input_count
     */
    [[nodiscard]] std::vector<uint8_t> encode_compaction_end(
        uint64_t ts_us,
        int level,
        const std::string& output,
        const std::vector<std::string>& inputs,
        uint64_t entries,
        const std::optional<std::string>& first_key_hex,
        const std::optional<std::string>& last_key_hex
    );

    /**
     * Encodes a Checkpoint payload.
     *
     * Payload fixed (26 bytes): [ts_us:u64][stripe:u64][last_seq:u64][name_len:u16]
     * Variable: name bytes
     *
     * stripe / last_seq use MANIFEST_ABSENT_U64 when not present.
     * name_len = 0 when no name.
     */
    [[nodiscard]] std::vector<uint8_t> encode_checkpoint(
        uint64_t ts_us,
        const std::optional<std::string>& name,
        const std::optional<uint64_t>& stripe,
        const std::optional<uint64_t>& last_seq
    );

    /**
     * Encodes a Truncate payload.
     * Payload fixed (10 bytes): [ts_us:u64][reason_len:u16]
     * Variable: reason bytes
     */
    [[nodiscard]] std::vector<uint8_t> encode_truncate(
        uint64_t ts_us,
        const std::optional<std::string>& reason
    );

    // ============================================================================
    // Decode helpers — parse payload bytes into structured fields
    // ============================================================================

    struct DecodedStripeCommit {
        uint64_t ts_us;
        uint64_t stripe_count;
    };

    struct DecodedSSTSeal {
        uint64_t ts_us;
        uint64_t entries;
        int      level;
        std::string name;
        std::optional<std::string> first_key_hex;
        std::optional<std::string> last_key_hex;
    };

    struct DecodedSSTDelete {
        uint64_t ts_us;
        std::string name;
    };

    struct DecodedCompactionStart {
        uint64_t ts_us;
        int level;
        std::vector<std::string> inputs;
    };

    struct DecodedCompactionEnd {
        uint64_t ts_us;
        uint64_t entries;
        int      level;
        std::string output;
        std::vector<std::string> inputs;
        std::optional<std::string> first_key_hex;
        std::optional<std::string> last_key_hex;
    };

    struct DecodedCheckpoint {
        uint64_t ts_us;
        std::optional<uint64_t> stripe;
        std::optional<uint64_t> last_seq;
        std::optional<std::string> name;
    };

    struct DecodedTruncate {
        uint64_t ts_us;
        std::optional<std::string> reason;
    };

    /**
     * Decode functions.  Return false if payload is malformed / too short.
     */
    [[nodiscard]] bool decode_stripe_commit(const uint8_t* payload, uint16_t len, DecodedStripeCommit& out);
    [[nodiscard]] bool decode_sst_seal(const uint8_t* payload, uint16_t len, DecodedSSTSeal& out);
    [[nodiscard]] bool decode_sst_delete(const uint8_t* payload, uint16_t len, DecodedSSTDelete& out);
    [[nodiscard]] bool decode_compaction_start(const uint8_t* payload, uint16_t len, DecodedCompactionStart& out);
    [[nodiscard]] bool decode_compaction_end(const uint8_t* payload, uint16_t len, DecodedCompactionEnd& out);
    [[nodiscard]] bool decode_checkpoint(const uint8_t* payload, uint16_t len, DecodedCheckpoint& out);
    [[nodiscard]] bool decode_truncate(const uint8_t* payload, uint16_t len, DecodedTruncate& out);

} // namespace akkaradb::engine::manifest
