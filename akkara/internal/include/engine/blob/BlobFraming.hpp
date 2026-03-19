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

// internal/include/engine/blob/BlobFraming.hpp
#pragma once

#include "core/CRC32C.hpp"
#include "engine/Compression.hpp"
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

namespace akkaradb::engine::blob {

    // ============================================================================
    // BlobFileHeader — Written once at the start of every .blob file
    // ============================================================================

    /**
     * BlobFileHeader — 32-byte fixed header at the start of every .blob file.
     *
     * On-disk layout (32 bytes, all fields LE):
     * [magic:u32][version:u16][flags:u16][blob_id:u64][total_size:u64][crc32c:u32][compressed_size:u32]
     *
     *  - magic           : "AKBL" = 0x414B424C  — format detection / corruption guard
     *  - version         : 0x0001
     *  - flags           : codec byte (0 = None, 1 = Zstd); same values as engine::Codec
     *  - blob_id         : globally-unique monotonic ID assigned by BlobManager
     *  - total_size      : uncompressed byte length of the blob content
     *  - crc32c          : CRC32C of this header with the crc32c field zeroed
     *  - compressed_size : on-disk payload size when compressed (0 = uncompressed)
     *
     * Content immediately follows the header: compressed_size bytes if non-zero,
     * otherwise total_size raw bytes.
     * The content's integrity is checked by BlobManager via a separate CRC
     * stored in the AKHdr32 value inline reference [blob_id:u64][total_size:u64][checksum:u32].
     * Checksum is always computed against the UNCOMPRESSED content.
     */
    #pragma pack(push, 1)
    struct BlobFileHeader {
        static constexpr uint32_t MAGIC   = 0x414B424C; ///< "AKBL"
        static constexpr uint16_t VERSION = 0x0001;
        static constexpr size_t   SIZE    = 32;

        uint32_t magic;
        uint16_t version;
        uint16_t flags; ///< codec: 0 = None, 1 = Zstd (engine::Codec)
        uint64_t blob_id;
        uint64_t total_size; ///< uncompressed content size
        uint32_t crc32c; ///< CRC32C of this header (crc32c field zeroed)
        uint32_t compressed_size; ///< on-disk payload size when compressed; 0 = uncompressed

        // ── factory / helpers ────────────────────────────────────────────────

        /**
         * Builds a valid BlobFileHeader.
         * Computes and fills crc32c automatically.
         *
         * @param blob_id         Unique blob identifier.
         * @param total_size      Uncompressed content size (always stored in this field).
         * @param codec           Compression codec used for the payload.
         * @param compressed_size On-disk payload size (0 = uncompressed / same as total_size).
         */
        [[nodiscard]] static BlobFileHeader build(
            uint64_t blob_id,
            uint64_t total_size,
            akkaradb::engine::Codec codec = akkaradb::engine::Codec::None,
            uint32_t compressed_size = 0
        ) noexcept;

        /**
         * Serializes this header to a 32-byte output buffer.
         */
        void serialize(uint8_t out[SIZE]) const noexcept;

        /**
         * Deserializes a header from 32 bytes.
         */
        [[nodiscard]] static BlobFileHeader deserialize(const uint8_t in[SIZE]) noexcept;

        [[nodiscard]] bool verify_magic()   const noexcept { return magic   == MAGIC;   }
        [[nodiscard]] bool verify_version() const noexcept { return version == VERSION; }

        /**
         * Verifies the header checksum.
         * Re-computes CRC with crc32c zeroed and compares against stored value.
         */
        [[nodiscard]] bool verify_checksum() const noexcept;
    };
    #pragma pack(pop)

    static_assert(sizeof(BlobFileHeader) == 32, "BlobFileHeader must be 32 bytes");

    // ============================================================================
    // Encode / decode helpers
    // ============================================================================

    /**
     * Builds a complete .blob file (header + content) into a byte vector.
     * Used when writing a blob to disk (or when shipping via ReplBlobPut).
     */
    [[nodiscard]] std::vector<uint8_t> encode_blob_file(
        uint64_t                 blob_id,
        std::span<const uint8_t> content
    );

    /**
     * Reads and validates a BlobFileHeader from the first 32 bytes.
     * Returns false if magic, version, or checksum is invalid.
     */
    [[nodiscard]] bool decode_blob_header(
        const uint8_t*  data,
        BlobFileHeader& out
    ) noexcept;

    // ============================================================================
    // Inline-value reference (stored in AKHdr32 value payload when FLAG_BLOB)
    // ============================================================================

    static constexpr size_t BLOB_REF_SIZE = 20; ///< blob_id(8) + total_size(8) + checksum(4)

    /**
     * Encodes a blob inline reference (20 bytes) into `out`.
     * This is the value stored in the MemTable / WAL for FLAG_BLOB records.
     */
    void encode_blob_ref(
        uint8_t*  out,          ///< must be at least BLOB_REF_SIZE bytes
        uint64_t  blob_id,
        uint64_t  total_size,
        uint32_t  checksum      ///< CRC32C of the blob content
    ) noexcept;

    struct BlobRef {
        uint64_t blob_id;
        uint64_t total_size;
        uint32_t checksum;
    };

    /**
     * Decodes a blob inline reference from `data` (20 bytes).
     */
    [[nodiscard]] BlobRef decode_blob_ref(const uint8_t* data) noexcept;

} // namespace akkaradb::engine::blob
