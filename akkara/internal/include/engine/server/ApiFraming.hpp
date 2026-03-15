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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/include/engine/server/ApiFraming.hpp
#pragma once

#include <cstdint>
#include <span>
#include <vector>

namespace akkaradb::server {

    // ── Opcodes ──────────────────────────────────────────────────────────────

    enum class ApiOp : uint8_t {
        Get    = 0x01,
        Put    = 0x02,
        Remove = 0x03,
        GetAt  = 0x04,  ///< VersionLog point-in-time read (requires seq param)
        // Scan  = 0x05,  // TODO
        // Batch = 0x06,  // TODO
    };

    enum class ApiStatus : uint8_t {
        Ok       = 0x00,
        NotFound = 0x01,
        Error    = 0xFF,
    };

    // ── Wire frames (little-endian, packed) ──────────────────────────────────
    //
    // TCP binary protocol frame layout:
    //
    //   Request:  [ApiRequestHeader : 16B] [key : key_len B] [val : val_len B] [crc32c : 4B]
    //   Response: [ApiResponseHeader: 13B] [val : val_len B]                   [crc32c : 4B]
    //
    // CRC32C covers key+val bytes only (not the header).
    // request_id is echoed in the response to support pipelining.

#pragma pack(push, 1)

    struct ApiRequestHeader {
        char     magic[4];      ///< "AKRQ"
        uint8_t  version;       ///< Protocol version = 1
        ApiOp    opcode;
        uint32_t request_id;    ///< Echoed in response; enables pipelining
        uint16_t key_len;       ///< Byte length of key (follows header)
        uint32_t val_len;       ///< Byte length of value (follows key); 0 for Get/Remove
        // Followed by: [key_len bytes] [val_len bytes] [crc32c: 4 bytes]
    };
    static_assert(sizeof(ApiRequestHeader) == 16, "ApiRequestHeader must be 16 bytes");

    struct ApiResponseHeader {
        char      magic[4];     ///< "AKRS"
        ApiStatus status;
        uint32_t  request_id;   ///< Echoed from request
        uint32_t  val_len;      ///< 0 when status != Ok, or for Remove
        // Followed by: [val_len bytes] [crc32c: 4 bytes]
    };
    static_assert(sizeof(ApiResponseHeader) == 13, "ApiResponseHeader must be 13 bytes");

#pragma pack(pop)

    inline constexpr char REQUEST_MAGIC[4]  = {'A','K','R','Q'};
    inline constexpr char RESPONSE_MAGIC[4] = {'A','K','R','S'};
    inline constexpr uint8_t PROTOCOL_VERSION = 1;

    // ── Framing helpers ───────────────────────────────────────────────────────

    /// Build a serialised response frame (header + val + crc32c) into buf.
    void encode_response(ApiStatus status,
                         uint32_t  request_id,
                         std::span<const uint8_t> val,
                         std::vector<uint8_t>& buf);

    /// Build an error response (status=Error, empty val) into buf.
    void encode_error(uint32_t request_id, std::vector<uint8_t>& buf);

} // namespace akkaradb::server
