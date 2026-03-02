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

// internal/include/engine/cluster/ReplFraming.hpp
#pragma once

#include "engine/wal/WalOp.hpp"
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

namespace akkaradb::engine::cluster {

    // ============================================================================
    // Message type discriminator
    // ============================================================================

    /**
     * ReplMsgType - First byte of every message on the replication stream.
     */
    enum class ReplMsgType : uint8_t {
        Entry   = 0x01, ///< WAL entry (put / delete)
        BlobPut = 0x02, ///< Blob file content transfer
        Ack     = 0x03, ///< Replica acknowledgement
    };

    // ============================================================================
    // Handshake frames (connection setup)
    // ============================================================================

    /**
     * ReplClientHello - Sent by Replica immediately after TCP connect.
     *
     * On-wire (18 bytes, LE):
     *   [magic:u32="AKRH"][node_id:u64][last_seq:u64][flags:u16]
     */
    #pragma pack(push, 1)
    struct ReplClientHello {
        static constexpr uint32_t MAGIC = 0x414B5248; ///< "AKRH"
        static constexpr size_t   SIZE  = 22; ///< 4+8+8+2

        uint32_t magic;
        uint64_t node_id;
        uint64_t last_seq;  ///< Last seq the Replica has applied
        uint16_t flags;     ///< Reserved (must be 0)

        void serialize(uint8_t out[SIZE]) const noexcept;
        [[nodiscard]] static ReplClientHello deserialize(const uint8_t in[SIZE]) noexcept;
        [[nodiscard]] bool verify_magic() const noexcept { return magic == MAGIC; }
    };
    #pragma pack(pop)
    static_assert(sizeof(ReplClientHello) == 22);

    /**
     * ReplServerHello - Sent by Primary in response to ReplClientHello.
     *
     * On-wire (22 bytes, LE):
     *   [magic:u32="AKPH"][node_id:u64][current_seq:u64][flags:u16]
     */
    #pragma pack(push, 1)
    struct ReplServerHello {
        static constexpr uint32_t MAGIC = 0x414B5048; ///< "AKPH"
        static constexpr size_t   SIZE  = 22; ///< 4+8+8+2

        uint32_t magic;
        uint64_t node_id;
        uint64_t current_seq; ///< Highest seq committed on Primary
        uint16_t flags;

        void serialize(uint8_t out[SIZE]) const noexcept;
        [[nodiscard]] static ReplServerHello deserialize(const uint8_t in[SIZE]) noexcept;
        [[nodiscard]] bool verify_magic() const noexcept { return magic == MAGIC; }
    };
    #pragma pack(pop)
    static_assert(sizeof(ReplServerHello) == 22);

    // ============================================================================
    // ReplEntry - WAL entry streamed from Primary to Replica
    // ============================================================================

    /**
     * ReplEntryHeader - Fixed prefix of a ReplEntry message.
     *
     * On-wire (20 bytes, LE):
     *   [msg_type:u8=0x01][seq:u64][wal_type:u8][key_len:u16][val_len:u32][crc32c:u32]
     *
     * Total message size = 20 + key_len + val_len bytes.
     * crc32c covers: key bytes + val bytes.
     */
    #pragma pack(push, 1)
    struct ReplEntryHeader {
        uint8_t  msg_type;  ///< ReplMsgType::Entry (0x01)
        uint64_t seq;       ///< Sequence number (Primary-assigned)
        uint8_t  wal_type;  ///< WalEntryType (Record=0x01, Commit=0x02, ...)
        uint16_t key_len;
        uint32_t val_len;
        uint32_t crc32c;    ///< CRC32C of key + val bytes

        static constexpr size_t SIZE = 20; ///< 1+8+1+2+4+4

        void serialize(uint8_t out[SIZE]) const noexcept;
        [[nodiscard]] static ReplEntryHeader deserialize(const uint8_t in[SIZE]) noexcept;
    };
    #pragma pack(pop)
    static_assert(sizeof(ReplEntryHeader) == 20);

    // ============================================================================
    // ReplBlobPut - BLOB content streamed from Primary to Replica
    // ============================================================================

    /**
     * ReplBlobPutHeader - Fixed prefix of a ReplBlobPut message.
     *
     * On-wire (21 bytes, LE):
     *   [msg_type:u8=0x02][blob_id:u64][content_len:u64][crc32c:u32]
     *
     * Total message size = 21 + content_len bytes.
     * crc32c covers the content bytes.
     *
     * Sending order (guaranteed over TCP):
     *   1. ReplBlobPut  ← Replica writes blob file
     *   2. ReplEntry    ← Replica applies WAL entry with FLAG_BLOB
     */
    #pragma pack(push, 1)
    struct ReplBlobPutHeader {
        uint8_t  msg_type;    ///< ReplMsgType::BlobPut (0x02)
        uint64_t blob_id;
        uint64_t content_len;
        uint32_t crc32c;      ///< CRC32C of content bytes

        static constexpr size_t SIZE = 21;

        void serialize(uint8_t out[SIZE]) const noexcept;
        [[nodiscard]] static ReplBlobPutHeader deserialize(const uint8_t in[SIZE]) noexcept;
    };
    #pragma pack(pop)
    static_assert(sizeof(ReplBlobPutHeader) == 21);

    // ============================================================================
    // ReplAck - Replica acknowledgement to Primary
    // ============================================================================

    /**
     * ReplAck - Sent by Replica after successfully applying a ReplEntry.
     *
     * On-wire (9 bytes, LE):
     *   [msg_type:u8=0x03][seq:u64]
     */
    #pragma pack(push, 1)
    struct ReplAck {
        uint8_t  msg_type; ///< ReplMsgType::Ack (0x03)
        uint64_t seq;

        static constexpr size_t SIZE = 9;

        void serialize(uint8_t out[SIZE]) const noexcept;
        [[nodiscard]] static ReplAck deserialize(const uint8_t in[SIZE]) noexcept;
    };
    #pragma pack(pop)
    static_assert(sizeof(ReplAck) == 9);

    // ============================================================================
    // Encode helpers — build complete wire messages into a byte vector
    // ============================================================================

    /**
     * Builds a complete ReplEntry wire message (header + key + val).
     */
    [[nodiscard]] std::vector<uint8_t> encode_repl_entry(
        uint64_t                    seq,
        wal::WalEntryType           wal_type,
        std::span<const uint8_t>    key,
        std::span<const uint8_t>    val
    );

    /**
     * Builds a complete ReplBlobPut wire message (header + content).
     */
    [[nodiscard]] std::vector<uint8_t> encode_repl_blob_put(
        uint64_t                    blob_id,
        std::span<const uint8_t>    content
    );

    /**
     * Builds a ReplAck wire message.
     */
    [[nodiscard]] std::vector<uint8_t> encode_repl_ack(uint64_t seq);

} // namespace akkaradb::engine::cluster
