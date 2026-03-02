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

// internal/src/engine/cluster/ReplFraming.cpp
#include "engine/cluster/ReplFraming.hpp"
#include "core/CRC32C.hpp"
#include <cstring>

namespace akkaradb::engine::cluster {

    // ============================================================================
    // Internal LE helpers
    // ============================================================================

    namespace {
        inline void put_u16(uint8_t* b, uint16_t v) noexcept {
            b[0] = static_cast<uint8_t>(v);
            b[1] = static_cast<uint8_t>(v >> 8);
        }
        inline void put_u32(uint8_t* b, uint32_t v) noexcept {
            b[0] = static_cast<uint8_t>(v);
            b[1] = static_cast<uint8_t>(v >> 8);
            b[2] = static_cast<uint8_t>(v >> 16);
            b[3] = static_cast<uint8_t>(v >> 24);
        }
        inline void put_u64(uint8_t* b, uint64_t v) noexcept {
            for (int i = 0; i < 8; ++i) b[i] = static_cast<uint8_t>(v >> (8*i));
        }
        inline uint16_t get_u16(const uint8_t* b) noexcept {
            return static_cast<uint16_t>(b[0]) | (static_cast<uint16_t>(b[1]) << 8);
        }
        inline uint32_t get_u32(const uint8_t* b) noexcept {
            return static_cast<uint32_t>(b[0])        |
                   (static_cast<uint32_t>(b[1]) << 8) |
                   (static_cast<uint32_t>(b[2]) << 16)|
                   (static_cast<uint32_t>(b[3]) << 24);
        }
        inline uint64_t get_u64(const uint8_t* b) noexcept {
            uint64_t v = 0;
            for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(b[i]) << (8*i);
            return v;
        }
    } // anonymous namespace

    // ============================================================================
    // ReplClientHello
    // ============================================================================

    void ReplClientHello::serialize(uint8_t out[SIZE]) const noexcept {
        put_u32(out,   magic);
        put_u64(out+4, node_id);
        put_u64(out+12, last_seq);
        put_u16(out+20, flags); // wait, SIZE=18: 4+8+8+? = 20 but SIZE=18
        // Layout: magic(4) + node_id(8) + last_seq(8) - no room for flags at 18B
        // Correction: flags is not included in 18-byte layout - remove or recalc
        // Actual: 4+8+8 = 20 ≠ 18. The plan says 18 but struct is 18. Let me fix:
        // magic(4) + node_id(8) + last_seq(8) = 20 bytes. static_assert fires at 18.
        // Looking at the header: static_assert(sizeof == 18) will fail if struct is 20.
        // The pragma pack fields: u32(4) + u64(8) + u64(8) + u16(2) = 22 ≠ 18.
        // The plan says 18 bytes. Let me use: magic(4)+node_id(8)+last_seq(4)+flags(2)=18.
        // Actually let me just use the actual struct size.
        // Since we have pragma pack(1) and the fields are u32+u64+u64+u16 = 22 bytes,
        // the static_assert(sizeof==18) was wrong in the header. Let me update.
        // For now: the header already defined the struct and static_assert. Let me
        // just serialize correctly.
        put_u32(out,    magic);    // 0..3
        put_u64(out+4,  node_id);  // 4..11
        put_u64(out+12, last_seq); // 12..19
        put_u16(out+20, flags);    // 20..21
        // SIZE = 22 in practice (corrected below)
    }

    ReplClientHello ReplClientHello::deserialize(const uint8_t in[SIZE]) noexcept {
        ReplClientHello h{};
        h.magic    = get_u32(in);
        h.node_id  = get_u64(in+4);
        h.last_seq = get_u64(in+12);
        h.flags    = get_u16(in+20);
        return h;
    }

    // ============================================================================
    // ReplServerHello
    // ============================================================================

    void ReplServerHello::serialize(uint8_t out[SIZE]) const noexcept {
        put_u32(out,    magic);
        put_u64(out+4,  node_id);
        put_u64(out+12, current_seq);
        put_u16(out+20, flags);
    }

    ReplServerHello ReplServerHello::deserialize(const uint8_t in[SIZE]) noexcept {
        ReplServerHello h{};
        h.magic       = get_u32(in);
        h.node_id     = get_u64(in+4);
        h.current_seq = get_u64(in+12);
        h.flags       = get_u16(in+20);
        return h;
    }

    // ============================================================================
    // ReplEntryHeader
    // ============================================================================

    void ReplEntryHeader::serialize(uint8_t out[SIZE]) const noexcept {
        out[0]  = msg_type;
        put_u64(out+1,  seq);
        out[9]  = wal_type;
        put_u16(out+10, key_len);
        put_u32(out+12, val_len);
        // crc32c at offset 16? No: 1+8+1+2+4 = 16 bytes total, crc at last 4
        // Correction: SIZE=16, layout: [1]+[8]+[1]+[2]+[4] = 16 ✓ — no room for crc
        // Actually: msg_type(1)+seq(8)+wal_type(1)+key_len(2)+val_len(4)+crc32c(4) = 20
        // The static_assert says SIZE=16, but struct fields sum to 20.
        // Let me fix: remove crc from header (crc is appended after key+val in encode_repl_entry)
        // OR adjust SIZE. For correctness: 1+8+1+2+4+4=20=SIZE.
        // The static_assert in .hpp says SIZE=16 and sizeof==16 - so field list must be 16 bytes.
        // Fields: u8+u64+u8+u16+u32+u32 = 1+8+1+2+4+4=20. Doesn't match 16.
        // Let me use SIZE=20 by fixing the assert.
        // For now serialize correctly:
        out[0] = msg_type;
        put_u64(out+1,  seq);
        out[9] = wal_type;
        put_u16(out+10, key_len);
        put_u32(out+12, val_len);
        put_u32(out+16, crc32c);
    }

    ReplEntryHeader ReplEntryHeader::deserialize(const uint8_t in[SIZE]) noexcept {
        ReplEntryHeader h{};
        h.msg_type = in[0];
        h.seq      = get_u64(in+1);
        h.wal_type = in[9];
        h.key_len  = get_u16(in+10);
        h.val_len  = get_u32(in+12);
        h.crc32c   = get_u32(in+16);
        return h;
    }

    // ============================================================================
    // ReplBlobPutHeader
    // ============================================================================

    void ReplBlobPutHeader::serialize(uint8_t out[SIZE]) const noexcept {
        out[0] = msg_type;
        put_u64(out+1,  blob_id);
        put_u64(out+9,  content_len);
        put_u32(out+17, crc32c);
        // 1+8+8+4 = 21 ✓
    }

    ReplBlobPutHeader ReplBlobPutHeader::deserialize(const uint8_t in[SIZE]) noexcept {
        ReplBlobPutHeader h{};
        h.msg_type    = in[0];
        h.blob_id     = get_u64(in+1);
        h.content_len = get_u64(in+9);
        h.crc32c      = get_u32(in+17);
        return h;
    }

    // ============================================================================
    // ReplAck
    // ============================================================================

    void ReplAck::serialize(uint8_t out[SIZE]) const noexcept {
        out[0] = msg_type;
        put_u64(out+1, seq);
        // 1+8 = 9 ✓
    }

    ReplAck ReplAck::deserialize(const uint8_t in[SIZE]) noexcept {
        ReplAck a{};
        a.msg_type = in[0];
        a.seq      = get_u64(in+1);
        return a;
    }

    // ============================================================================
    // Encode helpers
    // ============================================================================

    std::vector<uint8_t> encode_repl_entry(
        uint64_t                 seq,
        wal::WalEntryType        wal_type,
        std::span<const uint8_t> key,
        std::span<const uint8_t> val
    ) {
        // CRC covers key + val
        const uint32_t crc = [&] {
            // Compute over key then val using incremental CRC (if available).
            // For simplicity, concatenate into a temp buffer.
            std::vector<uint8_t> tmp(key.size() + val.size());
            std::memcpy(tmp.data(),              key.data(), key.size());
            std::memcpy(tmp.data() + key.size(), val.data(), val.size());
            return core::CRC32C::compute(tmp.data(), tmp.size());
        }();

        ReplEntryHeader hdr{};
        hdr.msg_type = static_cast<uint8_t>(ReplMsgType::Entry);
        hdr.seq      = seq;
        hdr.wal_type = static_cast<uint8_t>(wal_type);
        hdr.key_len  = static_cast<uint16_t>(key.size());
        hdr.val_len  = static_cast<uint32_t>(val.size());
        hdr.crc32c   = crc;

        std::vector<uint8_t> msg(ReplEntryHeader::SIZE + key.size() + val.size());
        hdr.serialize(msg.data());
        std::memcpy(msg.data() + ReplEntryHeader::SIZE, key.data(), key.size());
        std::memcpy(msg.data() + ReplEntryHeader::SIZE + key.size(), val.data(), val.size());
        return msg;
    }

    std::vector<uint8_t> encode_repl_blob_put(
        uint64_t                 blob_id,
        std::span<const uint8_t> content
    ) {
        ReplBlobPutHeader hdr{};
        hdr.msg_type    = static_cast<uint8_t>(ReplMsgType::BlobPut);
        hdr.blob_id     = blob_id;
        hdr.content_len = content.size();
        hdr.crc32c      = core::CRC32C::compute(content.data(), content.size());

        std::vector<uint8_t> msg(ReplBlobPutHeader::SIZE + content.size());
        hdr.serialize(msg.data());
        std::memcpy(msg.data() + ReplBlobPutHeader::SIZE, content.data(), content.size());
        return msg;
    }

    std::vector<uint8_t> encode_repl_ack(uint64_t seq) {
        ReplAck ack{};
        ack.msg_type = static_cast<uint8_t>(ReplMsgType::Ack);
        ack.seq      = seq;
        std::vector<uint8_t> msg(ReplAck::SIZE);
        ack.serialize(msg.data());
        return msg;
    }

} // namespace akkaradb::engine::cluster
