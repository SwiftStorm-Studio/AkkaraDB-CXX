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

// internal/src/engine/blob/BlobFraming.cpp
#include "engine/blob/BlobFraming.hpp"
#include <cstring>

namespace akkaradb::engine::blob {

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
            for (int i = 0; i < 8; ++i) b[i] = static_cast<uint8_t>(v >> (8 * i));
        }
        inline uint16_t get_u16(const uint8_t* b) noexcept {
            return static_cast<uint16_t>(b[0]) | (static_cast<uint16_t>(b[1]) << 8);
        }
        inline uint32_t get_u32(const uint8_t* b) noexcept {
            return  static_cast<uint32_t>(b[0])
                 | (static_cast<uint32_t>(b[1]) <<  8)
                 | (static_cast<uint32_t>(b[2]) << 16)
                 | (static_cast<uint32_t>(b[3]) << 24);
        }
        inline uint64_t get_u64(const uint8_t* b) noexcept {
            uint64_t v = 0;
            for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(b[i]) << (8 * i);
            return v;
        }
    } // anonymous namespace

    // ============================================================================
    // BlobFileHeader
    // ============================================================================

    void BlobFileHeader::serialize(uint8_t out[SIZE]) const noexcept {
        put_u32(out, magic); // 0..3
        put_u16(out + 4, version); // 4..5
        put_u16(out + 6, flags); // 6..7
        put_u64(out + 8, blob_id); // 8..15
        put_u64(out + 16, total_size); // 16..23
        put_u32(out + 24, crc32c); // 24..27
        put_u32(out + 28, compressed_size); // 28..31
    }

    BlobFileHeader BlobFileHeader::deserialize(const uint8_t in[SIZE]) noexcept {
        BlobFileHeader h{};
        h.magic = get_u32(in);
        h.version = get_u16(in + 4);
        h.flags = get_u16(in + 6);
        h.blob_id = get_u64(in + 8);
        h.total_size = get_u64(in + 16);
        h.crc32c = get_u32(in + 24);
        h.compressed_size = get_u32(in + 28);
        return h;
    }

    BlobFileHeader BlobFileHeader::build(uint64_t bid, uint64_t tsz, akkaradb::engine::Codec codec, uint32_t csz) noexcept {
        BlobFileHeader h{};
        h.magic = MAGIC;
        h.version = VERSION;
        h.flags = static_cast<uint16_t>(codec);
        h.blob_id = bid;
        h.total_size = tsz;
        h.crc32c = 0;
        h.compressed_size = csz;

        // Serialize with crc32c=0, compute CRC over all 32 bytes, store it
        uint8_t buf[SIZE];
        h.serialize(buf);
        h.crc32c = core::CRC32C::compute(buf, SIZE);
        return h;
    }

    bool BlobFileHeader::verify_checksum() const noexcept {
        uint8_t buf[SIZE];
        serialize(buf);
        // Zero the crc32c field before re-computing
        buf[24] = buf[25] = buf[26] = buf[27] = 0;
        return crc32c == core::CRC32C::compute(buf, SIZE);
    }

    // ============================================================================
    // encode_blob_file / decode_blob_header
    // ============================================================================

    std::vector<uint8_t> encode_blob_file(
            uint64_t                 blob_id,
            std::span<const uint8_t> content) {
        auto hdr = BlobFileHeader::build(blob_id, content.size());

        std::vector<uint8_t> out(BlobFileHeader::SIZE + content.size());
        hdr.serialize(out.data());
        if (!content.empty())
            std::memcpy(out.data() + BlobFileHeader::SIZE, content.data(), content.size());
        return out;
    }

    bool decode_blob_header(const uint8_t* data, BlobFileHeader& out) noexcept {
        out = BlobFileHeader::deserialize(data);
        return out.verify_magic() && out.verify_version() && out.verify_checksum();
    }

    // ============================================================================
    // Blob inline reference (20 bytes: blob_id:u64 + total_size:u64 + checksum:u32)
    // ============================================================================

    void encode_blob_ref(uint8_t* out, uint64_t blob_id,
                         uint64_t total_size, uint32_t checksum) noexcept {
        put_u64(out,      blob_id);
        put_u64(out + 8,  total_size);
        put_u32(out + 16, checksum);
    }

    BlobRef decode_blob_ref(const uint8_t* data) noexcept {
        BlobRef r{};
        r.blob_id    = get_u64(data);
        r.total_size = get_u64(data + 8);
        r.checksum   = get_u32(data + 16);
        return r;
    }

} // namespace akkaradb::engine::blob
