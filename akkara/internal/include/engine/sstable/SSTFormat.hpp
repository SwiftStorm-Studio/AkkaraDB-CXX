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

// internal/include/engine/sst/SSTFormat.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace akkaradb::engine::sst {

    // ── File magic & version ─────────────────────────────────────────────────

    /// Magic bytes at start of every .aksst file: "AKSS"
    inline constexpr uint32_t SST_MAGIC   = 0x414B5353u;
    inline constexpr uint16_t SST_VERSION = 0x0001u;

    // ── Tuning constants ─────────────────────────────────────────────────────

    /// Size of the per-record CRC32C trailer appended after each [AKHdr32][key][value].
    /// CRC32C is computed over the entire record (header + key + value bytes).
    inline constexpr uint32_t RECORD_CRC_SIZE = 4;

    /// One sparse index entry is emitted every INDEX_STRIDE records,
    /// plus always for the first and last record.
    inline constexpr size_t INDEX_STRIDE = 128;

    /// Default Bloom filter density: bits allocated per key.
    /// 10 bits/key yields ~1% false-positive rate.
    inline constexpr size_t BLOOM_BITS_PER_KEY = 10;

    // ── File header (64 bytes, at file offset 0) ─────────────────────────────
    //
    // Binary layout (little-endian, #pragma pack(1)):
    //
    //  [0]  magic[4]         = 0x414B5353 ("AKSS")
    //  [4]  version[2]       = 0x0001
    //  [6]  level[1]         = 0..7
    //  [7]  flags[1]         = reserved (0)
    //  [8]  entry_count[8]   = total records (tombstones included)
    //  [16] data_size[8]     = bytes in data section; data starts at offset 64
    //  [24] index_offset[8]  = absolute file offset of sparse index section
    //  [32] index_count[4]   = number of sparse index entries
    //  [36] bloom_offset[4]  = absolute file offset of Bloom filter (0 = none)
    //  [40] bloom_size[4]    = size of Bloom filter in bytes (0 = none)
    //  [44] pad0[4]          = reserved (0)
    //  [48] min_seq[8]       = minimum sequence number in file
    //  [56] max_seq[8]       = maximum sequence number in file
    //  [?]  ... but with pad0 we only reach 60 bytes. CRC is at [60].
    //
    // Wait: 4+2+1+1+8+8+8+4+4+4+4+8+8 = 64 bytes. Let me recount:
    //   magic(4) + version(2) + level(1) + flags(1)      = 8
    //   entry_count(8)                                    = 16
    //   data_size(8)                                      = 24
    //   index_offset(8)                                   = 32
    //   index_count(4) + bloom_offset(4) + bloom_size(4) = 44
    //   pad0(4) + min_seq(8) + max_seq(8)                = 64  (no crc fits!)
    //
    // We drop pad0 and add crc32c after max_seq:
    //   index_count(4) + bloom_offset(4) + bloom_size(4) = 44
    //   min_seq(8) + max_seq(8) + crc32c(4)             = 64  ✓
    //
    // Final layout:
    //  [0]  magic[4]
    //  [4]  version[2]
    //  [6]  level[1]
    //  [7]  flags[1]
    //  [8]  entry_count[8]
    //  [16] data_size[8]        — data section starts at offset 64
    //  [24] index_offset[8]
    //  [32] index_count[4]
    //  [36] bloom_offset[4]     — 0 = no bloom filter
    //  [40] bloom_size[4]       — 0 = no bloom filter
    //  [44] reserved[4]
    //  [48] min_seq[8]
    //  [56] max_seq[8]          — wait, 56+8=64: no room for crc!
    //
    // Actually: 8+8+8+4+4+4+4+8+8 = 56; + 8 for magic/version/level/flags = 64.
    // Let me just lay it out plainly:
    //   4+2+1+1 = 8
    //   8 = 16
    //   8 = 24
    //   8 = 32
    //   4 = 36
    //   4 = 40
    //   4 = 44
    //   4 = 48   (pad → becomes crc32c)
    //   8 = 56
    //   8 = 64   (no more room for both min_seq and max_seq + crc)
    //
    // Resolution: store crc32c in place of pad, drop one of min/max seq:
    //   keep min_seq and max_seq, remove crc from header (validated via data CRC in index entries).
    //   OR: keep crc32c and drop max_seq (min_seq is enough for WAL truncation).
    //
    // Decision: keep all fields, remove pad, put crc32c last at [60]:
    //   [0]  magic[4]
    //   [4]  version[2]
    //   [6]  level[1]
    //   [7]  flags[1]
    //   [8]  entry_count[8]     → [16]
    //   [16] data_size[8]       → [24]
    //   [24] index_offset[8]    → [32]
    //   [32] index_count[4]     → [36]
    //   [36] bloom_offset[4]    → [40]
    //   [40] bloom_size[4]      → [44]
    //   [44] min_seq[8]         → [52]
    //   [52] max_seq[8]         → [60]
    //   [60] crc32c[4]          → [64] ✓

    // ── Codec flag values (stored in SSTFileHeader::flags) ───────────────────
    //
    // flags == SST_CODEC_NONE (0x00): data section is raw (backward compatible).
    // flags == SST_CODEC_ZSTD (0x01): data section is Zstd-compressed.
    //   Compressed size  = index_offset - DATA_OFFSET  (derived, no extra field needed).
    //   Uncompressed size = data_size   (existing field; used for ZSTD_decompress bound).

    inline constexpr uint8_t SST_CODEC_NONE = 0x00u;
    inline constexpr uint8_t SST_CODEC_ZSTD = 0x01u;

    #pragma pack(push, 1)
    struct SSTFileHeader {
        uint32_t magic;          ///< [0]  "AKSS" = 0x414B5353
        uint16_t version;        ///< [4]  0x0001
        uint8_t  level;          ///< [6]  0..7
        uint8_t  flags;          ///< [7]  reserved (0)
        uint64_t entry_count;    ///< [8]  total records (tombstones included)
        uint64_t data_size;      ///< [16] bytes in data section (data starts at offset 64)
        uint64_t index_offset;   ///< [24] absolute file offset of sparse index section
        uint32_t index_count;    ///< [32] number of sparse index entries
        uint32_t bloom_offset;   ///< [36] absolute file offset of Bloom filter (0 = none)
        uint32_t bloom_size;     ///< [40] size of Bloom filter in bytes (0 = none)
        uint64_t min_seq;        ///< [44] minimum sequence number in file
        uint64_t max_seq;        ///< [52] maximum sequence number in file
        uint32_t crc32c;         ///< [60] CRC32C of bytes [0..59] (crc32c field zeroed)
    };
    #pragma pack(pop)

    static_assert(sizeof(SSTFileHeader) == 64, "SSTFileHeader must be exactly 64 bytes");

    /// Data section starts immediately after the header.
    inline constexpr uint64_t DATA_OFFSET = sizeof(SSTFileHeader); // = 64

    // ── Sparse index entry (variable-length, stored contiguously) ────────────
    //
    // Each entry:
    //   data_offset[8]  — byte offset from start of data section
    //   mini_key[8]     — AKHdr32.mini_key (first 8 bytes of key, LE)
    //   key_fp64[8]     — AKHdr32.key_fp64 (SipHash-2-4 fingerprint)
    //   key_len[2]      — key length
    //   key[key_len]    — key bytes
    //
    // Fixed-size prefix per entry: 8+8+8+2 = 26 bytes.

    inline constexpr size_t INDEX_ENTRY_FIXED_SIZE = 26; // 8+8+8+2

    // ── Bloom filter section ─────────────────────────────────────────────────
    //
    // Layout:
    //   BloomHeader (16B):
    //     num_bits[4]    — total bit count (power of 2)
    //     num_hashes[1]  — k (number of hash functions)
    //     pad[11]        — reserved (0)
    //   bits[num_bits/8] — bit array
    //
    // Hash function: double-hashing using key_fp64.
    //   h1 = (uint32_t)(key_fp64)
    //   h2 = (uint32_t)(key_fp64 >> 32) | 1  — always odd to avoid cycles
    //   bit_i = (h1 + i * h2) % num_bits,  for i in [0, num_hashes)

    /// Bloom filter layout variants (stored in BloomHeader::type).
    inline constexpr uint8_t BLOOM_TYPE_STANDARD = 0x00u; ///< legacy: double-hashing across full bit array
    inline constexpr uint8_t BLOOM_TYPE_BLOCKED = 0x01u; ///< cache-line blocked: SipHash-2-4
    inline constexpr uint8_t BLOOM_TYPE_BLOCKED_FAST = 0x02u; ///< cache-line blocked: fast multiply-xorshift hash (~5ns vs ~30ns)

    /// Fast 64-bit hash for bloom filter internal use ONLY.
    /// NOT a cryptographic hash and NOT interchangeable with AKHdr32::key_fp64.
    /// Produces sufficient bit distribution for bloom filter false-positive rates.
    /// ~5-8 ns for a 16-byte key (vs SipHash-2-4 ~25-35 ns).
    inline uint64_t bloom_fast_hash64(const uint8_t* key, size_t len) noexcept {
        uint64_t h = 0x9e3779b97f4a7c15ULL ^ static_cast<uint64_t>(len * 2654435761ULL);
        const uint8_t* p = key;
        const uint8_t* end = key + (len & ~static_cast<size_t>(7u));
        while (p < end) {
            uint64_t w;
            std::memcpy(&w, p, 8);
            h ^= w;
            h *= 0xbf58476d1ce4e5b9ULL;
            h ^= h >> 31;
            p += 8;
        }
        if (const size_t rem = len & 7u; rem > 0u) {
            uint64_t w = 0;
            for (size_t i = 0; i < rem; ++i) w |= static_cast<uint64_t>(p[i]) << (i * 8u);
            h ^= w;
            h *= 0x94d049bb133111ebULL;
            h ^= h >> 27;
        }
        // Final avalanche
        h ^= h >> 30;
        h *= 0xbf58476d1ce4e5b9ULL;
        h ^= h >> 27;
        h *= 0x94d049bb133111ebULL;
        h ^= h >> 31;
        return h;
    }

    #pragma pack(push, 1)
    struct BloomHeader {
        uint32_t num_bits; ///< total bit count; blocked: multiple of 512
        uint8_t num_hashes; ///< k = number of hash probes per key
        uint8_t type; ///< BLOOM_TYPE_STANDARD (0) or BLOOM_TYPE_BLOCKED (1)
        uint8_t pad[10]; ///< reserved (0)
    };
    #pragma pack(pop)

    static_assert(sizeof(BloomHeader) == 16, "BloomHeader must be exactly 16 bytes");

} // namespace akkaradb::engine::sst
