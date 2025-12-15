// AKHdr32.cpp
#include "akkara/core/record/AKHdr32.hpp"
#include <cstring>
#include <algorithm>

namespace akkaradb::core
{
    AKHdr32 AKHdr32::read_from(BufferView buf, size_t offset)
    {
        AKHdr32 hdr{};
        hdr.k_len = buf.read_u16_le(offset + 0);
        hdr.v_len = buf.read_u32_le(offset + 2);
        hdr.seq = buf.read_u64_le(offset + 6);
        hdr.flags = buf.read_u8(offset + 14);
        hdr.pad0 = buf.read_u8(offset + 15);
        hdr.key_fp64 = buf.read_u64_le(offset + 16);
        hdr.mini_key = buf.read_u64_le(offset + 24);
        return hdr;
    }

    void AKHdr32::write_to(BufferView buf, size_t offset) const
    {
        buf.write_u16_le(offset + 0, k_len);
        buf.write_u32_le(offset + 2, v_len);
        buf.write_u64_le(offset + 6, seq);
        buf.write_u8(offset + 14, flags);
        buf.write_u8(offset + 15, pad0);
        buf.write_u64_le(offset + 16, key_fp64);
        buf.write_u64_le(offset + 24, mini_key);
    }

    uint64_t AKHdr32::sip_hash_24(std::string_view key) noexcept
    {
        return sip_hash_24(key, DEFAULT_SIPHASH_SEED);
    }

    uint64_t AKHdr32::sip_hash_24(std::string_view key, uint64_t seed) noexcept
    {
        // SipHash-2-4 implementation
        const uint64_t k0 = seed;
        const uint64_t k1 = k0 ^ 0x9E3779B97F4A7C15ULL;

        uint64_t v0 = 0x736f6d6570736575ULL ^ k0;
        uint64_t v1 = 0x646f72616e646f6dULL ^ k1;
        uint64_t v2 = 0x6c7967656e657261ULL ^ k0;
        uint64_t v3 = 0x7465646279746573ULL ^ k1;

        const auto* data = reinterpret_cast<const uint8_t*>(key.data());
        size_t len = key.size();

        // SipRound macro
        auto sip_round = [](uint64_t& v0, uint64_t& v1, uint64_t& v2, uint64_t& v3)
        {
            v0 += v1;
            v2 += v3;
            v1 = std::rotl(v1, 13);
            v3 = std::rotl(v3, 16);
            v1 ^= v0;
            v3 ^= v2;
            v0 = std::rotl(v0, 32);
            v2 += v1;
            v0 += v3;
            v1 = std::rotl(v1, 17);
            v3 = std::rotl(v3, 21);
            v1 ^= v2;
            v3 ^= v0;
            v2 = std::rotl(v2, 32);
        };

        // Process 8-byte blocks
        size_t blocks = len / 8;
        for (size_t i = 0; i < blocks; ++i)
        {
            uint64_t m;
            std::memcpy(&m, data + i * 8, 8);

            v3 ^= m;
            sip_round(v0, v1, v2, v3);
            sip_round(v0, v1, v2, v3);
            v0 ^= m;
        }

        // Pack final 0-7 bytes
        uint64_t b = static_cast<uint64_t>(len) << 56;
        size_t left = len & 7;
        const uint8_t* tail = data + (blocks * 8);

        switch (left)
        {
        case 7: b |= static_cast<uint64_t>(tail[6]) << 48;
            [[fallthrough]];
        case 6: b |= static_cast<uint64_t>(tail[5]) << 40;
            [[fallthrough]];
        case 5: b |= static_cast<uint64_t>(tail[4]) << 32;
            [[fallthrough]];
        case 4: b |= static_cast<uint64_t>(tail[3]) << 24;
            [[fallthrough]];
        case 3: b |= static_cast<uint64_t>(tail[2]) << 16;
            [[fallthrough]];
        case 2: b |= static_cast<uint64_t>(tail[1]) << 8;
            [[fallthrough]];
        case 1: b |= static_cast<uint64_t>(tail[0]);
            [[fallthrough]];
        case 0: break;
        default: ;
        }

        v3 ^= b;
        sip_round(v0, v1, v2, v3);
        sip_round(v0, v1, v2, v3);
        v0 ^= b;

        // Finalization
        v2 ^= 0xFFULL;
        for (int i = 0; i < 4; ++i)
        {
            sip_round(v0, v1, v2, v3);
        }

        return v0 ^ v1 ^ v2 ^ v3;
    }

    uint64_t AKHdr32::build_mini_key(std::string_view key) noexcept
    {
        uint64_t mini = 0;
        const size_t n = std::min<size_t>(key.size(), 8);

        // Pack bytes in Little-Endian order
        for (size_t i = 0; i < n; ++i)
        {
            mini |= static_cast<uint64_t>(static_cast<uint8_t>(key[i])) << (i * 8);
        }

        return mini;
    }
} // namespace akkaradb::core