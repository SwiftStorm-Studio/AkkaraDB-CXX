#include <akkaradb/common/AKHdr32.h>
#include <cstring>
#include <bit>
#include <algorithm>

namespace akkaradb::common {

// ========================================
// Serialization
// ========================================

void AKHdr32::serialize_to(std::span<uint8_t, SIZE> dest) const noexcept {
    uint8_t* p = dest.data();
    
    write_le_u32(p + 0, key_len);
    write_le_u32(p + 4, value_len);
    write_le_u64(p + 8, seq);
    write_le_u32(p + 16, flags);
    write_le_u64(p + 20, key_fp64);
    std::memcpy(p + 28, mini_key.data(), 8);
}

void AKHdr32::serialize_to(ByteBufferL& buf) const {
    buf.put_u32(key_len);
    buf.put_u32(value_len);
    buf.put_u64(seq);
    buf.put_u32(flags);
    buf.put_u64(key_fp64);
    buf.put_bytes({mini_key.data(), mini_key.size()});
}

// ========================================
// Deserialization
// ========================================

AKHdr32 AKHdr32::deserialize_from(std::span<const uint8_t, SIZE> src) {
    const uint8_t* p = src.data();
    
    AKHdr32 hdr{};
    hdr.key_len = read_le_u32(p + 0);
    hdr.value_len = read_le_u32(p + 4);
    hdr.seq = read_le_u64(p + 8);
    hdr.flags = read_le_u32(p + 16);
    hdr.key_fp64 = read_le_u64(p + 20);
    std::memcpy(hdr.mini_key.data(), p + 28, 8);
    
    hdr.validate();
    return hdr;
}

AKHdr32 AKHdr32::deserialize_from(ByteBufferL& buf) {
    AKHdr32 hdr{};
    hdr.key_len = buf.get_u32();
    hdr.value_len = buf.get_u32();
    hdr.seq = buf.get_u64();
    hdr.flags = buf.get_u32();
    hdr.key_fp64 = buf.get_u64();
    buf.get_bytes(hdr.mini_key.data(), 8);
    
    hdr.validate();
    return hdr;
}

// ========================================
// Validation
// ========================================

void AKHdr32::validate() const {
    // Key length sanity check (max 64KB)
    if (key_len > 64 * 1024) {
        throw std::invalid_argument("key_len too large: " + std::to_string(key_len));
    }
    
    // Value length sanity check (max 16MB)
    if (value_len > 16 * 1024 * 1024) {
        throw std::invalid_argument("value_len too large: " + std::to_string(value_len));
    }
    
    // Flags sanity check (only known flags)
    constexpr uint32_t KNOWN_FLAGS = FLAG_TOMBSTONE;
    if ((flags & ~KNOWN_FLAGS) != 0) {
        throw std::invalid_argument("Unknown flags: " + std::to_string(flags));
    }
}

// ========================================
// Helper: Create from key-value
// ========================================

AKHdr32 AKHdr32::create(const std::span<const uint8_t> key, const std::span<const uint8_t> value, const uint64_t sequence, const bool is_tombstone) noexcept {
    AKHdr32 hdr{};
    hdr.key_len = static_cast<uint32_t>(key.size());
    hdr.value_len = static_cast<uint32_t>(value.size());
    hdr.seq = sequence;
    hdr.flags = is_tombstone ? FLAG_TOMBSTONE : 0;
    hdr.key_fp64 = sip_hash_24(key);
    hdr.mini_key = build_mini_key_le(key);
    return hdr;
}

// ========================================
// Mini-key construction
// ========================================

std::array<uint8_t, 8> AKHdr32::build_mini_key_le(const std::span<const uint8_t> key) noexcept {
    std::array<uint8_t, 8> result{};
    size_t n = std::min(key.size(), static_cast<size_t>(8));
    std::memcpy(result.data(), key.data(), n);
    // Remainder is already zero-initialized
    return result;
}

// ========================================
// SipHash-2-4 implementation
// ========================================

// SipHash-2-4 (reference implementation adapted)
// https://github.com/veorq/SipHash

namespace {

// SipHash round function
inline void sipround(uint64_t& v0, uint64_t& v1, uint64_t& v2, uint64_t& v3) {
    v0 += v1; v1 = std::rotl(v1, 13); v1 ^= v0; v0 = std::rotl(v0, 32);
    v2 += v3; v3 = std::rotl(v3, 16); v3 ^= v2;
    v0 += v3; v3 = std::rotl(v3, 21); v3 ^= v0;
    v2 += v1; v1 = std::rotl(v1, 17); v1 ^= v2; v2 = std::rotl(v2, 32);
}

} // anonymous namespace

uint64_t AKHdr32::sip_hash_24(const std::span<const uint8_t> data, const uint64_t seed) noexcept {
    // Initialize state with seed
    const uint64_t k0 = seed;
    const uint64_t k1 = ~seed; // Simple key derivation
    
    uint64_t v0 = 0x736f6d6570736575ULL ^ k0;
    uint64_t v1 = 0x646f72616e646f6dULL ^ k1;
    uint64_t v2 = 0x6c7967656e657261ULL ^ k0;
    uint64_t v3 = 0x7465646279746573ULL ^ k1;
    
    const uint8_t* ptr = data.data();
    const uint8_t* end = ptr + data.size();
    
    // Process 8-byte blocks
    while (ptr + 8 <= end) {
        const uint64_t m = read_le_u64(ptr);
        v3 ^= m;
        
        // 2 rounds (SipHash-2-4)
        sipround(v0, v1, v2, v3);
        sipround(v0, v1, v2, v3);
        
        v0 ^= m;
        ptr += 8;
    }
    
    // Process remaining bytes
    uint64_t b = data.size() << 56;

    switch (size_t remaining = end - ptr) {
        case 7: b |= static_cast<uint64_t>(ptr[6]) << 48; [[fallthrough]];
        case 6: b |= static_cast<uint64_t>(ptr[5]) << 40; [[fallthrough]];
        case 5: b |= static_cast<uint64_t>(ptr[4]) << 32; [[fallthrough]];
        case 4: b |= static_cast<uint64_t>(ptr[3]) << 24; [[fallthrough]];
        case 3: b |= static_cast<uint64_t>(ptr[2]) << 16; [[fallthrough]];
        case 2: b |= static_cast<uint64_t>(ptr[1]) << 8;  [[fallthrough]];
        case 1: b |= static_cast<uint64_t>(ptr[0]);       [[fallthrough]];
    default: break;
    }
    
    v3 ^= b;
    sipround(v0, v1, v2, v3);
    sipround(v0, v1, v2, v3);
    v0 ^= b;
    
    // Finalization (4 rounds)
    v2 ^= 0xff;
    sipround(v0, v1, v2, v3);
    sipround(v0, v1, v2, v3);
    sipround(v0, v1, v2, v3);
    sipround(v0, v1, v2, v3);
    
    return v0 ^ v1 ^ v2 ^ v3;
}

// ========================================
// Little-endian helpers (inline for performance)
// ========================================

void AKHdr32::write_le_u32(uint8_t* p, const uint32_t v) noexcept {
    if constexpr (std::endian::native == std::endian::little) {
        std::memcpy(p, &v, 4);
    } else {
        p[0] = static_cast<uint8_t>(v & 0xFF);
        p[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
        p[2] = static_cast<uint8_t>((v >> 16) & 0xFF);
        p[3] = static_cast<uint8_t>((v >> 24) & 0xFF);
    }
}

void AKHdr32::write_le_u64(uint8_t* p, const uint64_t v) noexcept {
    if constexpr (std::endian::native == std::endian::little) {
        std::memcpy(p, &v, 8);
    } else {
        for (int i = 0; i < 8; ++i) {
            p[i] = static_cast<uint8_t>((v >> (i * 8)) & 0xFF);
        }
    }
}

uint32_t AKHdr32::read_le_u32(const uint8_t* p) noexcept {
    if constexpr (std::endian::native == std::endian::little) {
        uint32_t v;
        std::memcpy(&v, p, 4);
        return v;
    } else {
        return static_cast<uint32_t>(p[0]) |
               (static_cast<uint32_t>(p[1]) << 8) |
               (static_cast<uint32_t>(p[2]) << 16) |
               (static_cast<uint32_t>(p[3]) << 24);
    }
}

uint64_t AKHdr32::read_le_u64(const uint8_t* p) noexcept {
    if constexpr (std::endian::native == std::endian::little) {
        uint64_t v;
        std::memcpy(&v, p, 8);
        return v;
    } else {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) {
            v |= static_cast<uint64_t>(p[i]) << (i * 8);
        }
        return v;
    }
}

} // namespace akkaradb::common