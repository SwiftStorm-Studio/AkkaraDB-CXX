/*
* AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/src/engine/sstable/BloomFilter.cpp
#include "engine/sstable/BloomFilter.hpp"
#include <stdexcept>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <bit>

namespace akkaradb::engine::sstable {
    namespace {
        // ---------------------------------------------------------------------------
        // SipHash-2-4 — flat single-function implementation.
        //
        // Key schedule matches AKHdr32.sipHash24() in the JVM codebase:
        //   k0 = seed
        //   k1 = seed XOR 0x9E3779B97F4A7C15   (golden ratio phi)
        //
        // This makes fingerprint64() produce identical values to the JVM's
        // BloomFilter.addKey() path, enabling cross-language SST compatibility.
        // ---------------------------------------------------------------------------

        [[nodiscard]] constexpr uint64_t rotl64(uint64_t x, int b) noexcept { return (x << b) | (x >> (64 - b)); }

        #define SIPHASH_ROUND(v0, v1, v2, v3) \
    do {                               \
        v0 += v1; v2 += v3;            \
        v1 = rotl64(v1, 13); v3 = rotl64(v3, 16); \
        v1 ^= v0;            v3 ^= v2;             \
        v0 = rotl64(v0, 32);                       \
        v2 += v1; v0 += v3;                        \
        v1 = rotl64(v1, 17); v3 = rotl64(v3, 21); \
        v1 ^= v2;            v3 ^= v0;             \
        v2 = rotl64(v2, 32);                       \
    } while (0)

        // SipHash-2-4: compatible with AKHdr32.sipHash24(key, seed64).
        [[nodiscard]] uint64_t siphash24(const uint8_t* data, size_t len, uint64_t seed) noexcept {
            // Key schedule matching JVM: k1 = seed ^ phi
            const uint64_t k0 = seed;
            const uint64_t k1 = seed ^ 0x9E3779B97F4A7C15ULL;

            uint64_t v0 = UINT64_C(0x736f6d6570736575) ^ k0;
            uint64_t v1 = UINT64_C(0x646f72616e646f6d) ^ k1;
            uint64_t v2 = UINT64_C(0x6c7967656e657261) ^ k0;
            uint64_t v3 = UINT64_C(0x7465646279746573) ^ k1;

            const uint8_t* p = data;
            const uint8_t* end = data + (len & ~size_t{7}); // round down to 8-byte boundary

            while (p != end) {
                uint64_t m;
                std::memcpy(&m, p, 8);
                v3 ^= m;
                SIPHASH_ROUND(v0, v1, v2, v3);
                SIPHASH_ROUND(v0, v1, v2, v3);
                v0 ^= m;
                p += 8;
            }

            // Tail: pack remaining bytes into b (LE), length in top byte.
            uint64_t b = static_cast<uint64_t>(len) << 56;
            switch (len & 7u) {
                case 7: b |= static_cast<uint64_t>(p[6]) << 48;
                    [[fallthrough]];
                case 6: b |= static_cast<uint64_t>(p[5]) << 40;
                    [[fallthrough]];
                case 5: b |= static_cast<uint64_t>(p[4]) << 32;
                    [[fallthrough]];
                case 4: b |= static_cast<uint64_t>(p[3]) << 24;
                    [[fallthrough]];
                case 3: b |= static_cast<uint64_t>(p[2]) << 16;
                    [[fallthrough]];
                case 2: b |= static_cast<uint64_t>(p[1]) << 8;
                    [[fallthrough]];
                case 1: b |= static_cast<uint64_t>(p[0]);
                    [[fallthrough]];
                default: break;
            }

            v3 ^= b;
            SIPHASH_ROUND(v0, v1, v2, v3);
            SIPHASH_ROUND(v0, v1, v2, v3);
            v0 ^= b;

            v2 ^= 0xff;
            SIPHASH_ROUND(v0, v1, v2, v3);
            SIPHASH_ROUND(v0, v1, v2, v3);
            SIPHASH_ROUND(v0, v1, v2, v3);
            SIPHASH_ROUND(v0, v1, v2, v3);

            return v0 ^ v1 ^ v2 ^ v3;
        }

        #undef SIPHASH_ROUND

        // ---------------------------------------------------------------------------
        // Bit-array size helpers.
        // ---------------------------------------------------------------------------

        [[nodiscard]] uint32_t round_up_pow2_min64(uint64_t x) noexcept {
            // Minimum 64 bits so that words_.size() >= 1 always holds.
            if (x <= 64) return 64;
            const uint64_t v = std::bit_ceil(x);
            constexpr uint64_t MAX_BITS = 1ULL << 26; // 64 MiB cap
            return static_cast<uint32_t>((std::min)(v, MAX_BITS));
        }

        [[nodiscard]] bool is_pow2(uint32_t x) noexcept { return x > 0 && (x & (x - 1)) == 0; }

        constexpr double LN2 = 0.6931471805599453;
        constexpr double LN2_SQ = LN2 * LN2;

        [[nodiscard]] uint64_t optimal_bits(uint64_t n, double p) noexcept {
            return static_cast<uint64_t>(std::ceil(-static_cast<double>(n) * std::log(p) / LN2_SQ));
        }

        [[nodiscard]] uint8_t optimal_hashes(uint32_t m_bits, uint64_t n) noexcept {
            if (n == 0) return 1;
            const double k = (static_cast<double>(m_bits) / static_cast<double>(n)) * LN2;
            return static_cast<uint8_t>((std::max)(2.0, (std::min)(16.0, std::round(k))));
        }
    } // anonymous namespace

    // ==================== BloomFilter ====================

    BloomFilter::BloomFilter(uint32_t m_bits, uint8_t k, uint64_t seed, std::vector<uint64_t> words)
        : m_bits_{m_bits}, mask_{m_bits - 1}, k_{k}, seed_{seed}, words_{std::move(words)} {}

    BloomFilter BloomFilter::read_from(core::BufferView buffer) {
        if (buffer.size() < HEADER_SIZE) { throw std::runtime_error("BloomFilter: buffer too small for header"); }
        const auto* data = reinterpret_cast<const uint8_t*>(buffer.data());

        uint32_t magic;
        std::memcpy(&magic, data, 4);
        if (magic != MAGIC) { throw std::runtime_error("BloomFilter: invalid magic"); }

        const uint8_t version = data[4];
        if (version != VERSION) { throw std::runtime_error("BloomFilter: unsupported version"); }

        const uint8_t k = data[5];
        if (k < 1 || k > 16) { throw std::runtime_error("BloomFilter: invalid k"); }

        uint32_t m_bits;
        std::memcpy(&m_bits, data + 8, 4);
        if (!is_pow2(m_bits) || m_bits < 64) { throw std::runtime_error("BloomFilter: mBits must be power-of-2 >= 64"); }

        uint64_t seed;
        std::memcpy(&seed, data + 12, 8);

        const size_t body_bytes = m_bits / 8;
        if (buffer.size() < HEADER_SIZE + body_bytes) { throw std::runtime_error("BloomFilter: buffer too small for bits"); }

        std::vector<uint64_t> words(body_bytes / 8);
        const uint8_t* bits_data = data + HEADER_SIZE;
        for (size_t i = 0; i < words.size(); ++i) { std::memcpy(&words[i], bits_data + i * 8, 8); }

        return BloomFilter{m_bits, k, seed, std::move(words)};
    }

    bool BloomFilter::might_contain_fp64(uint64_t fp64) const noexcept {
        if (words_.empty()) { return true; } // Defensive: empty filter is a pass-through.

        const uint64_t h1 = mix64(fp64);
        const uint64_t h2 = mix64(h1 ^ seed_);

        for (uint8_t i = 0; i < k_; ++i) {
            const uint32_t idx = static_cast<uint32_t>(h1 + static_cast<uint64_t>(i) * h2) & mask_;
            const size_t word_idx = idx >> 6u;
            const uint64_t bit = UINT64_C(1) << (idx & 63u);
            if ((words_[word_idx] & bit) == 0) { return false; }
        }
        return true;
    }

    bool BloomFilter::might_contain(std::span<const uint8_t> key) const noexcept {
        if (words_.empty()) { return true; }
        const uint64_t fp64 = fingerprint64(key, seed_);
        return might_contain_fp64(fp64);
    }

    core::OwnedBuffer BloomFilter::serialize() const {
        const size_t body_bytes = m_bits_ / 8;
        const size_t total_size = HEADER_SIZE + body_bytes;

        auto buffer = core::OwnedBuffer::allocate(total_size);
        auto* data = reinterpret_cast<uint8_t*>(buffer.view().data());

        std::memcpy(data, &MAGIC, 4);
        data[4] = VERSION;
        data[5] = k_;
        data[6] = 0;
        data[7] = 0;
        std::memcpy(data + 8, &m_bits_, 4);
        std::memcpy(data + 12, &seed_, 8);

        uint8_t* bits_data = data + HEADER_SIZE;
        for (size_t i = 0; i < words_.size(); ++i) { std::memcpy(bits_data + i * 8, &words_[i], 8); }
        return buffer;
    }

    uint64_t BloomFilter::mix64(uint64_t x) noexcept {
        // Murmur3/Stafford finalizer — used for double-hashing only (not key fingerprint).
        uint64_t z = x + UINT64_C(0x9E3779B97F4A7C15);
        z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
        return z ^ (z >> 31);
    }

    uint64_t BloomFilter::fingerprint64(std::span<const uint8_t> key, uint64_t seed) noexcept {
        // SipHash-2-4 with correct key schedule (matches JVM AKHdr32.sipHash24).
        return siphash24(key.data(), key.size(), seed);
    }

    // ==================== BloomFilter::Builder ====================

    BloomFilter::Builder BloomFilter::Builder::create(uint64_t expected_insertions, double fp_rate, uint64_t seed) {
        if (expected_insertions == 0) { expected_insertions = 1; }
        if (fp_rate <= 0.0 || fp_rate >= 0.5) { throw std::invalid_argument("BloomFilter: fp_rate out of range"); }

        const uint64_t raw_bits = optimal_bits(expected_insertions, fp_rate);
        const uint32_t m_bits = round_up_pow2_min64(raw_bits);
        const uint8_t k = optimal_hashes(m_bits, expected_insertions);

        return Builder{m_bits, k, seed};
    }

    BloomFilter::Builder::Builder(uint32_t m_bits, uint8_t k, uint64_t seed)
        : m_bits_{m_bits}, mask_{m_bits - 1}, k_{k}, seed_{seed}, words_(m_bits / 64, 0) {
        // words_.size() == m_bits/64 >= 1, guaranteed by round_up_pow2_min64.
    }

    BloomFilter::Builder& BloomFilter::Builder::add_fp64(uint64_t fp64) {
        if (words_.empty()) { return *this; }

        const uint64_t h1 = mix64(fp64);
        const uint64_t h2 = mix64(h1 ^ seed_);

        for (uint8_t i = 0; i < k_; ++i) {
            const uint32_t idx = static_cast<uint32_t>(h1 + static_cast<uint64_t>(i) * h2) & mask_;
            const size_t word_idx = idx >> 6u;
            const uint64_t bit = UINT64_C(1) << (idx & 63u);
            words_[word_idx] |= bit;
        }
        return *this;
    }

    BloomFilter::Builder& BloomFilter::Builder::add_key(std::span<const uint8_t> key) {
        const uint64_t fp64 = fingerprint64(key, seed_);
        return add_fp64(fp64);
    }

    BloomFilter BloomFilter::Builder::build() { return BloomFilter{m_bits_, k_, seed_, std::move(words_)}; }
} // namespace akkaradb::engine::sstable