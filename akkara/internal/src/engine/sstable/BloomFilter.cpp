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
#include <array>

namespace akkaradb::engine::sstable {
    namespace {
        /**
 * SipHash-2-4 with custom seed (for Bloom filter).
 */
        class SipHash24 {
        public:
            explicit SipHash24(uint64_t seed) noexcept {
                v0_ = 0x736f6d6570736575ULL ^ seed;
                v1_ = 0x646f72616e646f6dULL ^ seed;
                v2_ = 0x6c7967656e657261ULL ^ seed;
                v3_ = 0x7465646279746573ULL ^ seed;
            }

            void update(const uint8_t* data, size_t len) noexcept {
                const auto* ptr = data;
                const auto* end = data + len;

                // Process 8-byte chunks
                while (ptr + 8 <= end) {
                    uint64_t m;
                    std::memcpy(&m, ptr, 8);
                    compress(m);
                    ptr += 8;
                }

                // Handle remaining bytes
                tail_len_ = end - ptr;
                std::memcpy(tail_.data(), ptr, tail_len_);
            }

            [[nodiscard]] uint64_t finalize(size_t total_len) noexcept {
                // Pad tail
                uint64_t b = total_len << 56;
                for (size_t i = 0; i < tail_len_; ++i) { b |= static_cast<uint64_t>(tail_[i]) << (i * 8); }
                compress(b);

                v2_ ^= 0xff;
                for (int i = 0; i < 4; ++i) { round(); }

                return v0_ ^ v1_ ^ v2_ ^ v3_;
            }

        private:
            void compress(uint64_t m) noexcept {
                v3_ ^= m;
                for (int i = 0; i < 2; ++i) { round(); }
                v0_ ^= m;
            }

            void round() noexcept {
                v0_ += v1_;
                v1_ = rotl(v1_, 13);
                v1_ ^= v0_;
                v0_ = rotl(v0_, 32);

                v2_ += v3_;
                v3_ = rotl(v3_, 16);
                v3_ ^= v2_;

                v0_ += v3_;
                v3_ = rotl(v3_, 21);
                v3_ ^= v0_;

                v2_ += v1_;
                v1_ = rotl(v1_, 17);
                v1_ ^= v2_;
                v2_ = rotl(v2_, 32);
            }

            static constexpr uint64_t rotl(uint64_t x, int b) noexcept { return (x << b) | (x >> (64 - b)); }

            uint64_t v0_, v1_, v2_, v3_;
            std::array<uint8_t, 8> tail_{};
            size_t tail_len_{0};
        };

        /**
 * Round up to next power of 2.
 */
        uint32_t round_up_pow2(uint64_t x) {
            if (x == 0) return 1;
            uint64_t v = x - 1;
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            return static_cast<uint32_t>(v + 1);
        }

        /**
 * Check if power of 2.
 */
        bool is_pow2(uint32_t x) { return x > 0 && (x & (x - 1)) == 0; }

        constexpr double LN2 = 0.6931471805599453;
        constexpr double LN2_SQ = LN2 * LN2;

        /**
 * Calculate optimal bit array size.
 */
        uint64_t optimal_bits(uint64_t n, double p) { return static_cast<uint64_t>(std::ceil(-static_cast<double>(n) * std::log(p) / LN2_SQ)); }

        /**
 * Calculate optimal hash count.
 */
        uint8_t optimal_hashes(uint32_t m_bits, uint64_t n) {
            const double k = (static_cast<double>(m_bits) / static_cast<double>(n)) * LN2;
            return static_cast<uint8_t>((std::max)(2.0, std::round(k)));
        }
    } // anonymous namespace

    // ==================== BloomFilter ====================

    BloomFilter::BloomFilter(
        uint32_t m_bits,
        uint8_t k,
        uint64_t seed,
        std::vector<uint64_t> words
    ) : m_bits_{m_bits}
        , mask_{m_bits - 1}
        , k_{k}
        , seed_{seed}
        , words_{std::move(words)} {}

    BloomFilter BloomFilter::read_from(core::BufferView buffer) {
        if (buffer.size() < HEADER_SIZE) { throw std::runtime_error("BloomFilter: buffer too small for header"); }

        const auto* data = reinterpret_cast<const uint8_t*>(buffer.data());

        // Verify magic
        uint32_t magic;
        std::memcpy(&magic, data, 4);
        if (magic != MAGIC) { throw std::runtime_error("BloomFilter: invalid magic"); }

        // Verify version
        const uint8_t version = data[4];
        if (version != VERSION) { throw std::runtime_error("BloomFilter: unsupported version"); }

        // Read k
        const uint8_t k = data[5];
        if (k < 1 || k > 16) { throw std::runtime_error("BloomFilter: invalid k"); }

        // Read mBits
        uint32_t m_bits;
        std::memcpy(&m_bits, data + 8, 4);
        if (!is_pow2(m_bits)) { throw std::runtime_error("BloomFilter: mBits must be power-of-2"); }

        // Read seed
        uint64_t seed;
        std::memcpy(&seed, data + 12, 8);

        // Read bits
        const size_t body_bytes = m_bits / 8;
        if (buffer.size() < HEADER_SIZE + body_bytes) { throw std::runtime_error("BloomFilter: buffer too small for bits"); }

        std::vector<uint64_t> words(body_bytes / 8);
        const uint8_t* bits_data = data + HEADER_SIZE;
        for (size_t i = 0; i < words.size(); ++i) { std::memcpy(&words[i], bits_data + i * 8, 8); }

        return BloomFilter{m_bits, k, seed, std::move(words)};
    }

    bool BloomFilter::might_contain_fp64(uint64_t fp64) const noexcept {
        if (m_bits_ == 0 || k_ == 0) { return false; }

        const uint64_t h1 = mix64(fp64);
        const uint64_t h2 = mix64(h1 ^ seed_);

        for (uint8_t i = 0; i < k_; ++i) {
            const uint32_t idx = static_cast<uint32_t>((h1 + i * h2)) & mask_;
            const size_t word_idx = idx >> 6;
            const uint64_t bit = 1ULL << (idx & 63);

            if ((words_[word_idx] & bit) == 0) { return false; }
        }

        return true;
    }

    bool BloomFilter::might_contain(std::span<const uint8_t> key) const noexcept {
        const uint64_t fp64 = fingerprint64(key, seed_);
        return might_contain_fp64(fp64);
    }

    core::OwnedBuffer BloomFilter::serialize() const {
        const size_t body_bytes = m_bits_ / 8;
        const size_t total_size = HEADER_SIZE + body_bytes;

        auto buffer = core::OwnedBuffer::allocate(total_size);
        auto* data = reinterpret_cast<uint8_t*>(buffer.view().data());

        // Write header
        std::memcpy(data, &MAGIC, 4);
        data[4] = VERSION;
        data[5] = k_;
        data[6] = 0; // padding
        data[7] = 0; // padding
        std::memcpy(data + 8, &m_bits_, 4);
        std::memcpy(data + 12, &seed_, 8);

        // Write bits
        uint8_t* bits_data = data + HEADER_SIZE;
        for (size_t i = 0; i < words_.size(); ++i) { std::memcpy(bits_data + i * 8, &words_[i], 8); }

        return buffer;
    }

    uint64_t BloomFilter::mix64(uint64_t x) noexcept {
        uint64_t z = x + 0x9E3779B97F4A7C15ULL;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31);
    }

    uint64_t BloomFilter::fingerprint64(std::span<const uint8_t> key, uint64_t seed) {
        SipHash24 hasher{seed};
        hasher.update(key.data(), key.size());
        return hasher.finalize(key.size());
    }

    // ==================== BloomFilter::Builder ====================

    BloomFilter::Builder BloomFilter::Builder::create(
        uint64_t expected_insertions,
        double fp_rate,
        uint64_t seed
    ) {
        if (expected_insertions == 0) { throw std::invalid_argument("BloomFilter: expected_insertions must be > 0"); }
        if (fp_rate <= 0.0 || fp_rate >= 0.5) { throw std::invalid_argument("BloomFilter: fp_rate out of range"); }

        const uint64_t raw_bits = optimal_bits(expected_insertions, fp_rate);
        const uint32_t m_bits = round_up_pow2(raw_bits);
        uint8_t k = optimal_hashes(m_bits, expected_insertions);

        // Clamp k to [1, 16]
        if (k < 1) k = 1;
        if (k > 16) k = 16;

        return Builder{m_bits, k, seed};
    }

    BloomFilter::Builder::Builder(uint32_t m_bits, uint8_t k, uint64_t seed) : m_bits_{m_bits}
                                                                               , mask_{m_bits - 1}
                                                                               , k_{k}
                                                                               , seed_{seed}
                                                                               , words_(m_bits / 64, 0) {}

    BloomFilter::Builder& BloomFilter::Builder::add_fp64(uint64_t fp64) {
        const uint64_t h1 = mix64(fp64);
        const uint64_t h2 = mix64(h1 ^ seed_);

        for (uint8_t i = 0; i < k_; ++i) {
            const uint32_t idx = static_cast<uint32_t>((h1 + i * h2)) & mask_;
            const size_t word_idx = idx >> 6;
            const uint64_t bit = 1ULL << (idx & 63);
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