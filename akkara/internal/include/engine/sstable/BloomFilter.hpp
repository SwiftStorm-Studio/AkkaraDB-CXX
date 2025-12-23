/*
* AkkEngine
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkEngine.
 *
 * AkkEngine is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkEngine is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkEngine.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/include/engine/sstable/BloomFilter.hpp
#pragma once

#include "core/buffer/BufferView.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include <vector>
#include <cstdint>
#include <span>
#include <memory>

namespace akkaradb::engine::sstable {
    /**
 * BloomFilter - Probabilistic membership test.
 *
 * Format ('AKBL', Little-Endian):
 *   [magic:u32 'AKBL']       0x414B424C
 *   [version:u8]             1
 *   [k:u8]                   hash count (1-16)
 *   [padding:u16]            0
 *   [mBits:u32]              bit array size (power-of-2)
 *   [seed:u64]               hashing seed
 *   [bits:u8[mBits/8]]       bitset (as 64-bit words)
 *
 * Algorithm:
 *   Double hashing: idx_i = (h1 + i*h2) & mask
 *   where h1 = mix64(fp64), h2 = mix64(h1 xor seed)
 *
 * Thread-safety: BloomFilter is immutable after construction.
 *               Builder is NOT thread-safe.
 */
    class BloomFilter {
    public:
        static constexpr uint32_t MAGIC = 0x414B424C; // 'AKBL'
        static constexpr uint8_t VERSION = 1;
        static constexpr size_t HEADER_SIZE = 20;

        /**
     * Reads BloomFilter from buffer.
     *
     * @param buffer Input buffer
     * @return BloomFilter instance
     * @throws std::runtime_error if invalid format
     */
        [[nodiscard]] static BloomFilter read_from(core::BufferView buffer);

        /**
     * Tests if key might exist (uses fp64).
     *
     * @param fp64 Key fingerprint (SipHash-2-4)
     * @return true if might exist, false if definitely doesn't exist
     */
        [[nodiscard]] bool might_contain_fp64(uint64_t fp64) const noexcept;

        /**
     * Tests if key might exist.
     *
     * @param key Key bytes
     * @return true if might exist, false if definitely doesn't exist
     */
        [[nodiscard]] bool might_contain(std::span<const uint8_t> key) const noexcept;

        /**
     * Returns bit array size.
     */
        [[nodiscard]] uint32_t bits() const noexcept { return m_bits_; }

        /**
     * Returns hash count.
     */
        [[nodiscard]] uint8_t hash_count() const noexcept { return k_; }

        /**
     * Writes filter to buffer.
     *
     * @return Owned buffer containing serialized filter
     */
        [[nodiscard]] core::OwnedBuffer serialize() const;

        /**
     * Builder for creating BloomFilter.
     */
        class Builder {
        public:
            /**
         * Creates builder with optimal parameters.
         *
         * @param expected_insertions Expected number of keys
         * @param fp_rate False positive rate (default: 0.01)
         * @param seed Hashing seed (default: 0)
         * @return Builder instance
         */
            [[nodiscard]] static Builder create(
                uint64_t expected_insertions,
                double fp_rate = 0.01,
                uint64_t seed = 0
            );

            /**
         * Adds key fingerprint.
         */
            Builder& add_fp64(uint64_t fp64);

            /**
         * Adds key.
         */
            Builder& add_key(std::span<const uint8_t> key);

            /**
         * Builds immutable BloomFilter.
         */
            [[nodiscard]] BloomFilter build();

        private:
            Builder(uint32_t m_bits, uint8_t k, uint64_t seed);

            uint32_t m_bits_;
            uint32_t mask_;
            uint8_t k_;
            uint64_t seed_;
            std::vector<uint64_t> words_;
        };

    private:
        BloomFilter(uint32_t m_bits, uint8_t k, uint64_t seed, std::vector<uint64_t> words);

        static uint64_t mix64(uint64_t x) noexcept;
        static uint64_t fingerprint64(std::span<const uint8_t> key, uint64_t seed);

        uint32_t m_bits_;
        uint32_t mask_;
        uint8_t k_;
        uint64_t seed_;
        std::vector<uint64_t> words_;
    };
} // namespace akkaradb::engine::sstable