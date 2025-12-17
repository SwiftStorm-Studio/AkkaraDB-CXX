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

// internal/include/format-akk/parity/DualXorParityCoder.hpp
#pragma once

#include "format-api/ParityCoder.hpp"

namespace akkaradb::format::akk {
    /**
     * DualXorParityCoder - Implementation of ParityCoder using dual XOR parity (m=2).
     *
     * Computes two independent XOR parities over different subsets:
     *   P0 = D[0] ⊕ D[2] ⊕ D[4] ⊕ ... (even indices)
     *   P1 = D[1] ⊕ D[3] ⊕ D[5] ⊕ ... (odd indices)
     *
     * Can recover from up to 2 block failures:
     * - If 1 block missing: Use corresponding parity
     * - If 2 blocks missing: Use both parities
     *
     * Thread-safety: Fully thread-safe (stateless).
     */
    class DualXorParityCoderImpl : public DualXorParityCoder {
    public:
        DualXorParityCoderImpl() = default;
        ~DualXorParityCoderImpl() override = default;

        /**
         * Computes XOR of blocks at even indices.
         *
         * @param blocks Input blocks
         * @return XOR of D[0] ⊕ D[2] ⊕ D[4] ⊕ ...
         */
        [[nodiscard]] static core::OwnedBuffer compute_even_xor(
            std::span<const core::BufferView> blocks
        );

        /**
         * Computes XOR of blocks at odd indices.
         *
         * @param blocks Input blocks
         * @return XOR of D[1] ⊕ D[3] ⊕ D[5] ⊕ ...
         */
        [[nodiscard]] static core::OwnedBuffer compute_odd_xor(
            std::span<const core::BufferView> blocks
        );

        /**
         * XORs source into destination (in-place).
         *
         * @param dst Destination buffer (modified)
         * @param src Source buffer
         */
        static void xor_into(core::BufferView dst, core::BufferView src) noexcept;
    };
} // namespace akkaradb::format::akk