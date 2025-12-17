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

// internal/include/format-akk/parity/XorParityCoder.hpp
#pragma once

#include "format-api/ParityCoder.hpp"

namespace akkaradb::format::akk {
    /**
     * XorParityCoder - Implementation of ParityCoder using simple XOR parity (m=1).
     *
     * Computes parity as bitwise XOR of all data blocks:
     *   P = D[0] ⊕ D[1] ⊕ ... ⊕ D[k-1]
     *
     * Can recover from a single block failure by XORing all remaining blocks:
     *   D[i] = D[0] ⊕ ... ⊕ D[i-1] ⊕ D[i+1] ⊕ ... ⊕ D[k-1] ⊕ P
     *
     * Thread-safety: Fully thread-safe (stateless).
     */
    class XorParityCoderImpl : public XorParityCoder {
    public:
        XorParityCoderImpl() = default;
        ~XorParityCoderImpl() override = default;

        /**
         * Computes XOR of all blocks.
         *
         * @param blocks Input blocks (must all be same size)
         * @return XOR result
         */
        [[nodiscard]] static core::OwnedBuffer compute_xor(
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