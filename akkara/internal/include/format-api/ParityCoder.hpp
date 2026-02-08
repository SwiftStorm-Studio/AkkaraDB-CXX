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

// internal/include/format-api/ParityCoder.hpp
#pragma once

#include "core/buffer/BufferView.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include <vector>
#include <span>
#include <memory>

namespace akkaradb::format {
    /**
 * ParityCoder - Abstract interface for parity encoding/decoding.
 *
 * Implements erasure coding schemes to provide redundancy against block
 * failures. Given k data blocks, generates m parity blocks such that
 * any k blocks from the total k+m can reconstruct all original data.
 *
 * Supported schemes:
 * - m=0: NoParityCoder (no redundancy)
 * - m=1: XorParityCoder (simple XOR parity)
 * - m=2: DualXorParityCoder (two independent XOR parities)
 * - m≥3: ReedSolomonParityCoder (optimal erasure coding)
 *
 * Design principles:
 * - Abstract interface: Allows different parity schemes
 * - In-place operations: Modify buffers directly when possible
 * - Error detection: Verifies parity consistency
 * - Reconstruction: Recovers up to m missing blocks
 *
 * Typical usage:
 * ```cpp
 * auto coder = XorParityCoder::create();  // m=1
 *
 * // Encode
 * std::vector<BufferView> data = {block0, block1, block2, block3};
 * auto parity = coder->encode(data);
 *
 * // Verify
 * if (!coder->verify(data, {parity[0]})) {
 *     // Parity mismatch detected
 * }
 *
 * // Reconstruct (if block1 is corrupted)
 * std::vector<size_t> missing = {1};
 * auto reconstructed = coder->reconstruct(data, {parity[0]}, missing);
 * ```
 *
 * Thread-safety: Implementations may be stateless (thread-safe) or
 * stateful (NOT thread-safe). Check specific implementation docs.
 */
    class ParityCoder {
    public:
        virtual ~ParityCoder() = default;

        /**
     * Encodes parity blocks from data blocks.
     *
     * All data blocks must be the same size. Parity blocks will be
     * the same size as data blocks.
     *
     * @param data_blocks k data blocks (must be same size)
     * @return m parity blocks
     * @throws std::invalid_argument if data blocks have different sizes
     */
        [[nodiscard]] virtual std::vector<core::OwnedBuffer> encode(
            std::span<const core::BufferView> data_blocks
        ) = 0;

        /**
     * Verifies parity consistency.
     *
     * Checks if the parity blocks match the data blocks. Does NOT
     * check individual block CRCs (caller's responsibility).
     *
     * @param data_blocks k data blocks
     * @param parity_blocks m parity blocks
     * @return true if parity is consistent with data
     */
        [[nodiscard]] virtual bool verify(
            std::span<const core::BufferView> data_blocks,
            std::span<const core::BufferView> parity_blocks
        ) const noexcept = 0;

        /**
     * Reconstructs missing or corrupted blocks.
     *
     * Given k+m blocks with up to m missing/corrupted, reconstructs
     * the missing data blocks.
     *
     * @param data_blocks k data blocks (some may be empty/corrupted)
     * @param parity_blocks m parity blocks
     * @param missing_indices Indices of missing data blocks (0..k-1)
     * @return Reconstructed blocks in same order as missing_indices
     * @throws std::invalid_argument if >m blocks missing
     * @throws std::runtime_error if reconstruction fails
     */
        [[nodiscard]] virtual std::vector<core::OwnedBuffer> reconstruct(
            std::span<const core::BufferView> data_blocks,
            std::span<const core::BufferView> parity_blocks,
            std::span<const size_t> missing_indices
        ) = 0;

        /**
     * Returns the number of parity blocks (m).
     */
        [[nodiscard]] virtual size_t parity_count() const noexcept = 0;

        /**
     * Returns a human-readable name of the parity scheme.
     */
        [[nodiscard]] virtual const char* name() const noexcept = 0;
    };

    /**
 * NoParityCoder - No redundancy (m=0).
 *
 * Used when parity is disabled. Encode/verify/reconstruct are no-ops.
 *
 * Thread-safety: Fully thread-safe (stateless).
 */
    class NoParityCoder : public ParityCoder {
    public:
        [[nodiscard]] static std::unique_ptr<NoParityCoder> create();

        [[nodiscard]] std::vector<core::OwnedBuffer> encode(
            std::span<const core::BufferView> data_blocks
        ) override;

        [[nodiscard]] bool verify(
            std::span<const core::BufferView> data_blocks,
            std::span<const core::BufferView> parity_blocks
        ) const noexcept override;

        [[nodiscard]] std::vector<core::OwnedBuffer> reconstruct(
            std::span<const core::BufferView> data_blocks,
            std::span<const core::BufferView> parity_blocks,
            std::span<const size_t> missing_indices
        ) override;

        [[nodiscard]] size_t parity_count() const noexcept override { return 0; }
        [[nodiscard]] const char* name() const noexcept override { return "NoParity"; }
    };

    /**
 * XorParityCoder - Simple XOR parity (m=1).
 *
 * Computes parity as XOR of all data blocks. Can recover from a single
 * block failure.
 *
 * Algorithm:
 * - Encode: P = D[0] ⊕ D[1] ⊕ ... ⊕ D[k-1]
 * - Reconstruct: D[i] = D[0] ⊕ ... ⊕ D[i-1] ⊕ D[i+1] ⊕ ... ⊕ D[k-1] ⊕ P
 *
 * Thread-safety: Fully thread-safe (stateless).
 */
    class XorParityCoder : public ParityCoder {
    public:
        [[nodiscard]] static std::unique_ptr<XorParityCoder> create();

        [[nodiscard]] std::vector<core::OwnedBuffer> encode(
            std::span<const core::BufferView> data_blocks
        ) override;

        [[nodiscard]] bool verify(
            std::span<const core::BufferView> data_blocks,
            std::span<const core::BufferView> parity_blocks
        ) const noexcept override;

        [[nodiscard]] std::vector<core::OwnedBuffer> reconstruct(
            std::span<const core::BufferView> data_blocks,
            std::span<const core::BufferView> parity_blocks,
            std::span<const size_t> missing_indices
        ) override;

        [[nodiscard]] size_t parity_count() const noexcept override { return 1; }
        [[nodiscard]] const char* name() const noexcept override { return "XOR"; }
    };

    /**
 * DualXorParityCoder - Dual XOR parity (m=2).
 *
 * Uses two independent XOR parities computed over different subsets
 * of data blocks. Can recover from up to 2 block failures.
 *
 * Algorithm:
 * - P0 = D[0] ⊕ D[2] ⊕ D[4] ⊕ ... (even indices)
 * - P1 = D[1] ⊕ D[3] ⊕ D[5] ⊕ ... (odd indices)
 *
 * Thread-safety: Fully thread-safe (stateless).
 */
    class DualXorParityCoder : public ParityCoder {
    public:
        [[nodiscard]] static std::unique_ptr<DualXorParityCoder> create();

        [[nodiscard]] std::vector<core::OwnedBuffer> encode(
            std::span<const core::BufferView> data_blocks
        ) override;

        [[nodiscard]] bool verify(
            std::span<const core::BufferView> data_blocks,
            std::span<const core::BufferView> parity_blocks
        ) const noexcept override;

        [[nodiscard]] std::vector<core::OwnedBuffer> reconstruct(
            std::span<const core::BufferView> data_blocks,
            std::span<const core::BufferView> parity_blocks,
            std::span<const size_t> missing_indices
        ) override;

        [[nodiscard]] size_t parity_count() const noexcept override { return 2; }
        [[nodiscard]] const char* name() const noexcept override { return "DualXOR"; }
    };

    /**
 * ReedSolomonParityCoder - Reed-Solomon erasure coding (m≥3).
 *
 * Uses Reed-Solomon codes for optimal erasure correction. Can recover
 * from up to m block failures. More computationally expensive than XOR
 * but provides stronger guarantees.
 *
 * Implementation uses Vandermonde matrix over GF(2^8).
 *
 * Thread-safety: NOT thread-safe (uses internal state for Galois field ops).
 * Create separate instances for concurrent use.
 */
    class ReedSolomonParityCoder : public ParityCoder {
    public:
        /**
     * Creates a Reed-Solomon coder.
     *
     * @param m Number of parity blocks (must be ≥ 3)
     * @throws std::invalid_argument if m < 3
     */
        [[nodiscard]] static std::unique_ptr<ReedSolomonParityCoder> create(size_t m);

        [[nodiscard]] std::vector<core::OwnedBuffer> encode(
            std::span<const core::BufferView> data_blocks
        ) override;

        [[nodiscard]] bool verify(
            std::span<const core::BufferView> data_blocks,
            std::span<const core::BufferView> parity_blocks
        ) const noexcept override;

        [[nodiscard]] std::vector<core::OwnedBuffer> reconstruct(
            std::span<const core::BufferView> data_blocks,
            std::span<const core::BufferView> parity_blocks,
            std::span<const size_t> missing_indices
        ) override;

        [[nodiscard]] size_t parity_count() const noexcept override;
        [[nodiscard]] const char* name() const noexcept override { return "ReedSolomon"; }

        ~ReedSolomonParityCoder() override;

    private:
        ReedSolomonParityCoder(size_t m);

        class Impl;
        std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::format