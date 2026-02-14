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

// internal/src/format-akk/parity/DualXorParityCoder.cpp
#include "format-akk/parity/DualXorParityCoder.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include <stdexcept>
#include <cstring>
#include <algorithm>

namespace akkaradb::format {
    std::unique_ptr<DualXorParityCoder> DualXorParityCoder::create() { return std::make_unique<akk::DualXorParityCoderImpl>(); }

    std::vector<core::OwnedBuffer> DualXorParityCoder::encode(std::span<const core::BufferView> data_blocks) {
        if (data_blocks.empty()) { throw std::invalid_argument("DualXorParityCoder::encode: no data blocks"); }

        // Compute P0 (even indices) and P1 (odd indices)
        auto p0 = akk::DualXorParityCoderImpl::compute_even_xor(data_blocks);
        auto p1 = akk::DualXorParityCoderImpl::compute_odd_xor(data_blocks);

        std::vector<core::OwnedBuffer> result;
        result.push_back(std::move(p0));
        result.push_back(std::move(p1));
        return result;
    }

    bool DualXorParityCoder::verify(std::span<const core::BufferView> data_blocks, std::span<const core::BufferView> parity_blocks) const noexcept {
        if (data_blocks.empty() || parity_blocks.size() != 2) { return false; }

        try {
            // Recompute both parities
            auto expected_p0 = akk::DualXorParityCoderImpl::compute_even_xor(data_blocks);
            auto expected_p1 = akk::DualXorParityCoderImpl::compute_odd_xor(data_blocks);

            // Compare P0
            if (expected_p0.size() != parity_blocks[0].size() || std::memcmp(expected_p0.data(), parity_blocks[0].data(), expected_p0.size()) != 0) {
                return false;
            }

            // Compare P1
            if (expected_p1.size() != parity_blocks[1].size() || std::memcmp(expected_p1.data(), parity_blocks[1].data(), expected_p1.size()) != 0) {
                return false;
            }

            return true;
        }
        catch (...) { return false; }
    }

    std::vector<core::OwnedBuffer> DualXorParityCoder::reconstruct(
        std::span<const core::BufferView> data_blocks,
        std::span<const core::BufferView> parity_blocks,
        std::span<const size_t> missing_indices
    ) {
        if (missing_indices.empty()) { return {}; }

        if (missing_indices.size() > 2) { throw std::invalid_argument("DualXorParityCoder: can only recover up to 2 missing blocks"); }

        if (parity_blocks.size() != 2) { throw std::invalid_argument("DualXorParityCoder: expected 2 parity blocks"); }

        // Validate indices
        for (const size_t idx : missing_indices) {
            if (idx >= data_blocks.size()) { throw std::out_of_range("DualXorParityCoder: missing index out of range"); }
        }

        const auto& p0 = parity_blocks[0];
        const auto& p1 = parity_blocks[1];

        std::vector<core::OwnedBuffer> reconstructed;

        if (missing_indices.size() == 1) {
            // Single block missing: use appropriate parity
            const size_t missing_idx = missing_indices[0];
            const bool is_even = (missing_idx % 2) == 0;

            auto result = core::OwnedBuffer::allocate(p0.size(), 4096);
            if (is_even) {
                // Reconstruct from P0 (even parity)
                std::memcpy(result.data(), p0.data(), p0.size());
                auto result_view = result.view();

                // XOR with all other even-indexed blocks
                for (size_t i = 0; i < data_blocks.size(); i += 2) {
                    if (i != missing_idx) { akk::DualXorParityCoderImpl::xor_into(result_view, data_blocks[i]); }
                }
            }
            else {
                // Reconstruct from P1 (odd parity)
                std::memcpy(result.data(), p1.data(), p1.size());
                auto result_view = result.view();

                // XOR with all other odd-indexed blocks
                for (size_t i = 1; i < data_blocks.size(); i += 2) {
                    if (i != missing_idx) { akk::DualXorParityCoderImpl::xor_into(result_view, data_blocks[i]); }
                }
            }

            reconstructed.push_back(std::move(result));
        }
        else {
            // Two blocks missing
            const size_t idx0 = missing_indices[0];
            const size_t idx1 = missing_indices[1];

            const bool idx0_even = (idx0 % 2) == 0;

            if (const bool idx1_even = (idx1 % 2) == 0; idx0_even == idx1_even) {
                throw std::runtime_error("DualXorParityCoder: cannot recover two blocks from same parity group");
            }

            // Reconstruct each missing block from its parity
            for (const size_t missing_idx : missing_indices) {
                const bool is_even = (missing_idx % 2) == 0;

                auto result = core::OwnedBuffer::allocate(p0.size(), 4096);
                if (is_even) {
                    std::memcpy(result.data(), p0.data(), p0.size());
                    auto result_view = result.view();

                    for (size_t i = 0; i < data_blocks.size(); i += 2) {
                        if (i != missing_idx) { akk::DualXorParityCoderImpl::xor_into(result_view, data_blocks[i]); }
                    }
                }
                else {
                    std::memcpy(result.data(), p1.data(), p1.size());
                    auto result_view = result.view();

                    for (size_t i = 1; i < data_blocks.size(); i += 2) {
                        if (i != missing_idx) { akk::DualXorParityCoderImpl::xor_into(result_view, data_blocks[i]); }
                    }
                }

                reconstructed.push_back(std::move(result));
            }
        }

        return reconstructed;
    }
} // namespace akkaradb::format

namespace akkaradb::format::akk {
    core::OwnedBuffer DualXorParityCoderImpl::compute_even_xor(std::span<const core::BufferView> blocks) {
        if (blocks.empty()) { throw std::invalid_argument("compute_even_xor: no blocks provided"); }

        const size_t block_size = blocks[0].size();
        auto result = core::OwnedBuffer::allocate(block_size, 4096);
        result.zero_fill();

        // XOR blocks at even indices (0, 2, 4, ...)
        auto result_view = result.view();
        for (size_t i = 0; i < blocks.size(); i += 2) {
            if (blocks[i].size() != block_size) { throw std::invalid_argument("compute_even_xor: blocks have different sizes"); }
            xor_into(result_view, blocks[i]);
        }

        return result;
    }

    core::OwnedBuffer DualXorParityCoderImpl::compute_odd_xor(std::span<const core::BufferView> blocks) {
        if (blocks.size() < 2) {
            // No odd-indexed blocks
            const size_t block_size = blocks.empty()
                                          ? 0
                                          : blocks[0].size();
            auto result = core::OwnedBuffer::allocate(block_size, 4096);
            result.zero_fill();
            return result;
        }

        const size_t block_size = blocks[0].size();
        auto result = core::OwnedBuffer::allocate(block_size, 4096);
        result.zero_fill();

        // XOR blocks at odd indices (1, 3, 5, ...)
        auto result_view = result.view();
        for (size_t i = 1; i < blocks.size(); i += 2) {
            if (blocks[i].size() != block_size) { throw std::invalid_argument("compute_odd_xor: blocks have different sizes"); }
            xor_into(result_view, blocks[i]);
        }

        return result;
    }

    void DualXorParityCoderImpl::xor_into(core::BufferView dst, core::BufferView src) noexcept {
        if (dst.size() != src.size()) { return; }

        const size_t count = dst.size() / sizeof(uint64_t);

        // XOR in 64-bit chunks (using memcpy for alignment safety)
        for (size_t i = 0; i < count; ++i) {
            uint64_t dst_val, src_val;
            std::memcpy(&dst_val, dst.data() + i * sizeof(uint64_t), sizeof(uint64_t));
            std::memcpy(&src_val, src.data() + i * sizeof(uint64_t), sizeof(uint64_t));
            dst_val ^= src_val;
            std::memcpy(dst.data() + i * sizeof(uint64_t), &dst_val, sizeof(uint64_t));
        }

        // Handle remaining bytes
        if (const size_t remaining = dst.size() % sizeof(uint64_t); remaining > 0) {
            const size_t offset = count * sizeof(uint64_t);
            auto* dst_bytes = dst.data() + offset;
            const auto* src_bytes = src.data() + offset;

            for (size_t i = 0; i < remaining; ++i) {
                dst_bytes[i] = static_cast<std::byte>(static_cast<uint8_t>(dst_bytes[i]) ^ static_cast<uint8_t>(src_bytes[i]));
            }
        }
    }
} // namespace akkaradb::format::akk