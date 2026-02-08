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

// internal/src/format-akk/parity/XorParityCoder.cpp
#include "format-akk/parity/XorParityCoder.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include <stdexcept>
#include <cstring>

namespace akkaradb::format {
    std::unique_ptr<XorParityCoder> XorParityCoder::create() { return std::make_unique<akk::XorParityCoderImpl>(); }

    std::vector<core::OwnedBuffer> XorParityCoder::encode(
        std::span<const core::BufferView> data_blocks
    ) {
        if (data_blocks.empty()) { throw std::invalid_argument("XorParityCoder::encode: no data blocks"); }

        // Compute XOR of all data blocks
        auto parity = akk::XorParityCoderImpl::compute_xor(data_blocks);

        std::vector<core::OwnedBuffer> result;
        result.push_back(std::move(parity));
        return result;
    }

    bool XorParityCoder::verify(
        std::span<const core::BufferView> data_blocks,
        std::span<const core::BufferView> parity_blocks
    ) const noexcept {
        if (data_blocks.empty() || parity_blocks.size() != 1) { return false; }

        try {
            // Recompute parity
            auto expected_parity = akk::XorParityCoderImpl::compute_xor(data_blocks);

            // Compare with stored parity
            const auto& stored_parity = parity_blocks[0];
            if (expected_parity.size() != stored_parity.size()) { return false; }

            return std::memcmp(
                expected_parity.data(),
                stored_parity.data(),
                expected_parity.size()
            ) == 0;
        }
        catch (...) { return false; }
    }

    std::vector<core::OwnedBuffer> XorParityCoder::reconstruct(
        std::span<const core::BufferView> data_blocks,
        std::span<const core::BufferView> parity_blocks,
        std::span<const size_t> missing_indices
    ) {
        if (missing_indices.empty()) { return {}; }

        if (missing_indices.size() > 1) { throw std::invalid_argument("XorParityCoder: can only recover 1 missing block"); }

        if (parity_blocks.size() != 1) { throw std::invalid_argument("XorParityCoder: expected 1 parity block"); }

        const size_t missing_idx = missing_indices[0];
        if (missing_idx >= data_blocks.size()) { throw std::out_of_range("XorParityCoder: missing index out of range"); }

        // Reconstruct: D[i] = D[0] ⊕ ... ⊕ D[i-1] ⊕ D[i+1] ⊕ ... ⊕ D[k-1] ⊕ P

        // Start with parity
        const auto& parity = parity_blocks[0];
        auto reconstructed = core::OwnedBuffer::allocate(parity.size(), 4096);
        std::memcpy(reconstructed.data(), parity.data(), parity.size());

        // XOR with all non-missing data blocks
        auto dst_view = reconstructed.view();
        for (size_t i = 0; i < data_blocks.size(); ++i) { if (i != missing_idx) { akk::XorParityCoderImpl::xor_into(dst_view, data_blocks[i]); } }

        std::vector<core::OwnedBuffer> result;
        result.push_back(std::move(reconstructed));
        return result;
    }
} // namespace akkaradb::format

namespace akkaradb::format::akk {
    core::OwnedBuffer XorParityCoderImpl::compute_xor(
        std::span<const core::BufferView> blocks
    ) {
        if (blocks.empty()) { throw std::invalid_argument("compute_xor: no blocks provided"); }

        // Validate all blocks are same size
        const size_t block_size = blocks[0].size();
        for (size_t i = 1; i < blocks.size(); ++i) {
            if (blocks[i].size() != block_size) { throw std::invalid_argument("compute_xor: blocks have different sizes"); }
        }

        // Allocate result buffer
        auto result = core::OwnedBuffer::allocate(block_size, 4096);
        result.zero_fill();

        // XOR all blocks into result
        auto result_view = result.view();
        for (const auto& block : blocks) { xor_into(result_view, block); }

        return result;
    }

    void XorParityCoderImpl::xor_into(core::BufferView dst, core::BufferView src) noexcept {
        if (dst.size() != src.size()) {
            return; // Size mismatch, should not happen
        }

        auto* dst_ptr = reinterpret_cast<uint64_t*>(dst.data());
        const auto* src_ptr = reinterpret_cast<const uint64_t*>(src.data());
        const size_t count = dst.size() / sizeof(uint64_t);

        // XOR in 64-bit chunks for performance
        for (size_t i = 0; i < count; ++i) { dst_ptr[i] ^= src_ptr[i]; }

        // Handle remaining bytes
        if (const size_t remaining = dst.size() % sizeof(uint64_t); remaining > 0) {
            const size_t offset = count * sizeof(uint64_t);
            auto* dst_bytes = dst.data() + offset;
            const auto* src_bytes = src.data() + offset;

            for (size_t i = 0; i < remaining; ++i) {
                dst_bytes[i] = static_cast<std::byte>(
                    static_cast<uint8_t>(dst_bytes[i]) ^ static_cast<uint8_t>(src_bytes[i])
                );
            }
        }
    }
} // namespace akkaradb::format::akk