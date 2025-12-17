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

// internal/src/format-akk/AkkStripeReader.cpp
#include "format-akk/AkkStripeReader.hpp"
#include "core/buffer/BufferView.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include <vector>
#include <stdexcept>
#include <algorithm>

namespace akkaradb::format::akk {
    /**
     * AkkStripeReader::Impl - Private implementation (Pimpl idiom).
     */
    class AkkStripeReader::Impl {
    public:
        Impl(
            size_t k,
            size_t m,
            size_t block_size,
            std::shared_ptr<ParityCoder> parity_coder
        ) : k_{k}
            , m_{m}
            , block_size_{block_size}
            , parity_coder_{std::move(parity_coder)} {
            if (k_ == 0) { throw std::invalid_argument("AkkStripeReader: k must be > 0"); }

            if (parity_coder_->parity_count() != m_) { throw std::invalid_argument("AkkStripeReader: parity_coder mismatch"); }
        }

        [[nodiscard]] std::optional<ReadResult> read_stripe(
            std::span<const core::BufferView> blocks
        ) {
            // Validate input
            if (blocks.size() != k_ + m_) {
                return std::nullopt; // Wrong number of blocks
            }

            // Split into data and parity
            std::vector<core::BufferView> data_blocks(blocks.begin(), blocks.begin() + k_);
            std::vector<core::BufferView> parity_blocks(blocks.begin() + k_, blocks.end());

            // Validate block sizes
            for (const auto& block : blocks) {
                if (block.size() != block_size_) {
                    return std::nullopt; // Size mismatch
                }
            }

            // Check CRC for all blocks, identify corrupted ones
            std::vector<size_t> corrupted_data_indices;
            std::vector<size_t> corrupted_parity_indices;

            for (size_t i = 0; i < k_; ++i) { if (!validate_block_crc(data_blocks[i])) { corrupted_data_indices.push_back(i); } }

            for (size_t i = 0; i < m_; ++i) { if (!validate_block_crc(parity_blocks[i])) { corrupted_parity_indices.push_back(i); } }

            const size_t total_corrupted = corrupted_data_indices.size() + corrupted_parity_indices.size();

            // Check if recoverable
            if (corrupted_data_indices.size() > m_) {
                return std::nullopt; // Too many data blocks corrupted
            }

            // If no corruption, return clean data
            if (total_corrupted == 0) {
                std::vector<core::OwnedBuffer> result;
                result.reserve(k_);

                for (const auto& block : data_blocks) {
                    auto owned = core::OwnedBuffer::allocate(block_size_, 4096);
                    std::memcpy(owned.data(), block.data(), block_size_);
                    result.push_back(std::move(owned));
                }

                return ReadResult{
                    .data_blocks = std::move(result),
                    .corrupted_blocks = 0,
                    .reconstructed_blocks = 0,
                    .used_parity = false
                };
            }

            // Reconstruction needed
            if (corrupted_data_indices.empty()) {
                // Only parity corrupted, data is clean
                std::vector<core::OwnedBuffer> result;
                result.reserve(k_);

                for (const auto& block : data_blocks) {
                    auto owned = core::OwnedBuffer::allocate(block_size_, 4096);
                    std::memcpy(owned.data(), block.data(), block_size_);
                    result.push_back(std::move(owned));
                }

                return ReadResult{
                    .data_blocks = std::move(result),
                    .corrupted_blocks = total_corrupted,
                    .reconstructed_blocks = 0,
                    .used_parity = false
                };
            }

            // Reconstruct corrupted data blocks
            std::vector<core::OwnedBuffer> reconstructed;
            try { reconstructed = parity_coder_->reconstruct(data_blocks, parity_blocks, corrupted_data_indices); }
            catch (...) {
                return std::nullopt; // Reconstruction failed
            }

            // Build final result (mix of clean + reconstructed)
            std::vector<core::OwnedBuffer> final_data;
            final_data.reserve(k_);

            size_t reconstructed_idx = 0;
            for (size_t i = 0; i < k_; ++i) {
                if (std::find(corrupted_data_indices.begin(), corrupted_data_indices.end(), i) != corrupted_data_indices.end()) {
                    // Use reconstructed block
                    final_data.push_back(std::move(reconstructed[reconstructed_idx++]));
                }
                else {
                    // Copy clean block
                    auto owned = core::OwnedBuffer::allocate(block_size_, 4096);
                    std::memcpy(owned.data(), data_blocks[i].data(), block_size_);
                    final_data.push_back(std::move(owned));
                }
            }

            return ReadResult{
                .data_blocks = std::move(final_data),
                .corrupted_blocks = total_corrupted,
                .reconstructed_blocks = corrupted_data_indices.size(),
                .used_parity = true
            };
        }

        [[nodiscard]] bool validate_stripe(
            std::span<const core::BufferView> blocks
        ) const noexcept {
            if (blocks.size() != k_ + m_) { return false; }

            // Check all block sizes
            for (const auto& block : blocks) { if (block.size() != block_size_) { return false; } }

            // Check CRC for all blocks
            for (const auto& block : blocks) { if (!validate_block_crc(block)) { return false; } }

            // Verify parity consistency
            try {
                std::vector<core::BufferView> data_blocks(blocks.begin(), blocks.begin() + k_);
                std::vector<core::BufferView> parity_blocks(blocks.begin() + k_, blocks.end());

                return parity_coder_->verify(data_blocks, parity_blocks);
            }
            catch (...) { return false; }
        }

        [[nodiscard]] size_t data_lanes() const noexcept { return k_; }

        [[nodiscard]] size_t parity_lanes() const noexcept { return m_; }

        [[nodiscard]] size_t block_size() const noexcept { return block_size_; }

    private:
        [[nodiscard]] bool validate_block_crc(core::BufferView block) const noexcept {
            if (block.size() != block_size_) { return false; }

            try {
                // Read stored CRC at end of block
                const uint32_t stored_crc = block.read_u32_le(block_size_ - sizeof(uint32_t));

                // Compute CRC over block (excluding CRC field itself)
                const uint32_t computed_crc = block.crc32c(0, block_size_ - sizeof(uint32_t));

                return stored_crc == computed_crc;
            }
            catch (...) { return false; }
        }

        size_t k_;
        size_t m_;
        size_t block_size_;
        std::shared_ptr<ParityCoder> parity_coder_;
    };

    // ==================== AkkStripeReader Public API ====================

    std::unique_ptr<AkkStripeReader> AkkStripeReader::create(
        size_t k,
        size_t m,
        size_t block_size,
        std::shared_ptr<ParityCoder> parity_coder
    ) {
        return std::unique_ptr<AkkStripeReader>(new AkkStripeReader(
            k, m, block_size, std::move(parity_coder)
        ));
    }

    AkkStripeReader::AkkStripeReader(
        size_t k,
        size_t m,
        size_t block_size,
        std::shared_ptr<ParityCoder> parity_coder
    ) : impl_{std::make_unique<Impl>(k, m, block_size, std::move(parity_coder))} {}

    AkkStripeReader::~AkkStripeReader() = default;

    std::optional<StripeReader::ReadResult> AkkStripeReader::read_stripe(
        std::span<const core::BufferView> blocks
    ) { return impl_->read_stripe(blocks); }

    bool AkkStripeReader::validate_stripe(
        std::span<const core::BufferView> blocks
    ) const noexcept { return impl_->validate_stripe(blocks); }

    size_t AkkStripeReader::data_lanes() const noexcept { return impl_->data_lanes(); }

    size_t AkkStripeReader::parity_lanes() const noexcept { return impl_->parity_lanes(); }

    size_t AkkStripeReader::block_size() const noexcept { return impl_->block_size(); }
} // namespace akkaradb::format::akk
