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

// internal/include/format-akk/AkkStripeReader.hpp
#pragma once

#include "format-api/StripeReader.hpp"
#include "format-api/ParityCoder.hpp"
#include <memory>

namespace akkaradb::format::akk {
    /**
     * AkkStripeReader - Concrete implementation of StripeReader for AkkaraDB format.
     *
     * Reads and validates stripes with k data blocks + m parity blocks.
     * Supports error recovery via parity reconstruction.
     *
     * Design principles:
     * - CRC validation: Checks all blocks before use
     * - Parity reconstruction: Recovers up to m corrupted blocks
     * - Zero-copy when possible: Returns views for clean reads
     * - Detailed diagnostics: Reports corruption/reconstruction stats
     *
     * Typical usage:
     * ```cpp
     * auto reader = AkkStripeReader::create(
     *     4,  // k = 4 data lanes
     *     2,  // m = 2 parity lanes
     *     32 * 1024,  // block_size
     *     XorParityCoder::create()
     * );
     *
     * // Load k+m blocks from disk
     * std::vector<BufferView> blocks = load_stripe();
     *
     * auto result = reader->read_stripe(blocks);
     * if (!result) {
     *     // Unrecoverable (>m blocks corrupted)
     *     handle_fatal_error();
     * }
     *
     * // Use reconstructed data
     * for (const auto& block : result->data_blocks) {
     *     process(block);
     * }
     * ```
     *
     * Thread-safety: Stateless, so thread-safe if called with different buffers.
     */
    class AkkStripeReader : public StripeReader {
    public:
        /**
         * Creates an AkkStripeReader.
         *
         * @param k Number of data lanes
         * @param m Number of parity lanes
         * @param block_size Expected block size (e.g., 32 KiB)
         * @param parity_coder Parity decoder (must match m)
         * @return Unique pointer to reader
         * @throws std::invalid_argument if k == 0 or parity_coder->parity_count() != m
         */
        [[nodiscard]] static std::unique_ptr<AkkStripeReader> create(
            size_t k,
            size_t m,
            size_t block_size,
            std::shared_ptr<ParityCoder> parity_coder
        );

        ~AkkStripeReader() override;

        [[nodiscard]] std::optional<ReadResult> read_stripe(
            std::span<const core::BufferView> blocks
        ) override;

        [[nodiscard]] bool validate_stripe(
            std::span<const core::BufferView> blocks
        ) const noexcept override;

        [[nodiscard]] size_t data_lanes() const noexcept override;

        [[nodiscard]] size_t parity_lanes() const noexcept override;

        [[nodiscard]] size_t block_size() const noexcept override;

    private:
    AkkStripeReader(
        size_t k,
        size_t m,
        size_t block_size,
        std::shared_ptr<ParityCoder> parity_coder
    );

    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace akkaradb::format::akk