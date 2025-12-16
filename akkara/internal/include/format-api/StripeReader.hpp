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

// internal/include/format-api/StripeReader.hpp
#pragma once

#include "core/buffer/BufferView.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include <vector>
#include <optional>
#include <cstdint>

namespace akkaradb::format {
    /**
 * StripeReader - Abstract interface for reading stripes with error recovery.
 *
 * Reads k data blocks + m parity blocks from a stripe. If some blocks are
 * corrupted or missing, attempts to reconstruct them using parity data.
 *
 * Design principles:
 * - Error recovery: Reconstructs up to m missing/corrupted blocks
 * - Validation: Checks CRC for all blocks before use
 * - Zero-copy: Returns views when possible
 * - Abstract interface: Allows different stripe implementations
 *
 * Typical usage:
 * ```cpp
 * auto reader = AkkStripeReader::create(4, 2);  // k=4, m=2
 *
 * // Read stripe from disk (k+m blocks)
 * std::vector<core::BufferView> blocks = load_stripe_blocks();
 *
 * // Read and validate
 * auto result = reader->read_stripe(blocks);
 * if (!result) {
 *     // Unrecoverable error (>m blocks corrupted)
 *     handle_fatal_error();
 * }
 *
 * // Access data blocks
 * for (const auto& block : result->data_blocks) {
 *     // Process block...
 * }
 * ```
 *
 * Thread-safety: Stateless, so thread-safe if called with different buffers.
 */
    class StripeReader {
    public:
        /**
     * ReadResult - Result of reading a stripe.
     */
        struct ReadResult {
            std::vector<core::OwnedBuffer> data_blocks; ///< k data blocks (reconstructed if needed)
            size_t corrupted_blocks; ///< Number of blocks that were corrupted
            size_t reconstructed_blocks; ///< Number of blocks reconstructed from parity
            bool used_parity; ///< True if parity was needed for reconstruction

            [[nodiscard]] bool is_clean() const noexcept { return corrupted_blocks == 0; }
        };

        virtual ~StripeReader() = default;

        /**
     * Reads and validates a stripe.
     *
     * Expected layout: [data[0], ..., data[k-1], parity[0], ..., parity[m-1]]
     *
     * @param blocks k+m blocks (views into stripe storage)
     * @return ReadResult if recoverable, std::nullopt if >m blocks corrupted
     */
        [[nodiscard]] virtual std::optional<ReadResult> read_stripe(
            std::span<const core::BufferView> blocks
        ) = 0;

        /**
     * Validates a stripe without reconstructing.
     *
     * Checks CRC for all blocks and verifies parity consistency.
     *
     * @param blocks k+m blocks
     * @return true if stripe is valid and intact
     */
        [[nodiscard]] virtual bool validate_stripe(
            std::span<const core::BufferView> blocks
        ) const noexcept = 0;

        /**
     * Returns the number of data lanes (k).
     */
        [[nodiscard]] virtual size_t data_lanes() const noexcept = 0;

        /**
     * Returns the number of parity lanes (m).
     */
        [[nodiscard]] virtual size_t parity_lanes() const noexcept = 0;

        /**
     * Returns the expected block size.
     */
        [[nodiscard]] virtual size_t block_size() const noexcept = 0;
    };
} // namespace akkaradb::format