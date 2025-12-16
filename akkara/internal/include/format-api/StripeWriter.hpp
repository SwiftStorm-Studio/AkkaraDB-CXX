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

// internal/include/format-api/StripeWriter.hpp
#pragma once

#include "core/buffer/OwnedBuffer.hpp"
#include "FlushPolicy.hpp"
#include <functional>
#include <cstdint>
#include <vector>

namespace akkaradb::format {
    /**
 * StripeWriter - Abstract interface for writing blocks with parity redundancy.
 *
 * A stripe consists of k data blocks + m parity blocks, providing redundancy
 * against up to m block failures. Supports various parity schemes:
 * - m=0: No parity (single data lane)
 * - m=1: XOR parity
 * - m=2: Dual XOR parity
 * - m≥3: Reed-Solomon erasure coding
 *
 * Design principles:
 * - Group commit: Flush N blocks OR T microseconds (configurable)
 * - Single-producer: NOT thread-safe, caller must serialize
 * - Callback-based: Emits completed stripes via callback
 * - Abstract interface: Allows different stripe implementations
 *
 * Typical usage:
 * ```cpp
 * auto writer = AkkStripeWriter::create(
 *     4,  // k = 4 data lanes
 *     2,  // m = 2 parity lanes
 *     [](Stripe stripe) {
 *         // Write stripe to disk
 *     },
 *     FlushPolicy{.max_blocks = 32, .max_micros = 500}
 * );
 *
 * writer->add_block(std::move(block1));
 * writer->add_block(std::move(block2));
 * // ... more blocks
 *
 * writer->flush();  // Force flush pending stripe
 * ```
 *
 * Thread-safety: NOT thread-safe. Single producer only.
 */
    class StripeWriter {
    public:
        /**
     * Stripe - k data blocks + m parity blocks.
     *
     * Layout: [data[0], data[1], ..., data[k-1], parity[0], ..., parity[m-1]]
     */
        struct Stripe {
            std::vector<core::OwnedBuffer> data_blocks; ///< k data blocks
            std::vector<core::OwnedBuffer> parity_blocks; ///< m parity blocks
            uint64_t stripe_id; ///< Monotonic stripe ID

            [[nodiscard]] size_t total_blocks() const noexcept { return data_blocks.size() + parity_blocks.size(); }

            [[nodiscard]] bool is_complete() const noexcept { return !data_blocks.empty(); }
        };

        /**
     * Callback invoked when a stripe is ready.
     *
     * @param stripe Completed stripe (ownership transferred)
     */
        using StripeReadyCallback = std::function<void(Stripe stripe)>;

        virtual ~StripeWriter() = default;

        /**
     * Adds a block to the current stripe.
     *
     * If the stripe reaches max_blocks or max_micros, it's automatically
     * flushed and a new stripe is started.
     *
     * @param block Data block (ownership transferred)
     */
        virtual void add_block(core::OwnedBuffer block) = 0;

        /**
     * Flushes any pending stripe.
     *
     * Encodes parity blocks and emits the stripe via callback.
     * No-op if no blocks are pending.
     */
        virtual void flush() = 0;

        /**
     * Returns the number of data lanes (k).
     */
        [[nodiscard]] virtual size_t data_lanes() const noexcept = 0;

        /**
     * Returns the number of parity lanes (m).
     */
        [[nodiscard]] virtual size_t parity_lanes() const noexcept = 0;

        /**
     * Returns the flush policy.
     */
        [[nodiscard]] virtual const FlushPolicy& flush_policy() const noexcept = 0;

        /**
     * Returns the number of blocks in the current pending stripe.
     */
        [[nodiscard]] virtual size_t pending_blocks() const noexcept = 0;

        /**
     * Returns the total number of stripes emitted.
     */
        [[nodiscard]] virtual uint64_t stripes_written() const noexcept = 0;
    };
} // namespace akkaradb::format