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

// internal/include/format-akk/AkkStripeWriter.hpp
#pragma once

#include "format-api/StripeWriter.hpp"
#include "format-api/ParityCoder.hpp"
#include "format-api/FlushPolicy.hpp"
#include <memory>
#include <chrono>

namespace akkaradb::format::akk {
    /**
 * AkkStripeWriter - Concrete implementation of StripeWriter for AkkaraDB format.
 *
 * Writes stripes with k data blocks + m parity blocks. Supports group commit
 * based on FlushPolicy (N blocks OR T microseconds).
 *
 * Design principles:
 * - Group commit: Batches blocks for efficiency
 * - Parity encoding: Uses configurable ParityCoder
 * - Single-producer: NOT thread-safe
 * - Time-based flush: Uses high-resolution clock
 *
 * Typical usage:
 * ```cpp
 * auto writer = AkkStripeWriter::create(
 *     4,  // k = 4 data lanes
 *     2,  // m = 2 parity lanes
 *     XorParityCoder::create(),
 *     [](Stripe stripe) {
 *         // Write to disk
 *     },
 *     FlushPolicy::default_policy()
 * );
 *
 * writer->add_block(std::move(block1));
 * writer->add_block(std::move(block2));
 * // ...
 *
 * writer->flush();
 * ```
 *
 * Thread-safety: NOT thread-safe. Single producer only.
 */
    class AkkStripeWriter : public StripeWriter {
    public:
        /**
     * Creates an AkkStripeWriter.
     *
     * @param k Number of data lanes
     * @param m Number of parity lanes
     * @param parity_coder Parity encoder (must match m)
     * @param callback Callback invoked when stripe is ready
     * @param flush_policy Group commit policy
     * @return Unique pointer to writer
     * @throws std::invalid_argument if k == 0 or parity_coder->parity_count() != m
     */
        [[nodiscard]] static std::unique_ptr<AkkStripeWriter> create(
            size_t k,
            size_t m,
            std::shared_ptr<ParityCoder> parity_coder,
            StripeReadyCallback callback,
            FlushPolicy flush_policy = FlushPolicy::default_policy()
        );

        ~AkkStripeWriter() override;

        void add_block(core::OwnedBuffer block) override;

        void flush() override;

        [[nodiscard]] size_t data_lanes() const noexcept override;

        [[nodiscard]] size_t parity_lanes() const noexcept override;

        [[nodiscard]] const FlushPolicy& flush_policy() const noexcept override;

        [[nodiscard]] size_t pending_blocks() const noexcept override;

        [[nodiscard]] uint64_t stripes_written() const noexcept override;

    private:
        AkkStripeWriter(
            size_t k,
            size_t m,
            std::shared_ptr<ParityCoder> parity_coder,
            StripeReadyCallback callback,
            FlushPolicy flush_policy
        );

        class Impl;
        std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::format::akk