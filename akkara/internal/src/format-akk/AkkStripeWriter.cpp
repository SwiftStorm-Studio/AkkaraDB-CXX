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

// internal/src/format-akk/AkkStripeWriter.cpp
#include "format-akk/AkkStripeWriter.hpp"
#include "core/buffer/BufferView.hpp"
#include <vector>
#include <chrono>
#include <stdexcept>

namespace akkaradb::format::akk {
    using Clock = std::chrono::steady_clock;
    using TimePoint = std::chrono::time_point<Clock>;

    /**
 * AkkStripeWriter::Impl - Private implementation (Pimpl idiom).
 */
    class AkkStripeWriter::Impl {
        public:
            Impl(size_t k, size_t m, std::shared_ptr<ParityCoder> parity_coder, StripeReadyCallback callback, FlushPolicy flush_policy)
                : k_{k}, m_{m}, parity_coder_{std::move(parity_coder)}, callback_{std::move(callback)}, flush_policy_{flush_policy}, stripe_id_{0} {
                if (k_ == 0) { throw std::invalid_argument("AkkStripeWriter: k must be > 0"); }

                if (parity_coder_->parity_count() != m_) { throw std::invalid_argument("AkkStripeWriter: parity_coder mismatch"); }

                pending_blocks_.reserve(k_);
            }

            void add_block(core::OwnedBuffer block) {
                if (block.empty()) { throw std::invalid_argument("AkkStripeWriter::add_block: empty block"); }

                // Start timer on first block
                if (pending_blocks_.empty()) { stripe_start_time_ = Clock::now(); }

                pending_blocks_.push_back(std::move(block));

                // Check flush conditions
                const bool count_reached = pending_blocks_.size() >= flush_policy_.max_blocks;

                if (const bool time_reached = should_flush_by_time(); count_reached || time_reached) { flush_internal(); }
            }

            void flush() { if (!pending_blocks_.empty()) { flush_internal(); } }

            [[nodiscard]] size_t data_lanes() const noexcept { return k_; }

            [[nodiscard]] size_t parity_lanes() const noexcept { return m_; }

            [[nodiscard]] const FlushPolicy& flush_policy() const noexcept { return flush_policy_; }

            [[nodiscard]] size_t pending_blocks() const noexcept { return pending_blocks_.size(); }

            [[nodiscard]] uint64_t stripes_written() const noexcept { return stripe_id_; }

        private:
            void flush_internal() {
                if (pending_blocks_.empty()) { return; }

                // Emit stripes of exactly k_ data blocks each.
                // The final batch may be smaller (partial stripe) - that is allowed.
                size_t offset = 0;
                while (offset < pending_blocks_.size()) {
                    const size_t chunk = (std::min)(k_, pending_blocks_.size() - offset);

                    // Move chunk blocks into this stripe.
                    std::vector<core::OwnedBuffer> data_blocks;
                    data_blocks.reserve(chunk);
                    for (size_t i = 0; i < chunk; ++i) { data_blocks.push_back(std::move(pending_blocks_[offset + i])); }
                    offset += chunk;

                    // Encode parity only for full stripes.
                    std::vector<core::OwnedBuffer> parity_blocks;
                    if (m_ > 0 && chunk == k_) {
                        std::vector<core::BufferView> data_views;
                        data_views.reserve(chunk);
                        for (const auto& blk : data_blocks) { data_views.push_back(blk.view()); }
                        parity_blocks = parity_coder_->encode(data_views);
                    }

                    Stripe stripe{.data_blocks = std::move(data_blocks), .parity_blocks = std::move(parity_blocks), .stripe_id = stripe_id_++};
                    callback_(std::move(stripe));
                }

                // Reset state.
                pending_blocks_.clear();
                pending_blocks_.reserve(k_);
            }

            [[nodiscard]] bool should_flush_by_time() const noexcept {
                if (flush_policy_.max_micros == 0) {
                    return false; // Time-based flush disabled
                }

                const auto now = Clock::now();
                const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(now - stripe_start_time_);

                return elapsed.count() >= static_cast<int64_t>(flush_policy_.max_micros);
            }

            size_t k_;
            size_t m_;
            std::shared_ptr<ParityCoder> parity_coder_;
            StripeReadyCallback callback_;
            FlushPolicy flush_policy_;

            std::vector<core::OwnedBuffer> pending_blocks_;
            TimePoint stripe_start_time_;
            uint64_t stripe_id_;
    };

    // ==================== AkkStripeWriter Public API ====================

    std::unique_ptr<AkkStripeWriter> AkkStripeWriter::create(
        size_t k,
        size_t m,
        std::shared_ptr<ParityCoder> parity_coder,
        StripeReadyCallback callback,
        FlushPolicy flush_policy
    ) { return std::unique_ptr<AkkStripeWriter>(new AkkStripeWriter(k, m, std::move(parity_coder), std::move(callback), flush_policy)); }

    AkkStripeWriter::AkkStripeWriter(size_t k, size_t m, std::shared_ptr<ParityCoder> parity_coder, StripeReadyCallback callback, FlushPolicy flush_policy)
        : impl_{std::make_unique<Impl>(k, m, std::move(parity_coder), std::move(callback), flush_policy)} {}

    AkkStripeWriter::~AkkStripeWriter() = default;

    void AkkStripeWriter::add_block(core::OwnedBuffer block) { impl_->add_block(std::move(block)); }

    void AkkStripeWriter::flush() { impl_->flush(); }

    size_t AkkStripeWriter::data_lanes() const noexcept { return impl_->data_lanes(); }

    size_t AkkStripeWriter::parity_lanes() const noexcept { return impl_->parity_lanes(); }

    const FlushPolicy& AkkStripeWriter::flush_policy() const noexcept { return impl_->flush_policy(); }

    size_t AkkStripeWriter::pending_blocks() const noexcept { return impl_->pending_blocks(); }

    uint64_t AkkStripeWriter::stripes_written() const noexcept { return impl_->stripes_written(); }
} // namespace akkaradb::format::akk