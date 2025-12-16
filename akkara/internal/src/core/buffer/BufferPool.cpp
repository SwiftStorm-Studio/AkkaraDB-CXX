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

// internal/src/core/buffer/BufferPool.cpp
#include "core/buffer/BufferPool.hpp"
#include <vector>
#include <mutex>
#include <atomic>
#include <algorithm>

namespace akkaradb::core {
    /**
 * BufferPool::Impl - Private implementation (Pimpl idiom).
 *
 * This class contains all internal state and is hidden from the public API.
 * Changes to this class do not affect ABI compatibility.
 */
    class BufferPool::Impl {
    public:
        Impl(size_t block_size, size_t alignment, size_t max_pooled) : block_size_{block_size}, alignment_{alignment}, max_pooled_{max_pooled} {
            pool_.reserve(max_pooled > 0 ? max_pooled : 64);
        }

        ~Impl() = default;

        Impl(const Impl&) = delete;
        Impl& operator=(const Impl&) = delete;
        Impl(Impl&&) = delete;
        Impl& operator=(Impl&&) = delete;

        OwnedBuffer acquire() {
            total_acquired_.fetch_add(1, std::memory_order_relaxed);

            // Try to get from pool
            {
                std::lock_guard lock{mutex_};
                if (!pool_.empty()) {
                    auto buf = std::move(pool_.back());
                    pool_.pop_back();
                    pool_hits_.fetch_add(1, std::memory_order_relaxed);
                    buf.zero_fill();
                    return buf;
                }
            }

            // Pool miss - allocate new
            pool_misses_.fetch_add(1, std::memory_order_relaxed);

            auto allocated = current_allocated_.fetch_add(1, std::memory_order_relaxed) + 1;

            // Update peak
            size_t current_peak = peak_allocated_.load(std::memory_order_relaxed);
            while (allocated > current_peak) { if (peak_allocated_.compare_exchange_weak(current_peak, allocated, std::memory_order_relaxed)) { break; } }

            return OwnedBuffer::allocate(block_size_, alignment_);
        }

        void release(OwnedBuffer&& buffer) noexcept {
            if (buffer.empty()) { return; }

            total_released_.fetch_add(1, std::memory_order_relaxed);
            current_allocated_.fetch_sub(1, std::memory_order_relaxed);

            // Zero-fill for security
            buffer.zero_fill();

            // Try to return to pool
            std::lock_guard lock{mutex_};
            if (max_pooled_ == 0 || pool_.size() < max_pooled_) { pool_.push_back(std::move(buffer)); }
            // else: buffer is automatically deallocated
        }

        Stats stats() const noexcept {
            std::lock_guard lock{mutex_};

            return Stats{
                .total_acquired = total_acquired_.load(std::memory_order_relaxed),
                .total_released = total_released_.load(std::memory_order_relaxed),
                .pool_hits = pool_hits_.load(std::memory_order_relaxed),
                .pool_misses = pool_misses_.load(std::memory_order_relaxed),
                .current_pooled = pool_.size(),
                .peak_allocated = peak_allocated_.load(std::memory_order_relaxed)
            };
        }

        size_t block_size() const noexcept { return block_size_; }
        size_t alignment() const noexcept { return alignment_; }

    private:
        size_t block_size_;
        size_t alignment_;
        size_t max_pooled_;

        // Global pool (thread-safe with mutex)
        mutable std::mutex mutex_;
        std::vector<OwnedBuffer> pool_;

        // Statistics (atomic for lock-free access)
        mutable std::atomic<uint64_t> total_acquired_{0};
        mutable std::atomic<uint64_t> total_released_{0};
        mutable std::atomic<uint64_t> pool_hits_{0};
        mutable std::atomic<uint64_t> pool_misses_{0};
        mutable std::atomic<size_t> current_allocated_{0};
        mutable std::atomic<size_t> peak_allocated_{0};
    };

    // ==================== BufferPool Public API ====================

    std::unique_ptr<BufferPool> BufferPool::create(size_t block_size, size_t alignment, size_t max_pooled) {
        // Use unique_ptr with private constructor
        return std::unique_ptr<BufferPool>(new BufferPool(block_size, alignment, max_pooled));
    }

    BufferPool::BufferPool(size_t block_size, size_t alignment, size_t max_pooled) : impl_{std::make_unique<Impl>(block_size, alignment, max_pooled)} {}

    BufferPool::~BufferPool() = default;

    OwnedBuffer BufferPool::acquire() { return impl_->acquire(); }

    void BufferPool::release(OwnedBuffer&& buffer) noexcept { impl_->release(std::move(buffer)); }

    BufferPool::Stats BufferPool::stats() const noexcept { return impl_->stats(); }

    size_t BufferPool::block_size() const noexcept { return impl_->block_size(); }

    size_t BufferPool::alignment() const noexcept { return impl_->alignment(); }
} // namespace akkaradb::core