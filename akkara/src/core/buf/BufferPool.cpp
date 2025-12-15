// BufferPool.cpp
#include "../../../include/akkara/core/buf/BufferPool.hpp"
#include <algorithm>

namespace akkaradb::core
{
    std::shared_ptr<BufferPool> BufferPool::create(
        size_t block_size,
        size_t alignment,
        size_t max_pooled_buffers
    )
    {
        // Use std::make_shared with private constructor workaround
        struct EnableMakeShared : BufferPool
        {
            EnableMakeShared(size_t bs, size_t al, size_t mp)
                : BufferPool(bs, al, mp)
            {
            }
        };

        return std::make_shared<EnableMakeShared>(
            block_size, alignment, max_pooled_buffers
        );
    }

    BufferPool::BufferPool(size_t block_size, size_t alignment, size_t max_pooled)
        : block_size_{block_size}
          , alignment_{alignment}
          , max_pooled_{max_pooled}
    {
        pool_.reserve(max_pooled > 0 ? max_pooled : 64);
    }

    OwnedBuffer BufferPool::acquire()
    {
        total_acquired_.fetch_add(1, std::memory_order_relaxed);

        // Try to get from pool
        {
            std::lock_guard lock{mutex_};
            if (!pool_.empty())
            {
                auto buf = std::move(pool_.back());
                pool_.pop_back();
                pool_hits_.fetch_add(1, std::memory_order_relaxed);
                buf.zero_fill(); // Ensure clean state
                return buf;
            }
        }

        // Pool miss - allocate new
        pool_misses_.fetch_add(1, std::memory_order_relaxed);

        auto allocated = current_allocated_.fetch_add(1, std::memory_order_relaxed) + 1;

        // Update peak
        size_t current_peak = peak_allocated_.load(std::memory_order_relaxed);
        while (allocated > current_peak)
        {
            if (peak_allocated_.compare_exchange_weak(
                current_peak, allocated, std::memory_order_relaxed))
            {
                break;
            }
        }

        return OwnedBuffer::allocate(block_size_, alignment_);
    }

    void BufferPool::release(OwnedBuffer&& buffer) noexcept
    {
        if (buffer.empty())
        {
            return;
        }

        total_released_.fetch_add(1, std::memory_order_relaxed);
        current_allocated_.fetch_sub(1, std::memory_order_relaxed);

        // Zero-fill for security
        buffer.zero_fill();

        // Try to return to pool
        std::lock_guard lock{mutex_};
        if (max_pooled_ == 0 || pool_.size() < max_pooled_)
        {
            pool_.push_back(std::move(buffer));
        }
        // else: buffer is deallocated automatically
    }

    BufferPool::Stats BufferPool::stats() const noexcept
    {
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
} // namespace akkaradb::core