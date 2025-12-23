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
#include <array>
#include <mutex>
#include <atomic>
#include <thread>
#include <algorithm>
#include <cstdint>

namespace akkaradb::core {

namespace {

// ============================================================================
// Configuration Constants
// ============================================================================

/// Number of global pool shards (power of 2 for fast modulo)
constexpr size_t SHARD_COUNT = 8;

/// Mask for shard index calculation (SHARD_COUNT - 1)
constexpr size_t SHARD_MASK = SHARD_COUNT - 1;

/// Maximum buffers in thread-local cache
constexpr size_t TLS_CAPACITY = 8;

/// Batch size for TLS refill from global pool
constexpr size_t BATCH_REFILL = 4;

/// Batch size for TLS drain to global pool
constexpr size_t BATCH_DRAIN = 4;

/// Low watermark for TLS refill trigger
constexpr size_t TLS_LOW_WATERMARK = 2;

/// High watermark for TLS drain trigger
constexpr size_t TLS_HIGH_WATERMARK = TLS_CAPACITY - 2;

// ============================================================================
// Shard: Single partition of the global pool
// ============================================================================

/**
 * Shard - One partition of the global buffer pool.
 *
 * Each shard has its own mutex, reducing contention when multiple
 * threads access the pool simultaneously.
 */
class Shard {
public:
    explicit Shard(size_t max_per_shard) : max_size_{max_per_shard} {
        buffers_.reserve(max_per_shard > 0 ? max_per_shard : 32);
    }

    /**
     * Tries to take one buffer from this shard.
     * @return Buffer if available, empty buffer otherwise
     */
    [[nodiscard]] OwnedBuffer try_take_one() noexcept {
        std::lock_guard lock{mutex_};
        if (buffers_.empty()) {
            return OwnedBuffer{};
        }
        auto buf = std::move(buffers_.back());
        buffers_.pop_back();
        return buf;
    }

    /**
     * Takes up to `count` buffers from this shard.
     * @param out Output vector to append buffers to
     * @param count Maximum number of buffers to take
     * @return Actual number of buffers taken
     */
    size_t take_batch(std::vector<OwnedBuffer>& out, size_t count) noexcept {
        std::lock_guard lock{mutex_};
        const size_t available = buffers_.size();
        const size_t to_take = std::min(count, available);

        for (size_t i = 0; i < to_take; ++i) {
            out.push_back(std::move(buffers_.back()));
            buffers_.pop_back();
        }
        return to_take;
    }

    /**
     * Returns one buffer to this shard.
     * @param buffer Buffer to return (will be moved)
     * @return true if accepted, false if shard is full
     */
    bool try_return_one(OwnedBuffer&& buffer) noexcept {
        std::lock_guard lock{mutex_};
        if (max_size_ > 0 && buffers_.size() >= max_size_) {
            return false;  // Shard full, caller should deallocate
        }
        buffers_.push_back(std::move(buffer));
        return true;
    }

    /**
     * Returns a batch of buffers to this shard.
     * @param buffers Buffers to return (will be moved from)
     * @return Number of buffers actually accepted
     */
    size_t return_batch(std::vector<OwnedBuffer>& buffers) noexcept {
        std::lock_guard lock{mutex_};
        size_t accepted = 0;

        while (!buffers.empty()) {
            if (max_size_ > 0 && buffers_.size() >= max_size_) {
                break;  // Shard full
            }
            buffers_.push_back(std::move(buffers.back()));
            buffers.pop_back();
            ++accepted;
        }
        return accepted;
    }

    /**
     * Returns current buffer count in this shard.
     */
    [[nodiscard]] size_t size() const noexcept {
        std::lock_guard lock{mutex_};
        return buffers_.size();
    }

private:
    mutable std::mutex mutex_;
    std::vector<OwnedBuffer> buffers_;
    size_t max_size_;
};

// ============================================================================
// TLSCache: Thread-local buffer cache
// ============================================================================

/**
 * TLSCache - Per-thread buffer cache for lock-free fast path.
 *
 * This is a simple stack-based cache that avoids any synchronization
 * for the common case of acquire/release within the same thread.
 */
class TLSCache {
public:
    TLSCache() = default;

    // Non-copyable, non-movable (stored in thread_local)
    TLSCache(const TLSCache&) = delete;
    TLSCache& operator=(const TLSCache&) = delete;
    TLSCache(TLSCache&&) = delete;
    TLSCache& operator=(TLSCache&&) = delete;

    /**
     * Tries to pop a buffer from local cache.
     * @return Buffer if available, empty buffer otherwise
     */
    [[nodiscard]] OwnedBuffer try_pop() noexcept {
        if (size_ == 0) {
            return OwnedBuffer{};
        }
        --size_;
        return std::move(buffers_[size_]);
    }

    /**
     * Tries to push a buffer to local cache.
     * @param buffer Buffer to push (will be moved)
     * @return true if accepted, false if cache is full
     */
    bool try_push(OwnedBuffer&& buffer) noexcept {
        if (size_ >= TLS_CAPACITY) {
            return false;
        }
        buffers_[size_] = std::move(buffer);
        ++size_;
        return true;
    }

    /**
     * Refills cache from provided buffers.
     * @param buffers Source buffers (will be moved from back)
     */
    void refill_from(std::vector<OwnedBuffer>& buffers) noexcept {
        while (size_ < TLS_CAPACITY && !buffers.empty()) {
            buffers_[size_] = std::move(buffers.back());
            buffers.pop_back();
            ++size_;
        }
    }

    /**
     * Drains excess buffers to output vector.
     * @param out Output vector to append buffers to
     * @param keep_count Number of buffers to retain in cache
     */
    void drain_to(std::vector<OwnedBuffer>& out, size_t keep_count) noexcept {
        while (size_ > keep_count) {
            --size_;
            out.push_back(std::move(buffers_[size_]));
        }
    }

    [[nodiscard]] size_t size() const noexcept { return size_; }
    [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
    [[nodiscard]] bool full() const noexcept { return size_ >= TLS_CAPACITY; }
    [[nodiscard]] bool needs_refill() const noexcept { return size_ <= TLS_LOW_WATERMARK; }
    [[nodiscard]] bool needs_drain() const noexcept { return size_ >= TLS_HIGH_WATERMARK; }

private:
    std::array<OwnedBuffer, TLS_CAPACITY> buffers_;
    size_t size_{0};
};

// ============================================================================
// Hash function for thread ID to shard mapping
// ============================================================================

/**
 * Fast hash for thread ID to shard index mapping.
 * Uses multiplicative hashing with a prime-like constant.
 */
[[nodiscard]] inline size_t hash_thread_id(std::thread::id id) noexcept {
    // Convert thread::id to numeric value
    const auto h = std::hash<std::thread::id>{}(id);
    // Multiplicative hash for better distribution
    return (h * 0x9E3779B97F4A7C15ULL) >> 32;
}

/**
 * Returns shard index for current thread.
 */
[[nodiscard]] inline size_t current_shard_index() noexcept {
    return hash_thread_id(std::this_thread::get_id()) & SHARD_MASK;
}

} // anonymous namespace

// ============================================================================
// BufferPool::Impl - Private implementation
// ============================================================================

class BufferPool::Impl {
public:
    Impl(size_t block_size, size_t alignment, size_t max_pooled)
        : block_size_{block_size}
        , alignment_{alignment}
        , max_per_shard_{max_pooled / SHARD_COUNT}
    {
        // Initialize shards
        shards_.reserve(SHARD_COUNT);
        for (size_t i = 0; i < SHARD_COUNT; ++i) {
            shards_.push_back(std::make_unique<Shard>(max_per_shard_));
        }
    }

    ~Impl() = default;

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;
    Impl(Impl&&) = delete;
    Impl& operator=(Impl&&) = delete;

    // ------------------------------------------------------------------------
    // Core Operations
    // ------------------------------------------------------------------------

    [[nodiscard]] OwnedBuffer acquire(bool skip_zero_fill = false) {
        stats_total_acquired_.fetch_add(1, std::memory_order_relaxed);

        // Fast path: TLS cache hit
        auto& tls = get_tls_cache();
        if (auto buf = tls.try_pop(); !buf.empty()) {
            stats_tls_hits_.fetch_add(1, std::memory_order_relaxed);
            if (!skip_zero_fill) buf.zero_fill();
            return buf;
        }

        // Slow path: Refill TLS from global shards
        return acquire_slow_path(tls, skip_zero_fill);
    }

    [[nodiscard]] std::vector<OwnedBuffer> acquire_batch(size_t count, bool skip_zero_fill = false) {
        if (count == 0) return {};

        stats_total_acquired_.fetch_add(count, std::memory_order_relaxed);

        std::vector<OwnedBuffer> result;
        result.reserve(count);

        auto& tls = get_tls_cache();

        // First, drain TLS
        while (result.size() < count && !tls.empty()) {
            auto buf = tls.try_pop();
            if (!buf.empty()) {
                stats_tls_hits_.fetch_add(1, std::memory_order_relaxed);
                if (!skip_zero_fill) buf.zero_fill();
                result.push_back(std::move(buf));
            }
        }

        // Then, take from global shards directly (bypass TLS refill for batch)
        if (result.size() < count) {
            const size_t needed = count - result.size();
            const size_t primary = current_shard_index();

            // Try all shards
            for (size_t i = 0; i < SHARD_COUNT && result.size() < count; ++i) {
                const size_t idx = (primary + i) & SHARD_MASK;
                std::vector<OwnedBuffer> batch;
                shards_[idx]->take_batch(batch, needed - (result.size() - (count - needed)));

                for (auto& buf : batch) {
                    stats_shard_hits_.fetch_add(1, std::memory_order_relaxed);
                    if (!skip_zero_fill) buf.zero_fill();
                    result.push_back(std::move(buf));
                }
            }
        }

        // Allocate remaining
        while (result.size() < count) { result.push_back(allocate_new(skip_zero_fill)); }

        return result;
    }

    void release_batch(std::vector<OwnedBuffer>&& buffers) noexcept {
        if (buffers.empty()) return;

        stats_total_released_.fetch_add(buffers.size(), std::memory_order_relaxed);

        // Zero-fill all buffers first
        for (auto& buf : buffers) { buf.zero_fill(); }

        auto& tls = get_tls_cache();

        // Fill TLS first
        while (!buffers.empty() && !tls.full()) {
            if (tls.try_push(std::move(buffers.back()))) { buffers.pop_back(); }
            else { break; }
        }

        // Return rest directly to shards
        if (!buffers.empty()) {
            const size_t primary = current_shard_index();

            for (size_t i = 0; i < SHARD_COUNT && !buffers.empty(); ++i) {
                const size_t idx = (primary + i) & SHARD_MASK;
                shards_[idx]->return_batch(buffers);
            }
        }

        // Any remaining will be deallocated by vector destructor
    }

    void release(OwnedBuffer&& buffer) noexcept {
        if (buffer.empty()) return;

        stats_total_released_.fetch_add(1, std::memory_order_relaxed);

        // Security: zero-fill before pooling
        buffer.zero_fill();

        // Fast path: TLS cache has room
        auto& tls = get_tls_cache();
        if (tls.try_push(std::move(buffer))) {
            // Check if we should proactively drain
            if (tls.needs_drain()) {
                drain_tls_to_global(tls);
            }
            return;
        }

        // Slow path: TLS full, drain to global
        release_slow_path(tls, std::move(buffer));
    }

    [[nodiscard]] Stats stats() const noexcept {
        Stats s;
        s.total_acquired = stats_total_acquired_.load(std::memory_order_relaxed);
        s.total_released = stats_total_released_.load(std::memory_order_relaxed);
        s.tls_hits = stats_tls_hits_.load(std::memory_order_relaxed);
        s.shard_hits = stats_shard_hits_.load(std::memory_order_relaxed);
        s.allocations = stats_allocations_.load(std::memory_order_relaxed);
        s.peak_allocated = stats_peak_allocated_.load(std::memory_order_relaxed);

        // Sum current pooled across all shards
        for (const auto& shard : shards_) {
            s.current_pooled += shard->size();
        }

        return s;
    }

    [[nodiscard]] size_t block_size() const noexcept { return block_size_; }
    [[nodiscard]] size_t alignment() const noexcept { return alignment_; }

private:
    // ------------------------------------------------------------------------
    // TLS Management
    // ------------------------------------------------------------------------

    /**
     * Gets or creates the thread-local cache for current thread.
     */
    [[nodiscard]] TLSCache& get_tls_cache() noexcept {
        // thread_local ensures each thread gets its own cache
        thread_local TLSCache cache;
        return cache;
    }

    // ------------------------------------------------------------------------
    // Slow Paths
    // ------------------------------------------------------------------------

    /**
     * Slow path for acquire: refill TLS from global shards.
     */
    [[nodiscard]] OwnedBuffer acquire_slow_path(TLSCache& tls, bool skip_zero_fill = false) {
        // Try to refill TLS from global shards
        std::vector<OwnedBuffer> batch;
        batch.reserve(BATCH_REFILL);

        // Try primary shard first
        const size_t primary = current_shard_index();
        size_t taken = shards_[primary]->take_batch(batch, BATCH_REFILL);

        // If primary doesn't have enough, try other shards
        if (taken < BATCH_REFILL) {
            for (size_t i = 1; i < SHARD_COUNT && taken < BATCH_REFILL; ++i) {
                const size_t idx = (primary + i) & SHARD_MASK;
                taken += shards_[idx]->take_batch(batch, BATCH_REFILL - taken);
            }
        }

        if (!batch.empty()) {
            stats_shard_hits_.fetch_add(batch.size(), std::memory_order_relaxed);

            // Take one for immediate return
            auto result = std::move(batch.back());
            batch.pop_back();

            // Put rest in TLS
            tls.refill_from(batch);

            if (!skip_zero_fill) result.zero_fill();
            return result;
        }

        // Cold path: allocate new buffer
        return allocate_new(skip_zero_fill);
    }

    /**
     * Slow path for release: drain TLS overflow to global shards.
     */
    void release_slow_path(TLSCache& tls, OwnedBuffer&& buffer) noexcept {
        // Drain some buffers from TLS to make room
        std::vector<OwnedBuffer> drain_batch;
        drain_batch.reserve(BATCH_DRAIN + 1);

        // Add the new buffer
        drain_batch.push_back(std::move(buffer));

        // Drain excess from TLS
        tls.drain_to(drain_batch, TLS_CAPACITY - BATCH_DRAIN);

        // Return batch to global shard
        const size_t primary = current_shard_index();
        size_t returned = shards_[primary]->return_batch(drain_batch);

        // If primary is full, try other shards
        if (!drain_batch.empty()) {
            for (size_t i = 1; i < SHARD_COUNT && !drain_batch.empty(); ++i) {
                const size_t idx = (primary + i) & SHARD_MASK;
                returned += shards_[idx]->return_batch(drain_batch);
            }
        }

        // Any remaining buffers are deallocated (vector destructor)
        // This happens when all shards are full
    }

    /**
     * Proactive drain when TLS is getting full.
     */
    void drain_tls_to_global(TLSCache& tls) noexcept {
        std::vector<OwnedBuffer> drain_batch;
        drain_batch.reserve(BATCH_DRAIN);

        tls.drain_to(drain_batch, TLS_CAPACITY / 2);

        if (!drain_batch.empty()) {
            const size_t primary = current_shard_index();
            shards_[primary]->return_batch(drain_batch);

            // If still have buffers, try other shards
            for (size_t i = 1; i < SHARD_COUNT && !drain_batch.empty(); ++i) {
                const size_t idx = (primary + i) & SHARD_MASK;
                shards_[idx]->return_batch(drain_batch);
            }
        }
    }

    /**
     * Allocates a new buffer (cold path).
     */
    [[nodiscard]] OwnedBuffer allocate_new(bool skip_zero_fill = false) {
        stats_allocations_.fetch_add(1, std::memory_order_relaxed);

        // Track peak allocation
        const auto current = stats_current_allocated_.fetch_add(1, std::memory_order_relaxed) + 1;
        size_t peak = stats_peak_allocated_.load(std::memory_order_relaxed);
        while (current > peak) {
            if (stats_peak_allocated_.compare_exchange_weak(
                    peak, current, std::memory_order_relaxed)) {
                break;
            }
        }

        auto buf = OwnedBuffer::allocate(block_size_, alignment_);
        if (!skip_zero_fill) buf.zero_fill();
        return buf;
    }

    // ------------------------------------------------------------------------
    // Member Variables
    // ------------------------------------------------------------------------

    // Configuration
    size_t block_size_;
    size_t alignment_;
    size_t max_per_shard_;

    // Global sharded pool
    std::vector<std::unique_ptr<Shard>> shards_;

    // Statistics (atomic for lock-free access)
    std::atomic<uint64_t> stats_total_acquired_{0};
    std::atomic<uint64_t> stats_total_released_{0};
    std::atomic<uint64_t> stats_tls_hits_{0};
    std::atomic<uint64_t> stats_shard_hits_{0};
    std::atomic<uint64_t> stats_allocations_{0};
    std::atomic<size_t> stats_current_allocated_{0};
    std::atomic<size_t> stats_peak_allocated_{0};
};

// ============================================================================
// BufferPool Public API
// ============================================================================

std::unique_ptr<BufferPool> BufferPool::create(
    size_t block_size,
    size_t alignment,
    size_t max_pooled
) {
    return std::unique_ptr<BufferPool>(new BufferPool(block_size, alignment, max_pooled));
}

BufferPool::BufferPool(size_t block_size, size_t alignment, size_t max_pooled)
    : impl_{std::make_unique<Impl>(block_size, alignment, max_pooled)} {}

BufferPool::~BufferPool() = default;

OwnedBuffer BufferPool::acquire(bool skip_zero_fill) {
    return impl_->acquire(skip_zero_fill);
}

std::vector<OwnedBuffer> BufferPool::acquire_batch(size_t count, bool skip_zero_fill) { return impl_->acquire_batch(count, skip_zero_fill); }

void BufferPool::release(OwnedBuffer&& buffer) noexcept {
    impl_->release(std::move(buffer));
}

void BufferPool::release_batch(std::vector<OwnedBuffer>&& buffers) noexcept {
    impl_->release_batch(std::move(buffers));
}

BufferPool::Stats BufferPool::stats() const noexcept { return impl_->stats(); }

size_t BufferPool::block_size() const noexcept {
    return impl_->block_size();
}

size_t BufferPool::alignment() const noexcept {
    return impl_->alignment();
}

} // namespace akkaradb::core