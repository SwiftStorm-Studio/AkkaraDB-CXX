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

// internal/include/core/buffer/BufferPool.hpp
#pragma once

#include "OwnedBuffer.hpp"
#include <memory>

namespace akkaradb::core {
    /**
     * BufferPool - High-performance thread-safe pool of fixed-size aligned buffers.
     *
     * This implementation uses a hybrid TLS + Sharded Global Pool design for
     * maximum throughput with minimal contention.
     *
     * Architecture:
     * - Thread-Local Pool: Each thread maintains a small local cache (lock-free)
     * - Sharded Global Pool: Multiple shards reduce contention on fallback
     * - Batch Transfer: TLS refills/drains in batches to amortize lock cost
     *
     * Performance characteristics:
     * - Hot path (TLS hit): ~5-10 ns (no locks, no atomics)
     * - Warm path (shard hit): ~50-100 ns (single lock, low contention)
     * - Cold path (allocation): ~500-2000 ns (aligned_alloc)
     *
     * Design principles:
     * - Lock-free fast path: TLS access requires no synchronization
     * - Low contention: Sharding distributes global pool access
     * - Cache-friendly: TLS pools fit in L1/L2 cache
     * - Zero-copy: Buffers are moved, never copied
     *
     * Typical usage:
     * ```cpp
     * auto pool = BufferPool::create(32 * 1024, 4096, 256);
     * auto buf = pool->acquire();   // Fast: usually TLS hit
     * // ... use buffer ...
     * pool->release(std::move(buf)); // Fast: usually TLS return
     * ```
     *
     * Thread-safety: Fully thread-safe. All public methods can be called
     * concurrently from multiple threads.
     */
    class BufferPool {
    public:
        /**
         * Pool statistics for monitoring.
         */
        struct Stats {
            uint64_t total_acquired{0}; ///< Total buffers acquired
            uint64_t total_released{0}; ///< Total buffers released
            uint64_t tls_hits{0}; ///< Acquisitions from TLS (fast path)
            uint64_t shard_hits{0}; ///< Acquisitions from global shard
            uint64_t allocations{0}; ///< Fresh allocations (cold path)
            size_t current_pooled{0}; ///< Current buffers in global pool
            size_t peak_allocated{0}; ///< Peak simultaneous allocations

            /**
             * Returns TLS hit rate (0.0 to 1.0).
             * Higher is better - indicates lock-free fast path usage.
             */
            [[nodiscard]] double tls_hit_rate() const noexcept {
                const auto total = tls_hits + shard_hits + allocations;
                return total > 0 ? static_cast<double>(tls_hits) / total : 0.0;
            }

            /**
             * Returns overall pool hit rate (TLS + shard hits).
             */
            [[nodiscard]] double pool_hit_rate() const noexcept {
                const auto total = tls_hits + shard_hits + allocations;
                return total > 0 ? static_cast<double>(tls_hits + shard_hits) / total : 0.0;
            }
        };

        /**
         * Creates a new BufferPool.
         *
         * @param block_size Size of each buffer in bytes
         * @param alignment Buffer alignment (must be power of 2, typically 4096)
         * @param max_pooled Maximum buffers to retain in global pool (0 = unlimited)
         * @return Unique pointer to the pool
         * @throws std::bad_alloc if allocation fails
         * @throws std::invalid_argument if alignment is invalid
         */
        [[nodiscard]] static std::unique_ptr<BufferPool> create(
            size_t block_size = 32 * 1024,
            size_t alignment = 4096,
            size_t max_pooled = 256
        );

        /**
         * Destructor.
         */
        ~BufferPool();

        /**
         * Non-copyable.
         */
        BufferPool(const BufferPool&) = delete;
        BufferPool& operator=(const BufferPool&) = delete;

        /**
         * Non-movable (shared state with TLS).
         */
        BufferPool(BufferPool&&) = delete;
        BufferPool& operator=(BufferPool&&) = delete;

        /**
         * Acquires a buffer from the pool or allocates a new one.
         *
         * Fast path: Returns from thread-local cache (no locks).
         * Slow path: Refills TLS from global shard, then returns.
         *
         * The returned buffer is guaranteed to be zero-filled.
         *
         * @return An OwnedBuffer of size block_size
         * @throws std::bad_alloc if allocation fails
         */
        [[nodiscard]] OwnedBuffer acquire();

        /**
         * Releases a buffer back to the pool.
         *
         * Fast path: Returns to thread-local cache (no locks).
         * Slow path: Drains TLS overflow to global shard.
         *
         * The buffer is automatically zero-filled before pooling.
         *
         * @param buffer Buffer to release (moved)
         */
        void release(OwnedBuffer&& buffer) noexcept;

        /**
         * Returns current pool statistics.
         *
         * Thread-safe: Can be called concurrently with other operations.
         * Note: Statistics are approximate due to relaxed memory ordering.
         *
         * @return Pool statistics snapshot
         */
        [[nodiscard]] Stats stats() const noexcept;

        /**
         * Returns the configured block size.
         */
    [[nodiscard]] size_t block_size() const noexcept;

    /**
     * Returns the configured alignment.
     */
    [[nodiscard]] size_t alignment() const noexcept;

private:
    /**
     * Private constructor (use create() factory method).
     */
    BufferPool(size_t block_size, size_t alignment, size_t max_pooled);

    /**
     * Pimpl: Implementation class (defined in .cpp).
     */
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace akkaradb::core