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

// internal/include/core/buffer/PerThreadArena.hpp
#pragma once

#include "OwnedBuffer.hpp"
#include <memory>
#include <atomic>

namespace akkaradb::core {
    /**
     * PerThreadArena - High-performance thread-local buffer allocator.
     *
     * This allocator combines three strategies for maximum performance:
     * 1. Clean buffer pool: Zero-filled buffers ready for immediate use
     * 2. Dirty buffer queue: Returned buffers awaiting zero-fill
     * 3. Bump allocator: Fast linear allocation from pre-allocated arenas
     *
     * Architecture:
     * - Thread-local state: Each thread has independent state (zero contention)
     * - Batch zero-fill: Opportunistically cleans multiple buffers at once
     * - Lazy allocation: Arenas are allocated on-demand per thread
     *
     * Performance characteristics:
     * - acquire() from clean pool: ~10 ns (common case)
     * - acquire() from arena: ~15 ns (initial allocations)
     * - release() to dirty queue: ~5 ns (deferred zero-fill)
     * - batch zero-fill: ~1 μs per buffer, amortized over batch
     *
     * Design principles:
     * - Lock-free: All operations are thread-local (no locks, no atomics in fast path)
     * - Zero-copy: Buffers are moved, never copied
     * - Predictable: Bump allocation has O(1) worst-case latency
     * - Memory efficient: Arenas are shared across buffer reuses
     *
     * Typical usage:
     * ```cpp
     * auto arena = PerThreadArena::create(32 * 1024, 4096);
     * auto buf = arena->acquire();     // Fast: usually clean pool hit
     * // ... use buffer ...
     * arena->release(std::move(buf));  // Fast: push to dirty queue
     * ```
     *
     * Thread-safety: Fully thread-safe. Each thread has isolated state.
     * Buffers should not be passed between threads (undefined behavior).
     */
    class PerThreadArena {
        public:
            /**
             * Arena statistics for monitoring.
             */
            struct Stats {
                uint64_t total_acquired{0}; ///< Total buffers acquired
                uint64_t total_released{0}; ///< Total buffers released
                uint64_t clean_hits{0}; ///< Acquisitions from clean pool
                uint64_t dirty_hits{0}; ///< Acquisitions requiring immediate zero-fill
                uint64_t arena_allocs{0}; ///< Fresh allocations from bump allocator
                uint64_t fallback_allocs{0}; ///< Fallback to aligned_alloc (arena exhausted)
                uint64_t batch_cleanings{0}; ///< Number of batch zero-fill operations
                size_t current_clean{0}; ///< Current buffers in clean pool
                size_t current_dirty{0}; ///< Current buffers in dirty queue

                /**
                 * Returns clean pool hit rate (0.0 to 1.0).
                 * Higher is better - indicates buffers are ready without processing.
                 */
                [[nodiscard]] double clean_hit_rate() const noexcept {
                    const auto total = clean_hits + dirty_hits + arena_allocs + fallback_allocs;
                    return total > 0
                               ? static_cast<double>(clean_hits) / total
                               : 0.0;
                }

                /**
                 * Returns overall pool hit rate (clean + dirty).
                 */
                [[nodiscard]] double pool_hit_rate() const noexcept {
                    const auto total = clean_hits + dirty_hits + arena_allocs + fallback_allocs;
                    return total > 0
                               ? static_cast<double>(clean_hits + dirty_hits) / total
                               : 0.0;
                }

                /**
                 * Returns arena allocation hit rate (avoiding aligned_alloc).
                 */
                [[nodiscard]] double arena_hit_rate() const noexcept {
                    const auto new_allocs = arena_allocs + fallback_allocs;
                    return new_allocs > 0
                               ? static_cast<double>(arena_allocs) / new_allocs
                               : 0.0;
                }
            };

            /**
             * Creates a new PerThreadArena.
             *
             * @param block_size Size of each buffer in bytes
             * @param alignment Buffer alignment (must be power of 2, typically 4096)
             * @param arena_capacity Number of blocks per thread-local arena (default: 256)
             * @param clean_capacity Maximum buffers in clean pool per thread (default: 16)
             * @param dirty_capacity Maximum buffers in dirty queue per thread (default: 16)
             * @return Unique pointer to the arena
             * @throws std::bad_alloc if allocation fails
             * @throws std::invalid_argument if alignment is invalid
             */
            [[nodiscard]] static std::unique_ptr<PerThreadArena> create(
                size_t block_size = 32 * 1024,
                size_t alignment = 4096,
                size_t arena_capacity = 256,
                size_t clean_capacity = 16,
                size_t dirty_capacity = 16
            );

            /**
             * Destructor.
             */
            ~PerThreadArena();

            /**
             * Non-copyable.
             */
            PerThreadArena(const PerThreadArena&) = delete;
            PerThreadArena& operator=(const PerThreadArena&) = delete;

            /**
             * Non-movable (shared state with thread-local storage).
             */
            PerThreadArena(PerThreadArena&&) = delete;
            PerThreadArena& operator=(PerThreadArena&&) = delete;

            /**
             * Acquires a buffer from the arena.
             *
             * Fast path (common): Returns from clean pool (~10 ns).
             * Slow path: Cleans dirty buffers or allocates from arena.
             *
             * @param skip_zero_fill If true, skip zero-filling (caller will overwrite entirely)
             * @return An OwnedBuffer of size block_size
             * @throws std::bad_alloc if allocation fails
             */
            [[nodiscard]] OwnedBuffer acquire(bool skip_zero_fill = false);

            /**
             * Releases a buffer back to the arena.
             *
             * Fast path (common): Pushes to dirty queue (~5 ns).
             * Opportunistically triggers batch zero-fill if conditions are met.
             *
             * The buffer will be zero-filled before being returned to clean pool,
             * either immediately (if batch threshold reached) or lazily (on next acquire).
             *
             * @param buffer Buffer to release (moved)
             */
            void release(OwnedBuffer&& buffer) noexcept;

            /**
             * Returns current arena statistics.
             *
             * Thread-safe: Can be called concurrently with other operations.
             * Note: Statistics are approximate due to relaxed memory ordering.
             *
             * @return Arena statistics snapshot
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

            /**
             * Forces batch zero-fill of all dirty buffers.
             *
             * Useful for ensuring buffers are cleaned before low-priority periods.
             * Normally not needed - batch cleaning happens automatically.
             */
            void force_clean_all() noexcept;

        private:
            /**
             * Private constructor (use create() factory method).
             */
            PerThreadArena(size_t block_size, size_t alignment, size_t arena_capacity, size_t clean_capacity, size_t dirty_capacity);

            /**
             * Pimpl: Implementation class (defined in .cpp).
             */
            class Impl;
            std::unique_ptr<Impl> impl_;

            // Shared configuration (read-only, can be accessed without synchronization)
            size_t block_size_;
            size_t alignment_;
            size_t arena_capacity_;
            size_t clean_capacity_;
            size_t dirty_capacity_;

            // Global statistics (atomic for lock-free aggregation)
            std::atomic<uint64_t> stats_total_acquired_{0};
            std::atomic<uint64_t> stats_total_released_{0};
            std::atomic<uint64_t> stats_clean_hits_{0};
            std::atomic<uint64_t> stats_dirty_hits_{0};
            std::atomic<uint64_t> stats_arena_allocs_{0};
            std::atomic<uint64_t> stats_fallback_allocs_{0};
            std::atomic<uint64_t> stats_batch_cleanings_{0};
    };
} // namespace akkaradb::core
