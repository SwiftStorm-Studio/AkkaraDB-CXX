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
#include <cstddef>

namespace akkaradb::core {
    /**
 * BufferPool - Thread-safe pool of fixed-size aligned buffers.
 *
 * This class provides efficient reuse of aligned buffers to reduce
 * allocation overhead and memory fragmentation. Uses Pimpl idiom
 * for ABI stability in shared library builds.
 *
 * Design principles:
 * - Thread-safe: All operations are protected by internal synchronization
 * - Zero-filled: Buffers are automatically zero-filled on release for security
 * - Configurable: Pool size limit can be set to prevent unbounded growth
 * - Pimpl idiom: Implementation details hidden for ABI stability
 *
 * Typical usage:
 * ```cpp
 * auto pool = BufferPool::create(32 * 1024, 4096, 256);
 * auto buf = pool->acquire();
 * // ... use buffer ...
 * pool->release(std::move(buf));
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
            uint64_t pool_hits{0}; ///< Acquisitions from pool
            uint64_t pool_misses{0}; ///< Acquisitions requiring allocation
            size_t current_pooled{0}; ///< Current buffers in pool
            size_t peak_allocated{0}; ///< Peak simultaneous allocations

            [[nodiscard]] double hit_rate() const noexcept {
                auto total = pool_hits + pool_misses;
                return total > 0 ? static_cast<double>(pool_hits) / total : 0.0;
            }
        };

        /**
     * Creates a new BufferPool.
     *
     * @param block_size Size of each buffer in bytes
     * @param alignment Buffer alignment (must be power of 2, typically 4096)
     * @param max_pooled Maximum buffers to retain in pool (0 = unlimited)
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
     * Non-movable (Pimpl idiom requires explicit move implementation).
     */
        BufferPool(BufferPool&&) = delete;
        BufferPool& operator=(BufferPool&&) = delete;

        /**
     * Acquires a buffer from the pool or allocates a new one.
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
     * The buffer is automatically zero-filled before being returned to
     * the pool for security. If the pool is full, the buffer is deallocated.
     *
     * @param buffer Buffer to release (moved)
     */
        void release(OwnedBuffer&& buffer) noexcept;

        /**
     * Returns current pool statistics.
     *
     * Thread-safe: Can be called concurrently with other operations.
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