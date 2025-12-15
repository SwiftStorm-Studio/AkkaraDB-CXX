// BufferPool.hpp
#pragma once

#include "OwnedBuffer.hpp"
#include <vector>
#include <memory>
#include <atomic>
#include <mutex>

namespace akkaradb::core
{
    /**
 * BufferPool - Thread-safe pool of fixed-size buffers.
 *
 * This class provides efficient reuse of aligned buffers to reduce
 * allocation overhead and memory fragmentation. It serves as the C++
 * equivalent of JVM's BufferPool.
 *
 * Design principles:
 * - Lock-free fast path for single-threaded access via thread-local cache
 * - Lock-based global pool for cross-thread sharing
 * - Automatic buffer zeroing on release for security
 * - Configurable pool size limits
 *
 * Thread-safety: Fully thread-safe. Uses thread-local caching with
 * a global fallback pool protected by mutex.
 *
 * Typical usage:
 * ```cpp
 * auto pool = BufferPool::create(32 * 1024); // 32 KiB blocks
 * auto buf = pool->acquire();
 * // ... use buffer ...
 * pool->release(std::move(buf));
 * ```
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
     * @param alignment Buffer alignment (must be power of 2)
     * @param max_pooled_buffers Maximum buffers to retain in pool (0 = unlimited)
     * @return Shared pointer to the pool
     */
        [[nodiscard]] static std::shared_ptr<BufferPool> create(
            size_t block_size = 32 * 1024,
            size_t alignment = 4096,
            size_t max_pooled_buffers = 256
        );

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
     */
        [[nodiscard]] Stats stats() const noexcept;

        /**
     * Returns the configured block size.
     */
        [[nodiscard]] size_t block_size() const noexcept { return block_size_; }

        /**
     * Returns the configured alignment.
     */
        [[nodiscard]] size_t alignment() const noexcept { return alignment_; }

    private:
        BufferPool(size_t block_size, size_t alignment, size_t max_pooled);

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
} // namespace akkaradb::core