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

// internal/include/core/buffer/PooledBuffer.hpp
#pragma once

#include "BufferPool.hpp"
#include "OwnedBuffer.hpp"
#include <memory>

namespace akkaradb::core {
    /**
 * PooledBuffer - RAII guard that automatically returns buffer to pool.
 *
 * This class provides exception-safe buffer management by ensuring
 * that buffers are always returned to the pool when going out of scope,
 * even if an exception is thrown.
 *
 * Key benefits:
 * - Exception safety: Buffer is returned even on exception
 * - Automatic release: No manual release() calls needed
 * - Move semantics: Efficient ownership transfer
 * - Zero overhead: Destructor is trivial when released
 *
 * Usage patterns:
 *
 * 1. Scoped usage (auto-return):
 * ```cpp
 * {
 *     auto buf = PooledBuffer::acquire(pool);
 *     buf->zero_fill();
 *     // ... use buffer ...
 * } // Automatically returned to pool here
 * ```
 *
 * 2. Transfer ownership (no auto-return):
 * ```cpp
 * auto buf = PooledBuffer::acquire(pool);
 * process_data(buf.view());
 * callback(buf.release());  // Caller now owns buffer
 * ```
 *
 * 3. Conditional release:
 * ```cpp
 * auto buf = PooledBuffer::acquire(pool);
 * if (should_keep) {
 *     result = buf.release();  // Transfer ownership
 * }
 * // If not released, returned to pool in destructor
 * ```
 *
 * Thread-safety: NOT thread-safe. Each PooledBuffer should be used
 * by only one thread at a time.
 */
    class PooledBuffer {
    public:
        /**
     * Acquires a buffer from pool and wraps it in RAII guard.
     *
     * @param pool Pool to acquire from (must outlive this PooledBuffer)
     * @return PooledBuffer wrapping the acquired buffer
     * @throws std::bad_alloc if allocation fails
     */
        [[nodiscard]] static PooledBuffer acquire(BufferPool& pool) { return PooledBuffer{pool, pool.acquire()}; }

        /**
     * Acquires a buffer from shared pool.
     *
     * @param pool Shared pointer to pool
     * @return PooledBuffer wrapping the acquired buffer
     * @throws std::bad_alloc if allocation fails
     */
        [[nodiscard]] static PooledBuffer acquire(std::shared_ptr<BufferPool> pool) {
            auto buf = pool->acquire();
            return PooledBuffer{std::move(pool), std::move(buf)};
        }

        /**
     * Default constructor (empty buffer, no pool).
     */
        PooledBuffer() noexcept = default;

        /**
     * Move constructor.
     */
        PooledBuffer(PooledBuffer&& other) noexcept : pool_ref_{other.pool_ref_}
                                                      , pool_shared_{std::move(other.pool_shared_)}
                                                      , buffer_{std::move(other.buffer_)} { other.pool_ref_ = nullptr; }

        /**
     * Move assignment.
     */
        PooledBuffer& operator=(PooledBuffer&& other) noexcept {
            if (this != &other) {
                return_to_pool();
                pool_ref_ = other.pool_ref_;
                pool_shared_ = std::move(other.pool_shared_);
                buffer_ = std::move(other.buffer_);
                other.pool_ref_ = nullptr;
            }
            return *this;
        }

        /**
     * Destructor - returns buffer to pool if not released.
     */
        ~PooledBuffer() noexcept { return_to_pool(); }

        // Non-copyable
        PooledBuffer(const PooledBuffer&) = delete;
        PooledBuffer& operator=(const PooledBuffer&) = delete;

        // ==================== Accessors ====================

        /**
     * Returns a BufferView over the buffer.
     */
        [[nodiscard]] BufferView view() noexcept { return buffer_.view(); }

        /**
     * Returns a const BufferView over the buffer.
     */
        [[nodiscard]] BufferView view() const noexcept { return buffer_.view(); }

        /**
     * Returns raw pointer to buffer data.
     */
        [[nodiscard]] std::byte* data() noexcept { return buffer_.data(); }

        /**
     * Returns const raw pointer to buffer data.
     */
        [[nodiscard]] const std::byte* data() const noexcept { return buffer_.data(); }

        /**
     * Returns buffer size.
     */
        [[nodiscard]] size_t size() const noexcept { return buffer_.size(); }

        /**
     * Returns true if buffer is empty.
     */
        [[nodiscard]] bool empty() const noexcept { return buffer_.empty(); }

        /**
     * Pointer-like access to underlying OwnedBuffer.
     */
        [[nodiscard]] OwnedBuffer* operator->() noexcept { return &buffer_; }

        /**
     * Const pointer-like access to underlying OwnedBuffer.
     */
        [[nodiscard]] const OwnedBuffer* operator->() const noexcept { return &buffer_; }

        /**
     * Reference access to underlying OwnedBuffer.
     */
        [[nodiscard]] OwnedBuffer& operator*() noexcept { return buffer_; }

        /**
     * Const reference access to underlying OwnedBuffer.
     */
        [[nodiscard]] const OwnedBuffer& operator*() const noexcept { return buffer_; }

        // ==================== Ownership Transfer ====================

        /**
     * Releases ownership of the buffer.
     *
     * After this call, the PooledBuffer is empty and the returned
     * OwnedBuffer is the caller's responsibility.
     *
     * @return The owned buffer
     */
        [[nodiscard]] OwnedBuffer release() noexcept {
            pool_ref_ = nullptr;
            pool_shared_.reset();
            return std::move(buffer_);
        }

        /**
     * Checks if buffer has been released.
     */
        [[nodiscard]] bool released() const noexcept { return pool_ref_ == nullptr && !pool_shared_; }

    private:
        /**
     * Constructor from pool reference.
     */
        PooledBuffer(BufferPool& pool, OwnedBuffer buffer) noexcept : pool_ref_{&pool}
                                                                      , buffer_{std::move(buffer)} {}

        /**
     * Constructor from shared pool.
     */
        PooledBuffer(std::shared_ptr<BufferPool> pool, OwnedBuffer buffer) noexcept : pool_shared_{std::move(pool)}
                                                                                      , buffer_{std::move(buffer)} {}

        /**
     * Returns buffer to pool (if not released).
     */
        void return_to_pool() noexcept {
            if (!buffer_.empty()) {
                if (pool_ref_) { pool_ref_->release(std::move(buffer_)); }
                else if (pool_shared_) { pool_shared_->release(std::move(buffer_)); }
            }
            pool_ref_ = nullptr;
            pool_shared_.reset();
        }

        BufferPool* pool_ref_{nullptr}; // Raw pointer (non-owning)
        std::shared_ptr<BufferPool> pool_shared_; // Shared pointer (owning)
        OwnedBuffer buffer_;
    };
} // namespace akkaradb::core