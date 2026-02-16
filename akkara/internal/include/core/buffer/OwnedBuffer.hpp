/*
* AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkEngine is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkEngine is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkEngine.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/include/core/buffer/OwnedBuffer.hpp
#pragma once

#include "BufferView.hpp"
#include <memory>
#include <cstring>

namespace akkaradb::core {
    /**
 * OwnedBuffer - RAII-managed buffer with aligned allocation.
 *
 * This class owns a contiguous block of memory with specified alignment.
 * Memory is automatically freed when the buffer is destroyed or moved from.
 *
 * Design principles:
 * - RAII: Automatic memory management via unique_ptr
 * - Move-only: Non-copyable to prevent accidental duplications
 * - Aligned allocation: Supports Direct I/O (e.g., 4096-byte alignment)
 * - Zero-copy views: Provides BufferView without copying data
 *
 * Typical usage:
 * ```cpp
 * auto buf = OwnedBuffer::allocate(32 * 1024, 4096);  // 32 KiB, 4K-aligned
 * buf.zero_fill();
 * auto view = buf.view();
 * view.write_u32_le(0, 0x12345678);
 * ```
 *
 * Thread-safety: NOT thread-safe. Caller must ensure exclusive access.
 */
    class OwnedBuffer {
        public:
            /**
     * Constructs an empty buffer.
     */
            OwnedBuffer() noexcept = default;

            /**
     * Allocates a new buffer with specified size and alignment.
     *
     * @param size Size in bytes
     * @param alignment Buffer alignment (must be power of 2, typically 4096)
     * @return Newly allocated buffer
     * @throws std::bad_alloc if allocation fails
     * @throws std::invalid_argument if alignment is not power of 2
     */
            [[nodiscard]] static OwnedBuffer allocate(size_t size, size_t alignment = 4096);

            /**
     * Wraps externally-managed memory as a non-owning buffer.
     *
     * This is useful for arena allocators where memory lifetime is managed
     * by the arena, not by individual buffers.
     *
     * IMPORTANT: The caller must ensure that the memory remains valid for
     * the lifetime of this OwnedBuffer. The memory will NOT be freed when
     * this buffer is destroyed.
     *
     * @param data Pointer to externally-managed memory
     * @param size Size in bytes
     * @return Non-owning OwnedBuffer wrapping the memory
     */
            [[nodiscard]] static OwnedBuffer wrap_non_owning(std::byte* data, size_t size) noexcept;

            /**
     * Move constructor.
     */
            OwnedBuffer(OwnedBuffer&& other) noexcept = default;

            /**
     * Move assignment operator.
     */
            OwnedBuffer& operator=(OwnedBuffer&& other) noexcept = default;

            /**
     * Deleted copy constructor (move-only).
     */
            OwnedBuffer(const OwnedBuffer&) = delete;

            /**
     * Deleted copy assignment operator (move-only).
     */
            OwnedBuffer& operator=(const OwnedBuffer&) = delete;

            /**
     * Returns a BufferView over the entire buffer (const).
     */
            [[nodiscard]] BufferView view() const noexcept { return BufferView{data_.get(), size_}; }

            /**
     * Returns a BufferView over the entire buffer (non-const).
     */
            [[nodiscard]] BufferView view() noexcept { return BufferView{data_.get(), size_}; }

            /**
     * Returns the raw pointer to the buffer data.
     */
            [[nodiscard]] std::byte* data() noexcept { return data_.get(); }

            /**
     * Returns the raw pointer to the buffer data (const).
     */
            [[nodiscard]] const std::byte* data() const noexcept { return data_.get(); }

            /**
     * Returns the size of the buffer in bytes.
     */
            [[nodiscard]] size_t size() const noexcept { return size_; }

            /**
     * Creates a deep copy of this buffer.
     *
     * Allocates new memory and copies all data.
     *
     * @return New OwnedBuffer with copied data
     * @throws std::bad_alloc if allocation fails
     */
            [[nodiscard]] OwnedBuffer clone() const {
                if (empty()) { return OwnedBuffer{}; }

                auto new_buf = allocate(size_);
                std::memcpy(new_buf.data(), data_.get(), size_);
                return new_buf;
            }

            /**
     * Returns true if the buffer is empty (size == 0).
     */
            [[nodiscard]] bool empty() const noexcept { return size_ == 0; }

            /**
     * Fills the entire buffer with zeros.
     * Uses optimized SIMD implementation via BufferView.
     */
            void zero_fill() noexcept { if (data_ && size_ > 0) { view().zero_fill(); } }

            /**
     * Releases ownership of the buffer and returns the raw pointer.
     *
     * After calling this, the OwnedBuffer becomes empty and the caller
     * is responsible for freeing the memory using the platform-specific
     * aligned deallocation function.
     *
     * @return Raw pointer to the buffer, or nullptr if empty
     */
            [[nodiscard]] std::byte* release() noexcept {
                size_ = 0;
                return data_.release();
            }

        private:
            /**
     * Custom deleter for aligned memory.
     *
     * Uses platform-specific aligned deallocation:
     * - Windows: _aligned_free
     * - POSIX: free (works for both aligned_alloc and posix_memalign)
     *
     * Supports non-owning mode for arena-allocated buffers.
     */
            struct AlignedDeleter {
                bool owns_memory{true};
                void operator()(std::byte* ptr) const noexcept;
            };

            std::unique_ptr<std::byte[], AlignedDeleter> data_;
            size_t size_{0};

            /**
     * Private constructor (use allocate() or wrap_non_owning() factory methods).
     */
            OwnedBuffer(std::byte* data, size_t size, bool owns_memory) noexcept : data_{data, AlignedDeleter{owns_memory}}, size_{size} {}
    };
} // namespace akkaradb::core
