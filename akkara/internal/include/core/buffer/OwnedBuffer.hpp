/*
* AkkEngine
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkEngine.
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
     * Move constructor.
     */
            OwnedBuffer(OwnedBuffer&& other) noexcept
                : data_{std::move(other.data_)}, size_{other.size_} {
                other.size_ = 0; // Reset source size so empty() stays consistent with data_
            }

            /**
     * Move assignment operator.
     */
            OwnedBuffer& operator=(OwnedBuffer&& other) noexcept {
                if (this != &other) {
                    data_ = std::move(other.data_);
                    size_ = other.size_;
                    other.size_ = 0; // Reset source size so empty() stays consistent with data_
                }
                return *this;
            }

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
     */
            void zero_fill() noexcept { if (data_ && size_ > 0) { std::memset(data_.get(), 0, size_); } }

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
     */
            struct AlignedDeleter {
                void operator()(std::byte* ptr) const noexcept;
            };

            std::unique_ptr<std::byte[], AlignedDeleter> data_;
            size_t size_{0};

            /**
     * Private constructor (use allocate() factory method).
     */
            OwnedBuffer(std::byte* data, size_t size) noexcept : data_{data}, size_{size} {}
    };
} // namespace akkaradb::core