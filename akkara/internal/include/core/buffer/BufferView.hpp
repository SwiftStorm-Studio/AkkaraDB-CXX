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

// internal/include/core/buffer/BufferView.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace akkaradb::core {
    /**
     * BufferView - Zero-copy, non-owning view over a contiguous byte region.
     *
     * This class provides Little-Endian (LE) accessors and serves as the C++
     * equivalent of JVM's ByteBufferL for zero-copy operations.
     *
     * Design principles:
     * - No ownership: lifetime managed externally
     * - Stack-allocated: trivial copy/move
     * - Immutable view: all write operations modify in-place
     * - LE-only: all multi-byte reads/writes are Little-Endian
     *
     * Thread-safety: Read-only operations are thread-safe. Concurrent writes
     * to the same region require external synchronization.
     */
    class BufferView {
        public:
            /**
         * Constructs an empty BufferView.
         */
            constexpr BufferView() noexcept : data_{nullptr}, size_{0} {}

            /**
         * Constructs a BufferView from raw pointer and size.
         *
         * @param data Pointer to the beginning of the buffer
         * @param size Size in bytes
         */
            constexpr BufferView(std::byte* data, size_t size) noexcept : data_{data}, size_{size} {}

            /**
         * Constructs a BufferView from a std::span.
         *
         * @param span Byte span
         */
            constexpr explicit BufferView(std::span<std::byte> span) noexcept : data_{span.data()}, size_{span.size()} {}

            // ==================== Basic Accessors ====================

            /**
         * Returns the raw pointer to the buffer.
         */
            [[nodiscard]] constexpr std::byte* data() const noexcept { return data_; }

            /**
         * Returns the size of the buffer in bytes.
         */
            [[nodiscard]] constexpr size_t size() const noexcept { return size_; }

            /**
         * Returns true if the buffer is empty.
         */
            [[nodiscard]] constexpr bool empty() const noexcept { return size_ == 0; }

            /**
         * Returns a std::span view of the buffer.
         */
            [[nodiscard]] constexpr std::span<std::byte> as_span() const noexcept { return {data_, size_}; }

            /**
         * Returns a typed span view of the buffer.
         * T must be a byte-sized trivial type.
         */
            template <typename T>
            [[nodiscard]] constexpr std::span<const T> as_span() const noexcept {
                static_assert(sizeof(T) == 1);
                return {reinterpret_cast<const T*>(data_), size_};
            }

            /**
         * Returns a const std::span view of the buffer.
         */
            [[nodiscard]] constexpr std::span<const std::byte> as_const_span() const noexcept { return {data_, size_}; }

            // ==================== Slicing ====================

            /**
         * Creates a sub-view starting at offset with specified length.
         *
         * @param offset Starting offset (in bytes)
         * @param length Length of the slice (in bytes)
         * @return A new BufferView representing the slice
         * @throws std::out_of_range if offset + length > size()
         */
            [[nodiscard]] BufferView slice(size_t offset, size_t length) const;

            /**
         * Creates a sub-view starting at offset to the end.
         *
         * @param offset Starting offset (in bytes)
         * @return A new BufferView from offset to end
         * @throws std::out_of_range if offset > size()
         */
            [[nodiscard]] BufferView slice(size_t offset) const;

            // ==================== Little-Endian Read Operations ====================

            /**
         * Reads an unsigned 8-bit integer.
         *
         * @param offset Offset in bytes
         * @return uint8_t value
         * @throws std::out_of_range if offset >= size()
         */
            [[nodiscard]] uint8_t read_u8(size_t offset) const;

            /**
         * Reads an unsigned 16-bit integer (Little-Endian).
         *
         * @param offset Offset in bytes
         * @return uint16_t value
         * @throws std::out_of_range if offset + 2 > size()
         */
            [[nodiscard]] uint16_t read_u16_le(size_t offset) const;

            /**
         * Reads an unsigned 32-bit integer (Little-Endian).
         *
         * @param offset Offset in bytes
         * @return uint32_t value
         * @throws std::out_of_range if offset + 4 > size()
         */
            [[nodiscard]] uint32_t read_u32_le(size_t offset) const;

            /**
         * Reads an unsigned 64-bit integer (Little-Endian).
         *
         * @param offset Offset in bytes
         * @return uint64_t value
         * @throws std::out_of_range if offset + 8 > size()
         */
            [[nodiscard]] uint64_t read_u64_le(size_t offset) const;

            // ==================== Little-Endian Write Operations ====================

            /**
         * Writes an unsigned 8-bit integer.
         *
         * @param offset Offset in bytes
         * @param value Value to write
         * @throws std::out_of_range if offset >= size()
         */
            void write_u8(size_t offset, uint8_t value) const;

            /**
         * Writes an unsigned 16-bit integer (Little-Endian).
         *
         * @param offset Offset in bytes
         * @param value Value to write
         * @throws std::out_of_range if offset + 2 > size()
         */
            void write_u16_le(size_t offset, uint16_t value) const;

            /**
         * Writes an unsigned 32-bit integer (Little-Endian).
         *
         * @param offset Offset in bytes
         * @param value Value to write
         * @throws std::out_of_range if offset + 4 > size()
         */
            void write_u32_le(size_t offset, uint32_t value) const;

            /**
         * Writes an unsigned 64-bit integer (Little-Endian).
         *
         * @param offset Offset in bytes
         * @param value Value to write
         * @throws std::out_of_range if offset + 8 > size()
         */
            void write_u64_le(size_t offset, uint64_t value) const;

            // ==================== Bulk Operations ====================

            /**
         * Copies data from source buffer to this buffer.
         *
         * @param offset Destination offset
         * @param src Source buffer
         * @param src_offset Source offset
         * @param length Number of bytes to copy
         * @throws std::out_of_range if bounds are violated
         */
            void copy_from(size_t offset, BufferView src, size_t src_offset, size_t length) const;

            /**
         * Fills a region with a specific byte value.
         *
         * @param offset Starting offset
         * @param length Number of bytes to fill
         * @param value Byte value to fill with
         * @throws std::out_of_range if offset + length > size()
         */
            void fill(size_t offset, size_t length, std::byte value) const;

            /**
         * Fills the entire buffer with zeros.
         */
            void zero_fill() const noexcept;

            // ==================== CRC Computation ====================

            /**
         * Computes CRC32C checksum over a range.
         *
         * Uses hardware acceleration (SSE4.2 crc32c instruction) when available.
         *
         * @param offset Starting offset
         * @param length Number of bytes to checksum
         * @return 32-bit CRC32C value
         * @throws std::out_of_range if offset + length > size()
         */
            [[nodiscard]] uint32_t crc32c(size_t offset, size_t length) const;

            /**
         * Computes CRC32C checksum over the entire buffer.
         *
         * @return 32-bit CRC32C value
         */
            [[nodiscard]] uint32_t crc32c() const noexcept { return crc32c(0, size_); }

            // ==================== String Operations ====================

            /**
         * Creates a string_view from a region of the buffer.
         *
         * @param offset Starting offset
         * @param length Number of bytes
         * @return std::string_view over the region
         * @throws std::out_of_range if offset + length > size()
         */
            [[nodiscard]] std::string_view as_string_view(size_t offset, size_t length) const;

            /**
         * Creates a string_view over the entire buffer.
         */
            [[nodiscard]] std::string_view as_string_view() const noexcept { return {reinterpret_cast<const char*>(data_), size_}; }

        private:
            std::byte* data_;
            size_t size_;

            void check_bounds(size_t offset, size_t length) const;
    };
} // namespace akkaradb::core