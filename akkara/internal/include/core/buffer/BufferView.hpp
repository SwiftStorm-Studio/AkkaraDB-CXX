/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/include/core/buffer/BufferView.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <type_traits>

namespace akkaradb::core {
    class OwnedBuffer;

    /**
     * @brief Non-owning, read-only view over a contiguous byte region.
     *
     * BufferView provides zero-copy access to a byte sequence without owning it.
     *
     * Design principles:
     * - Non-owning: lifetime is managed externally
     * - Read-only: no mutation allowed through this type
     * - Trivially copyable: cheap to pass by value
     * - Little-Endian only (for multi-byte reads)
     *
     * @warning
     * The underlying memory is NOT owned. The data is only valid while the source
     * buffer remains alive.
     *
     * If you need to retain the data, call to_owned().
     */
    class BufferView {
        public:
            // ==================== Constructors ====================

            constexpr BufferView() noexcept : data_{nullptr}, size_{0} {}

            constexpr BufferView(const std::byte* data, size_t size) noexcept : data_{data}, size_{size} {}

            constexpr explicit BufferView(std::span<const std::byte> span) noexcept : data_{span.data()}, size_{span.size()} {}

            // ==================== Basic Accessors ====================

            [[nodiscard]] constexpr const std::byte* data() const noexcept { return data_; }

            [[nodiscard]] constexpr size_t size() const noexcept { return size_; }

            [[nodiscard]] constexpr bool empty() const noexcept { return size_ == 0; }

            [[nodiscard]] constexpr std::span<const std::byte> as_span() const noexcept { return {data_, size_}; }

            /**
             * @brief Returns a typed span view (byte-compatible types only).
             *
             * Only char / unsigned char / std::byte are allowed to avoid
             * strict aliasing violations.
             */
            template <typename T>
            [[nodiscard]] constexpr std::span<const T> as_span() const noexcept {
                static_assert(std::is_same_v<T, std::byte> || std::is_same_v<T, char> || std::is_same_v<T, unsigned char>, "T must be a byte-like type");
                return {reinterpret_cast<const T*>(data_), size_};
            }

            // ==================== Slicing ====================

            [[nodiscard]] BufferView slice(size_t offset, size_t length) const;

            [[nodiscard]] BufferView slice(size_t offset) const;

            // ==================== Ownership ====================

            /**
             * @brief Creates an owning copy of this buffer.
             *
             * Performs a deep copy of the underlying byte sequence.
             *
             * @note O(n) operation.
             */
            [[nodiscard]] OwnedBuffer to_owned() const;

            // ==================== Little-Endian Reads ====================

            [[nodiscard]] uint8_t read_u8(size_t offset) const noexcept {
                check_bounds_inline(offset, 1);
                return static_cast<uint8_t>(data_[offset]);
            }

            [[nodiscard]] uint16_t read_u16_le(size_t offset) const noexcept {
                check_bounds_inline(offset, 2);
                uint16_t v;
                std::memcpy(&v, data_ + offset, 2);
                return v;
            }

            [[nodiscard]] uint32_t read_u32_le(size_t offset) const noexcept {
                check_bounds_inline(offset, 4);
                uint32_t v;
                std::memcpy(&v, data_ + offset, 4);
                return v;
            }

            [[nodiscard]] uint64_t read_u64_le(size_t offset) const noexcept {
                check_bounds_inline(offset, 8);
                uint64_t v;
                std::memcpy(&v, data_ + offset, 8);
                return v;
            }

            // ==================== CRC ====================

            [[nodiscard]] uint32_t crc32c(size_t offset, size_t length) const;

            [[nodiscard]] uint32_t crc32c() const noexcept { return crc32c(0, size_); }

            // ==================== String ====================

            /**
             * @brief Creates a string_view over a region.
             *
             * @warning Not null-terminated. Lifetime follows BufferView.
             */
            [[nodiscard]] std::string_view as_string_view(size_t offset, size_t length) const;

            [[nodiscard]] std::string_view as_string_view() const noexcept { return {reinterpret_cast<const char*>(data_), size_}; }

        private:
            const std::byte* data_;
            size_t size_;

            void check_bounds(size_t offset, size_t length) const;

            void check_bounds_inline(size_t offset, size_t length) const noexcept {
                #ifndef NDEBUG
                if (offset > size_ || length > size_ - offset) [[unlikely]] { check_bounds(offset, length); }
                #else
                (void)offset;
                (void)length;
                #endif
            }
    };

    static_assert(std::is_trivially_copyable_v<BufferView>);
    static_assert(std::is_trivially_destructible_v<BufferView>);
} // namespace
