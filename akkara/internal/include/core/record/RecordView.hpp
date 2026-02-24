/*
 * AkkaraDB - Low-latency, crash-safe JVM KV store with WAL & stripe parity
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

// internal/include/core/record/RecordView.hpp
#pragma once

#include "AKHdr32.hpp"
#include "core/buffer/BufferView.hpp"
#include <string_view>
#include <span>

namespace akkaradb::core {
    /**
     * RecordView - Zero-copy view of a record stored in a block.
     *
     * This class provides a non-owning view over a record consisting of:
     * - AKHdr32 header (32 bytes)
     * - Key bytes (k_len bytes)
     * - Value bytes (v_len bytes)
     *
     * Design principles:
     * - Zero-copy: No data duplication, only pointer arithmetic
     * - Non-owning: Lifetime tied to the underlying buffer
     * - Immutable: Read-only access to record data
     * - Stack-allocated: Trivial copy/move
     *
     * Typical usage:
     * ```cpp
     * // Assuming block contains packed records
     * RecordView view = RecordView::from_buffer(block_view, offset);
     *
     * auto key = view.key();
     * auto value = view.value();
     * uint64_t seq = view.header().seq;
     *
     * if (view.is_tombstone()) {
     *     // Handle deletion
     * }
     * ```
     *
     * Thread-safety: Read-only operations are thread-safe. The underlying
     * buffer must remain valid during the lifetime of this view.
     */
    class RecordView {
        public:
            /**
             * Constructs an empty RecordView.
             */
            constexpr RecordView() noexcept = default;

            /**
             * Constructs a RecordView from header and data pointers.
             *
             * @param header Pointer to AKHdr32 header
             * @param key Pointer to key data
             * @param value Pointer to value data
             */
            constexpr RecordView(const AKHdr32* header, const uint8_t* key, const uint8_t* value) noexcept : header_{header}, key_{key}, value_{value} {}

            /**
             * Parses a RecordView from a buffer at the given offset.
             *
             * Layout expectation:
             * [offset]              AKHdr32 (32 bytes)
             * [offset + 32]         key (k_len bytes)
             * [offset + 32 + k_len] value (v_len bytes)
             *
             * @param buffer Buffer containing the record
             * @param offset Starting offset of the record
             * @return RecordView pointing into the buffer
             * @throws std::out_of_range if buffer is too small
             */
            [[nodiscard]] static RecordView from_buffer(BufferView buffer, size_t offset);

            /**
             * Parses a RecordView assuming offset = 0.
             *
             * @param buffer Buffer containing the record at position 0
             * @return RecordView pointing into the buffer
             * @throws std::out_of_range if buffer is too small
             */
            [[nodiscard]] static RecordView from_buffer(BufferView buffer) { return from_buffer(buffer, 0); }

            // ==================== Accessors ====================

            /**
             * Returns a reference to the header.
             *
             * @return Const reference to AKHdr32
             * @throws std::runtime_error if RecordView is empty
             */
            [[nodiscard]] const AKHdr32& header() const;

            /**
             * Returns a span over the key bytes.
             */
            [[nodiscard]] std::span<const uint8_t> key() const noexcept { return {key_, header_->k_len}; }

            /**
             * Returns a span over the value bytes.
             */
            [[nodiscard]] std::span<const uint8_t> value() const noexcept { return {value_, header_->v_len}; }

            /**
             * Returns a string_view over the key (assumes UTF-8 or binary).
             */
            [[nodiscard]] std::string_view key_string() const noexcept { return {reinterpret_cast<const char*>(key_), header_->k_len}; }

            /**
             * Returns a string_view over the value (assumes UTF-8 or binary).
             */
            [[nodiscard]] std::string_view value_string() const noexcept { return {reinterpret_cast<const char*>(value_), header_->v_len}; }

            /**
             * Returns the sequence number.
             */
            [[nodiscard]] uint64_t seq() const noexcept { return header_->seq; }

            /**
             * Returns the flags byte.
             */
            [[nodiscard]] uint8_t flags() const noexcept { return header_->flags; }

            /**
             * Checks if this record is a tombstone (deleted).
             */
            [[nodiscard]] bool is_tombstone() const noexcept { return header_->is_tombstone(); }

            /**
             * Returns the total size of the record (header + key + value).
             */
            [[nodiscard]] size_t total_size() const noexcept { return header_->total_size(); }

            /**
             * Checks if the RecordView is empty (uninitialized).
             */
            [[nodiscard]] bool empty() const noexcept { return header_ == nullptr; }

            /**
             * Returns the key fingerprint (SipHash-2-4).
             */
            [[nodiscard]] uint64_t key_fp64() const noexcept { return header_->key_fp64; }

            /**
             * Returns the mini_key (first 8 bytes of key, LE-packed).
             */
            [[nodiscard]] uint64_t mini_key() const noexcept { return header_->mini_key; }

            // ==================== Comparison ====================

            /**
             * Compares two RecordViews by key (lexicographic order).
             *
             * @param other Other RecordView
             * @return -1 if this < other, 0 if equal, +1 if this > other
             */
            [[nodiscard]] int compare_key(const RecordView& other) const noexcept;

            /**
             * Checks if two RecordViews have the same key (byte-wise comparison).
             */
            [[nodiscard]] bool key_equals(const RecordView& other) const noexcept;

            /**
             * Compares key with a raw byte span.
             *
             * @param other_key Key bytes to compare
             * @return -1 if this < other, 0 if equal, +1 if this > other
             */
            [[nodiscard]] int compare_key(std::span<const uint8_t> other_key) const noexcept;

        private:
            const AKHdr32* header_{nullptr};
            const uint8_t* key_{nullptr};
            const uint8_t* value_{nullptr};
    };
} // namespace akkaradb::core