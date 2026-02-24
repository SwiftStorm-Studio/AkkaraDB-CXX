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

// internal/include/core/record/MemRecord.hpp
#pragma once

#include "AKHdr32.hpp"
#include "RecordView.hpp"
#include <vector>
#include <string_view>
#include <span>

namespace akkaradb::core {
    /**
    * MemRecord - Owning in-memory representation of a database record.
    *
    * This class owns the key and value data, unlike RecordView which is
    * a non-owning view. MemRecord is used in MemTable and for operations
    * that require data ownership.
    *
    * Design principles:
    * - Ownership: Holds actual key/value bytes in memory
    * - Movable: Supports efficient transfer of ownership
    * - Comparable: Provides key comparison for sorted structures
    * - Approximate size tracking: For memory management
    *
    * Typical usage:
    * ```cpp
    * auto record = MemRecord::create(
    *     key_bytes,
    *     value_bytes,
    *     seq,
    *     MemRecord::FLAG_NORMAL
    * );
    *
    * if (record.is_tombstone()) {
    *     // Handle deletion
    * }
    *
    * auto view = record.as_view();  // Zero-copy view
    * ```
    *
    * Thread-safety: NOT thread-safe. External synchronization required
    * for concurrent access.
    */
    class MemRecord {
        public:
            /**
             * Default constructor (empty record).
             */
            MemRecord() = default;

            /**
             * Creates a MemRecord with copied data.
             *
             * @param key Key bytes (will be copied)
             * @param value Value bytes (will be copied)
             * @param seq Sequence number
             * @param flags Flags (FLAG_NORMAL or FLAG_TOMBSTONE)
             * @return New MemRecord
             */
            [[nodiscard]] static MemRecord create(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags = AKHdr32::FLAG_NORMAL
            );

            /**
             * Creates a MemRecord from string key/value.
             *
             * @param key Key string
             * @param value Value string
             * @param seq Sequence number
             * @param flags Flags
             * @return New MemRecord
             */
            [[nodiscard]] static MemRecord create(std::string_view key, std::string_view value, uint64_t seq, uint8_t flags = AKHdr32::FLAG_NORMAL);

            /**
             * Creates a tombstone (deletion marker).
             *
             * @param key Key bytes
             * @param seq Sequence number
             * @return Tombstone MemRecord
             */
            [[nodiscard]] static MemRecord tombstone(std::span<const uint8_t> key, uint64_t seq);

            /**
             * Creates a tombstone from string key.
             *
             * @param key Key string
             * @param seq Sequence number
             * @return Tombstone MemRecord
             */
            [[nodiscard]] static MemRecord tombstone(std::string_view key, uint64_t seq);

            /**
             * Creates a MemRecord from a RecordView (copies data).
             *
             * @param view RecordView to copy from
             * @return New MemRecord
             */
            [[nodiscard]] static MemRecord from_view(const RecordView& view);

            // ==================== Accessors ====================

            /**
             * Returns the header.
             */
            [[nodiscard]] const AKHdr32& header() const noexcept { return header_; }

            /**
             * Returns a span over the key bytes.
             */
            [[nodiscard]] std::span<const uint8_t> key() const noexcept {
                // Return empty span if key_ is empty (e.g., after move)
                return key_.empty()
                           ? std::span<const uint8_t>{}
                           : std::span<const uint8_t>{key_.data(), header_.k_len};
            }

            /**
             * Returns a span over the value bytes.
             */
            [[nodiscard]] std::span<const uint8_t> value() const noexcept {
                // Return empty span if value_ is empty (e.g., after move)
                return value_.empty()
                           ? std::span<const uint8_t>{}
                           : std::span<const uint8_t>{value_.data(), header_.v_len};
            }

            /**
            * Returns a string_view over the key.
            */
            [[nodiscard]] std::string_view key_string() const noexcept {
                return key_.empty()
                           ? std::string_view{}
                           : std::string_view{reinterpret_cast<const char*>(key_.data()), header_.k_len};
            }

            /**
            * Returns a string_view over the value.
            */
            [[nodiscard]] std::string_view value_string() const noexcept {
                return value_.empty()
                           ? std::string_view{}
                           : std::string_view{reinterpret_cast<const char*>(value_.data()), header_.v_len};
            }

            /**
            * Returns the sequence number.
            */
            [[nodiscard]] uint64_t seq() const noexcept { return header_.seq; }

            /**
            * Returns the flags byte.
            */
            [[nodiscard]] uint8_t flags() const noexcept { return header_.flags; }

            /**
            * Checks if this record is a tombstone.
            */
            [[nodiscard]] bool is_tombstone() const noexcept { return header_.is_tombstone(); }

            /**
            * Returns the key fingerprint.
            */
            [[nodiscard]] uint64_t key_fp64() const noexcept { return header_.key_fp64; }

            /**
            * Returns the mini_key.
            */
            [[nodiscard]] uint64_t mini_key() const noexcept { return header_.mini_key; }

            /**
            * Returns approximate size in bytes (header + key + value + overhead).
            */
            [[nodiscard]] size_t approx_size() const noexcept { return approx_size_; }

            /**
            * Creates a zero-copy RecordView pointing to this record's data.
            *
            * WARNING: The returned view is only valid while this MemRecord exists.
            *
            * @return RecordView over this record
            */
            [[nodiscard]] RecordView as_view() const noexcept { return RecordView{&header_, key_.data(), value_.data()}; }

            /**
            * Checks if the record is empty.
            */
            [[nodiscard]] bool empty() const noexcept { return key_.empty(); }

            // ==================== Comparison ====================

            /**
            * Compares keys lexicographically.
            *
            * @param other Other MemRecord
            * @return -1 if this < other, 0 if equal, +1 if this > other
            */
            [[nodiscard]] int compare_key(const MemRecord& other) const noexcept;

            /**
            * Compares key with raw bytes.
            *
            * @param other_key Key bytes to compare
            * @return -1 if this < other, 0 if equal, +1 if this > other
            */
            [[nodiscard]] int compare_key(std::span<const uint8_t> other_key) const noexcept;

            /**
            * Checks if keys are equal.
            */
            [[nodiscard]] bool key_equals(const MemRecord& other) const noexcept;

            /**
            * Checks if key equals raw bytes.
            */
            [[nodiscard]] bool key_equals(std::span<const uint8_t> other_key) const noexcept;

            // ==================== Operators ====================

            /**
            * Less-than operator (compares keys).
            */
            [[nodiscard]] bool operator<(const MemRecord& other) const noexcept { return compare_key(other) < 0; }

            /**
            * Equality operator (compares keys only, not values or seq).
            */
            [[nodiscard]] bool operator==(const MemRecord& other) const noexcept { return key_equals(other); }

        private:
            AKHdr32 header_{};
            std::vector<uint8_t> key_;
            std::vector<uint8_t> value_;
            size_t approx_size_{0};

            MemRecord(const AKHdr32& header, std::vector<uint8_t> key, std::vector<uint8_t> value) noexcept;

            void compute_approx_size() noexcept;
    };
} // namespace akkaradb::core