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
    * Storage layout:
    *   data_ = [key bytes | value bytes]  — single contiguous heap allocation.
    *   key()   → span{ data_.data(),          k_len }
    *   value() → span{ data_.data() + k_len,  v_len }
    *
    * Benefits over two separate vectors:
    *   - One heap allocation instead of two
    *   - sizeof(MemRecord) = 64 bytes (was 88) → BPTree fits ~37% more records per leaf
    *   - Key and value are cache-adjacent; value reads follow key reads with no extra miss
    *
    * Design principles:
    *   - Movable: Supports efficient transfer of ownership
    *   - Comparable: Provides key comparison for sorted structures
    *   - Approximate size tracking: For memory management
    *
    * Typical usage:
    * ```cpp
    * auto record = MemRecord::create(key_bytes, value_bytes, seq);
    *
    * if (record.is_tombstone()) { // Handle deletion }
    *
    * auto view = record.as_view();  // Zero-copy view
    * ```
    *
    * Thread-safety: NOT thread-safe. External synchronization required.
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
             * @param key   Key bytes (copied into data_[0..k_len))
             * @param value Value bytes (copied into data_[k_len..k_len+v_len))
             * @param seq   Sequence number
             * @param flags Flags (FLAG_NORMAL or FLAG_TOMBSTONE)
             * @return New MemRecord backed by a single heap allocation
             */
            [[nodiscard]] static MemRecord create(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags = AKHdr32::FLAG_NORMAL
            );

            /**
             * Creates a MemRecord from string key/value.
             */
            [[nodiscard]] static MemRecord create(std::string_view key, std::string_view value, uint64_t seq, uint8_t flags = AKHdr32::FLAG_NORMAL);

            /**
             * Creates a tombstone (deletion marker). value is empty; data_ holds key only.
             */
            [[nodiscard]] static MemRecord tombstone(std::span<const uint8_t> key, uint64_t seq);

            /**
             * Creates a tombstone from string key.
             */
            [[nodiscard]] static MemRecord tombstone(std::string_view key, uint64_t seq);

            /**
             * Creates a MemRecord from a RecordView (copies data).
             */
            [[nodiscard]] static MemRecord from_view(const RecordView& view);

            // ==================== Accessors ====================

            /**
             * Returns the header.
             */
            [[nodiscard]] const AKHdr32& header() const noexcept { return header_; }

            /**
             * Returns a span over the key bytes  (data_[0..k_len)).
             */
            [[nodiscard]] std::span<const uint8_t> key() const noexcept {
                if (data_.empty()) return {};
                return {data_.data(), header_.k_len};
            }

            /**
             * Returns a span over the value bytes  (data_[k_len..k_len+v_len)).
             * Empty span for tombstones (v_len == 0).
             */
            [[nodiscard]] std::span<const uint8_t> value() const noexcept {
                if (header_.v_len == 0) return {};
                return {data_.data() + header_.k_len, header_.v_len};
            }

            /**
            * Returns a string_view over the key.
            */
            [[nodiscard]] std::string_view key_string() const noexcept {
                if (data_.empty()) return {};
                return {reinterpret_cast<const char*>(data_.data()), header_.k_len};
            }

            /**
            * Returns a string_view over the value.
            */
            [[nodiscard]] std::string_view value_string() const noexcept {
                if (header_.v_len == 0) return {};
                return {reinterpret_cast<const char*>(data_.data() + header_.k_len), header_.v_len};
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
            * Returns approximate size in bytes (header + data + vector overhead).
            */
            [[nodiscard]] size_t approx_size() const noexcept { return approx_size_; }

            /**
            * Creates a zero-copy RecordView pointing to this record's data.
            *
            * WARNING: The returned view is only valid while this MemRecord exists.
            *
            * @return RecordView over this record's contiguous data buffer
            */
            [[nodiscard]] RecordView as_view() const noexcept { return RecordView{&header_, data_.data(), data_.data() + header_.k_len}; }

            /**
            * Checks if the record is empty (default-constructed or moved-from).
            */
            [[nodiscard]] bool empty() const noexcept { return data_.empty(); }

            // ==================== Comparison ====================

            /**
            * Compares keys lexicographically.
            *
            * @return -1 if this < other, 0 if equal, +1 if this > other
            */
            [[nodiscard]] int compare_key(const MemRecord& other) const noexcept;

            /**
            * Compares key with raw bytes.
            *
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
            /// Contiguous buffer: [key bytes (k_len)] [value bytes (v_len)]
            /// Single heap allocation — eliminates the second alloc of the old key_+value_ design.
            std::vector<uint8_t> data_;
            size_t approx_size_{0};

            MemRecord(const AKHdr32& header, std::vector<uint8_t> data) noexcept;

            void compute_approx_size() noexcept;
    };

    static_assert(sizeof(MemRecord) == 64, "MemRecord must be 64 bytes (AKHdr32=32 + vector=24 + size_t=8)");
} // namespace akkaradb::core
