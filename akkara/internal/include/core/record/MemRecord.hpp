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

#include <cstring>
#include <span>
#include <string_view>
#include "AKHdr32.hpp"
#include "RecordView.hpp"

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
                uint8_t flags = AKHdr32::FLAG_NORMAL,
                uint64_t precomputed_fp64 = 0 ///< 0 = compute internally
            );

            /**
             * Creates a MemRecord from string key/value.
             */
            [[nodiscard]] static MemRecord create(std::string_view key, std::string_view value, uint64_t seq, uint8_t flags = AKHdr32::FLAG_NORMAL);

            /**
             * Creates a tombstone (deletion marker). value is empty; data_ holds key only.
             */
            [[nodiscard]] static MemRecord tombstone(std::span<const uint8_t> key, uint64_t seq, uint64_t precomputed_fp64 = 0);

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
            [[nodiscard]] size_t approx_size() const noexcept { return sizeof(AKHdr32) + data_.size() + sizeof(SmallBuffer); }

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
            /**
             * SmallBuffer — SSO-style inline/heap data buffer.
             *
             * Replaces std::vector<uint8_t> (24 B) + size_t approx_size_ (8 B) = 32 B total.
             * Records with total data <= INLINE_CAP (22 bytes) are stored completely inline
             * with zero heap allocation. Larger records fall back to a single heap allocation.
             *
             * Layout (32 bytes):
             *   uint8_t* active_ptr_ (8 B) — always points to live data: inl_ when inline,
             *                                heap pointer otherwise.  data() = return active_ptr_
             *                                with zero branches on the hot read path.
             *   uint16_t meta_       (2 B) — data size (max 65535)
             *   uint8_t  inl_[22]   (22 B) — inline storage (INLINE_CAP = 22)
             */
            struct SmallBuffer {
                static constexpr size_t INLINE_CAP = 22;

                // active_ptr_ always points to the live data: inl_ when inline, heap when heap-allocated.
                // data() = return active_ptr_ — zero branches on the hot read path.
                uint8_t* active_ptr_ = nullptr; // 8 bytes (offset  0)
                uint16_t meta_ = 0; // 2 bytes (offset  8) — data size, max 65535
                uint8_t inl_[INLINE_CAP] = {}; // 22 bytes (offset 10) — inline storage
                // Total: 8 + 2 + 22 = 32 bytes

                [[nodiscard]] size_t size() const noexcept { return meta_; }
                [[nodiscard]] bool is_heap() const noexcept { return active_ptr_ != inl_; }
                [[nodiscard]] bool empty() const noexcept { return meta_ == 0; }

                // Branch-free: active_ptr_ always points to valid data.
                [[nodiscard]] const uint8_t* data() const noexcept { return active_ptr_; }
                [[nodiscard]] uint8_t* data() noexcept { return active_ptr_; }

                SmallBuffer() noexcept : active_ptr_{inl_} {}

                // Constructs contiguous [key | value] buffer. Inline if k_len+v_len <= 22, else heap.
                SmallBuffer(const uint8_t* key, size_t k_len, const uint8_t* val, size_t v_len) {
                    const size_t n = k_len + v_len;
                    meta_ = static_cast<uint16_t>(n);
                    active_ptr_ = (n <= INLINE_CAP) ? inl_ : new uint8_t[n];
                    if (k_len) std::memcpy(active_ptr_, key, k_len);
                    if (v_len) std::memcpy(active_ptr_ + k_len, val, v_len);
                }

                ~SmallBuffer() noexcept { if (active_ptr_ != inl_) delete[] active_ptr_; }

                SmallBuffer(SmallBuffer&& o) noexcept
                    : meta_{o.meta_} {
                    if (o.active_ptr_ != o.inl_) {
                        active_ptr_ = o.active_ptr_;
                        o.active_ptr_ = o.inl_;
                        o.meta_ = 0;
                    }
                    else {
                        active_ptr_ = inl_;
                        std::memcpy(inl_, o.inl_, meta_);
                    }
                }

                SmallBuffer& operator=(SmallBuffer&& o) noexcept {
                    if (this == &o) return *this;
                    if (active_ptr_ != inl_) delete[] active_ptr_;
                    meta_ = o.meta_;
                    if (o.active_ptr_ != o.inl_) {
                        active_ptr_ = o.active_ptr_;
                        o.active_ptr_ = o.inl_;
                        o.meta_ = 0;
                    }
                    else {
                        active_ptr_ = inl_;
                        std::memcpy(inl_, o.inl_, meta_);
                    }
                    return *this;
                }

                SmallBuffer(const SmallBuffer& o)
                    : meta_{o.meta_} {
                    if (o.active_ptr_ != o.inl_) {
                        active_ptr_ = new uint8_t[meta_];
                        std::memcpy(active_ptr_, o.active_ptr_, meta_);
                    }
                    else {
                        active_ptr_ = inl_;
                        std::memcpy(inl_, o.inl_, meta_);
                    }
                }

                SmallBuffer& operator=(const SmallBuffer& o) {
                    if (this == &o) return *this;
                    if (active_ptr_ != inl_) delete[] active_ptr_;
                    meta_ = o.meta_;
                    if (o.active_ptr_ != o.inl_) {
                        active_ptr_ = new uint8_t[meta_];
                        std::memcpy(active_ptr_, o.active_ptr_, meta_);
                    }
                    else {
                        active_ptr_ = inl_;
                        std::memcpy(inl_, o.inl_, meta_);
                    }
                    return *this;
                }
            };

            static_assert(sizeof(SmallBuffer) == 32, "SmallBuffer must be 32 bytes");

            AKHdr32 header_{};
            /// Contiguous buffer: [key bytes (k_len)] [value bytes (v_len)]
            /// Inline (zero heap alloc) when total <= 24 bytes; heap-allocated otherwise.
            SmallBuffer data_;

            MemRecord(const AKHdr32& header, SmallBuffer data) noexcept;
    };

    // sizeof(MemRecord) == 64 in both Debug and Release:
    //   AKHdr32=32 + SmallBuffer=32 (meta_=4 + _pad=4 + union Body[24]).
    // SmallBuffer has no std::vector internals so no debug-mode size variance.
    static_assert(sizeof(MemRecord) == 64, "MemRecord must be 64 bytes (AKHdr32=32 + SmallBuffer=32)");
} // namespace akkaradb::core
