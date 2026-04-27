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

// internal/include/core/record/RecordView.hpp
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

namespace akkaradb::core {
    /**
     * RecordView — unified zero-copy record view (MemTable + SST)
     *
     * Purpose:
     *   - Provide a single read interface across:
     *       - OwnedRecord (in-memory, arena-backed)
     *       - SST record (on-disk / mmap)
     *
     *   - Eliminate copies in read/iterator path
     *   - Enable type unification (no variant, no branching on type)
     *
     * Design:
     *   - Non-owning (view only)
     *   - Trivially copyable (just pointers + integers)
     *   - Immutable
     *
     * Layout (no fixed memory layout; logical structure):
     *
     *   key_ptr_   → key bytes
     *   val_ptr_   → value bytes
     *   k_len_     → key length
     *   v_len_     → value length
     *   seq_       → sequence number
     *   flags_     → tombstone etc.
     *   key_fp64_  → hash fingerprint
     *   mini_key_  → first ≤8 bytes of key (LE packed)
     *
     * Lifetime:
     *   - MemTable: tied to BufferArena lifetime
     *   - SST: tied to mmap / block buffer lifetime
     *
     * Thread-safety:
     *   - Safe if underlying memory is immutable
     */
    class RecordView {
        public:
            // ==================== Constructors ====================

            /**
             * Empty view (null state).
             */
            constexpr RecordView() noexcept = default;

            /**
             * Full constructor.
             */
            constexpr RecordView(
                const uint8_t* key,
                uint16_t k_len,
                const uint8_t* value,
                uint16_t v_len,
                uint64_t seq,
                uint8_t flags,
                uint64_t key_fp64,
                uint64_t mini_key
            ) noexcept : key_{key}, value_{value}, k_len_{k_len}, v_len_{v_len}, seq_{seq}, flags_{flags}, key_fp64_{key_fp64}, mini_key_{mini_key} {}

            // ==================== Accessors ====================

            [[nodiscard]] bool empty() const noexcept { return key_ == nullptr; }

            [[nodiscard]] std::span<const uint8_t> key() const noexcept { return {key_, k_len_}; }

            [[nodiscard]] std::span<const uint8_t> value() const noexcept { return {value_, v_len_}; }

            [[nodiscard]] std::string_view key_string() const noexcept { return {reinterpret_cast<const char*>(key_), k_len_}; }

            [[nodiscard]] std::string_view value_string() const noexcept { return {reinterpret_cast<const char*>(value_), v_len_}; }

            [[nodiscard]] uint16_t key_size() const noexcept { return k_len_; }
            [[nodiscard]] uint16_t value_size() const noexcept { return v_len_; }

            [[nodiscard]] uint64_t seq() const noexcept { return seq_; }
            [[nodiscard]] uint8_t flags() const noexcept { return flags_; }

            [[nodiscard]] bool is_tombstone() const noexcept { return (flags_ & FLAG_TOMBSTONE) != 0; }

            [[nodiscard]] uint64_t key_fp64() const noexcept { return key_fp64_; }
            [[nodiscard]] uint64_t mini_key() const noexcept { return mini_key_; }

            // ==================== Comparison ====================

            /**
             * Lexicographic key comparison (fast-path optimized).
             *
             * Fast path:
             *   - Compare mini_key (≤8 bytes, register-only)
             *
             * Slow path:
             *   - memcmp remaining bytes
             */
            [[nodiscard]] int compare_key(const RecordView& other) const noexcept {
                const size_t prefix = std::min<size_t>(std::min(k_len_, other.k_len_), 8);

                if (prefix > 0) { if (int c = std::memcmp(&mini_key_, &other.mini_key_, prefix); c != 0) return c < 0 ? -1 : 1; }

                const size_t min_len = std::min(k_len_, other.k_len_);

                if (min_len > 8) { if (int c = std::memcmp(key_ + 8, other.key_ + 8, min_len - 8); c != 0) return c < 0 ? -1 : 1; }

                if (k_len_ < other.k_len_) return -1;
                if (k_len_ > other.k_len_) return 1;
                return 0;
            }

            /**
             * Compare with raw key.
             */
            [[nodiscard]] int compare_key(std::span<const uint8_t> other) const noexcept {
                const size_t min_len = std::min<size_t>(k_len_, other.size());
                const size_t prefix = std::min<size_t>(min_len, 8);

                if (prefix > 0) { if (int c = std::memcmp(&mini_key_, other.data(), prefix); c != 0) return c < 0 ? -1 : 1; }

                if (min_len > 8) { if (int c = std::memcmp(key_ + 8, other.data() + 8, min_len - 8); c != 0) return c < 0 ? -1 : 1; }

                if (k_len_ < other.size()) return -1;
                if (k_len_ > other.size()) return 1;
                return 0;
            }

            /**
             * Equality check.
             */
            [[nodiscard]] bool key_equals(const RecordView& other) const noexcept {
                if (k_len_ != other.k_len_) return false;
                if (k_len_ == 0) return true;

                if (mini_key_ != other.mini_key_) return false;
                if (k_len_ <= 8) return true;

                return std::memcmp(key_ + 8, other.key_ + 8, k_len_ - 8) == 0;
            }

            // ==================== Operators ====================

            [[nodiscard]] bool operator<(const RecordView& o) const noexcept { return compare_key(o) < 0; }

            [[nodiscard]] bool operator==(const RecordView& o) const noexcept { return key_equals(o); }

            // ==================== Flags ====================

            static constexpr uint8_t FLAG_TOMBSTONE = 0x1;

        private:
            const uint8_t* key_{nullptr};
            const uint8_t* value_{nullptr};

            uint16_t k_len_{0};
            uint16_t v_len_{0};

            uint64_t seq_{0};
            uint8_t flags_{0};

            uint64_t key_fp64_{0};
            uint64_t mini_key_{0};
    };
} // namespace akkaradb::core
