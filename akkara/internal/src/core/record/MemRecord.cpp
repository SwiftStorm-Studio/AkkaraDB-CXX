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

// internal/src/core/record/MemRecord.cpp
#include "core/record/MemRecord.hpp"
#include <algorithm>
#include <cstring>

namespace akkaradb::core {
    // ── Private constructor ────────────────────────────────────────────────────

    MemRecord::MemRecord(const AKHdr32& header, SmallBuffer data) noexcept : header_{header}, data_{std::move(data)} {}

    // ── Factories ─────────────────────────────────────────────────────────────

    MemRecord MemRecord::create(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq, uint8_t flags, uint64_t precomputed_fp64) {
        const AKHdr32 header{
            .k_len = static_cast<uint16_t>(key.size()),
            .v_len = static_cast<uint16_t>(value.size()),
            .reserved1 = 0,
            .seq = seq,
            .flags = flags,
            .pad0 = 0,
            .key_fp64 = precomputed_fp64 != 0 ? precomputed_fp64 : AKHdr32::compute_key_fp64(key.data(), key.size()),
            .mini_key = AKHdr32::build_mini_key(key.data(), key.size()),
        };
        // SmallBuffer: inline for key+value <= 24 bytes (zero heap alloc), heap otherwise.
        return MemRecord{header, SmallBuffer{key.data(), key.size(), value.data(), value.size()}};
    }

    MemRecord MemRecord::create(std::string_view key, std::string_view value, uint64_t seq, uint8_t flags) {
        return create(
            std::span{reinterpret_cast<const uint8_t*>(key.data()), key.size()},
            std::span{reinterpret_cast<const uint8_t*>(value.data()), value.size()},
            seq,
            flags
        );
    }

    MemRecord MemRecord::tombstone(std::span<const uint8_t> key, uint64_t seq, uint64_t precomputed_fp64) {
        // Tombstone: value is empty → data_ holds only key bytes.
        return create(key, {}, seq, AKHdr32::FLAG_TOMBSTONE, precomputed_fp64);
    }

    MemRecord MemRecord::tombstone(std::string_view key, uint64_t seq) {
        return tombstone(std::span{reinterpret_cast<const uint8_t*>(key.data()), key.size()}, seq);
    }

    MemRecord MemRecord::from_view(const RecordView& view) { return create(view.key(), view.value(), view.seq(), view.flags()); }

    // ── Key comparison ────────────────────────────────────────────────────────
    //
    // Fast path: header_.mini_key caches the first 8 raw key bytes (LE-packed).
    // On a little-endian machine, &header_.mini_key lays out as
    // [key[0], key[1], ..., key[7]], so memcmp on it is lexicographically
    // correct without any byte-swap.  This avoids loading data_.active_ptr_
    // (a separate pointer dereference, and a potential cache miss for heap
    // allocated data) when the prefix comparison can decide the result.

    int MemRecord::compare_key(const MemRecord& other) const noexcept {
        // Both mini_keys live in the header — same cache line as k_len, seq, flags.
        // Compare min(k_len, other.k_len, 8) bytes without touching data_.
        const size_t prefix = std::min<size_t>(std::min<size_t>(header_.k_len, other.header_.k_len), 8u);
        if (prefix > 0) { if (const int c = std::memcmp(&header_.mini_key, &other.header_.mini_key, prefix); c != 0) return c < 0 ? -1 : 1; }

        const size_t min_len = std::min<size_t>(header_.k_len, other.header_.k_len);
        if (min_len > 8) {
            // First 8 bytes matched — compare the rest from data_
            if (const int c = std::memcmp(data_.data() + 8, other.data_.data() + 8, min_len - 8); c != 0) return c < 0 ? -1 : 1;
        }

        if (header_.k_len < other.header_.k_len) return -1;
        if (header_.k_len > other.header_.k_len) return 1;
        return 0;
    }

    int MemRecord::compare_key(std::span<const uint8_t> other_key) const noexcept {
        const size_t min_len = std::min<size_t>(header_.k_len, other_key.size());

        // Use cached mini_key for the first ≤8 bytes (no data_ pointer dereference on miss)
        const size_t prefix = std::min<size_t>(min_len, 8u);
        if (prefix > 0) { if (const int c = std::memcmp(&header_.mini_key, other_key.data(), prefix); c != 0) return c < 0 ? -1 : 1; }

        if (min_len > 8) { if (const int c = std::memcmp(data_.data() + 8, other_key.data() + 8, min_len - 8); c != 0) return c < 0 ? -1 : 1; }

        if (header_.k_len < other_key.size()) return -1;
        if (header_.k_len > other_key.size()) return 1;
        return 0;
    }

    bool MemRecord::key_equals(const MemRecord& other) const noexcept {
        if (header_.k_len != other.header_.k_len) return false;
        if (header_.k_len == 0) return true;

        // Fast: uint64_t equality on both mini_keys (no pointer indirection)
        if (header_.mini_key != other.header_.mini_key) return false;
        if (header_.k_len <= 8) return true;

        return std::memcmp(data_.data() + 8, other.data_.data() + 8, header_.k_len - 8) == 0;
    }

    bool MemRecord::key_equals(std::span<const uint8_t> other_key) const noexcept {
        if (header_.k_len != static_cast<uint16_t>(other_key.size())) return false;
        if (header_.k_len == 0) return true;

        // Fast: compare first ≤8 bytes via mini_key (cached in header)
        const size_t prefix = std::min<size_t>(header_.k_len, 8u);
        if (std::memcmp(&header_.mini_key, other_key.data(), prefix) != 0) return false;
        if (header_.k_len <= 8) return true;

        return std::memcmp(data_.data() + 8, other_key.data() + 8, header_.k_len - 8) == 0;
    }
} // namespace akkaradb::core
