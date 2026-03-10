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
            .v_len = static_cast<uint32_t>(value.size()),
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

    int MemRecord::compare_key(const MemRecord& other) const noexcept { return compare_key(other.key()); }

    int MemRecord::compare_key(std::span<const uint8_t> other_key) const noexcept {
        const size_t min_len = std::min(static_cast<size_t>(header_.k_len), other_key.size());

        if (min_len > 0 && !data_.empty() && other_key.data()) {
            if (const int cmp = std::memcmp(data_.data(), other_key.data(), min_len); cmp != 0) {
                return cmp < 0
                           ? -1
                           : 1;
            }
        }

        if (header_.k_len < other_key.size()) return -1;
        if (header_.k_len > other_key.size()) return 1;
        return 0;
    }

    bool MemRecord::key_equals(const MemRecord& other) const noexcept { return key_equals(other.key()); }

    bool MemRecord::key_equals(std::span<const uint8_t> other_key) const noexcept {
        if (header_.k_len != other_key.size()) return false;
        if (header_.k_len == 0) return true;
        return std::memcmp(data_.data(), other_key.data(), header_.k_len) == 0;
    }
} // namespace akkaradb::core
