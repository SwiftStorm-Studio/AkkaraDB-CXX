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

// internal/src/core/record/MemRecord.cpp
#include "core/record/MemRecord.hpp"
#include <cstring>
#include <algorithm>

namespace akkaradb::core {
    MemRecord::MemRecord(const AKHdr32& header, std::vector<uint8_t> key, std::vector<uint8_t> value) noexcept
        : header_{header}, key_{std::move(key)}, value_{std::move(value)} { compute_approx_size(); }

    void MemRecord::compute_approx_size() noexcept {
        // Header + key + value + vector overhead (approximation)
        constexpr size_t vector_overhead = sizeof(std::vector<uint8_t>) * 2;
        approx_size_ = sizeof(AKHdr32) + key_.size() + value_.size() + vector_overhead;
    }

    MemRecord MemRecord::create(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq, uint8_t flags) {
        std::vector key_vec(key.begin(), key.end());
        std::vector value_vec(value.begin(), value.end());

        const AKHdr32 header{
            .k_len = static_cast<uint16_t>(key.size()),
            .v_len = static_cast<uint32_t>(value.size()),
            .seq = seq,
            .flags = flags,
            .pad0 = 0,
            .key_fp64 = AKHdr32::compute_key_fp64(key.data(), key.size()),
            .mini_key = AKHdr32::build_mini_key(key.data(), key.size())
        };

        return MemRecord{header, std::move(key_vec), std::move(value_vec)};
    }

    MemRecord MemRecord::create(std::string_view key, std::string_view value, uint64_t seq, uint8_t flags) {
        const auto key_span = std::span{reinterpret_cast<const uint8_t*>(key.data()), key.size()};
        const auto value_span = std::span{reinterpret_cast<const uint8_t*>(value.data()), value.size()};
        return create(key_span, value_span, seq, flags);
    }

    MemRecord MemRecord::tombstone(std::span<const uint8_t> key, uint64_t seq) { return create(key, {}, seq, AKHdr32::FLAG_TOMBSTONE); }

    MemRecord MemRecord::tombstone(std::string_view key, uint64_t seq) {
        const auto key_span = std::span{reinterpret_cast<const uint8_t*>(key.data()), key.size()};
        return tombstone(key_span, seq);
    }

    MemRecord MemRecord::from_view(const RecordView& view) { return create(view.key(), view.value(), view.seq(), view.flags()); }

    int MemRecord::compare_key(const MemRecord& other) const noexcept { return compare_key(other.key()); }

    int MemRecord::compare_key(std::span<const uint8_t> other_key) const noexcept {
        const size_t min_len = std::min(key().size(), other_key.size());

        // Only call memcmp if both pointers are valid and min_len > 0
        if (min_len > 0 && key_.data() && other_key.data()) {
            if (const int cmp = std::memcmp(key_.data(), other_key.data(), min_len); cmp != 0) {
                return cmp < 0
                           ? -1
                           : 1;
            }
        }

        // If prefixes are equal, shorter key comes first
        if (header_.k_len < other_key.size()) { return -1; }
        if (header_.k_len > other_key.size()) { return 1; }
        return 0;
    }

    bool MemRecord::key_equals(const MemRecord& other) const noexcept { return key_equals(other.key()); }

    bool MemRecord::key_equals(std::span<const uint8_t> other_key) const noexcept {
        if (header_.k_len != other_key.size()) { return false; }
        return std::memcmp(key_.data(), other_key.data(), header_.k_len) == 0;
    }
} // namespace akkaradb::core