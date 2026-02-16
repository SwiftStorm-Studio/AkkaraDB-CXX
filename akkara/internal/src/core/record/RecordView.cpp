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

// internal/src/core/record/RecordView.cpp
#include "core/record/RecordView.hpp"
#include <stdexcept>
#include <cstring>
#include <algorithm>

namespace akkaradb::core {
    RecordView RecordView::from_buffer(BufferView buffer, size_t offset) {
        // Validate buffer size
        if (offset + sizeof(AKHdr32) > buffer.size()) { throw std::out_of_range("RecordView::from_buffer: buffer too small for header"); }

        // Read header
        const auto* header_ptr = reinterpret_cast<const AKHdr32*>(buffer.data() + offset);

        // Validate total record fits in buffer
        if (const size_t total_size = header_ptr->total_size(); offset + total_size > buffer.size()) {
            throw std::out_of_range("RecordView::from_buffer: buffer too small for record");
        }

        // Calculate key and value pointers
        const auto* key_ptr = reinterpret_cast<const uint8_t*>(header_ptr + 1);
        const auto* value_ptr = key_ptr + header_ptr->k_len;

        return RecordView{header_ptr, key_ptr, value_ptr};
    }

    const AKHdr32& RecordView::header() const {
        if (empty()) { throw std::runtime_error("RecordView::header: RecordView is empty"); }
        return *header_;
    }

    int RecordView::compare_key(const RecordView& other) const noexcept { return compare_key(other.key()); }

    bool RecordView::key_equals(const RecordView& other) const noexcept {
        if (header_->k_len != other.header_->k_len) { return false; }
        return std::memcmp(key_, other.key_, header_->k_len) == 0;
    }

    int RecordView::compare_key(std::span<const uint8_t> other_key) const noexcept {
        const size_t min_len = std::min<size_t>(header_->k_len, other_key.size());

        if (const int cmp = std::memcmp(key_, other_key.data(), min_len); cmp != 0) {
            return cmp < 0
                       ? -1
                       : 1;
        }

        // If prefixes are equal, shorter key comes first
        if (header_->k_len < other_key.size()) { return -1; }
        if (header_->k_len > other_key.size()) { return 1; }
        return 0;
    }
} // namespace akkaradb::core
