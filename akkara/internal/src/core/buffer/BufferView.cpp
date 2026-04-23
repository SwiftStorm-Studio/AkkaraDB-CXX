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

// internal/src/core/buffer/BufferView.cpp
#include "core/buffer/BufferView.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include "cpu/CRC32C.hpp"

#include <bit>
#include <cstring>
#include <stdexcept>

static_assert(std::endian::native == std::endian::little, "AkkaraDB requires Little-Endian architecture");

namespace akkaradb::core {

    // ==================== Slice ====================

    BufferView BufferView::slice(size_t offset, size_t length) const {
        check_bounds(offset, length);
        return BufferView{data_ + offset, length};
    }

    BufferView BufferView::slice(size_t offset) const {
        if (offset > size_) {
            throw std::out_of_range("BufferView::slice: offset out of range");
        }
        return BufferView{data_ + offset, size_ - offset};
    }

    // ==================== Ownership ====================

    OwnedBuffer BufferView::to_owned() const {
        if (size_ == 0) {
            return OwnedBuffer::allocate(0);
        }

        auto out = OwnedBuffer::allocate(size_);
        std::memcpy(out.data(), data_, size_);
        return out;
    }

    // ==================== CRC32C ====================

    uint32_t BufferView::crc32c(size_t offset, size_t length) const {
        check_bounds(offset, length);
        return cpu::CRC32C(data_ + offset, length);
    }

    // ==================== String ====================

    std::string_view BufferView::as_string_view(size_t offset, size_t length) const {
        check_bounds(offset, length);
        return {reinterpret_cast<const char*>(data_ + offset), length};
    }

    // ==================== Bounds ====================

    void BufferView::check_bounds(size_t offset, size_t length) const {
        if (offset > size_ || length > size_ - offset) {
            throw std::out_of_range("BufferView: out of range");
        }
    }

} // namespace akkaradb::core
