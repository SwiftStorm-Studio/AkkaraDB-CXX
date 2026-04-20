#include "core/buffer/BufferView.hpp"
#include "core/buffer/OwnedBuffer.hpp"

#include <bit>
#include <cstring>
#include <stdexcept>

#if !defined(__SSE4_2__) && !(defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86)))
#error "AkkaraDB requires SSE4.2 for CRC32C"
#endif

#include <nmmintrin.h>

static_assert(std::endian::native == std::endian::little, "AkkaraDB requires Little-Endian architecture");

namespace akkaradb::core {
    // ==================== Slice ====================

    BufferView BufferView::slice(size_t offset, size_t length) const {
        check_bounds(offset, length);
        return BufferView{data_ + offset, length};
    }

    BufferView BufferView::slice(size_t offset) const {
        if (offset > size_) { throw std::out_of_range("BufferView::slice"); }
        return BufferView{data_ + offset, size_ - offset};
    }

    // ==================== Ownership ====================

    OwnedBuffer BufferView::to_owned() const {
        if (size_ == 0) { return OwnedBuffer::allocate(0); }

        OwnedBuffer out = OwnedBuffer::allocate(size_);
        std::memcpy(out.data(), data_, size_);
        return out;
    }

    // ==================== CRC32C ====================

    uint32_t BufferView::crc32c(size_t offset, size_t length) const {
        check_bounds(offset, length);

        uint32_t crc = 0xFFFFFFFF;
        const auto* ptr = reinterpret_cast<const uint8_t*>(data_ + offset);

        size_t remaining = length;

        while (remaining >= 8) {
            uint64_t chunk;
            std::memcpy(&chunk, ptr, 8);
            crc = static_cast<uint32_t>(_mm_crc32_u64(crc, chunk));
            ptr += 8;
            remaining -= 8;
        }

        if (remaining >= 4) {
            uint32_t chunk;
            std::memcpy(&chunk, ptr, 4);
            crc = _mm_crc32_u32(crc, chunk);
            ptr += 4;
            remaining -= 4;
        }

        while (remaining > 0) {
            crc = _mm_crc32_u8(crc, *ptr++);
            --remaining;
        }

        return ~crc;
    }

    // ==================== String ====================

    std::string_view BufferView::as_string_view(size_t offset, size_t length) const {
        check_bounds(offset, length);
        return {reinterpret_cast<const char*>(data_ + offset), length};
    }

    // ==================== Bounds ====================

    void BufferView::check_bounds(size_t offset, size_t length) const {
        if (offset > size_ || length > size_ - offset) { throw std::out_of_range("BufferView: out of range"); }
    }
} // namespace
