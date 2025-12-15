// BufferView.cpp
#include "../../../include/akkara/core/buf/BufferView.hpp"
#include <stdexcept>
#include <cstring>

static_assert(
    std::endian::native == std::endian::little,
    "AkkaraDB requires Little-Endian architecture"
);

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

namespace akkaradb::core {
    // ==================== Slicing ====================

    BufferView BufferView::slice(size_t offset, size_t length) const {
        check_bounds(offset, length);
        return BufferView{data_ + offset, length};
    }

    BufferView BufferView::slice(size_t offset) const {
        if (offset > size_)
        {
            throw std::out_of_range("BufferView::slice: offset out of range");
        }
        return BufferView{data_ + offset, size_ - offset};
    }

    // ==================== Little-Endian Read Operations ====================

    uint8_t BufferView::read_u8(size_t offset) const {
        check_bounds(offset, 1);
        return static_cast<uint8_t>(data_[offset]);
    }

    uint16_t BufferView::read_u16_le(size_t offset) const {
        check_bounds(offset, 2);
        uint16_t value;
        std::memcpy(&value, data_ + offset, 2);
        return value;
    }

    uint32_t BufferView::read_u32_le(size_t offset) const {
        check_bounds(offset, 4);
        uint32_t value;
        std::memcpy(&value, data_ + offset, 4);
        return value;
    }

    uint64_t BufferView::read_u64_le(size_t offset) const {
        check_bounds(offset, 8);
        uint64_t value;
        std::memcpy(&value, data_ + offset, 8);
        return value;
    }

    // ==================== Little-Endian Write Operations ====================

    void BufferView::write_u8(size_t offset, uint8_t value) const {
        check_bounds(offset, 1);
        data_[offset] = static_cast<std::byte>(value);
    }

    void BufferView::write_u16_le(size_t offset, uint16_t value) const {
        check_bounds(offset, 2);
        std::memcpy(data_ + offset, &value, 2);
    }

    void BufferView::write_u32_le(size_t offset, uint32_t value) const {
        check_bounds(offset, 4);
        std::memcpy(data_ + offset, &value, 4);
    }

    void BufferView::write_u64_le(size_t offset, uint64_t value) const {
        check_bounds(offset, 8);
        std::memcpy(data_ + offset, &value, 8);
    }

    // ==================== Bulk Operations ====================

    void BufferView::copy_from(size_t offset, BufferView src, size_t src_offset, size_t length) const {
        check_bounds(offset, length);
        src.check_bounds(src_offset, length);

        // Handle overlapping regions
        std::memmove(data_ + offset, src.data_ + src_offset, length);
    }

    void BufferView::fill(size_t offset, size_t length, std::byte value) const {
        check_bounds(offset, length);
        std::memset(data_ + offset, static_cast<int>(value), length);
    }

    void BufferView::zero_fill() const noexcept {
        if (data_&& size_ > 0) {
            std::memset(data_, 0, size_);
        }
    }

    // ==================== CRC Computation ====================

    uint32_t BufferView::crc32c(size_t offset, size_t length) const {
        check_bounds(offset, length);

        uint32_t crc = 0xFFFFFFFF;
        const auto* ptr = reinterpret_cast<const uint8_t*>(data_ + offset);

#ifdef __SSE4_2__
        size_t remaining = length;

        // Hardware-accelerated CRC32C (SSE4.2)
        while (remaining >= 8) {
            uint64_t chunk;
            std::memcpy(&chunk, ptr, 8);
            crc = _mm_crc32_u64(crc, chunk);
            ptr += 8;
            remaining -= 8;
        }

        while (remaining >= 4) {
            uint32_t chunk;
            std::memcpy(&chunk, ptr, 4);
            crc = _mm_crc32_u32(crc, chunk);
            ptr += 4;
            remaining -= 4;
        }

        while (remaining > 0) {
            crc = _mm_crc32_u8(crc, *ptr);
            ++ptr;
            --remaining;
        }
#else
        // Software fallback (Castagnoli polynomial 0x1EDC6F41)
        static constexpr uint32_t poly = 0x82F63B78;

        for (size_t i = 0; i < length; ++i) {
            crc ^= ptr[i];
            for (int j = 0; j < 8; ++j) {
                crc = (crc >> 1) ^ (poly & (-(crc & 1)));
            }
        }
#endif

        return ~crc;
    }

    // ==================== String Operations ====================

    std::string_view BufferView::as_string_view(size_t offset, size_t length) const {
        check_bounds(offset, length);
        return {reinterpret_cast<const char*>(data_ + offset), length};
    }

    // ==================== Bounds Checking ====================

    void BufferView::check_bounds(size_t offset, size_t length) const {
        if (offset + length > size_ || offset + length < offset) {
            throw std::out_of_range("BufferView: access out of range");
        }
    }
} // namespace akkaradb::core