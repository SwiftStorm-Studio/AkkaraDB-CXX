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

// internal/src/core/buffer/BufferView.cpp
#include "core/buffer/BufferView.hpp"

#include <bit>
#include <stdexcept>
#include <cstring>

static_assert(std::endian::native == std::endian::little, "AkkaraDB requires Little-Endian architecture");

// ==================== Feature Detection ====================

#if defined(__SSE4_2__) || (defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86)))
#define AKKARADB_HAS_SSE42 1
#else
#define AKKARADB_HAS_SSE42 0
#endif

#if defined(__AVX2__)
#define AKKARADB_HAS_AVX2 1
#else
#define AKKARADB_HAS_AVX2 0
#endif

#if defined(__AVX512F__)
#define AKKARADB_HAS_AVX512 1
#else
#define AKKARADB_HAS_AVX512 0
#endif

// ==================== SIMD Includes ====================

#if AKKARADB_HAS_SSE42
#include <nmmintrin.h>
#endif

#if AKKARADB_HAS_AVX2
#include <immintrin.h>
#endif


namespace akkaradb::core {
    // ==================== Slicing ====================

    BufferView BufferView::slice(size_t offset, size_t length) const {
        check_bounds(offset, length);
        return BufferView{data_ + offset, length};
    }

    BufferView BufferView::slice(size_t offset) const {
        if (offset > size_) { throw std::out_of_range("BufferView::slice: offset out of range"); }
        return BufferView{data_ + offset, size_ - offset};
    }

    // ==================== Little-Endian Read/Write Operations ====================
    // Note: These are now inlined in the header for zero overhead

    // ==================== Bulk Operations ====================

    void BufferView::copy_from(size_t offset, BufferView src, size_t src_offset, size_t length) const {
        check_bounds(offset, length);
        src.check_bounds(src_offset, length);
        std::memmove(data_ + offset, src.data_ + src_offset, length);
    }

    void BufferView::fill(size_t offset, size_t length, std::byte value) const {
        check_bounds(offset, length);
        std::memset(data_ + offset, static_cast<int>(value), length);
    }

    void BufferView::zero_fill() const noexcept {
        if (!data_ || size_ == 0) { return; }

        // Small buffers: Use standard memset (compiler optimizes)
        if (size_ < 256) {
            std::memset(data_, 0, size_);
            return;
        }

        auto* ptr = data_;
        size_t remaining = size_;

        #if AKKARADB_HAS_AVX512
        // AVX-512: 64 bytes per iteration
        const __m512i zero = _mm512_setzero_si512(); while (remaining >= 64) {
            _mm512_storeu_si512(reinterpret_cast<void*>(ptr), zero);
            ptr += 64;
            remaining -= 64;
        }
        #elif AKKARADB_HAS_AVX2
        // AVX2: 32 bytes per iteration
        const __m256i zero = _mm256_setzero_si256(); while (remaining >= 32) {
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(ptr), zero);
            ptr += 32;
            remaining -= 32;
        }
        #elif defined(__x86_64__) || defined(_M_X64)
        // x86-64: Use rep stosb (optimized on modern CPUs)
        // Note: This is often as fast as SIMD on Intel/AMD CPUs with ERMSB
        std::memset(ptr, 0, remaining); return;
        #endif

        // Handle remaining bytes
        if (remaining > 0) { std::memset(ptr, 0, remaining); }
    }

    // ==================== CRC Computation ====================

    uint32_t BufferView::crc32c(size_t offset, size_t length) const {
        check_bounds(offset, length);

        auto crc = 0xFFFFFFFF;
        const auto* ptr = reinterpret_cast<const uint8_t*>(data_ + offset);

        #if AKKARADB_HAS_SSE42
        size_t remaining = length; while (remaining >= 8) {
            uint64_t chunk;
            std::memcpy(&chunk, ptr, 8);
            crc = static_cast<uint32_t>(_mm_crc32_u64(crc, chunk));
            ptr += 8;
            remaining -= 8;
        } while (remaining >= 4) {
            uint32_t chunk;
            std::memcpy(&chunk, ptr, 4);
            crc = _mm_crc32_u32(crc, chunk);
            ptr += 4;
            remaining -= 4;
        } while (remaining > 0) {
            crc = _mm_crc32_u8(crc, *ptr);
            ++ptr;
            --remaining;
        }
        #else
        static constexpr uint32_t poly = 0x82F63B78;
        for (size_t i = 0; i < length; ++i) {
            crc ^= ptr[i];
            for (int j = 0; j < 8; ++j) { crc = (crc >> 1) ^ (poly & (-(crc & 1))); }
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
        if (offset + length > size_ || offset + length < offset) { throw std::out_of_range("BufferView: access out of range"); }
    }
} // namespace akkaradb::core