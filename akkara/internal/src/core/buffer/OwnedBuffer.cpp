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

// internal/src/core/buffer/OwnedBuffer.cpp
#include "core/buffer/OwnedBuffer.hpp"
#include <stdexcept>
#include <new>
#include <cstdlib>

// Platform-specific includes
#if defined(_WIN32)
#include <malloc.h>  // _aligned_malloc, _aligned_free
#endif

namespace akkaradb::core {
    namespace {
        /**
 * Platform-agnostic aligned memory allocation.
 *
 * @param size Size in bytes
 * @param alignment Alignment (must be power of 2)
 * @return Pointer to aligned memory, or nullptr on failure
 */
        void* allocate_aligned(size_t size, size_t alignment) {
            #if defined(_WIN32)
            // Windows: _aligned_malloc
            return _aligned_malloc(size, alignment);

            #elif defined(__APPLE__) || (defined(__ANDROID__) && __ANDROID_API__ < 28)
            // macOS or old Android: posix_memalign
            void* ptr = nullptr; if (posix_memalign(&ptr, alignment, size) != 0) { return nullptr; } return ptr;

            #else
            // Linux (glibc >= 2.16), FreeBSD: aligned_alloc
            // Note: size must be multiple of alignment for aligned_alloc
            const size_t adjusted_size = (size + alignment - 1) & ~(alignment - 1); return std::aligned_alloc(alignment, adjusted_size);
            #endif
        }

        /**
         * Platform-agnostic aligned memory deallocation.
         */
        void deallocate_aligned(void* ptr) noexcept {
            #if defined(_WIN32)
            _aligned_free(ptr);
            #else
            std::free(ptr);
            #endif
        }
    } // anonymous namespace

    // ==================== AlignedDeleter Implementation ====================

    void OwnedBuffer::AlignedDeleter::operator()(std::byte* ptr) const noexcept {
        if (owns_memory && ptr) { deallocate_aligned(ptr); }
        // Non-owning buffers: no-op (memory managed externally)
    }

    // ==================== OwnedBuffer Implementation ====================

    OwnedBuffer OwnedBuffer::allocate(size_t size, size_t alignment) {
        // Zero-sized buffer is represented as empty OwnedBuffer
        if (size == 0) { return OwnedBuffer{}; }

        // Validate alignment (must be power of 2)
        if (alignment == 0 || (alignment & (alignment - 1)) != 0) { throw std::invalid_argument("Alignment must be a power of 2"); }

        // Ensure alignment is at least alignof(std::max_align_t)
        if (alignment < alignof(std::max_align_t)) { alignment = alignof(std::max_align_t); }

        // Allocate
        void* raw_ptr = allocate_aligned(size, alignment);
        if (!raw_ptr) { throw std::bad_alloc{}; }

        auto* byte_ptr = static_cast<std::byte*>(raw_ptr);
        return OwnedBuffer{byte_ptr, size, true}; // owns_memory = true
    }

    OwnedBuffer OwnedBuffer::wrap_non_owning(std::byte* data, size_t size) noexcept {
        return OwnedBuffer{data, size, false}; // owns_memory = false
    }
} // namespace akkaradb::core
