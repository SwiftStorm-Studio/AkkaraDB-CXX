// OwnedBuffer.cpp
#include "../../../include/akkara/core/buf/OwnedBuffer.hpp"
#include <stdexcept>
#include <new>
#include <cstdlib>

// Platform-specific includes
#if defined(_WIN32)
#include <malloc.h>  // _aligned_malloc
#elif defined(__APPLE__)
#include <cstdlib>   // posix_memalign
#else
#include <cstdlib>   // aligned_alloc or posix_memalign
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
            void* ptr = nullptr;
            if (posix_memalign(&ptr, alignment, size) != 0) {
                return nullptr;
            }
            return ptr;

#else
            // Linux (glibc >= 2.16), FreeBSD: aligned_alloc
            // Note: size must be multiple of alignment for aligned_alloc
            const size_t adjusted_size = (size + alignment - 1) & ~(alignment - 1);
            return std::aligned_alloc(alignment, adjusted_size);
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

    // Custom deleter implementation
    void OwnedBuffer::AlignedDeleter::operator()(std::byte* ptr) const noexcept {
        if (ptr) {
            deallocate_aligned(ptr);
        }
    }

    OwnedBuffer OwnedBuffer::allocate(size_t size, size_t alignment) {
        if (size == 0) {
            return OwnedBuffer{};
        }

        // Validate alignment
        if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
            throw std::invalid_argument("Alignment must be a power of 2");
        }

        // Ensure alignment is at least sizeof(void*)
        if (alignment < alignof(std::max_align_t)) {
            alignment = alignof(std::max_align_t);
        }

        // Allocate
        void* raw_ptr = allocate_aligned(size, alignment);
        if (!raw_ptr) {
            throw std::bad_alloc{};
        }

        auto* byte_ptr = static_cast<std::byte*>(raw_ptr);
        return OwnedBuffer{byte_ptr, size};
    }
} // namespace akkaradb::core