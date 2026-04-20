// internal/src/core/buffer/OwnedBuffer.cpp

#include "core/buffer/OwnedBuffer.hpp"
#include "core/buffer/BufferView.hpp"

#include <new>

namespace akkaradb::core {
    namespace {
        // ==================== Heap Deleter ====================

        void heap_deleter(void* ptr, size_t /*size*/, void* /*ctx*/) { ::operator delete(ptr); }
    }

    // ==================== Factory ====================

    OwnedBuffer OwnedBuffer::allocate(size_t size) {
        if (size == 0) { return OwnedBuffer{}; }

        void* ptr = ::operator new(size);

        return {static_cast<std::byte*>(ptr), size, &heap_deleter, nullptr};
    }

    // ==================== View ====================

    BufferView OwnedBuffer::as_view() const noexcept { return BufferView{data_, size_}; }

} // namespace akkaradb::core