// OwnedBuffer.hpp
#pragma once

#include <cstring>

#include "BufferView.hpp"
#include <memory>

namespace akkaradb::core {
    class OwnedBuffer {
    public:
        OwnedBuffer() noexcept = default;

        [[nodiscard]] static OwnedBuffer allocate(size_t size,
                                                  size_t alignment = 4096);

        OwnedBuffer(OwnedBuffer&&) noexcept = default;
        OwnedBuffer& operator=(OwnedBuffer&&) noexcept = default;

        OwnedBuffer(const OwnedBuffer&) = delete;
        OwnedBuffer& operator=(const OwnedBuffer&) = delete;

        [[nodiscard]] BufferView view() const noexcept {
            return BufferView{data_.get(), size_};
        }

        [[nodiscard]] BufferView view() noexcept {
            return BufferView{data_.get(), size_};
        }

        [[nodiscard]] std::byte* data() noexcept { return data_.get(); }
        [[nodiscard]] const std::byte* data() const noexcept { return data_.get(); }
        [[nodiscard]] size_t size() const noexcept { return size_; }
        [[nodiscard]] bool empty() const noexcept { return size_ == 0; }

        void zero_fill() noexcept {
            if (data_ && size_ > 0) {
                std::memset(data_.get(), 0, size_);
            }
        }

        [[nodiscard]] std::byte* release() noexcept {
            size_ = 0;
            return data_.release();
        }

    private:
        // Custom deleter (defined in .cpp)
        struct AlignedDeleter {
            void operator()(std::byte* ptr) const noexcept;
        };

        std::unique_ptr<std::byte[], AlignedDeleter> data_;
        size_t size_{0};

        OwnedBuffer(std::byte* data, size_t size) noexcept
            : data_{data}, size_{size} {
        }
    };
} // namespace akkaradb::core