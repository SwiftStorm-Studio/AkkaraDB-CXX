#pragma once
#include <cstdint>
#include <vector>
#include <span>
#include <stdexcept>
#include <cstring>
#include <bit>

namespace akkaradb::common
{
    /**
     * @brief Little-endian byte buffer with position-based read/write operations.
     *
     * ByteBufferL provides Java NIO ByteBuffer-like API with guaranteed little-endian
     * byte order for cross-platform compatibility. Supports both owning (vector-backed)
     * and non-owning (span-backed) modes.
     *
     * Key differences from std::vector:
     * - Maintains internal position cursor (like Java ByteBuffer)
     * - All integer read/write operations are little-endian
     * - Supports zero-copy slicing
     *
     * Thread safety: Not thread-safe. Caller must synchronize access.
     *
     * Example usage:
     * @code
     * ByteBufferL buf(1024);  // Allocate 1KB
     * buf.put_i32(42);
     * buf.put_i64(0x123456789ABCDEF0);
     *
     * buf.position(0);  // Rewind
     * int32_t val = buf.get_i32();
     * @endcode
     */
    class ByteBufferL
    {
    public:
        /**
         * @brief Construct an empty buffer.
         */
        ByteBufferL() = default;

        /**
         * @brief Construct a buffer with initial capacity.
         * @param capacity Initial capacity in bytes.
         */
        explicit ByteBufferL(const size_t capacity)
        {
            data_.reserve(capacity);
        }

        /**
         * @brief Construct from existing data (copy).
         * @param data Data to copy.
         * @param size Size in bytes.
         */
        ByteBufferL(const uint8_t* data, const size_t size)
            : data_(data, data + size)
        {
        }

        /**
         * @brief Construct from vector (move).
         * @param data Vector to move from.
         */
        explicit ByteBufferL(std::vector<uint8_t>&& data)
            : data_(std::move(data))
        {
        }

        /**
         * @brief Wrap existing memory (non-owning view).
         * @param data Memory span to wrap.
         * @return ByteBufferL with non-owning view.
         */
        static ByteBufferL wrap(const std::span<const uint8_t> data)
        {
            ByteBufferL buf;
            buf.view_ = data;
            buf.is_view_ = true;
            return buf;
        }

        /**
         * @brief Wrap mutable memory (non-owning view).
         * @param data Mutable memory span to wrap.
         * @return ByteBufferL with non-owning mutable view.
         */
        static ByteBufferL wrap_mut(const std::span<uint8_t> data)
        {
            ByteBufferL buf;
            buf.view_mut_ = data;
            buf.is_view_ = true;
            buf.is_mutable_ = true;
            return buf;
        }

        // ========================================
        // Position and Capacity
        // ========================================

        /**
         * @brief Get current position.
         */
        [[nodiscard]]
        size_t position() const noexcept
        {
            return pos_;
        }

        /**
         * @brief Set position.
         * @param pos New position.
         * @throws std::out_of_range if pos > size().
         */
        void position(const size_t pos)
        {
            if (pos > size())
            {
                throw std::out_of_range("Position out of range");
            }
            pos_ = pos;
        }

        /**
         * @brief Get buffer size (total valid data).
         */
        [[nodiscard]]
        size_t size() const noexcept
        {
            if (is_view_)
            {
                return is_mutable_ ? view_mut_.size() : view_.size();
            }
            return data_.size();
        }

        /**
         * @brief Get remaining bytes from current position.
         */
        [[nodiscard]]
        size_t remaining() const noexcept
        {
            return size() - pos_;
        }

        /**
         * @brief Check if any bytes remain.
         */
        [[nodiscard]]
        bool has_remaining() const noexcept
        {
            return pos_ < size();
        }

        /**
         * @brief Get buffer capacity (for owning mode).
         */
        [[nodiscard]]
        size_t capacity() const noexcept
        {
            return is_view_ ? size() : data_.capacity();
        }

        /**
         * @brief Rewind position to 0.
         */
        void rewind() noexcept
        {
            pos_ = 0;
        }

        /**
         * @brief Clear buffer and reset position.
         */
        void clear()
        {
            if (is_view_)
            {
                throw std::logic_error("Cannot clear a view");
            }
            data_.clear();
            pos_ = 0;
        }

        // ========================================
        // Little-Endian Write Operations
        // ========================================

        /**
         * @brief Write int8_t at current position and advance.
         */
        void put_i8(const int8_t value)
        {
            put_u8(static_cast<uint8_t>(value));
        }

        /**
         * @brief Write uint8_t at current position and advance.
         */
        void put_u8(const uint8_t value)
        {
            if (is_view_)
            {
                ensure_writable(1);
                view_mut_[pos_++] = value;
            }
            else
            {
                data_.push_back(value);
                pos_++;
            }
        }

        /**
         * @brief Write int16_t (little-endian) at current position and advance.
         */
        void put_i16(const int16_t value)
        {
            put_u16(static_cast<uint16_t>(value));
        }

        /**
         * @brief Write uint16_t (little-endian) at current position and advance.
         */
        void put_u16(const uint16_t value)
        {
            uint8_t bytes[2];
            write_le_u16(bytes, value);
            put_bytes({bytes, 2});
        }

        /**
         * @brief Write int32_t (little-endian) at current position and advance.
         */
        void put_i32(const int32_t value)
        {
            put_u32(static_cast<uint32_t>(value));
        }

        /**
         * @brief Write uint32_t (little-endian) at current position and advance.
         */
        void put_u32(const uint32_t value)
        {
            uint8_t bytes[4];
            write_le_u32(bytes, value);
            put_bytes({bytes, 4});
        }

        /**
         * @brief Write int64_t (little-endian) at current position and advance.
         */
        void put_i64(const int64_t value)
        {
            put_u64(static_cast<uint64_t>(value));
        }

        /**
         * @brief Write uint64_t (little-endian) at current position and advance.
         */
        void put_u64(const uint64_t value)
        {
            uint8_t bytes[8];
            write_le_u64(bytes, value);
            put_bytes({bytes, 8});
        }

        /**
         * @brief Write byte array at current position and advance.
         */
        void put_bytes(std::span<const uint8_t> bytes)
        {
            if (is_view_)
            {
                ensure_writable(bytes.size());
                std::memcpy(view_mut_.data() + pos_, bytes.data(), bytes.size());
                pos_ += bytes.size();
            }
            else
            {
                data_.insert(data_.end(), bytes.begin(), bytes.end());
                pos_ += bytes.size();
            }
        }

        // ========================================
        // Little-Endian Read Operations
        // ========================================

        /**
         * @brief Read int8_t at current position and advance.
         */
        [[nodiscard]]
        int8_t get_i8()
        {
            return static_cast<int8_t>(get_u8());
        }

        /**
         * @brief Read uint8_t at current position and advance.
         */
        [[nodiscard]]
        uint8_t get_u8()
        {
            ensure_readable(1);
            return data_ptr()[pos_++];
        }

        /**
         * @brief Read int16_t (little-endian) at current position and advance.
         */
        [[nodiscard]]
        int16_t get_i16()
        {
            return static_cast<int16_t>(get_u16());
        }

        /**
         * @brief Read uint16_t (little-endian) at current position and advance.
         */
        [[nodiscard]]
        uint16_t get_u16()
        {
            ensure_readable(2);
            uint16_t value = read_le_u16(data_ptr() + pos_);
            pos_ += 2;
            return value;
        }

        /**
         * @brief Read int32_t (little-endian) at current position and advance.
         */
        [[nodiscard]]
        int32_t get_i32()
        {
            return static_cast<int32_t>(get_u32());
        }

        /**
         * @brief Read uint32_t (little-endian) at current position and advance.
         */
        [[nodiscard]]
        uint32_t get_u32()
        {
            ensure_readable(4);
            uint32_t value = read_le_u32(data_ptr() + pos_);
            pos_ += 4;
            return value;
        }

        /**
         * @brief Read int64_t (little-endian) at current position and advance.
         */
        [[nodiscard]]
        int64_t get_i64()
        {
            return static_cast<int64_t>(get_u64());
        }

        /**
         * @brief Read uint64_t (little-endian) at current position and advance.
         */
        [[nodiscard]]
        uint64_t get_u64()
        {
            ensure_readable(8);
            const uint64_t value = read_le_u64(data_ptr() + pos_);
            pos_ += 8;
            return value;
        }

        /**
         * @brief Read bytes into buffer and advance.
         * @param dest Destination buffer.
         * @param length Number of bytes to read.
         */
        void get_bytes(uint8_t* dest, const size_t length)
        {
            ensure_readable(length);
            std::memcpy(dest, data_ptr() + pos_, length);
            pos_ += length;
        }

        /**
         * @brief Read bytes as vector and advance.
         * @param length Number of bytes to read.
         * @return Vector containing the bytes.
         */
        [[nodiscard]]
        std::vector<uint8_t> get_bytes(const size_t length)
        {
            ensure_readable(length);
            std::vector result(data_ptr() + pos_, data_ptr() + pos_ + length);
            pos_ += length;
            return result;
        }

        // ========================================
        // Absolute Read/Write (without moving position)
        // ========================================

        /**
         * @brief Read uint8_t at absolute offset.
         */
        [[nodiscard]]
        uint8_t get_u8_at(const size_t offset) const
        {
            if (offset >= size())
            {
                throw std::out_of_range("Read past end");
            }
            return data_ptr()[offset];
        }

        /**
         * @brief Read uint32_t at absolute offset (little-endian).
         */
        [[nodiscard]]
        uint32_t get_u32_at(const size_t offset) const
        {
            if (offset + 4 > size())
            {
                throw std::out_of_range("Read past end");
            }
            return read_le_u32(data_ptr() + offset);
        }

        /**
         * @brief Read uint64_t at absolute offset (little-endian).
         */
        [[nodiscard]]
        uint64_t get_u64_at(const size_t offset) const
        {
            if (offset + 8 > size())
            {
                throw std::out_of_range("Read past end");
            }
            return read_le_u64(data_ptr() + offset);
        }

        /**
         * @brief Write uint32_t at absolute offset (little-endian).
         */
        void put_u32_at(const size_t offset, const uint32_t value)
        {
            if (is_view_ && !is_mutable_)
            {
                throw std::logic_error("Cannot write to const view");
            }
            if (offset + 4 > size())
            {
                throw std::out_of_range("Write past end");
            }
            write_le_u32(mut_data_ptr() + offset, value);
        }

        /**
         * @brief Write uint64_t at absolute offset (little-endian).
         */
        void put_u64_at(const size_t offset, const uint64_t value)
        {
            if (is_view_ && !is_mutable_)
            {
                throw std::logic_error("Cannot write to const view");
            }
            if (offset + 8 > size())
            {
                throw std::out_of_range("Write past end");
            }
            write_le_u64(mut_data_ptr() + offset, value);
        }

        // ========================================
        // Slicing (zero-copy views)
        // ========================================

        /**
         * @brief Create a zero-copy slice from current position.
         * @param length Slice length.
         * @return ByteBufferL view (non-owning).
         */
        [[nodiscard]]
        ByteBufferL slice(const size_t length) const
        {
            ensure_readable(length);
            return slice_at(pos_, length);
        }

        /**
         * @brief Create a zero-copy slice at absolute offset.
         * @param offset Start offset.
         * @param length Slice length.
         * @return ByteBufferL view (non-owning).
         */
        [[nodiscard]]
        ByteBufferL slice_at(const size_t offset, size_t length) const
        {
            if (offset + length > size())
            {
                throw std::out_of_range("Slice out of range");
            }
            return wrap({data_ptr() + offset, length});
        }

        /**
         * @brief Duplicate this buffer (shared data, independent position).
         */
        [[nodiscard]]
        ByteBufferL duplicate() const
        {
            ByteBufferL dup;
            if (is_view_)
            {
                dup = is_mutable_ ? wrap_mut(view_mut_) : wrap(view_);
            }
            else
            {
                dup = wrap({data_.data(), data_.size()});
            }
            dup.pos_ = pos_;
            return dup;
        }

        // ========================================
        // Direct Access
        // ========================================

        /**
         * @brief Get const pointer to underlying data.
         */
        [[nodiscard]]
        const uint8_t* data() const noexcept
        {
            return data_ptr();
        }

        /**
         * @brief Get span of all data.
         */
        [[nodiscard]]
        std::span<const uint8_t> span() const noexcept
        {
            return {data_ptr(), size()};
        }

        /**
         * @brief Get span of remaining data from current position.
         */
        [[nodiscard]]
        std::span<const uint8_t> remaining_span() const noexcept
        {
            return {data_ptr() + pos_, remaining()};
        }

        /**
         * @brief Convert to vector (copy).
         */
        [[nodiscard]]
        std::vector<uint8_t> to_vector() const
        {
            return {data_ptr(), data_ptr() + size()};
        }

    private:
        std::vector<uint8_t> data_; // Owning storage
        std::span<const uint8_t> view_; // Non-owning const view
        std::span<uint8_t> view_mut_; // Non-owning mutable view
        size_t pos_ = 0;
        bool is_view_ = false;
        bool is_mutable_ = false;

        [[nodiscard]]
        const uint8_t* data_ptr() const noexcept
        {
            if (is_view_)
            {
                return is_mutable_ ? view_mut_.data() : view_.data();
            }
            return data_.data();
        }

        uint8_t* mut_data_ptr()
        {
            if (is_view_)
            {
                if (!is_mutable_)
                {
                    throw std::logic_error("Cannot modify const view");
                }
                return view_mut_.data();
            }
            return data_.data();
        }

        void ensure_readable(const size_t n) const
        {
            if (pos_ + n > size())
            {
                throw std::out_of_range("Read past end");
            }
        }

        void ensure_writable(const size_t n) const
        {
            if (!is_mutable_)
            {
                throw std::logic_error("Cannot write to const view");
            }
            if (pos_ + n > size())
            {
                throw std::out_of_range("Write past end");
            }
        }

        // ========================================
        // Little-endian helpers (C++20 compatible)
        // ========================================

        static void write_le_u16(uint8_t* p, const uint16_t v) noexcept
        {
            if constexpr (std::endian::native == std::endian::little)
            {
                std::memcpy(p, &v, 2);
            }
            else
            {
                p[0] = static_cast<uint8_t>(v & 0xFF);
                p[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
            }
        }

        static void write_le_u32(uint8_t* p, const uint32_t v) noexcept
        {
            if constexpr (std::endian::native == std::endian::little)
            {
                std::memcpy(p, &v, 4);
            }
            else
            {
                p[0] = static_cast<uint8_t>(v & 0xFF);
                p[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
                p[2] = static_cast<uint8_t>((v >> 16) & 0xFF);
                p[3] = static_cast<uint8_t>((v >> 24) & 0xFF);
            }
        }

        static void write_le_u64(uint8_t* p, const uint64_t v) noexcept
        {
            if constexpr (std::endian::native == std::endian::little)
            {
                std::memcpy(p, &v, 8);
            }
            else
            {
                for (int i = 0; i < 8; ++i)
                {
                    p[i] = static_cast<uint8_t>((v >> (i * 8)) & 0xFF);
                }
            }
        }

        static uint16_t read_le_u16(const uint8_t* p) noexcept
        {
            if constexpr (std::endian::native == std::endian::little)
            {
                uint16_t v;
                std::memcpy(&v, p, 2);
                return v;
            }
            else
            {
                return static_cast<uint16_t>(p[0]) |
                    (static_cast<uint16_t>(p[1]) << 8);
            }
        }

        static uint32_t read_le_u32(const uint8_t* p) noexcept
        {
            if constexpr (std::endian::native == std::endian::little)
            {
                uint32_t v;
                std::memcpy(&v, p, 4);
                return v;
            }
            else
            {
                return static_cast<uint32_t>(p[0]) |
                    (static_cast<uint32_t>(p[1]) << 8) |
                    (static_cast<uint32_t>(p[2]) << 16) |
                    (static_cast<uint32_t>(p[3]) << 24);
            }
        }

        static uint64_t read_le_u64(const uint8_t* p) noexcept
        {
            if constexpr (std::endian::native == std::endian::little)
            {
                uint64_t v;
                std::memcpy(&v, p, 8);
                return v;
            }
            else
            {
                uint64_t v = 0;
                for (int i = 0; i < 8; ++i)
                {
                    v |= static_cast<uint64_t>(p[i]) << (i * 8);
                }
                return v;
            }
        }
    };
}
