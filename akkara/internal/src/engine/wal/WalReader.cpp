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

// internal/src/engine/wal/WalReader.cpp
#include "engine/wal/WalReader.hpp"
#include "engine/wal/WalFraming.hpp"
#include <fstream>
#include <stdexcept>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#endif

namespace akkaradb::engine::wal {
    /**
     * Platform-specific file reader.
     */
    class FileReader {
        public:
            #ifdef _WIN32
            using NativeHandle = HANDLE; inline static const auto INVALID = INVALID_HANDLE_VALUE;
            #else
            using NativeHandle = int;
            static constexpr NativeHandle INVALID = -1;
            #endif

            FileReader() : handle_{INVALID}, file_size_{0} {}

            ~FileReader() { close(); }

            FileReader(const FileReader&) = delete;
            FileReader& operator=(const FileReader&) = delete;

            FileReader(FileReader&& other) noexcept
                : handle_{other.handle_}, file_size_{other.file_size_} {
                other.handle_ = INVALID;
                other.file_size_ = 0;
            }

            FileReader& operator=(FileReader&& other) noexcept {
                if (this != &other) {
                    close();
                    handle_ = other.handle_;
                    file_size_ = other.file_size_;
                    other.handle_ = INVALID;
                    other.file_size_ = 0;
                }
                return *this;
            }

            [[nodiscard]] static FileReader open(const std::filesystem::path& path) {
                FileReader reader;

                #ifdef _WIN32
                reader.handle_ = ::CreateFileW(
                    path.c_str(),
                    GENERIC_READ,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    nullptr,
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL,
                    nullptr
                ); if (reader.handle_ == INVALID) { throw std::runtime_error("Failed to open WAL file: " + path.string()); } LARGE_INTEGER size; if (
                    ::GetFileSizeEx(reader.handle_, &size)) { reader.file_size_ = static_cast<uint64_t>(size.QuadPart); }
                #else
                reader.handle_ = ::open(path.c_str(), O_RDONLY);

                if (reader.handle_ < 0) { throw std::runtime_error("Failed to open WAL file: " + path.string()); }

                struct stat st;
                if (::fstat(reader.handle_, &st) == 0) { reader.file_size_ = static_cast<uint64_t>(st.st_size); }
                #endif

                return reader;
            }

            void close() noexcept {
                if (handle_ != INVALID) {
                    #ifdef _WIN32
                    ::CloseHandle(handle_);
                    #else
                    ::close(handle_);
                    #endif
                    handle_ = INVALID;
                }
            }

            [[nodiscard]] size_t read(uint8_t* buffer, size_t size) {
                #ifdef _WIN32
                DWORD bytes_read = 0; if (!::ReadFile(handle_, buffer, static_cast<DWORD>(size), &bytes_read, nullptr)) { return 0; } return bytes_read;
                #else
                ssize_t result = ::read(handle_, buffer, size);
                return result > 0
                           ? static_cast<size_t>(result)
                           : 0;
                #endif
            }

            [[nodiscard]] uint64_t file_size() const noexcept { return file_size_; }

        private:
            NativeHandle handle_;
            uint64_t file_size_;
    };

    /**
     * WalReader::Impl - Private implementation.
     */
    class WalReader::Impl {
        public:
            explicit Impl(const std::filesystem::path& wal_file)
                : file_reader_{FileReader::open(wal_file)},
                  read_buffer_{core::OwnedBuffer::allocate(64 * 1024, 4096)},
                  current_position_{0},
                  operations_read_{0},
                  error_type_{ErrorType::NONE},
                  error_position_{0} {}

            [[nodiscard]] std::optional<WalOp> next() {
                if (error_type_ != ErrorType::NONE) { return std::nullopt; }

                uint8_t header_buf[WalFraming::FRAME_HEADER_SIZE];
                const size_t header_read = file_reader_.read(header_buf, WalFraming::FRAME_HEADER_SIZE);

                if (header_read == 0) { return std::nullopt; }

                if (header_read < WalFraming::FRAME_HEADER_SIZE) {
                    error_type_ = ErrorType::TRUNCATED_FRAME;
                    error_position_ = current_position_;
                    return std::nullopt;
                }

                current_position_ += header_read;

                auto header_view = core::BufferView{reinterpret_cast<std::byte*>(header_buf), WalFraming::FRAME_HEADER_SIZE};
                auto frame_len_opt = WalFraming::try_read_frame_length(header_view);

                if (!frame_len_opt) {
                    error_type_ = ErrorType::INVALID_MAGIC;
                    error_position_ = current_position_ - header_read;
                    return std::nullopt;
                }

                const size_t frame_len = *frame_len_opt;
                const size_t remaining_len = frame_len - WalFraming::FRAME_HEADER_SIZE;

                if (read_buffer_.size() < frame_len) { read_buffer_ = core::OwnedBuffer::allocate(frame_len, 4096); }

                std::memcpy(read_buffer_.data(), header_buf, WalFraming::FRAME_HEADER_SIZE);

                const size_t body_read = file_reader_.read(reinterpret_cast<uint8_t*>(read_buffer_.data()) + WalFraming::FRAME_HEADER_SIZE, remaining_len);

                if (body_read < remaining_len) {
                    error_type_ = ErrorType::TRUNCATED_FRAME;
                    error_position_ = current_position_;
                    return std::nullopt;
                }

                current_position_ += body_read;

                auto frame_view = read_buffer_.view().slice(0, frame_len);
                auto op_opt = WalFraming::decode(frame_view);

                if (!op_opt) {
                    error_type_ = ErrorType::CRC_MISMATCH;
                    error_position_ = current_position_ - frame_len;
                    return std::nullopt;
                }

                ++operations_read_;
                return op_opt;
            }

            [[nodiscard]] bool has_error() const noexcept { return error_type_ != ErrorType::NONE; }

            [[nodiscard]] ErrorType error_type() const noexcept { return error_type_; }

            [[nodiscard]] uint64_t error_position() const noexcept { return error_position_; }

            [[nodiscard]] uint64_t current_position() const noexcept { return current_position_; }

            [[nodiscard]] uint64_t file_size() const noexcept { return file_reader_.file_size(); }

            [[nodiscard]] uint64_t operations_read() const noexcept { return operations_read_; }

        private:
            FileReader file_reader_;
            core::OwnedBuffer read_buffer_;
            uint64_t current_position_;
            uint64_t operations_read_;
            ErrorType error_type_;
            uint64_t error_position_;
    };

    // ==================== WalReader Public API ====================

    std::unique_ptr<WalReader> WalReader::open(const std::filesystem::path& wal_file) { return std::unique_ptr<WalReader>(new WalReader(wal_file)); }

    WalReader::WalReader(const std::filesystem::path& wal_file) : impl_{std::make_unique<Impl>(wal_file)} {}

    WalReader::~WalReader() = default;

    std::optional<WalOp> WalReader::next() { return impl_->next(); }

    bool WalReader::has_error() const noexcept { return impl_->has_error(); }

    WalReader::ErrorType WalReader::error_type() const noexcept { return impl_->error_type(); }

    uint64_t WalReader::error_position() const noexcept { return impl_->error_position(); }

    uint64_t WalReader::current_position() const noexcept { return impl_->current_position(); }

    uint64_t WalReader::file_size() const noexcept { return impl_->file_size(); }

    uint64_t WalReader::operations_read() const noexcept { return impl_->operations_read(); }
} // namespace akkaradb::engine::wal