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

// internal/src/engine/wal/WalWriter.cpp
#include "engine/wal/WalWriter.hpp"
#include "engine/wal/WalFraming.hpp"
#include "format-akk/AkkStripeWriter.hpp"
#include <stdexcept>
#include <chrono>
#include <utility>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#endif

namespace akkaradb::engine::wal {
    /**
     * Platform-specific file handle wrapper.
     */
    class FileHandle {
    public:
#ifdef _WIN32
        using NativeHandle = HANDLE;
        inline static const auto INVALID = INVALID_HANDLE_VALUE; // C++17
#else
        using NativeHandle = int;
        static constexpr NativeHandle INVALID = -1;
#endif

        FileHandle() : handle_{INVALID} {}

        explicit FileHandle(NativeHandle handle) : handle_{handle} {}

        ~FileHandle() { close(); }

        FileHandle(const FileHandle&) = delete;
        FileHandle& operator=(const FileHandle&) = delete;

        FileHandle(FileHandle&& other) noexcept : handle_{other.handle_} { other.handle_ = INVALID; }

        FileHandle& operator=(FileHandle&& other) noexcept {
            if (this != &other) {
                close();
                handle_ = other.handle_;
                other.handle_ = INVALID;
            }
            return *this;
        }

        [[nodiscard]] bool is_valid() const noexcept { return handle_ != INVALID; }

        [[nodiscard]] NativeHandle get() const noexcept { return handle_; }

        void close() noexcept {
            if (is_valid()) {
#ifdef _WIN32
                ::CloseHandle(handle_);
#else
                ::close(handle_);
#endif
                handle_ = INVALID;
            }
        }

        static FileHandle open(const std::filesystem::path& path) {
#ifdef _WIN32
            HANDLE h = ::CreateFileW(
                path.c_str(),
                GENERIC_WRITE,
                0, // No sharing
                nullptr,
                CREATE_ALWAYS,
                FILE_ATTRIBUTE_NORMAL,
                nullptr
            );

            if (h == INVALID_HANDLE_VALUE) { throw std::runtime_error("Failed to create WAL file: " + path.string()); }

            return FileHandle{h};
#else
            int fd = ::open(
                path.c_str(),
                O_WRONLY | O_CREAT | O_TRUNC,
                S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH // 0644
            );

            if (fd < 0) { throw std::runtime_error("Failed to create WAL file: " + path.string()); }

            return FileHandle{fd};
#endif
        }

        void write(const uint8_t* data, size_t size) {
#ifdef _WIN32
            DWORD bytes_written = 0;
            if (!::WriteFile(handle_, data, static_cast<DWORD>(size), &bytes_written, nullptr)) { throw std::runtime_error("WriteFile failed"); }
            if (bytes_written != size) { throw std::runtime_error("Incomplete write"); }
#else
            ssize_t result = ::write(handle_, data, size);
            if (result < 0) { throw std::runtime_error("write() failed"); }
            if (static_cast<size_t>(result) != size) { throw std::runtime_error("Incomplete write"); }
#endif
        }

        void fsync(WalWriter::SyncMode sync_mode) {
#ifdef _WIN32
            (void)sync_mode; // Windows doesn't distinguish
            if (!::FlushFileBuffers(handle_)) { throw std::runtime_error("FlushFileBuffers failed"); }
#elif defined(__APPLE__)
            (void)sync_mode; // macOS: always use F_FULLFSYNC
            if (::fcntl(handle_, F_FULLFSYNC) < 0) { throw std::runtime_error("fcntl(F_FULLFSYNC) failed"); }
#else
            // Linux
            int result;
            if (sync_mode == WalWriter::SyncMode::DATA_ONLY) {
                result = ::fdatasync(handle_); // Data only (faster)
            }
            else {
                result = ::fsync(handle_); // Data + metadata
            }

            if (result < 0) { throw std::runtime_error("fsync/fdatasync failed"); }
#endif
        }

    private:
        NativeHandle handle_;
    };

    /**
     * WalWriter::Impl - Private implementation.
     */
    class WalWriter::Impl {
    public:
        Impl(
            std::filesystem::path wal_dir,
            std::shared_ptr<core::BufferPool> buffer_pool,
            format::FlushPolicy flush_policy,
            size_t k,
            size_t m,
            std::shared_ptr<format::ParityCoder> parity_coder,
            FlushMode flush_mode,
            SyncMode sync_mode
        ) : wal_dir_{std::move(wal_dir)}
            , buffer_pool_{std::move(buffer_pool)}
            , flush_policy_{flush_policy}
            , k_{k}
            , m_{m}
            , flush_mode_{flush_mode}
            , sync_mode_{sync_mode}
            , operations_written_{0}
            , blocks_written_{0}
            , bytes_written_{0} {
            if (!std::filesystem::exists(wal_dir_)) { std::filesystem::create_directories(wal_dir_); }

            if (!parity_coder) {
                if (m_ == 0) { parity_coder = format::NoParityCoder::create(); }
                else if (m_ == 1) { parity_coder = format::XorParityCoder::create(); }
                else { throw std::invalid_argument("WalWriter: parity_coder required for m > 1"); }
            }

            const auto timestamp = std::chrono::system_clock::now().time_since_epoch().count();
            current_file_path_ = wal_dir_ / ("wal-" + std::to_string(timestamp) + ".dat");

            file_handle_ = FileHandle::open(current_file_path_);

            stripe_writer_ = format::akk::AkkStripeWriter::create(
                k_,
                m_,
                std::move(parity_coder),
                [this](format::StripeWriter::Stripe stripe) { this->on_stripe_ready(std::move(stripe)); },
                flush_policy_
            );

            current_block_ = buffer_pool_->acquire();
            current_block_.zero_fill();
            block_offset_ = 0;
        }

        ~Impl() {
            try { flush_internal(); }
            catch (...) {
                // Destructor must not throw
            }
        }

        void append(const WalOp& op) {
            auto frame = WalFraming::encode(op);
            const size_t frame_size = frame.size();

            if (block_offset_ + frame_size > buffer_pool_->block_size()) { flush_current_block(); }

            auto block_view = current_block_.view();
            std::memcpy(block_view.data() + block_offset_, frame.data(), frame_size);
            block_offset_ += frame_size;

            ++operations_written_;
        }

        void flush() { flush_internal(); }

        [[nodiscard]] uint64_t operations_written() const noexcept { return operations_written_; }

        [[nodiscard]] uint64_t blocks_written() const noexcept { return blocks_written_; }

        [[nodiscard]] uint64_t stripes_written() const noexcept { return stripe_writer_->stripes_written(); }

        [[nodiscard]] uint64_t bytes_written() const noexcept { return bytes_written_; }

        [[nodiscard]] std::filesystem::path current_file_path() const { return current_file_path_; }

    private:
        void flush_current_block() {
            if (block_offset_ == 0) { return; }

            stripe_writer_->add_block(std::move(current_block_));
            ++blocks_written_;

            current_block_ = buffer_pool_->acquire();
            current_block_.zero_fill();
            block_offset_ = 0;
        }

        void flush_internal() {
            flush_current_block();
            stripe_writer_->flush();

            if (flush_mode_ == FlushMode::SYNC && file_handle_.is_valid()) { file_handle_.fsync(sync_mode_); }
        }

        // on_stripe_ready
        void on_stripe_ready(const format::StripeWriter::Stripe& stripe) {
            for (const auto& block : stripe.data_blocks) {
                file_handle_.write(
                    reinterpret_cast<const uint8_t*>(block.data()),
                    block.size()
                );
                bytes_written_ += block.size();
            }

            for (const auto& block : stripe.parity_blocks) {
                file_handle_.write(
                    reinterpret_cast<const uint8_t*>(block.data()),
                    block.size()
                );
                bytes_written_ += block.size();
            }

            if (flush_mode_ == FlushMode::SYNC) {
            file_handle_.fsync(sync_mode_);
        }
    }

    std::filesystem::path wal_dir_;
    std::filesystem::path current_file_path_;
    std::shared_ptr<core::BufferPool> buffer_pool_;
    format::FlushPolicy flush_policy_;
    size_t k_;
    size_t m_;
    FlushMode flush_mode_;
    SyncMode sync_mode_;

    FileHandle file_handle_;
    std::unique_ptr<format::akk::AkkStripeWriter> stripe_writer_;

    core::OwnedBuffer current_block_;
    size_t block_offset_;

    uint64_t operations_written_;
    uint64_t blocks_written_;
    uint64_t bytes_written_;
};

// ==================== WalWriter Public API ====================

std::unique_ptr<WalWriter> WalWriter::create(
    const std::filesystem::path& wal_dir,
    std::shared_ptr<core::BufferPool> buffer_pool,
    format::FlushPolicy flush_policy,
    size_t k,
    size_t m,
    std::shared_ptr<format::ParityCoder> parity_coder,
    FlushMode flush_mode,
    SyncMode sync_mode
) {
    return std::unique_ptr<WalWriter>(new WalWriter(
        wal_dir, std::move(buffer_pool), flush_policy, k, m, std::move(parity_coder), flush_mode, sync_mode
    ));
}

WalWriter::WalWriter(
    const std::filesystem::path& wal_dir,
    std::shared_ptr<core::BufferPool> buffer_pool,
    format::FlushPolicy flush_policy,
    size_t k,
    size_t m,
    std::shared_ptr<format::ParityCoder> parity_coder,
    FlushMode flush_mode,
    SyncMode sync_mode
)
    : impl_{std::make_unique<Impl>(
        wal_dir, std::move(buffer_pool), flush_policy, k, m, std::move(parity_coder), flush_mode, sync_mode
    )} {}

WalWriter::~WalWriter() = default;

void WalWriter::append(const WalOp& op) {
    impl_->append(op);
}

void WalWriter::flush() {
    impl_->flush();
}

uint64_t WalWriter::operations_written() const noexcept {
    return impl_->operations_written();
}

uint64_t WalWriter::blocks_written() const noexcept {
    return impl_->blocks_written();
}

uint64_t WalWriter::stripes_written() const noexcept {
    return impl_->stripes_written();
}

uint64_t WalWriter::bytes_written() const noexcept {
    return impl_->bytes_written();
}

std::filesystem::path WalWriter::current_file_path() const {
    return impl_->current_file_path();
}

} // namespace akkaradb::engine::wal