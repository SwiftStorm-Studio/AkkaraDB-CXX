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
#include "core/buffer/OwnedBuffer.hpp"
#include <stdexcept>
#include <chrono>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <thread>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cerrno>
#include <cstring>
#endif

namespace akkaradb::engine::wal {
    namespace {
        /**
         * Platform-specific file handle for direct I/O with fsync.
         * Handles partial write detection and recovery.
         */
        class FileHandle {
            public:
                #ifdef _WIN32
                using NativeHandle = HANDLE; inline static const NativeHandle INVALID = INVALID_HANDLE_VALUE;
                #else
                using NativeHandle = int;
                static constexpr NativeHandle INVALID = -1;
                #endif

                FileHandle() : handle_{INVALID} {}

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

                [[nodiscard]] static FileHandle open(const std::filesystem::path& path) {
                    FileHandle fh;

                    #ifdef _WIN32
                    fh.handle_ = ::CreateFileW(
                        path.c_str(),
                        GENERIC_WRITE,
                        FILE_SHARE_READ,
                        nullptr,
                        OPEN_ALWAYS,
                        FILE_ATTRIBUTE_NORMAL,
                        nullptr
                    ); if (fh.handle_ == INVALID) { throw std::runtime_error("Failed to open WAL: " + path.string()); } ::SetFilePointer(
                        fh.handle_,
                        0,
                        nullptr,
                        FILE_END
                    );
                    #else
                    fh.handle_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
                    if (fh.handle_ < 0) { throw std::runtime_error("Failed to open WAL: " + path.string()); }
                    #endif

                    return fh;
                }

                void write(const uint8_t* data, size_t size) {
                    size_t total_written = 0;

                    #ifdef _WIN32
                    while (total_written < size) {
                        DWORD written = 0;
                        const auto remaining = static_cast<DWORD>(size - total_written);
                        if (!::WriteFile(handle_, data + total_written, remaining, &written, nullptr)) { throw std::runtime_error("WAL write failed"); }
                        if (written == 0) { throw std::runtime_error("WAL write returned 0 bytes"); }
                        total_written += written;
                    }
                    #else
                    while (total_written < size) {
                        const ssize_t written = ::write(handle_, data + total_written, size - total_written);
                        if (written < 0) {
                            if (errno == EINTR) continue;
                            throw std::runtime_error("WAL write failed: " + std::string(strerror(errno)));
                        }
                        if (written == 0) { throw std::runtime_error("WAL write returned 0 bytes"); }
                        total_written += static_cast<size_t>(written);
                    }
                    #endif

                    if (total_written != size) {
                        throw std::runtime_error("WAL incomplete write: expected " + std::to_string(size) + " bytes, wrote " + std::to_string(total_written));
                    }
                }

                void fsync_data() {
                    #ifdef _WIN32
                    if (!::FlushFileBuffers(handle_)) { throw std::runtime_error("WAL fsync failed"); }
                    #elif defined(__APPLE__)
                    if (::fcntl(handle_, F_FULLFSYNC) < 0) { throw std::runtime_error("WAL fsync failed"); }
                    #else
                    if (::fdatasync(handle_) < 0) { throw std::runtime_error("WAL fsync failed"); }
                    #endif
                }

                void fsync_full() {
                    #ifdef _WIN32
                    if (!::FlushFileBuffers(handle_)) { throw std::runtime_error("WAL fsync failed"); }
                    #elif defined(__APPLE__)
                    if (::fcntl(handle_, F_FULLFSYNC) < 0) { throw std::runtime_error("WAL fsync failed"); }
                    #else
                    if (::fsync(handle_) < 0) { throw std::runtime_error("WAL fsync failed"); }
                    #endif
                }

                void truncate_file() {
                    #ifdef _WIN32
                    ::SetFilePointer(handle_, 0, nullptr, FILE_BEGIN);
                    if (!::SetEndOfFile(handle_)) { throw std::runtime_error("WAL truncate failed"); }
                    #else
                    if (::ftruncate(handle_, 0) < 0) { throw std::runtime_error("WAL truncate failed"); }
                    #endif
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

            private:
                NativeHandle handle_;
        };

        /**
         * Waiter for durable mode synchronization.
         */
        class Waiter {
            public:
                Waiter() : done_{false}, error_{nullptr} {}

                void wait(std::chrono::microseconds timeout) {
                    const auto deadline = std::chrono::steady_clock::now() + timeout;
                    while (!done_.load(std::memory_order_acquire)) {
                        if (std::chrono::steady_clock::now() >= deadline) { break; }
                        std::this_thread::sleep_for(std::chrono::microseconds(10));
                    }
                }

                void signal() { done_.store(true, std::memory_order_release); }

                void signal_error(std::exception_ptr e) {
                    error_ = e;
                    done_.store(true, std::memory_order_release);
                }

                [[nodiscard]] bool is_done() const noexcept { return done_.load(std::memory_order_acquire); }
                void check_error() const { if (error_) { std::rethrow_exception(error_); } }

            private:
                std::atomic<bool> done_;
                std::exception_ptr error_;
        };

        struct Command {
            enum class Type { WRITE, FORCE_SYNC, TRUNCATE, SHUTDOWN };

            Type type;
            core::OwnedBuffer frame;
            std::shared_ptr<Waiter> waiter;

            Command(Command&&) noexcept = default;
            Command& operator=(Command&&) noexcept = default;
            Command(const Command&) = delete;
            Command& operator=(const Command&) = delete;

            static Command write(core::OwnedBuffer frame, std::shared_ptr<Waiter> waiter) {
                Command cmd;
                cmd.type = Type::WRITE;
                cmd.frame = std::move(frame);
                cmd.waiter = std::move(waiter);
                return cmd;
            }

            static Command force_sync(std::shared_ptr<Waiter> waiter) {
                Command cmd;
                cmd.type = Type::FORCE_SYNC;
                cmd.waiter = std::move(waiter);
                return cmd;
            }

            static Command truncate(std::shared_ptr<Waiter> waiter) {
                Command cmd;
                cmd.type = Type::TRUNCATE;
                cmd.waiter = std::move(waiter);
                return cmd;
            }

            static Command shutdown() {
                Command cmd;
                cmd.type = Type::SHUTDOWN;
                return cmd;
            }

            private:
                Command() = default;
        };
    } // anonymous namespace

    class WalWriter::Impl {
        public:
            Impl(const std::filesystem::path& wal_file, size_t group_n, size_t group_micros, bool fast_mode)
                : file_handle_{FileHandle::open(wal_file)},
                  group_n_{group_n},
                  group_micros_{group_micros},
                  fast_mode_{fast_mode},
                  next_lsn_{1},
                  running_{true},
                  queue_size_{0} {
                flusher_thread_ = std::thread([this] { this->flush_loop(); });
            }

            ~Impl() {}

            uint64_t append(const WalOp& op) {
                if (!running_.load(std::memory_order_acquire)) { throw std::runtime_error("WAL is closed"); }

                const uint64_t lsn = next_lsn_.fetch_add(1, std::memory_order_relaxed);

                // Encode into thread-local buffer to avoid malloc contention across threads.
                auto frame_buf = WalFraming::encode_tls(op);

                std::shared_ptr<Waiter> waiter;
                if (!fast_mode_) { waiter = std::make_shared<Waiter>(); }

                // Enqueue and notify under mutex.
                // notify_one() is called inside the lock so the flusher cannot miss
                // the wakeup between checking queue_.empty() and sleeping.
                {
                    std::lock_guard lock{queue_mutex_};
                    const size_t prev_size = queue_size_.fetch_add(1, std::memory_order_relaxed);
                    queue_.emplace_back(Command::write(std::move(frame_buf), waiter));
                    // Notify only on 0 → 1 transition to avoid thundering-herd wakeups.
                    if (prev_size == 0) { queue_cv_.notify_one(); }
                }

                if (!fast_mode_) {
                    waiter->wait(std::chrono::microseconds(group_micros_ * 10));
                    if (!waiter->is_done()) { throw std::runtime_error("WAL fsync timeout"); }
                    waiter->check_error();
                }

                return lsn;
            }

            void force_sync() {
                if (!running_.load(std::memory_order_acquire)) { return; }

                auto waiter = std::make_shared<Waiter>();
                {
                    std::lock_guard lock{queue_mutex_};
                    queue_.emplace_back(Command::force_sync(waiter));
                    queue_size_.fetch_add(1, std::memory_order_relaxed);
                }
                queue_cv_.notify_one();

                waiter->wait(std::chrono::seconds(300));
                if (!waiter->is_done()) { throw std::runtime_error("WAL forceSync timeout"); }
                waiter->check_error();
            }

            void truncate() {
                if (!running_.load(std::memory_order_acquire)) { return; }

                auto waiter = std::make_shared<Waiter>();
                {
                    std::lock_guard lock{queue_mutex_};
                    queue_.emplace_back(Command::truncate(waiter));
                    queue_size_.fetch_add(1, std::memory_order_relaxed);
                }
                queue_cv_.notify_one();

                waiter->wait(std::chrono::seconds(300));
                if (!waiter->is_done()) { throw std::runtime_error("WAL truncate timeout"); }
                waiter->check_error();
            }

            void close() {
                if (!running_.exchange(false, std::memory_order_acq_rel)) { return; }

                {
                    std::lock_guard lock{queue_mutex_};
                    queue_.emplace_back(Command::shutdown());
                    queue_size_.fetch_add(1, std::memory_order_relaxed);
                }
                queue_cv_.notify_all();

                if (flusher_thread_.joinable()) { flusher_thread_.join(); }
                file_handle_.close();
            }

            [[nodiscard]] uint64_t next_lsn() const noexcept { return next_lsn_.load(std::memory_order_relaxed); }

        private:
            void flush_loop() {
                std::vector<Command> write_batch;
                write_batch.reserve(group_n_);
                // Local swap buffer: drain queue_ into here under a brief lock,
                // then process entirely without holding the mutex.
                std::deque<Command> local;

                try {
                    while (true) {
                        // Wait for work.
                        {
                            std::unique_lock lock{queue_mutex_};
                            const auto timeout = std::chrono::microseconds(group_micros_);
                            queue_cv_.wait_for(lock, timeout, [this]() {
                                return !queue_.empty() || !running_.load(std::memory_order_acquire);
                            });

                            if (!running_.load(std::memory_order_acquire) && queue_.empty()) { break; }
                            if (queue_.empty()) { continue; }

                            // Swap entire queue out in O(1) — producers unblocked immediately.
                            std::swap(queue_, local);
                            queue_size_.store(0, std::memory_order_relaxed);
                        } // mutex released here

                        // Process local batch without holding queue_mutex_.
                        bool shutdown = false;
                        while (!local.empty() && !shutdown) {
                            Command& front = local.front();

                            if (front.type == Command::Type::WRITE) {
                                write_batch.push_back(std::move(front));
                                local.pop_front();

                                // Accumulate consecutive WRITEs up to group_n_.
                                while (!local.empty()
                                       && local.front().type == Command::Type::WRITE
                                       && write_batch.size() < group_n_) {
                                    write_batch.push_back(std::move(local.front()));
                                    local.pop_front();
                                }

                                flush_write_batch(write_batch);
                                write_batch.clear();
                            } else {
                                local.pop_front();
                                shutdown = handle_command(front);
                            }
                        }

                        if (shutdown) { break; }
                    }
                }
                catch (...) {
                    for (auto& c : write_batch) { if (c.waiter) { c.waiter->signal_error(std::current_exception()); } }
                    write_batch.clear();
                    for (auto& c : local)       { if (c.waiter) { c.waiter->signal_error(std::current_exception()); } }
                    local.clear();
                    drain_queue_on_error();
                }

                // Final fsync on clean shutdown.
                try { file_handle_.fsync_data(); }
                catch (...) {}
            }

            void flush_write_batch(std::vector<Command>& batch) {
                try {
                    for (auto& cmd : batch) {
                        auto view = cmd.frame.view();
                        file_handle_.write(reinterpret_cast<const uint8_t*>(view.data()), view.size());
                    }
                    file_handle_.fsync_data();
                    for (auto& cmd : batch) { if (cmd.waiter) { cmd.waiter->signal(); } }
                }
                catch (...) {
                    auto ep = std::current_exception();
                    for (auto& cmd : batch) { if (cmd.waiter) { cmd.waiter->signal_error(ep); } }
                    throw;
                }
            }

            bool handle_command(Command& cmd) {
                try {
                    switch (cmd.type) {
                        case Command::Type::FORCE_SYNC:
                            file_handle_.fsync_data();
                            if (cmd.waiter) { cmd.waiter->signal(); }
                            return false;

                        case Command::Type::TRUNCATE:
                            file_handle_.truncate_file();
                            file_handle_.fsync_full();
                            if (cmd.waiter) { cmd.waiter->signal(); }
                            return false;

                        case Command::Type::SHUTDOWN:
                            return true;

                        default:
                            return false;
                    }
                }
                catch (...) {
                    if (cmd.waiter) { cmd.waiter->signal_error(std::current_exception()); }
                    throw;
                }
            }

            void drain_queue_on_error() {
                std::lock_guard lock{queue_mutex_};
                auto ep = std::make_exception_ptr(std::runtime_error("WAL flusher thread terminated"));
                while (!queue_.empty()) {
                    Command cmd = std::move(queue_.front());
                    queue_.pop_front();
                    if (cmd.waiter) { cmd.waiter->signal_error(ep); }
                }
                queue_size_.store(0, std::memory_order_relaxed);
            }

            FileHandle file_handle_;
            size_t group_n_;
            size_t group_micros_;
            bool fast_mode_;

            std::atomic<uint64_t> next_lsn_;
            std::atomic<bool> running_;
            std::atomic<size_t> queue_size_;

            std::mutex queue_mutex_;
            std::condition_variable queue_cv_;
            std::deque<Command> queue_;

            std::thread flusher_thread_;
    };

    // ==================== WalWriter Public API ====================

    std::unique_ptr<WalWriter> WalWriter::create(const std::filesystem::path& wal_file, size_t group_n, size_t group_micros, bool fast_mode) {
        return std::unique_ptr<WalWriter>(new WalWriter(wal_file, group_n, group_micros, fast_mode));
    }

    WalWriter::WalWriter(const std::filesystem::path& wal_file, size_t group_n, size_t group_micros, bool fast_mode)
        : impl_{std::make_unique<Impl>(wal_file, group_n, group_micros, fast_mode)} {}

    WalWriter::~WalWriter() = default;

    uint64_t WalWriter::append(const WalOp& op) { return impl_->append(op); }
    void WalWriter::force_sync() { impl_->force_sync(); }
    void WalWriter::truncate() { impl_->truncate(); }
    void WalWriter::close() { impl_->close(); }
    uint64_t WalWriter::next_lsn() const noexcept { return impl_->next_lsn(); }

} // namespace akkaradb::engine::wal