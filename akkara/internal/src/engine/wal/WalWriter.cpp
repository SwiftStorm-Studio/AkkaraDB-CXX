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
#include "core/buffer/OwnedBuffer.hpp"  // ← 追加
#include <stdexcept>
#include <chrono>
#include <deque>

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
            using NativeHandle = HANDLE;
            inline static const NativeHandle INVALID = INVALID_HANDLE_VALUE;
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
                    // Allow concurrent reads
                    nullptr,
                    OPEN_ALWAYS,
                    FILE_ATTRIBUTE_NORMAL,
                    nullptr
                );

                if (fh.handle_ == INVALID) { throw std::runtime_error("Failed to open WAL: " + path.string()); }

                ::SetFilePointer(fh.handle_, 0, nullptr, FILE_END);
#else
                fh.handle_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);

                if (fh.handle_ < 0) { throw std::runtime_error("Failed to open WAL: " + path.string()); }
#endif

                return fh;
            }

            // Handles partial writes and EINTR
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
                        if (errno == EINTR) continue; // Interrupted, retry
                        throw std::runtime_error("WAL write failed: " + std::string(strerror(errno)));
                    }

                    if (written == 0) { throw std::runtime_error("WAL write returned 0 bytes"); }

                    total_written += static_cast<size_t>(written);
                }
#endif

                // Verify complete write
                if (total_written != size) {
                    throw std::runtime_error(
                        "WAL incomplete write: expected " + std::to_string(size) +
                        " bytes, wrote " + std::to_string(total_written)
                    );
                }
            }

            void fsync_data() {
#ifdef _WIN32
                if (!::FlushFileBuffers(handle_)) { throw std::runtime_error("WAL fsync failed"); }
#elif defined(__APPLE__)
                if (::fcntl(handle_, F_FULLFSYNC) < 0) { throw std::runtime_error("WAL fsync failed"); }
#else
                if (::fdatasync(handle_) < 0) {
                    throw std::runtime_error("WAL fsync failed");
                }
#endif
            }

            void fsync_full() {
#ifdef _WIN32
                if (!::FlushFileBuffers(handle_)) { throw std::runtime_error("WAL fsync failed"); }
#elif defined(__APPLE__)
                if (::fcntl(handle_, F_FULLFSYNC) < 0) {
                    throw std::runtime_error("WAL fsync failed");
                }
#else
                if (::fsync(handle_) < 0) { throw std::runtime_error("WAL fsync failed"); }
#endif
            }

            void truncate_file() {
#ifdef _WIN32
                ::SetFilePointer(handle_, 0, nullptr, FILE_BEGIN);
                if (!::SetEndOfFile(handle_)) { throw std::runtime_error("WAL truncate failed"); }
#else
                if (::ftruncate(handle_, 0) < 0) {
                    throw std::runtime_error("WAL truncate failed");
                }
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
 * Uses busy-wait (similar to JVM's LockSupport) for lower latency.
 */
        class Waiter {
        public:
            Waiter() : done_{false}, error_{nullptr} {}

            void wait(std::chrono::microseconds timeout) {
                const auto deadline = std::chrono::steady_clock::now() + timeout;

                while (!done_.load(std::memory_order_acquire)) {
                    if (std::chrono::steady_clock::now() >= deadline) {
                        break;
                    }
                    // Busy-wait with 10µs sleep (matches JVM's LockSupport.parkNanos(10_000))
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
                }
            }

            void signal() { done_.store(true, std::memory_order_release); }

            void signal_error(std::exception_ptr e) {
                error_ = e;
                done_.store(true, std::memory_order_release);
            }

            [[nodiscard]] bool is_done() const noexcept {
                return done_.load(std::memory_order_acquire);
            }

            void check_error() const { if (error_) { std::rethrow_exception(error_); } }

        private:
            std::atomic<bool> done_;
            std::exception_ptr error_;
        };

        /**
 * Command for queue-based coordination.
 *
 * OPTIMIZATION: Store OwnedBuffer directly instead of copying to vector.
 */
        struct Command {
            enum class Type { WRITE, FORCE_SYNC, TRUNCATE, SHUTDOWN };

            Type type;
            core::OwnedBuffer frame; // ← std::vector<uint8_t> から変更
            std::shared_ptr<Waiter> waiter;

            // Move-only (OwnedBuffer is move-only)
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
                Command() = default; // Private default constructor
        };
    } // anonymous namespace

    /**
 * WalWriter::Impl - Private implementation.
 */
    class WalWriter::Impl {
        public:
            Impl(
                const std::filesystem::path& wal_file,
                size_t group_n,
                size_t group_micros,
                bool fast_mode
            )
                : file_handle_{FileHandle::open(wal_file)},
                  group_n_{group_n},
                  group_micros_{group_micros},
                  fast_mode_{fast_mode}
                , next_lsn_{1}
                , running_{true} {
            // Start flusher thread
            flusher_thread_ = std::thread([this] { this->flush_loop(); });
        }

        ~Impl() {
            // Destructor should not call close() to avoid exceptions
            // close() must be called explicitly
        }

        uint64_t append(const WalOp& op) {
            if (!running_.load(std::memory_order_acquire)) {
                throw std::runtime_error("WAL is closed");
            }

            // 1. Assign LSN
            const uint64_t lsn = next_lsn_.fetch_add(1, std::memory_order_relaxed);

            // 2. Encode frame (returns OwnedBuffer)
            auto frame_buf = WalFraming::encode(op);

            // ★ OPTIMIZATION: OwnedBufferを直接使う（vectorへのコピーなし）

            // 3. Create waiter
            auto waiter = std::make_shared<Waiter>();

            // 4. Enqueue WITHOUT backpressure (unlimited queue)
            {
                std::lock_guard lock{queue_mutex_};
                queue_.emplace_back(Command::write(std::move(frame_buf), waiter));
            }
            queue_cv_.notify_one();

            // 5. Wait for durability (durable mode only)
            if (!fast_mode_) {
                waiter->wait(std::chrono::microseconds(group_micros_ * 10));
                if (!waiter->is_done()) {
                    throw std::runtime_error("WAL fsync timeout");
                }
                waiter->check_error(); // Throw if fsync failed
            }

            return lsn;
            }

            void force_sync() {
                if (!running_.load(std::memory_order_acquire)) {
                return;
            }

                auto waiter = std::make_shared<Waiter>();

                {
                    std::lock_guard lock{queue_mutex_};
                    queue_.emplace_back(Command::force_sync(waiter));
                }
                queue_cv_.notify_one();

                waiter->wait(std::chrono::seconds(300));
                if (!waiter->is_done()) {
                    throw std::runtime_error("WAL forceSync timeout");
            }
            waiter->check_error();
        }

            void truncate() {
                if (!running_.load(std::memory_order_acquire)) {
                return;
                }

                auto waiter = std::make_shared<Waiter>();

                {
                    std::lock_guard lock{queue_mutex_};
                    queue_.emplace_back(Command::truncate(waiter));
                }
                queue_cv_.notify_one();

                waiter->wait(std::chrono::seconds(300));
                if (!waiter->is_done()) {
                throw std::runtime_error("WAL truncate timeout");
            }
                waiter->check_error();
            }

            void close() {
                if (!running_.exchange(false, std::memory_order_acq_rel)) {
                    return; // Already closed
                }

                // Submit shutdown command
                {
                    std::lock_guard lock{queue_mutex_};
                    queue_.emplace_back(Command::shutdown());
                }
                queue_cv_.notify_all();

                // Wait for flusher thread to finish
            if (flusher_thread_.joinable()) {
                flusher_thread_.join();
                }

                // Close file handle after thread terminates
                file_handle_.close();
            }

            [[nodiscard]] uint64_t next_lsn() const noexcept { return next_lsn_.load(std::memory_order_relaxed); }

        private:
            void flush_loop() {
                std::vector<Command> write_batch;
                write_batch.reserve(group_n_);

                try {
                    while (running_.load(std::memory_order_acquire) || !is_queue_empty()) {
                    std::unique_lock lock{queue_mutex_};

                    // 1. Poll first command (with timeout)
                    const auto timeout = std::chrono::microseconds(group_micros_);
                    queue_cv_.wait_for(lock, timeout, [this]() {
                        return !queue_.empty() || !running_.load(std::memory_order_acquire);
                    });

                    if (!running_.load(std::memory_order_acquire) && queue_.empty()) { break; }

                    if (queue_.empty()) {
                        continue; // Timeout, retry
                    }

                    Command cmd = std::move(queue_.front());
                    queue_.pop_front();

                    lock.unlock();

                    // 2. Process command
                    if (cmd.type == Command::Type::WRITE) {
                        write_batch.push_back(std::move(cmd));

                        // 3. Drain additional Write commands (non-blocking)
                        while (write_batch.size() < group_n_) {
                            std::unique_lock drain_lock{queue_mutex_};
                            if (queue_.empty()) {
                                break;
                            }

                            Command next = std::move(queue_.front());
                            queue_.pop_front();

                            drain_lock.unlock();

                            if (next.type == Command::Type::WRITE) {
                                write_batch.push_back(std::move(next));
                            }
                            else {
                                // Non-Write command: flush batch FIRST
                                if (!write_batch.empty()) {
                                    flush_write_batch(write_batch);
                                    write_batch.clear();
                                }

                                // Then handle the command
                                if (handle_command(next)) {
                                    return; // SHUTDOWN received
                                }
                                break; // Exit drain loop
                            }
                        }

                        // 4. Flush write batch
                        if (!write_batch.empty()) {
                            flush_write_batch(write_batch);
                            write_batch.clear();
                        }
                    }
                    else {
                        // 5. Non-Write command at start: handle immediately
                        if (handle_command(cmd)) {
                            return; // SHUTDOWN received
                        }
                    }
                }
            }
            catch (...) {
                // Notify all pending waiters of the error
                for (auto& cmd : write_batch) {
                    if (cmd.waiter) {
                        cmd.waiter->signal_error(std::current_exception());
                    }
                }
                write_batch.clear();

                // Drain queue and notify all waiters
                drain_queue_on_error();
                }

                // Final fsync (best effort)
                try { file_handle_.fsync_data(); }
                catch (...) {
                    // Suppress exception during shutdown
                }
            }

            void flush_write_batch(std::vector<Command>& batch) {
            std::exception_ptr batch_error = nullptr;

            try {
                // Write all frames sequentially
                for (auto& cmd : batch) {
                    auto view = cmd.frame.view();
                    file_handle_.write(
                        reinterpret_cast<const uint8_t*>(view.data()),
                        view.size()
                    );
                }

                // Single fsync for entire batch (use force(false) - fdatasync)
                file_handle_.fsync_data();

                // Notify all waiters of success
                for (auto& cmd : batch) {
                    if (cmd.waiter) { cmd.waiter->signal(); }
                }
            }
            catch (...) {
                batch_error = std::current_exception();

                // Notify all waiters of failure
                for (auto& cmd : batch) { if (cmd.waiter) { cmd.waiter->signal_error(batch_error); } }

                throw; // Re-throw to crash flusher thread
            }
            }

            bool handle_command(Command& cmd) {
                std::exception_ptr error = nullptr;

                try {
                    switch (cmd.type) {
                        case Command::Type::FORCE_SYNC:
                            file_handle_.fsync_data();
                            if (cmd.waiter) cmd.waiter->signal();
                    return false;

                case Command::Type::TRUNCATE:
                            file_handle_.truncate_file();
                            file_handle_.fsync_full(); // Use force(true) for metadata
                            if (cmd.waiter) cmd.waiter->signal();
                            return false;

                        case Command::Type::SHUTDOWN:
                            return true; // Signal shutdown

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

                while (!queue_.empty()) {
                    Command cmd = std::move(queue_.front());
                    queue_.pop_front();

                    if (cmd.waiter) {
                        cmd.waiter->signal_error(
                            std::make_exception_ptr(
                                std::runtime_error("WAL flusher thread terminated")
                            )
                        );
                    }
                }
            }

            bool is_queue_empty() {
                std::lock_guard lock{queue_mutex_};
                return queue_.empty();
            }

            FileHandle file_handle_;
            size_t group_n_;
            size_t group_micros_;
            bool fast_mode_;

            std::atomic<uint64_t> next_lsn_;
            std::atomic<bool> running_;

            std::mutex queue_mutex_;
            std::condition_variable queue_cv_;
            std::deque<Command> queue_;  // Unlimited size

        std::thread flusher_thread_;
    };

    // ==================== WalWriter Public API ====================

    std::unique_ptr<WalWriter> WalWriter::create(
        const std::filesystem::path& wal_file,
        size_t group_n,
        size_t group_micros,
        bool fast_mode
    ) {
        return std::unique_ptr<WalWriter>(new WalWriter(
            wal_file, group_n, group_micros, fast_mode
        ));
    }

    WalWriter::WalWriter(
        const std::filesystem::path& wal_file,
        size_t group_n,
        size_t group_micros,
        bool fast_mode
    ) : impl_{std::make_unique<Impl>(wal_file, group_n, group_micros, fast_mode)} {}

    WalWriter::~WalWriter() = default;

    uint64_t WalWriter::append(const WalOp& op) { return impl_->append(op); }

    void WalWriter::force_sync() { impl_->force_sync(); }

    void WalWriter::truncate() { impl_->truncate(); }

    void WalWriter::close() { impl_->close(); }

    uint64_t WalWriter::next_lsn() const noexcept { return impl_->next_lsn(); }
} // namespace akkaradb::engine::wal