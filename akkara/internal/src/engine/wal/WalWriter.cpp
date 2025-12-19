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
#include <stdexcept>
#include <chrono>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#endif

namespace akkaradb::engine::wal {
    namespace {
        /**
 * Platform-specific file handle for direct I/O with fsync.
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
                    0,
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

            void write(const uint8_t* data, size_t size) {
#ifdef _WIN32
                DWORD written = 0;
                if (!::WriteFile(handle_, data, static_cast<DWORD>(size), &written, nullptr)) { throw std::runtime_error("WAL write failed"); }
                if (written != size) { throw std::runtime_error("WAL incomplete write"); }
#else
                ssize_t result = ::write(handle_, data, size);
                if (result < 0 || static_cast<size_t>(result) != size) { throw std::runtime_error("WAL write failed"); }
#endif
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
            Waiter() : done_{false} {}

            void wait(std::chrono::microseconds timeout) {
                std::unique_lock lock{mutex_};
                cv_.wait_for(lock, timeout, [this]() { return done_.load(); });
            }

            void signal() {
                done_.store(true);
                cv_.notify_all();
            }

            [[nodiscard]] bool is_done() const noexcept { return done_.load(); }

        private:
            std::atomic<bool> done_;
            std::mutex mutex_;
            std::condition_variable cv_;
        };

        /**
 * Command for queue-based coordination.
 */

        struct Command {
            enum class Type { WRITE, FORCE_SYNC, TRUNCATE, SHUTDOWN };

            Type type;
            std::vector<uint8_t> frame;
            std::shared_ptr<Waiter> waiter;

            static Command write(std::vector<uint8_t> frame, std::shared_ptr<Waiter> waiter) {
                return Command{Type::WRITE, std::move(frame), std::move(waiter)}; // lsn削除
            }

            static Command force_sync(std::shared_ptr<Waiter> waiter) { return Command{Type::FORCE_SYNC, {}, std::move(waiter)}; }

            static Command truncate(std::shared_ptr<Waiter> waiter) { return Command{Type::TRUNCATE, {}, std::move(waiter)}; }

            static Command shutdown() { return Command{Type::SHUTDOWN, {}, nullptr}; }
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
        ) : file_handle_{FileHandle::open(wal_file)}
            , group_n_{group_n}
            , group_micros_{group_micros}
            , fast_mode_{fast_mode}
            , next_lsn_{1}
            , running_{true} {
            // Start flusher thread
            flusher_thread_ = std::thread([this] { this->flush_loop(); });
        }

        ~Impl() { close(); }

        uint64_t append(const WalOp& op) {
            if (!running_.load()) { throw std::runtime_error("WAL is closed"); }

            // 1. Assign LSN
            const uint64_t lsn = next_lsn_.fetch_add(1);

            // 2. Encode frame (returns OwnedBuffer)
            auto frame_buf = WalFraming::encode(op);

            // 3. Convert to vector
            auto frame_view = frame_buf.view();
            std::vector<uint8_t> frame_vec(frame_view.data(), frame_view.data() + frame_view.size());

            // 4. Create waiter
            auto waiter = std::make_shared<Waiter>();

            // 5. Enqueue
            {
                std::lock_guard lock{queue_mutex_};
                queue_.emplace(Command::write(std::move(frame_vec), waiter));
            }
            queue_cv_.notify_one();

            // 6. Wait for durability (durable mode only)
            if (!fast_mode_) {
                waiter->wait(std::chrono::microseconds(group_micros_ * 10));
                if (!waiter->is_done()) { throw std::runtime_error("WAL fsync timeout"); }
            }

            return lsn;
        }

        void force_sync() {
            if (!running_.load()) { return; }

            auto waiter = std::make_shared<Waiter>();

            {
                std::lock_guard lock{queue_mutex_};
                queue_.emplace(Command::force_sync(waiter));
            }
            queue_cv_.notify_one();

            waiter->wait(std::chrono::seconds(5));
            if (!waiter->is_done()) { throw std::runtime_error("WAL forceSync timeout"); }
        }

        void truncate() {
            if (!running_.load()) { return; }

            auto waiter = std::make_shared<Waiter>();

            {
                std::lock_guard lock{queue_mutex_};
                queue_.emplace(Command::truncate(waiter));
            }
            queue_cv_.notify_one();

            waiter->wait(std::chrono::seconds(5));
            if (!waiter->is_done()) { throw std::runtime_error("WAL truncate timeout"); }
        }

        void close() {
            if (!running_.exchange(false)) {
                return; // Already closed
            }

            // Submit shutdown command
            {
                std::lock_guard lock{queue_mutex_};
                queue_.emplace(Command::shutdown());
            }
            queue_cv_.notify_one();

            // Wait for flusher
            if (flusher_thread_.joinable()) { flusher_thread_.join(); }
        }

        [[nodiscard]] uint64_t next_lsn() const noexcept { return next_lsn_.load(); }

    private:
        void flush_loop() {
            std::vector<Command> write_batch;
            write_batch.reserve(group_n_);

            try {
                while (true) {
                    std::unique_lock lock{queue_mutex_};

                    // 1. Poll first command (with timeout)
                    const auto timeout = std::chrono::microseconds(group_micros_);
                    queue_cv_.wait_for(lock, timeout, [this]() { return !queue_.empty() || !running_.load(); });

                    if (!running_.load() && queue_.empty()) { break; }

                    if (queue_.empty()) {
                        continue; // Timeout, retry
                    }

                    Command cmd = std::move(queue_.front());
                    queue_.pop();
                    lock.unlock();

                    // 2. Process command
                    if (cmd.type == Command::Type::WRITE) {
                        write_batch.push_back(std::move(cmd));

                        // 3. Drain additional Write commands (non-blocking)
                        while (write_batch.size() < group_n_) {
                            std::unique_lock drain_lock{queue_mutex_};
                            if (queue_.empty()) { break; }

                            Command next = std::move(queue_.front());
                            queue_.pop();
                            drain_lock.unlock();

                            if (next.type == Command::Type::WRITE) { write_batch.push_back(std::move(next)); }
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
                // Exception in flush loop
            }

            // Final fsync (best effort)
            try { file_handle_.fsync_data(); }
            catch (...) {}
        }

        void flush_write_batch(std::vector<Command>& batch) {
            // Write all frames sequentially
            for (auto& cmd : batch) { file_handle_.write(cmd.frame.data(), cmd.frame.size()); }

            // Single fsync for entire batch
            file_handle_.fsync_data();

            // Notify all waiters
            for (auto& cmd : batch) { if (cmd.waiter) { cmd.waiter->signal(); } }
        }

        bool handle_command(Command& cmd) {
            switch (cmd.type) {
            case Command::Type::FORCE_SYNC:
                file_handle_.fsync_data();
                if (cmd.waiter) cmd.waiter->signal();
                return false;

            case Command::Type::TRUNCATE:
                file_handle_.truncate_file();
                file_handle_.fsync_full();
                if (cmd.waiter) cmd.waiter->signal();
                return false;

            case Command::Type::SHUTDOWN:
                return true; // Signal shutdown

            default:
                return false;
            }
        }

        FileHandle file_handle_;
        size_t group_n_;
        size_t group_micros_;
        bool fast_mode_;

        std::atomic<uint64_t> next_lsn_;
        std::atomic<bool> running_;

        std::mutex queue_mutex_;
        std::condition_variable queue_cv_;
        std::queue<Command> queue_;

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
