/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/src/engine/wal/WalWriter.cpp
#include "engine/wal/WalWriter.hpp"
#include "engine/wal/WalOp.hpp"
#include "engine/wal/WalFraming.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include "core/buffer/PerThreadArena.hpp"

#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <format>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#  include <windows.h>   // YieldProcessor(), HANDLE, ...
#  include <intrin.h>    // _mm_pause()
#  define AKK_CPU_PAUSE() _mm_pause()
// C4324: structure padded due to alignas — intentional for cache-line isolation.
#  pragma warning(disable: 4324)
#else
#  include <cerrno>
#  include <cstring>
#  include <fcntl.h>
#  include <sys/stat.h>
#  include <unistd.h>
#  define AKK_CPU_PAUSE() __builtin_ia32_pause()
#endif

namespace akkaradb::wal {
    // ============================================================================
    // FileHandle - RAII platform-abstracted file I/O
    // ============================================================================

    class FileHandle {
        public:
            #ifdef _WIN32
            using NativeHandle = HANDLE; static const NativeHandle INVALID_H;
            #else
            using NativeHandle = int;
            static constexpr NativeHandle INVALID_H = -1;
            #endif

            FileHandle() noexcept : handle_{INVALID_H} {}
            ~FileHandle() noexcept { close(); }

            FileHandle(const FileHandle&) = delete;
            FileHandle& operator=(const FileHandle&) = delete;

            FileHandle(FileHandle&& o) noexcept : handle_{o.handle_} { o.handle_ = INVALID_H; }

            FileHandle& operator=(FileHandle&& o) noexcept {
                if (this != &o) {
                    close();
                    handle_ = o.handle_;
                    o.handle_ = INVALID_H;
                }
                return *this;
            }

            [[nodiscard]] static FileHandle open(const std::filesystem::path& path) {
                FileHandle fh;
                #ifdef _WIN32
                fh.handle_ = ::CreateFileW(path.c_str(), GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
                if (fh.handle_ == INVALID_H) { throw std::runtime_error("FileHandle::open failed: " + path.string()); } ::SetFilePointer(
                    fh.handle_,
                    0,
                    nullptr,
                    FILE_END
                );
                #else
                fh.handle_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
                if (fh.handle_ < 0) { throw std::runtime_error("FileHandle::open failed: " + path.string() + ": " + std::strerror(errno)); }
                #endif
                return fh;
            }

            void write(const void* data, size_t size) {
                if (size == 0) return;
                size_t written = 0;
                const auto* ptr = static_cast<const uint8_t*>(data);
                #ifdef _WIN32
                while (written < size) {
                    DWORD w = 0;
                    const auto rem = static_cast<DWORD>(size - written);
                    if (!::WriteFile(handle_, ptr + written, rem, &w, nullptr)) { throw std::runtime_error("FileHandle::write failed"); }
                    if (w == 0) throw std::runtime_error("FileHandle::write returned 0");
                    written += w;
                }
                #else
                while (written < size) {
                    const ssize_t w = ::write(handle_, ptr + written, size - written);
                    if (w < 0) {
                        if (errno == EINTR) continue;
                        throw std::runtime_error(std::string("FileHandle::write failed: ") + std::strerror(errno));
                    }
                    if (w == 0) throw std::runtime_error("FileHandle::write returned 0");
                    written += static_cast<size_t>(w);
                }
                #endif
            }

            void fdatasync() {
                #ifdef _WIN32
                if (!::FlushFileBuffers(handle_)) { throw std::runtime_error("FileHandle::fdatasync failed"); }
                #elif defined(__APPLE__)
                if (::fcntl(handle_, F_FULLFSYNC) < 0) { throw std::runtime_error("FileHandle::fdatasync failed"); }
                #else
                if (::fdatasync(handle_) < 0) { throw std::runtime_error(std::string("FileHandle::fdatasync failed: ") + std::strerror(errno)); }
                #endif
            }

            void fsync_full() {
                #ifdef _WIN32
                if (!::FlushFileBuffers(handle_)) { throw std::runtime_error("FileHandle::fsync_full failed"); }
                #elif defined(__APPLE__)
                if (::fcntl(handle_, F_FULLFSYNC) < 0) { throw std::runtime_error("FileHandle::fsync_full failed"); }
                #else
                if (::fsync(handle_) < 0) { throw std::runtime_error(std::string("FileHandle::fsync_full failed: ") + std::strerror(errno)); }
                #endif
            }

            /**
             * Returns current file size (bytes), or 0 if unavailable.
             * Used to detect whether SegmentHeader needs to be written.
             */
            [[nodiscard]] uint64_t file_size() const noexcept {
                #ifdef _WIN32
                LARGE_INTEGER sz{}; if (!::GetFileSizeEx(handle_, &sz)) return 0; return static_cast<uint64_t>(sz.QuadPart);
                #else
                // fstat is correct for O_APPEND fds; lseek return value can be
                // ignored by the kernel on append-mode files in some Linux versions.
                struct stat st{};
                if (::fstat(handle_, &st) < 0) return 0;
                return static_cast<uint64_t>(st.st_size);
                #endif
            }

            void truncate() {
                #ifdef _WIN32
                ::SetFilePointer(handle_, 0, nullptr, FILE_BEGIN); if (!::SetEndOfFile(handle_)) { throw std::runtime_error("FileHandle::truncate failed"); }
                ::SetFilePointer(handle_, 0, nullptr, FILE_END);
                #else
                if (::ftruncate(handle_, 0) < 0) { throw std::runtime_error(std::string("FileHandle::truncate failed: ") + std::strerror(errno)); }
                #endif
            }

            void close() noexcept {
                if (handle_ != INVALID_H) {
                    #ifdef _WIN32
                    ::CloseHandle(handle_);
                    #else
                    ::close(handle_);
                    #endif
                    handle_ = INVALID_H;
                }
            }

            [[nodiscard]] bool is_open() const noexcept { return handle_ != INVALID_H; }

        private:
            NativeHandle handle_;
    };

    #ifdef _WIN32
    const FileHandle::NativeHandle FileHandle::INVALID_H = INVALID_HANDLE_VALUE;
    #endif

    // ============================================================================
    // Waiter - lightweight fsync completion notification
    // ============================================================================

    class Waiter {
        public:
            Waiter() noexcept : done_{false}, error_{nullptr} {}

            void signal() noexcept {
                {
                    std::lock_guard lock{mu_};
                    done_.store(true, std::memory_order_relaxed);
                }
                cv_.notify_one();
            }

            void signal_error(const std::exception_ptr& ep) noexcept {
                {
                    std::lock_guard lock{mu_};
                    error_ = ep;
                    done_.store(true, std::memory_order_relaxed);
                }
                cv_.notify_one();
            }

            void wait() {
                std::unique_lock lock{mu_};
                cv_.wait(lock, [this] { return done_.load(std::memory_order_relaxed); });
                if (error_) std::rethrow_exception(error_);
            }

        private:
            std::atomic<bool> done_;
            std::exception_ptr error_;
            std::mutex mu_;
            std::condition_variable cv_;
    };

    // ============================================================================
    // SpinLock  (Plan 1)
    //
    // Replaces std::mutex for the enqueue critical section (body ≤ 15 ns).
    // Cache-line aligned to prevent false sharing with adjacent ShardWriter fields.
    //
    // Design:
    //   lock()   – optimistic test_and_set; spins with AKK_CPU_PAUSE() on contention.
    //   unlock() – atomic clear (release).
    // ============================================================================

    struct alignas(64) SpinLock {
        std::atomic_flag flag_ = ATOMIC_FLAG_INIT;

        void lock() noexcept {
            for (;;) {
                if (!flag_.test_and_set(std::memory_order_acquire)) return;
                // Back-off: yields hyper-thread slot, reduces cache-line bouncing.
                while (flag_.test(std::memory_order_relaxed))
                    AKK_CPU_PAUSE();
            }
        }

        void unlock() noexcept { flag_.clear(std::memory_order_release); }

        struct Guard {
            SpinLock& sl_;
            explicit Guard(SpinLock& s) noexcept : sl_{s} { sl_.lock(); }
            ~Guard() noexcept { sl_.unlock(); }
        };
    };

    // ============================================================================
    // Command - queue element (sync mode writes + control commands)
    // ============================================================================

    struct Command {
        enum class Type {
            WRITE, FORCE_SYNC, TRUNCATE, SHUTDOWN
        };

        Type type{Type::WRITE};
        size_t entry_offset{0}; ///< Byte offset into the processing arena (WRITE only)
        size_t entry_size{0}; ///< Byte count of the entry in the arena (WRITE only)
        std::shared_ptr<Waiter> waiter; ///< null = fast_mode (no notification needed)

        Command() = default;
        Command(Command&&) noexcept = default;
        Command& operator=(Command&&) noexcept = default;
        Command(const Command&) = delete;
        Command& operator=(const Command&) = delete;

        static Command write(size_t offset, size_t size, std::shared_ptr<Waiter> w) {
            Command c;
            c.type = Type::WRITE;
            c.entry_offset = offset;
            c.entry_size = size;
            c.waiter = std::move(w);
            return c;
        }

        static Command force_sync(std::shared_ptr<Waiter> w) {
            Command c;
            c.type = Type::FORCE_SYNC;
            c.waiter = std::move(w);
            return c;
        }

        static Command truncate(std::shared_ptr<Waiter> w) {
            Command c;
            c.type = Type::TRUNCATE;
            c.waiter = std::move(w);
            return c;
        }

        static Command shutdown() {
            Command c;
            c.type = Type::SHUTDOWN;
            return c;
        }
    };

    // ============================================================================
    // FastEntry  (Plan 2 - MPSC)
    //
    // Lightweight descriptor for fast-mode (no-waiter) write entries.
    //   8 bytes vs Command's ~40 bytes (shared_ptr + type + padding).
    //   No heap allocation: no shared_ptr<Waiter> construction/destruction.
    //
    // Fast-mode writes push a FastEntry into fast_queue_ instead of a Command
    // into queue_. The flusher drains fast_queue_ as a single batch before
    // processing sync-mode Commands.
    // ============================================================================

    struct FastEntry {
        uint32_t offset; ///< Byte offset into the processing arena
        uint32_t size; ///< Byte count of the entry
    };

    // ============================================================================
    // ShardWriter - one flusher thread per shard
    // ============================================================================

    static constexpr size_t BATCH_ARENA_BLOCK_SIZE = 256 * 1024; // 256 KB

    class ShardWriter {
        public:
            ShardWriter(
                uint32_t shard_id,
                std::filesystem::path wal_dir,
                size_t group_n,
                size_t group_micros,
                uint64_t max_segment_bytes,
                bool nosync_mode,
                std::atomic<uint64_t>& global_batch_seq
            )
                : shard_id_{shard_id},
                  wal_dir_{std::move(wal_dir)},
                  group_n_{group_n},
                  group_micros_{group_micros},
                  max_segment_bytes_{max_segment_bytes},
                  nosync_mode_{nosync_mode},
                  global_batch_seq_{global_batch_seq},
                  stopped_{false},
                  needs_segment_header_{false},
                  current_segment_bytes_{0} {
                file_ = FileHandle::open(segment_path(shard_id_, segment_id_));

                // If the file was newly created (size == 0), write the segment header now.
                // If it already exists (crash recovery scenario), skip - WalReader handles it.
                if (file_.file_size() == 0) {
                    write_segment_header();
                    current_segment_bytes_ = WalSegmentHeader::SIZE;
                }
                else { current_segment_bytes_ = file_.file_size(); }

                batch_arena_ = core::PerThreadArena::create(BATCH_ARENA_BLOCK_SIZE, 4096);
                // Pre-reserve entry arenas to avoid first-batch reallocation.
                entry_buf_[0].reserve(group_n_ * 128);
                entry_buf_[1].reserve(group_n_ * 128);
                // Pre-reserve fast_queue_ for the typical batch size (Plan 2).
                fast_queue_.reserve(group_n_);
                flusher_ = std::thread([this] { flusher_loop(); });
            }

            ~ShardWriter() { stop(); }

            ShardWriter(const ShardWriter&) = delete;
            ShardWriter& operator=(const ShardWriter&) = delete;

            // ── enqueue  ─────────────────────────────────────────────────────────
            //
            // Plan 1: spin_ (SpinLock) replaces queue_mutex_ (std::mutex).
            //   Critical section: ~15 ns (memcpy + push_back).  Spinlock overhead:
            //   ~2–5 ns vs ~50–80 ns for an uncontended std::mutex.
            //
            // Plan 2: fast_mode (waiter == null) pushes a FastEntry (8 B, no heap
            //   alloc) instead of a Command (~40 B with shared_ptr).  The flusher
            //   drains fast_queue_ as a dedicated batch before processing Commands.
            // ─────────────────────────────────────────────────────────────────────
            void enqueue(const void* data, size_t size, std::shared_ptr<Waiter> waiter) {
                bool was_empty;
                {
                    SpinLock::Guard g{spin_};
                    was_empty = queue_.empty() && fast_queue_.empty();
                    auto& buf = entry_buf_[write_idx_];
                    const size_t offset = write_pos_[write_idx_];
                    if (buf.size() < offset + size) buf.resize((offset + size) * 2);
                    std::memcpy(buf.data() + offset, data, size);
                    write_pos_[write_idx_] += size;
                    if (waiter) {
                        // Sync mode: Command with waiter (offset/size for signaling).
                        queue_.push_back(Command::write(offset, size, std::move(waiter)));
                    }
                    else {
                        // Fast mode (Plan 2): lightweight FastEntry, no shared_ptr.
                        fast_queue_.push_back(FastEntry{static_cast<uint32_t>(offset), static_cast<uint32_t>(size)});
                    }
                }
                // Notify outside the lock: avoids the "notify-then-block" pattern.
                // Only notify when transitioning empty→non-empty; flusher is already
                // awake while work is queued.
                if (was_empty) {
                    has_pending_.store(true, std::memory_order_release);
                    sleep_cv_.notify_one();
                }
            }

            void enqueue_force_sync(std::shared_ptr<Waiter> waiter) {
                {
                    SpinLock::Guard g{spin_};
                    queue_.push_back(Command::force_sync(std::move(waiter)));
                }
                has_pending_.store(true, std::memory_order_release);
                sleep_cv_.notify_one();
            }

            void enqueue_truncate(std::shared_ptr<Waiter> waiter) {
                {
                    SpinLock::Guard g{spin_};
                    queue_.push_back(Command::truncate(std::move(waiter)));
                }
                has_pending_.store(true, std::memory_order_release);
                sleep_cv_.notify_one();
            }

            void stop() {
                if (stopped_.exchange(true, std::memory_order_acq_rel)) return;
                {
                    SpinLock::Guard g{spin_};
                    queue_.push_back(Command::shutdown());
                }
                has_pending_.store(true, std::memory_order_release);
                sleep_cv_.notify_all();
                if (flusher_.joinable()) flusher_.join();
                file_.close();
            }

        private:
            // ── Segment path helper ───────────────────────────────────────────────

            [[nodiscard]] std::filesystem::path segment_path(uint32_t shard_id, uint64_t seg_id) const {
                return wal_dir_ / std::format("shard_{:04d}_seg{:04d}.akwal", shard_id, seg_id);
            }

            // ── Segment header / rotation ─────────────────────────────────────────

            void write_segment_header() {
                uint8_t hdr_buf[WalSegmentHeader::SIZE];
                core::BufferView hdr_view{reinterpret_cast<std::byte*>(hdr_buf), WalSegmentHeader::SIZE};
                WalSegmentHeader::write(hdr_view, static_cast<uint16_t>(shard_id_), segment_id_);
                file_.write(hdr_buf, WalSegmentHeader::SIZE);
            }

            /**
             * Closes the current segment, increments segment_id_, opens the next
             * segment file, writes its header, and resets current_segment_bytes_.
             *
             * Called from the flusher thread only - no locking needed.
             */
            void rotate_segment() {
                // Ensure the current segment is fully durable before closing.
                file_.fdatasync();
                file_.close();

                ++segment_id_;
                file_ = FileHandle::open(segment_path(shard_id_, segment_id_));
                write_segment_header();
                current_segment_bytes_ = WalSegmentHeader::SIZE;
            }

            // ── Flusher loop ──────────────────────────────────────────────────────
            //
            // Separation of concerns:
            //   sleep_mu_ / sleep_cv_  — sleeping/waking only; held for microseconds.
            //   spin_                  — data structure access; held for nanoseconds.
            //
            // Flusher never holds both simultaneously, eliminating priority inversion
            // between the short-lived spin_ and the longer-held sleep lock.
            // ─────────────────────────────────────────────────────────────────────

            void flusher_loop() {
                std::vector<Command> local;
                std::vector<FastEntry> local_fast;
                local.reserve(64);
                local_fast.reserve(group_n_);

                std::vector<Command> write_batch;
                write_batch.reserve(group_n_);

                try {
                    while (true) {
                        // ── Sleep phase: hold sleep_mu_ only for the wait ─────────
                        {
                            std::unique_lock sleep_lock{sleep_mu_};
                            sleep_cv_.wait_for(
                                sleep_lock,
                                std::chrono::microseconds(group_micros_),
                                [this] { return has_pending_.load(std::memory_order_acquire) || stopped_.load(std::memory_order_relaxed); }
                            );
                        } // sleep_lock released; spin_ not held during sleep

                        // ── Grab phase: hold spin_ briefly to swap queues/arena ───
                        {
                            SpinLock::Guard g{spin_};
                            const bool both_empty = queue_.empty() && fast_queue_.empty();
                            if (both_empty) {
                                has_pending_.store(false, std::memory_order_relaxed);
                                if (stopped_.load(std::memory_order_relaxed)) break;
                                continue;
                            }
                            std::swap(queue_, local);
                            std::swap(fast_queue_, local_fast);
                            // Atomically flip write arena; flusher owns proc_arena_idx_.
                            proc_arena_idx_ = write_idx_;
                            write_idx_ ^= 1;
                            write_pos_[write_idx_] = 0;
                            has_pending_.store(false, std::memory_order_relaxed);
                        }

                        // ── Process fast-mode entries as a single batch (Plan 2) ──
                        if (!local_fast.empty()) {
                            flush_fast_batch(local_fast);
                            local_fast.clear();
                        }

                        // ── Process sync-mode Commands + control commands ──────────
                        bool do_shutdown = false;
                        size_t i = 0;
                        while (i < local.size() && !do_shutdown) {
                            if (local[i].type == Command::Type::WRITE) {
                                write_batch.push_back(std::move(local[i++]));
                                while (i < local.size() && local[i].type == Command::Type::WRITE && write_batch.size() < group_n_) {
                                    write_batch.push_back(std::move(local[i++]));
                                }
                                flush_write_batch(write_batch);
                                write_batch.clear();
                            }
                            else {
                                do_shutdown = handle_control(local[i]);
                                ++i;
                            }
                        }

                        local.clear();
                        if (do_shutdown) break;
                    }
                }
                catch (...) {
                    auto ep = std::current_exception();
                    for (auto& cmd : write_batch) { if (cmd.waiter) cmd.waiter->signal_error(ep); }
                    for (auto& cmd : local) { if (cmd.waiter) cmd.waiter->signal_error(ep); }
                    drain_queue_on_error(ep);
                    return;
                }

                // Final flush: skip if nosync_mode — caller uses force_sync() when needed.
                if (!nosync_mode_) {
                    try { file_.fdatasync(); }
                    catch (...) {}
                }
            }

            // ── flush_fast_batch  (Plan 2) ────────────────────────────────────────
            //
            // Writes all fast-mode (no-waiter) entries as a single WAL batch.
            // No waiters to signal on completion.
            // ─────────────────────────────────────────────────────────────────────
            void flush_fast_batch(const std::vector<FastEntry>& entries) {
                if (entries.empty()) return;

                if (needs_segment_header_) {
                    write_segment_header();
                    needs_segment_header_ = false;
                    current_segment_bytes_ = WalSegmentHeader::SIZE;
                }
                if (max_segment_bytes_ > 0 && current_segment_bytes_ >= max_segment_bytes_) { rotate_segment(); }

                size_t entries_total = 0;
                for (const auto& e : entries) entries_total += e.size;

                const size_t batch_total = WalBatchHeader::SIZE + entries_total;

                core::OwnedBuffer batch_buf = (batch_total <= BATCH_ARENA_BLOCK_SIZE)
                                                  ? batch_arena_->acquire(/*skip_zero_fill=*/true)
                                                  : core::OwnedBuffer::allocate(batch_total, 4096);

                core::BufferView view = batch_buf.view();

                const uint64_t bseq = global_batch_seq_.fetch_add(1, std::memory_order_relaxed);
                WalBatchHeader::write(view, bseq, static_cast<uint32_t>(entries.size()), static_cast<uint32_t>(batch_total));

                const auto& proc_arena = entry_buf_[proc_arena_idx_];
                size_t offset = WalBatchHeader::SIZE;
                for (const auto& e : entries) {
                    std::memcpy(reinterpret_cast<uint8_t*>(view.data()) + offset, proc_arena.data() + e.offset, e.size);
                    offset += e.size;
                }

                WalBatchHeader::finalize_checksum(view, batch_total);

                std::exception_ptr write_ex;
                try {
                    file_.write(view.data(), batch_total);
                    if (!nosync_mode_) file_.fdatasync();
                    current_segment_bytes_ += batch_total;
                }
                catch (...) { write_ex = std::current_exception(); }

                if (batch_total <= BATCH_ARENA_BLOCK_SIZE) { batch_arena_->release(std::move(batch_buf)); }
                if (write_ex) std::rethrow_exception(write_ex);
            }

            // ── flush_write_batch  (sync mode + mixed) ────────────────────────────
            void flush_write_batch(std::vector<Command>& batch) {
                if (batch.empty()) return;

                // Re-write SegmentHeader if this shard was truncated since the
                // last flush. Must happen before any batch data so that
                // WalReader always finds a valid header at offset 0.
                if (needs_segment_header_) {
                    write_segment_header();
                    needs_segment_header_ = false;
                    current_segment_bytes_ = WalSegmentHeader::SIZE;
                }

                // Rotate to a new segment if the current one is at or beyond the limit.
                // Check before writing so the new batch lands in the fresh segment.
                if (max_segment_bytes_ > 0 && current_segment_bytes_ >= max_segment_bytes_) { rotate_segment(); }

                size_t entries_total = 0;
                for (const auto& cmd : batch) entries_total += cmd.entry_size;

                const size_t batch_total = WalBatchHeader::SIZE + entries_total;

                core::OwnedBuffer batch_buf = (batch_total <= BATCH_ARENA_BLOCK_SIZE)
                                                  ? batch_arena_->acquire(/*skip_zero_fill=*/true)
                                                  : core::OwnedBuffer::allocate(batch_total, 4096);

                core::BufferView view = batch_buf.view();

                const uint64_t bseq = global_batch_seq_.fetch_add(1, std::memory_order_relaxed);
                WalBatchHeader::write(view, bseq, static_cast<uint32_t>(batch.size()), static_cast<uint32_t>(batch_total));

                const auto& proc_arena = entry_buf_[proc_arena_idx_];
                size_t offset = WalBatchHeader::SIZE;
                for (const auto& cmd : batch) {
                    std::memcpy(reinterpret_cast<uint8_t*>(view.data()) + offset, proc_arena.data() + cmd.entry_offset, cmd.entry_size);
                    offset += cmd.entry_size;
                }

                WalBatchHeader::finalize_checksum(view, batch_total);

                // SyncMode::Sync  (fast_mode=false): waiter present → caller blocks until signal.
                // SyncMode::Async (fast_mode=true, nosync_mode=false): fdatasync on every batch;
                //   put() already returned, durability still guaranteed within group_micros.
                // SyncMode::Off   (fast_mode=true, nosync_mode=true): skip fdatasync entirely;
                //   data is in the OS page cache only, force_sync() for explicit flush.
                std::exception_ptr write_ex;
                try {
                    file_.write(view.data(), batch_total);
                    if (!nosync_mode_) file_.fdatasync();
                    current_segment_bytes_ += batch_total;
                }
                catch (...) { write_ex = std::current_exception(); }

                if (batch_total <= BATCH_ARENA_BLOCK_SIZE) { batch_arena_->release(std::move(batch_buf)); }

                if (write_ex) {
                    for (auto& cmd : batch) { if (cmd.waiter) cmd.waiter->signal_error(write_ex); }
                    std::rethrow_exception(write_ex);
                }

                for (auto& cmd : batch) { if (cmd.waiter) cmd.waiter->signal(); }
            }

            bool handle_control(Command& cmd) {
                try {
                    switch (cmd.type) {
                        case Command::Type::FORCE_SYNC:
                            file_.fdatasync();
                            if (cmd.waiter) cmd.waiter->signal();
                            return false;

                        case Command::Type::TRUNCATE:
                            // Truncate all segment files for this shard, then reset
                            // to segment 0 and schedule a fresh SegmentHeader.
                            truncate_all_segments();
                            needs_segment_header_ = true;
                            if (cmd.waiter) cmd.waiter->signal();
                            return false;

                        case Command::Type::SHUTDOWN:
                            return true;

                        default:
                            return false;
                    }
                }
                catch (...) {
                    if (cmd.waiter) cmd.waiter->signal_error(std::current_exception());
                    throw;
                }
            }

            /**
             * Truncates the active segment and deletes any older segment files
             * for this shard (shard_{id}_seg0001.akwal, seg0002.akwal, ...).
             * Resets segment_id_ to 0 and current_segment_bytes_ to 0.
             */
            void truncate_all_segments() {
                // Close and delete all segments beyond seg0000.
                for (uint64_t s = segment_id_; s > 0; --s) {
                    if (s == segment_id_) file_.close(); // close active before deleting
                    std::filesystem::remove(segment_path(shard_id_, s));
                }

                // If we were already on seg0, file_ is still open; just truncate it.
                // If we closed it above (segment_id_ > 0), reopen seg0 and truncate.
                if (segment_id_ > 0) {
                    segment_id_ = 0;
                    file_ = FileHandle::open(segment_path(shard_id_, 0));
                }

                file_.truncate();
                file_.fsync_full();
                current_segment_bytes_ = 0;
            }

            void drain_queue_on_error(const std::exception_ptr& ep) {
                SpinLock::Guard g{spin_};
                for (auto& cmd : queue_) { if (cmd.waiter) cmd.waiter->signal_error(ep); }
                queue_.clear();
                fast_queue_.clear();
            }

            // ── Members ──────────────────────────────────────────────────────────

            uint32_t shard_id_;
            std::filesystem::path wal_dir_;
            uint64_t segment_id_{0};
            size_t group_n_;
            size_t group_micros_;
            uint64_t max_segment_bytes_;
            bool nosync_mode_;

            std::atomic<uint64_t>& global_batch_seq_;

            FileHandle file_;
            std::unique_ptr<core::PerThreadArena> batch_arena_;

            // ── Synchronization (Plan 1 + Plan 2) ────────────────────────────────
            //
            //  spin_       – SpinLock; protects queue_, fast_queue_, and the arena
            //                write-head (write_idx_, write_pos_).  Critical section
            //                is ≤ 15 ns; spinlock overhead ~2–5 ns vs ~50–80 ns for
            //                std::mutex (no syscall, no kernel transition).
            //
            //  sleep_mu_   – std::mutex used ONLY by the flusher's condition_variable
            //                wait.  Never held concurrently with spin_.
            //
            //  sleep_cv_   – Wakes the flusher when new work arrives.
            //
            //  has_pending_ – Atomic flag checked by the CV predicate.  Set by writers
            //                 (outside spin_) when the queue transitions empty→non-empty.
            //                 Cleared by the flusher after grabbing the queues.
            // ─────────────────────────────────────────────────────────────────────
            SpinLock spin_;
            std::mutex sleep_mu_;
            std::condition_variable sleep_cv_;
            std::atomic<bool> has_pending_{false};

            std::thread flusher_;
            std::atomic<bool> stopped_;

            bool needs_segment_header_;
            uint64_t current_segment_bytes_; ///< Bytes written to the active segment so far

            // ── Command queues ────────────────────────────────────────────────────
            //
            //  fast_queue_  (Plan 2) – No-waiter WRITE entries.  FastEntry = 8 B;
            //                 no shared_ptr construction/destruction.
            //
            //  queue_       – Sync-mode WRITE Commands (with waiters) + control
            //                 commands (FORCE_SYNC, TRUNCATE, SHUTDOWN).
            // ─────────────────────────────────────────────────────────────────────
            std::vector<FastEntry> fast_queue_;
            std::vector<Command> queue_;

            // ── Double-buffered entry arenas ─────────────────────────────────────
            // Eliminates per-entry heap allocation.  put() threads copy serialised
            // WAL entries into entry_buf_[write_idx_] under spin_.  When the
            // flusher swaps the queue it atomically flips write_idx_ and resets the
            // new write arena's position to 0.  The flusher then reads exclusively
            // from entry_buf_[proc_arena_idx_] without holding any lock.
            std::vector<uint8_t> entry_buf_[2];
            int write_idx_{0}; ///< Arena index currently used by put() (under spin_)
            size_t write_pos_[2]{0, 0}; ///< Write-head position for each arena (under spin_)
            int proc_arena_idx_{0}; ///< Arena the flusher is consuming (flusher thread only)
    };

    // ============================================================================
    // Resolve shard count
    // ============================================================================

    static uint32_t resolve_shard_count(uint32_t requested) {
        if (requested == 0) return compute_shard_count(); // auto: hardware_concurrency, [2, 16]
        if (requested == 1) return 1;
        uint32_t n = 2;
        while (n < requested) n <<= 1;
        return std::min(n, 64u); // explicit: up to 64 (file-based, higher than MemTable cap is rarely useful)
    }

    // ============================================================================
    // WalWriter::Impl
    // ============================================================================

    class WalWriter::Impl {
        public:
            explicit Impl(WalOptions opts)
                : wal_dir_{std::move(opts.wal_dir)},
                  group_n_{opts.group_n},
                  group_micros_{opts.group_micros},
                  fast_mode_{opts.sync_mode == SyncMode::Async || opts.sync_mode == SyncMode::Off},
                  nosync_mode_{opts.sync_mode == SyncMode::Off},
                  shard_count_{resolve_shard_count(opts.shard_count)},
                  max_segment_bytes_{opts.max_segment_bytes},
                  global_batch_seq_{0},
                  running_{true} {
                std::filesystem::create_directories(wal_dir_);
                shards_.reserve(shard_count_);
                for (uint32_t id = 0; id < shard_count_; ++id) {
                    shards_.push_back(std::make_unique<ShardWriter>(
                            id,
                            wal_dir_,
                            group_n_,
                            group_micros_,
                            max_segment_bytes_,
                            nosync_mode_,
                            global_batch_seq_
                        )
                    );
                }
            }

            ~Impl() { close(); }

            void append_put(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq, uint64_t key_fp64, uint64_t mini_key, uint8_t flags) {
                if (!running_.load(std::memory_order_acquire)) { throw std::runtime_error("WalWriter is closed"); }
                thread_local std::vector<uint8_t> tls_buf;
                const size_t needed = WalEntryHeader::SIZE + sizeof(core::AKHdr32) + key.size() + value.size();
                if (tls_buf.size() < needed) tls_buf.resize(needed);
                core::BufferView view{reinterpret_cast<std::byte*>(tls_buf.data()), needed};
                const size_t written = serialize_add_direct(view, key, value, seq, key_fp64, mini_key, flags);
                enqueue_entry(key_fp64, tls_buf.data(), written);
            }

            void append_delete(std::span<const uint8_t> key, uint64_t seq, uint64_t key_fp64, uint64_t mini_key) {
                if (!running_.load(std::memory_order_acquire)) { throw std::runtime_error("WalWriter is closed"); }
                thread_local std::vector<uint8_t> tls_buf;
                const size_t needed = WalEntryHeader::SIZE + sizeof(core::AKHdr32) + key.size();
                if (tls_buf.size() < needed) tls_buf.resize(needed);
                core::BufferView view{reinterpret_cast<std::byte*>(tls_buf.data()), needed};
                const size_t written = serialize_delete_direct(view, key, seq, key_fp64, mini_key);
                enqueue_entry(key_fp64, tls_buf.data(), written);
            }

            void force_sync() {
                if (!running_.load(std::memory_order_acquire)) return;
                std::vector<std::shared_ptr<Waiter>> waiters;
                waiters.reserve(shard_count_);
                for (auto& shard : shards_) {
                    auto w = std::make_shared<Waiter>();
                    shard->enqueue_force_sync(w);
                    waiters.push_back(std::move(w));
                }
                for (auto& w : waiters) w->wait();
            }

            void truncate() {
                if (!running_.load(std::memory_order_acquire)) return;
                std::vector<std::shared_ptr<Waiter>> waiters;
                waiters.reserve(shard_count_);
                for (auto& shard : shards_) {
                    auto w = std::make_shared<Waiter>();
                    shard->enqueue_truncate(w);
                    waiters.push_back(std::move(w));
                }
                for (auto& w : waiters) w->wait();
            }

            void close() {
                if (!running_.exchange(false, std::memory_order_acq_rel)) return;
                for (auto& shard : shards_) shard->stop();
            }

        private:
            void enqueue_entry(uint64_t key_fp64, const void* data, size_t size) {
                const uint32_t shard_id = (shard_count_ == 1) ? 0u : shard_for(key_fp64, shard_count_);
                if (fast_mode_) { shards_[shard_id]->enqueue(data, size, nullptr); }
                else {
                    auto waiter = std::make_shared<Waiter>();
                    shards_[shard_id]->enqueue(data, size, waiter);
                    waiter->wait();
                }
            }

            std::filesystem::path wal_dir_;
            size_t group_n_;
            size_t group_micros_;
            bool fast_mode_;
            bool nosync_mode_;
            uint32_t shard_count_;
            uint64_t max_segment_bytes_;

            std::atomic<uint64_t> global_batch_seq_;
            std::atomic<bool> running_;

            std::vector<std::unique_ptr<ShardWriter>> shards_;
    };

    // ============================================================================
    // WalWriter public API
    // ============================================================================

    WalWriter::WalWriter() = default;
    WalWriter::~WalWriter() = default;

    std::unique_ptr<WalWriter> WalWriter::create(WalOptions options) {
        auto w = std::unique_ptr < WalWriter > (new WalWriter());
        w->impl_ = std::make_unique<Impl>(std::move(options));
        return w;
    }

    void WalWriter::append_put(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint64_t key_fp64,
        uint64_t mini_key,
        uint8_t flags
    ) { impl_->append_put(key, value, seq, key_fp64, mini_key, flags); }

    void WalWriter::append_delete(std::span<const uint8_t> key, uint64_t seq, uint64_t key_fp64, uint64_t mini_key) {
        impl_->append_delete(key, seq, key_fp64, mini_key);
    }

    void WalWriter::force_sync() { impl_->force_sync(); }
    void WalWriter::truncate() { impl_->truncate(); }
    void WalWriter::close() { impl_->close(); }
} // namespace akkaradb::wal
