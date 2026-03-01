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

// internal/src/engine/wal/WalReader.cpp
#include "engine/wal/WalReader.hpp"

#include <stdexcept>

#ifdef _WIN32
#  include <windows.h>
#else
#  include <fcntl.h>
#  include <sys/stat.h>
#  include <unistd.h>
#endif

namespace akkaradb::wal {
    // ============================================================================
    // ReadFileHandle - read-only, sequential, no O_DIRECT (let OS page-cache work)
    // ============================================================================

    class ReadFileHandle {
        public:
            #ifdef _WIN32
            using NativeHandle = HANDLE; static const NativeHandle INVALID_H;
            #else
            using NativeHandle = int;
            static constexpr NativeHandle INVALID_H = -1;
            #endif

            ReadFileHandle() noexcept : handle_{INVALID_H}, file_size_{0} {}
            ~ReadFileHandle() noexcept { close(); }

            ReadFileHandle(const ReadFileHandle&) = delete;
            ReadFileHandle& operator=(const ReadFileHandle&) = delete;

            ReadFileHandle(ReadFileHandle&& o) noexcept
                : handle_{o.handle_}, file_size_{o.file_size_} {
                o.handle_ = INVALID_H;
                o.file_size_ = 0;
            }

            ReadFileHandle& operator=(ReadFileHandle&& o) noexcept {
                if (this != &o) {
                    close();
                    handle_ = o.handle_;
                    file_size_ = o.file_size_;
                    o.handle_ = INVALID_H;
                    o.file_size_ = 0;
                }
                return *this;
            }

            [[nodiscard]] static ReadFileHandle open(const std::filesystem::path& path) {
                ReadFileHandle fh;
                #ifdef _WIN32
                fh.handle_ = CreateFileW(
                    path.c_str(),
                    GENERIC_READ,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    nullptr,
                    OPEN_EXISTING,
                    FILE_FLAG_SEQUENTIAL_SCAN,
                    // hint to prefetcher
                    nullptr
                ); if (fh.handle_ == INVALID_H) { throw std::runtime_error("WalReader: failed to open: " + path.string()); } LARGE_INTEGER sz{}; if (
                    ::GetFileSizeEx(fh.handle_, &sz)) { fh.file_size_ = static_cast<uint64_t>(sz.QuadPart); }
                #else
                fh.handle_ = ::open(path.c_str(), O_RDONLY);
                if (fh.handle_ < 0) { throw std::runtime_error("WalReader: failed to open: " + path.string()); }
                struct stat st{};
                if (::fstat(fh.handle_, &st) == 0) { fh.file_size_ = static_cast<uint64_t>(st.st_size); }
                #  if defined(POSIX_FADV_SEQUENTIAL)
                ::posix_fadvise(fh.handle_, 0, 0, POSIX_FADV_SEQUENTIAL);
                #  endif
                #endif
                return fh;
            }

            /**
             * Reads up to `size` bytes into `buf`.
             * Returns bytes actually read (0 = EOF, never throws on EOF).
             * Throws on I/O error.
             */
            [[nodiscard]] size_t read(void* buf, size_t size) {
                if (size == 0) return 0;
                #ifdef _WIN32
                DWORD got = 0; if (!ReadFile(handle_, buf, static_cast<DWORD>(size), &got, nullptr)) {
                    throw std::runtime_error("WalReader: ReadFile failed");
                } return got;
                #else
                auto* ptr = static_cast<uint8_t*>(buf);
                size_t total = 0;
                while (total < size) {
                    const ssize_t n = ::read(handle_, ptr + total, size - total);
                    if (n == 0) break; // EOF
                    if (n < 0) {
                        if (errno == EINTR) continue;
                        throw std::runtime_error("WalReader: read failed");
                    }
                    total += static_cast<size_t>(n);
                }
                return total;
                #endif
            }

            [[nodiscard]] uint64_t file_size() const noexcept { return file_size_; }

            void close() noexcept {
                if (handle_ != INVALID_H) {
                    #ifdef _WIN32
                    CloseHandle(handle_);
                    #else
                    close(handle_);
                    #endif
                    handle_ = INVALID_H;
                }
            }

        private:
            NativeHandle handle_;
            uint64_t file_size_;
    };

    #ifdef _WIN32
    const ReadFileHandle::NativeHandle ReadFileHandle::INVALID_H = INVALID_HANDLE_VALUE;
    #endif

    // ============================================================================
    // WalReader::Impl
    // ============================================================================

    // Read buffer: large enough to pull many batches in one syscall.
    // 256 KB matches the ShardWriter's BATCH_ARENA_BLOCK_SIZE so that a typical
    // batch fits in one read even during recovery of compacted files.
    static constexpr size_t READ_BUF_SIZE = 256 * 1024; // 256 KB
    static constexpr size_t READ_BUF_ALIGN = 4096;

    class WalReader::Impl {
        public:
            explicit Impl(const std::filesystem::path& path)
                : file_{ReadFileHandle::open(path)},
                  // Two buffers: one for the sliding read window, one for the
                  // current batch payload (may be larger than READ_BUF_SIZE).
                  read_buf_{core::OwnedBuffer::allocate(READ_BUF_SIZE, READ_BUF_ALIGN)},
                  batch_buf_{core::OwnedBuffer::allocate(READ_BUF_SIZE, READ_BUF_ALIGN)},
                  buf_data_{0},
                  buf_offset_{0},
                  file_pos_{0},
                  error_type_{ErrorType::NONE},
                  error_position_{0},
                  shard_id_{0},
                  segment_id_{0} { read_segment_header(); }

            [[nodiscard]] std::optional<BatchView> next_batch() {
                if (error_type_ != ErrorType::NONE) return std::nullopt;

                // ── 1. Read WalBatchHeader ────────────────────────────────────
                // Probe: try to read the first byte of the next batch header to
                // distinguish clean EOF (no bytes at all) from truncation (some
                // bytes present but not enough for a full header).
                // fill_exact with n=0 always returns true and is a no-op, so we
                // must check for available data before calling fill_exact.
                if (buffered_available() == 0 && is_at_eof()) {
                    return std::nullopt; // clean EOF between batches
                }

                uint8_t hdr_raw[WalBatchHeader::SIZE];
                if (!fill_exact(hdr_raw, WalBatchHeader::SIZE)) {
                    // We had at least one byte (checked above) but not enough
                    // for a full BatchHeader → genuine truncation.
                    set_error(ErrorType::TRUNCATED);
                    return std::nullopt;
                }

                WalBatchHeader hdr{};
                std::memcpy(&hdr, hdr_raw, WalBatchHeader::SIZE);

                if (!hdr.verify_magic()) {
                    set_error(ErrorType::INVALID_MAGIC);
                    return std::nullopt;
                }

                // batch_size includes the header itself
                if (hdr.batch_size < WalBatchHeader::SIZE) {
                    set_error(ErrorType::INVALID_MAGIC);
                    return std::nullopt;
                }

                const size_t entries_len = hdr.batch_size - WalBatchHeader::SIZE;

                // ── 2. Ensure batch_buf is large enough ───────────────────────
                if (batch_buf_.size() < hdr.batch_size) { batch_buf_ = core::OwnedBuffer::allocate(hdr.batch_size, READ_BUF_ALIGN); }

                // Copy header into batch_buf (CRC covers header + entries)
                std::memcpy(batch_buf_.data(), hdr_raw, WalBatchHeader::SIZE);

                // ── 3. Read entry payload ─────────────────────────────────────
                if (entries_len > 0) {
                    auto* dst = reinterpret_cast<uint8_t*>(batch_buf_.data()) + WalBatchHeader::SIZE;
                    if (!fill_exact(dst, entries_len)) {
                        set_error(ErrorType::TRUNCATED);
                        return std::nullopt;
                    }
                }

                // ── 4. Verify batch CRC ───────────────────────────────────────
                const core::BufferView batch_view{batch_buf_.data(), hdr.batch_size};
                if (!hdr.verify_checksum(batch_view)) {
                    set_error(ErrorType::CRC_MISMATCH);
                    return std::nullopt;
                }

                // ── 5. Return BatchView (zero-copy into batch_buf_) ───────────
                const core::BufferView entries_view = batch_view.slice(WalBatchHeader::SIZE, entries_len);

                return BatchView{.batch_seq = hdr.batch_seq, .entry_count = hdr.entry_count, .entries_buf = entries_view,};
            }

            [[nodiscard]] bool has_error() const noexcept { return error_type_ != ErrorType::NONE; }
            [[nodiscard]] ErrorType error_type() const noexcept { return error_type_; }
            [[nodiscard]] uint64_t error_position() const noexcept { return error_position_; }
            [[nodiscard]] uint64_t file_size() const noexcept { return file_.file_size(); }
            [[nodiscard]] uint64_t bytes_read() const noexcept { return file_pos_; }
            [[nodiscard]] uint16_t shard_id() const noexcept { return shard_id_; }
            [[nodiscard]] uint64_t segment_id() const noexcept { return segment_id_; }

        private:
            // ── Segment header validation ─────────────────────────────────────

            void read_segment_header() {
                uint8_t raw[WalSegmentHeader::SIZE];
                if (!fill_exact(raw, WalSegmentHeader::SIZE)) {
                    set_error(ErrorType::TRUNCATED);
                    return;
                }

                // memcpy into a properly typed struct — avoids UB from
                // reinterpret_cast on a stack buffer and eliminates the
                // false-positive "unreachable code" warning that CLion's
                // flow analysis emits when calling verify_magic() /
                // verify_version() via a reinterpret_cast pointer
                // (the analyser assumes the fields always match the constants).
                WalSegmentHeader hdr{};
                std::memcpy(&hdr, raw, WalSegmentHeader::SIZE);

                if (!hdr.verify_magic()) {
                    set_error(ErrorType::INVALID_MAGIC);
                    return;
                }
                if (!hdr.verify_version()) {
                    set_error(ErrorType::INVALID_VERSION);
                    return;
                }

                const core::BufferView v{reinterpret_cast<std::byte*>(raw), WalSegmentHeader::SIZE};
                if (!hdr.verify_checksum(v)) {
                    set_error(ErrorType::CRC_MISMATCH);
                    return;
                }

                shard_id_ = hdr.shard_id;
                segment_id_ = hdr.segment_id;
            }

            // ── Buffered read helpers ─────────────────────────────────────────

            /**
             * Fills exactly `n` bytes into `dst`.
             * Refills read_buf_ from file as needed.
             * Returns false if fewer than `n` bytes are available (EOF or error).
             */
            [[nodiscard]] bool fill_exact(void* dst, size_t n) {
                auto* out = static_cast<uint8_t*>(dst);
                size_t done = 0;

                while (done < n) {
                    const size_t avail = buf_data_ - buf_offset_;

                    if (avail > 0) {
                        const size_t take = (avail < n - done)
                                                ? avail
                                                : (n - done);
                        std::memcpy(out + done, reinterpret_cast<const uint8_t*>(read_buf_.data()) + buf_offset_, take);
                        buf_offset_ += take;
                        done += take;
                    }
                    else {
                        // Refill
                        const size_t got = file_.read(read_buf_.data(), READ_BUF_SIZE);
                        if (got == 0) return done == n; // EOF
                        file_pos_ += got;
                        buf_data_ = got;
                        buf_offset_ = 0;
                    }
                }
                return true;
            }

            [[nodiscard]] size_t buffered_available() const noexcept { return buf_data_ - buf_offset_; }

            [[nodiscard]] bool is_at_eof() const noexcept { return file_pos_ >= file_.file_size(); }

            void set_error(ErrorType t) noexcept {
                error_type_ = t;
                // Report the position of the start of the failed structure,
                // accounting for bytes already consumed from read_buf_
                error_position_ = file_pos_ - buffered_available();
            }

            // ── Members ───────────────────────────────────────────────────────

            ReadFileHandle file_;
            core::OwnedBuffer read_buf_; ///< Sliding read window (~256 KB)
            core::OwnedBuffer batch_buf_; ///< Current batch (header + entries)

            size_t buf_data_; ///< Valid bytes in read_buf_
            size_t buf_offset_; ///< Current consume position in read_buf_
            uint64_t file_pos_; ///< Bytes consumed from file so far

            ErrorType error_type_;
            uint64_t error_position_;

            uint16_t shard_id_;
            uint64_t segment_id_;
    };

    // ============================================================================
    // WalReader public API
    // ============================================================================

    WalReader::WalReader(const std::filesystem::path& path) : impl_{std::make_unique<Impl>(path)} {}

    WalReader::~WalReader() = default;

    std::unique_ptr<WalReader> WalReader::open(const std::filesystem::path& path) { return std::unique_ptr < WalReader > (new WalReader(path)); }

    std::optional<WalReader::BatchView> WalReader::next_batch() { return impl_->next_batch(); }

    bool WalReader::has_error() const noexcept { return impl_->has_error(); }
    WalReader::ErrorType WalReader::error_type() const noexcept { return impl_->error_type(); }
    uint64_t WalReader::error_position() const noexcept { return impl_->error_position(); }
    uint64_t WalReader::file_size() const noexcept { return impl_->file_size(); }
    uint64_t WalReader::bytes_read() const noexcept { return impl_->bytes_read(); }
    uint16_t WalReader::shard_id() const noexcept { return impl_->shard_id(); }
    uint64_t WalReader::segment_id() const noexcept { return impl_->segment_id(); }
} // namespace akkaradb::wal