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

// internal/src/engine/StripeLaneWriter.cpp
#include "engine/StripeLaneWriter.hpp"
#include "core/buffer/OwnedBuffer.hpp"

#include <stdexcept>
#include <algorithm>

#if defined(_WIN32)
#  define WIN32_LEAN_AND_MEAN
#  define NOMINMAX
#  include <windows.h>
#else
#  include <fcntl.h>
#  include <unistd.h>
#  include <sys/stat.h>
#endif

namespace akkaradb::engine {
    // ============================================================================
    // Platform-specific file handle
    // ============================================================================

    #if defined(_WIN32)

    struct LaneFile {
        HANDLE h = INVALID_HANDLE_VALUE;

        bool open(const std::filesystem::path& path) {
            h = ::CreateFileW(
                path.c_str(),
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ,
                nullptr,
                OPEN_ALWAYS,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN,
                nullptr
            );
            return h != INVALID_HANDLE_VALUE;
        }

        void close() {
            if (h != INVALID_HANDLE_VALUE) {
                ::CloseHandle(h);
                h = INVALID_HANDLE_VALUE;
            }
        }

        // Append exactly data.size() bytes.  Returns false on error.
        bool write_append(const uint8_t* data, size_t size) {
            LARGE_INTEGER pos{};
            pos.QuadPart = 0;
            if (!::SetFilePointerEx(h, pos, nullptr, FILE_END)) return false;

            DWORD written = 0;
            const DWORD to_write = static_cast<DWORD>(size);
            return ::WriteFile(h, data, to_write, &written, nullptr) && written == to_write;
        }

        bool read_at(int64_t offset, uint8_t* dst, size_t size) const {
            LARGE_INTEGER pos{};
            pos.QuadPart = offset;
            if (!::SetFilePointerEx(h, pos, nullptr, FILE_BEGIN)) return false;

            DWORD read_bytes = 0;
            return ::ReadFile(h, dst, static_cast<DWORD>(size), &read_bytes, nullptr) && read_bytes == static_cast<DWORD>(size);
        }

        void force() { ::FlushFileBuffers(h); }

        void truncate(int64_t byte_size) {
            LARGE_INTEGER pos{};
            pos.QuadPart = byte_size;
            ::SetFilePointerEx(h, pos, nullptr, FILE_BEGIN);
            ::SetEndOfFile(h);
        }

        int64_t file_size() const {
            LARGE_INTEGER sz{};
            if (!::GetFileSizeEx(h, &sz)) return -1;
            return sz.QuadPart;
        }
    };

    #else // POSIX

    struct LaneFile {
        int fd = -1;

        bool open(const std::filesystem::path& path) {
            fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
            return fd >= 0;
        }

        void close() {
            if (fd >= 0) {
                ::close(fd);
                fd = -1;
            }
        }

        bool write_append(const uint8_t* data, size_t size) {
            size_t written = 0;
            while (written < size) {
                const ssize_t r = ::write(fd, data + written, size - written);
                if (r < 0) {
                    if (errno == EINTR) continue;
                    return false;
                }
                written += static_cast<size_t>(r);
            }
            return true;
        }

        bool read_at(int64_t offset, uint8_t* dst, size_t size) const {
            size_t done = 0;
            while (done < size) {
                const ssize_t r = ::pread(fd, dst + done, size - done, static_cast<off_t>(offset + static_cast<int64_t>(done)));
                if (r <= 0) {
                    if (r < 0 && errno == EINTR) continue;
                    return false;
                }
                done += static_cast<size_t>(r);
            }
            return true;
        }

        void force() { ::fdatasync(fd); }

        void truncate(int64_t byte_size) { ::ftruncate(fd, static_cast<off_t>(byte_size)); }

        int64_t file_size() const {
            struct stat st{};
            if (::fstat(fd, &st) < 0) return -1;
            return static_cast<int64_t>(st.st_size);
        }
    };

    #endif

    // ============================================================================
    // Impl
    // ============================================================================

    class StripeLaneWriter::Impl {
        public:
            Impl(const std::filesystem::path& lane_dir, size_t k, size_t m, size_t block_size, bool fast_mode)
                : lane_dir_{lane_dir}, k_{k}, m_{m}, block_size_{block_size}, fast_mode_{fast_mode}, last_sealed_{-1}, last_durable_{-1} {
                if (k_ == 0) throw std::invalid_argument("StripeLaneWriter: k must be > 0");
                if (block_size_ == 0) throw std::invalid_argument("StripeLaneWriter: block_size must be > 0");

                std::filesystem::create_directories(lane_dir_);

                const size_t total = k_ + m_;
                lanes_.resize(total);
                for (size_t i = 0; i < total; ++i) {
                    const auto path = lane_path(i);
                    if (!lanes_[i].open(path)) { throw std::runtime_error("StripeLaneWriter: cannot open lane file: " + path.string()); }
                }

                // Initialise last_sealed from existing files (best-effort; full recovery via recover()).
                const int64_t sz = lanes_[0].file_size();
                if (sz > 0 && static_cast<size_t>(sz) % block_size_ == 0) {
                    last_sealed_ = static_cast<int64_t>(sz) / static_cast<int64_t>(block_size_) - 1;
                    last_durable_ = last_sealed_;
                }
            }

            ~Impl() { close_all(); }

            // ---- Write path -------------------------------------------------------

            void on_stripe_ready(format::StripeWriter::Stripe stripe) {
                // data_blocks goes to lanes 0..k-1; parity_blocks to lanes k..k+m-1.
                const size_t n_data = stripe.data_blocks.size();
                const size_t n_parity = stripe.parity_blocks.size();

                if (n_data == 0) { throw std::runtime_error("StripeLaneWriter: stripe has 0 data blocks"); }

                // A partial stripe (n_data < k_) is allowed: occurs when a time-based
                // flush fires before k_ blocks have accumulated. Parity is only valid
                // when all k_ data blocks are present; skip parity writes for partial stripes.
                const bool full_stripe = (n_data == k_);

                // Write data lanes.
                for (size_t i = 0; i < n_data; ++i) {
                    const auto& blk = stripe.data_blocks[i];
                    if (blk.size() != block_size_) { throw std::runtime_error("StripeLaneWriter: data block size mismatch"); }
                    if (!lanes_[i].write_append(reinterpret_cast<const uint8_t*>(blk.data()), blk.size())) {
                        throw std::runtime_error("StripeLaneWriter: I/O error writing data lane " + std::to_string(i));
                    }
                }

                // Write parity lanes only when all k_ data blocks are present.
                if (full_stripe) {
                    for (size_t i = 0; i < n_parity; ++i) {
                        const auto& blk = stripe.parity_blocks[i];
                        if (blk.size() != block_size_) { throw std::runtime_error("StripeLaneWriter: parity block size mismatch"); }
                        if (!lanes_[k_ + i].write_append(reinterpret_cast<const uint8_t*>(blk.data()), blk.size())) {
                            throw std::runtime_error("StripeLaneWriter: I/O error writing parity lane " + std::to_string(i));
                        }
                    }
                }

                ++last_sealed_;

                if (!fast_mode_) { force_all(); }
            }

            // ---- Durability -------------------------------------------------------

            void force() { force_all(); }

            // ---- Recovery ---------------------------------------------------------

            RecoveryResult recover() const {
                RecoveryResult result{};

                const size_t total = k_ + m_;
                int64_t min_blocks = INT64_MAX;
                int64_t max_blocks = 0;
                bool size_mismatch = false;

                for (size_t i = 0; i < total; ++i) {
                    const int64_t sz = lanes_[i].file_size();
                    if (sz < 0) {
                        // Cannot stat — treat as empty.
                        min_blocks = 0;
                        size_mismatch = true;
                        continue;
                    }
                    const int64_t full_blocks = sz / static_cast<int64_t>(block_size_);
                    const bool has_partial = (sz % static_cast<int64_t>(block_size_)) != 0;

                    if (has_partial) size_mismatch = true;

                    min_blocks = (std::min)(min_blocks, full_blocks);
                    max_blocks = (std::max)(
                        max_blocks,
                        full_blocks + (has_partial
                                           ? 1
                                           : 0)
                    );
                }

                if (min_blocks == INT64_MAX) min_blocks = 0;

                result.last_sealed = min_blocks - 1; // -1 if empty
                result.last_durable = result.last_sealed;

                // Truncated tail: any lane has more data than the minimum, or has a partial block.
                result.truncated_tail = size_mismatch || (max_blocks > min_blocks);

                return result;
            }

            void truncate(int64_t stripe_count) {
                const int64_t byte_target = stripe_count * static_cast<int64_t>(block_size_);
                const size_t total = k_ + m_;
                for (size_t i = 0; i < total; ++i) { lanes_[i].truncate(byte_target); }
                last_sealed_ = stripe_count - 1;
                last_durable_ = last_sealed_;
            }

            // ---- Read path --------------------------------------------------------

            std::vector<core::OwnedBuffer> read_stripe_blocks(int64_t stripe_index) const {
                if (stripe_index < 0 || stripe_index > last_sealed_) { return {}; }

                const int64_t byte_offset = stripe_index * static_cast<int64_t>(block_size_);
                const size_t total = k_ + m_;
                std::vector<core::OwnedBuffer> blocks;
                blocks.reserve(total);

                for (size_t i = 0; i < total; ++i) {
                    auto buf = core::OwnedBuffer::allocate(block_size_);
                    if (!lanes_[i].read_at(byte_offset, reinterpret_cast<uint8_t*>(buf.data()), block_size_)) {
                        return {}; // I/O error — caller skips this stripe
                    }
                    blocks.push_back(std::move(buf));
                }
                return blocks;
            }

            // ---- State ------------------------------------------------------------

            int64_t last_sealed_stripe() const noexcept { return last_sealed_; }
            int64_t last_durable_stripe() const noexcept { return last_durable_; }
            size_t total_lanes() const noexcept { return k_ + m_; }

            void close() { close_all(); }

        private:
            std::filesystem::path lane_path(size_t idx) const {
                const std::string ext = (idx < k_)
                                            ? ".akd"
                                            : ".akp";
                return lane_dir_ / ("lane_" + std::to_string(idx) + ext);
            }

            void force_all() {
                for (auto& lane : lanes_) lane.force();
                last_durable_ = last_sealed_;
            }

            void close_all() { for (auto& lane : lanes_) lane.close(); }

            std::filesystem::path lane_dir_;
            size_t k_;
            size_t m_;
            size_t block_size_;
            bool fast_mode_;

            std::vector<LaneFile> lanes_;

            int64_t last_sealed_;
            int64_t last_durable_;
    };

    // ============================================================================
    // Public API
    // ============================================================================

    std::unique_ptr<StripeLaneWriter> StripeLaneWriter::create(const std::filesystem::path& lane_dir, size_t k, size_t m, size_t block_size, bool fast_mode) {
        return std::unique_ptr<StripeLaneWriter>(new StripeLaneWriter(lane_dir, k, m, block_size, fast_mode));
    }

    StripeLaneWriter::StripeLaneWriter(const std::filesystem::path& lane_dir, size_t k, size_t m, size_t block_size, bool fast_mode)
        : impl_{std::make_unique<Impl>(lane_dir, k, m, block_size, fast_mode)} {}

    StripeLaneWriter::~StripeLaneWriter() = default;

    void StripeLaneWriter::on_stripe_ready(format::StripeWriter::Stripe stripe) { impl_->on_stripe_ready(std::move(stripe)); }

    void StripeLaneWriter::force() { impl_->force(); }

    StripeLaneWriter::RecoveryResult StripeLaneWriter::recover() const { return impl_->recover(); }

    void StripeLaneWriter::truncate(int64_t stripe_count) { impl_->truncate(stripe_count); }

    std::vector<core::OwnedBuffer> StripeLaneWriter::read_stripe_blocks(int64_t stripe_index) const { return impl_->read_stripe_blocks(stripe_index); }

    int64_t StripeLaneWriter::last_sealed_stripe() const noexcept { return impl_->last_sealed_stripe(); }
    int64_t StripeLaneWriter::last_durable_stripe() const noexcept { return impl_->last_durable_stripe(); }
    size_t StripeLaneWriter::total_lanes() const noexcept { return impl_->total_lanes(); }

    void StripeLaneWriter::close() { impl_->close(); }
} // namespace akkaradb::engine