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

// internal/src/engine/blob/BlobManager.cpp
#include "engine/blob/BlobManager.hpp"
#include "core/CRC32C.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <cassert>

// ============================================================================
// Platform file I/O (same #ifdef pattern as Manifest.cpp)
// ============================================================================
#ifdef _WIN32
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  include <windows.h>

namespace {
    // Wide-string conversion helper (UTF-8 → UTF-16)
    static std::wstring to_wstr(const std::string& s) {
        if (s.empty()) return {};
        int n = MultiByteToWideChar(CP_UTF8, 0, s.c_str(), -1, nullptr, 0);
        std::wstring w(n, 0);
        MultiByteToWideChar(CP_UTF8, 0, s.c_str(), -1, w.data(), n);
        return w;
    }
    static std::wstring to_wpath(const std::filesystem::path& p) {
        return p.wstring();
    }
}

#else
#  include <fcntl.h>
#  include <sys/stat.h>
#  include <unistd.h>
#endif

namespace akkaradb::engine::blob {

    // ============================================================================
    // Low-level file helpers (platform-specific)
    // ============================================================================

    namespace {
        // Writes two contiguous byte regions to `path` atomically via .tmp + rename.
        // Used to write BlobFileHeader (hdr) followed by blob content without
        // allocating an intermediate vector that would double memory usage.
        static void write_atomic_split(const std::filesystem::path& path, const uint8_t* hdr, size_t hdr_len, const uint8_t* content, size_t content_len) {
            std::filesystem::create_directories(path.parent_path());

            std::filesystem::path tmp = path;
            tmp += ".tmp";

#ifdef _WIN32
            HANDLE h = CreateFileW(
                to_wpath(tmp).c_str(),
                GENERIC_WRITE, 0, nullptr,
                CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (h == INVALID_HANDLE_VALUE)
                throw std::runtime_error("BlobManager: cannot open tmp file for write");

            // Helper lambda: writes a region in ≤1GB chunks (WriteFile int limit)
            auto write_region = [&](const uint8_t* data, size_t len) {
                while (len > 0) {
                    DWORD written = 0;
                    DWORD chunk = (len > 0x40000000u) ? 0x40000000u : static_cast<DWORD>(len);
                    if (!WriteFile(h, data, chunk, &written, nullptr) || written == 0) {
                        CloseHandle(h);
                        DeleteFileW(to_wpath(tmp).c_str());
                        throw std::runtime_error("BlobManager: write error");
                    }
                    data += written;
                    len -= written;
                }
            }; write_region(hdr, hdr_len); write_region(content, content_len); FlushFileBuffers(h);
            CloseHandle(h);
            MoveFileExW(to_wpath(tmp).c_str(), to_wpath(path).c_str(),
                        MOVEFILE_REPLACE_EXISTING);
#else
            int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd < 0)
                throw std::runtime_error("BlobManager: cannot open tmp file for write");

            auto write_region = [&](const uint8_t* data, size_t len) {
                while (len > 0) {
                    ssize_t n = ::write(fd, data, len);
                    if (n <= 0) {
                        ::close(fd);
                        ::unlink(tmp.c_str());
                        throw std::runtime_error("BlobManager: write error");
                    }
                    data += n;
                    len -= static_cast<size_t>(n);
                }
            };

            write_region(hdr, hdr_len);
            write_region(content, content_len);
            ::fsync(fd);
            ::close(fd);
            ::rename(tmp.c_str(), path.c_str());
#endif
        }

        // Reads entire file into a vector.
        static std::vector<uint8_t> read_file(const std::filesystem::path& path) {
#ifdef _WIN32
            HANDLE h = CreateFileW(
                to_wpath(path).c_str(),
                GENERIC_READ, FILE_SHARE_READ, nullptr,
                OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (h == INVALID_HANDLE_VALUE)
                throw std::runtime_error("BlobManager: blob file not found: " + path.string());

            LARGE_INTEGER sz{};
            GetFileSizeEx(h, &sz);
            std::vector<uint8_t> buf(static_cast<size_t>(sz.QuadPart));

            DWORD nread = 0;
            size_t offset = 0;
            while (offset < buf.size()) {
                DWORD chunk = (buf.size() - offset > 0x40000000u)
                    ? 0x40000000u
                    : static_cast<DWORD>(buf.size() - offset);
                if (!ReadFile(h, buf.data() + offset, chunk, &nread, nullptr)) {
                    CloseHandle(h);
                    throw std::runtime_error("BlobManager: read error");
                }
                offset += nread;
            }
            CloseHandle(h);
            return buf;
#else
            int fd = ::open(path.c_str(), O_RDONLY);
            if (fd < 0)
                throw std::runtime_error("BlobManager: blob file not found: " + path.string());

            off_t sz = ::lseek(fd, 0, SEEK_END);
            ::lseek(fd, 0, SEEK_SET);
            std::vector<uint8_t> buf(static_cast<size_t>(sz));

            size_t offset = 0;
            while (offset < buf.size()) {
                ssize_t n = ::read(fd, buf.data() + offset, buf.size() - offset);
                if (n <= 0) {
                    ::close(fd);
                    throw std::runtime_error("BlobManager: read error");
                }
                offset += static_cast<size_t>(n);
            }
            ::close(fd);
            return buf;
#endif
        }

        // Renames src → dst.
        static bool rename_file(const std::filesystem::path& src,
                                const std::filesystem::path& dst) noexcept {
#ifdef _WIN32
            return MoveFileExW(to_wpath(src).c_str(), to_wpath(dst).c_str(),
                               MOVEFILE_REPLACE_EXISTING) != 0;
#else
            return ::rename(src.c_str(), dst.c_str()) == 0;
#endif
        }

        // Deletes a file; ignores errors.
        static void delete_file(const std::filesystem::path& p) noexcept {
#ifdef _WIN32
            DeleteFileW(to_wpath(p).c_str());
#else
            ::unlink(p.c_str());
#endif
        }

        // Parses a hex blob_id from a filename stem (16 hex chars).
        // Returns 0 on failure.
        static uint64_t parse_blob_id_from_stem(const std::string& stem) noexcept {
            if (stem.size() != 16) return 0;
            uint64_t v = 0;
            for (char c : stem) {
                v <<= 4;
                if (c >= '0' && c <= '9') v |= static_cast<uint64_t>(c - '0');
                else if (c >= 'a' && c <= 'f') v |= static_cast<uint64_t>(c - 'a' + 10);
                else if (c >= 'A' && c <= 'F') v |= static_cast<uint64_t>(c - 'A' + 10);
                else return 0;
            }
            return v;
        }

    } // anonymous namespace

    // ============================================================================
    // BlobManager::Impl
    // ============================================================================

    struct BlobManager::Impl {
        std::filesystem::path   blobs_dir;
        uint64_t                threshold;

        std::atomic<bool>       running { false };
        std::thread             gc_thr;

        std::mutex              del_mtx;
        std::condition_variable del_cv;
        std::deque<uint64_t>    del_queue; // blob_ids to delete

        // ── path helpers ─────────────────────────────────────────────────────

        std::filesystem::path id_to_path(uint64_t id) const {
            char dir_part[3];
            char file_part[17];
            uint8_t hi = static_cast<uint8_t>(id >> 56);
            std::snprintf(dir_part,  sizeof(dir_part),  "%02x", hi);
            std::snprintf(file_part, sizeof(file_part), "%016llx",
                          static_cast<unsigned long long>(id));
            return blobs_dir / dir_part / (std::string(file_part) + ".blob");
        }

        // ── startup ──────────────────────────────────────────────────────────

        void startup_cleanup() {
            // Delete any leftover *.blob.del files (from a previous crash after rename)
            std::error_code ec;
            for (auto& entry : std::filesystem::recursive_directory_iterator(blobs_dir, ec)) {
                if (ec) break;
                if (entry.path().extension() == ".del") {
                    delete_file(entry.path());
                }
            }
        }

        // ── write helpers ────────────────────────────────────────────────────

        void do_write(uint64_t id, std::span<const uint8_t> content) {
            // Build only the 32-byte header; write header + content separately.
            // This avoids duplicating the (potentially large) content in memory.
            auto hdr = BlobFileHeader::build(id, content.size());
            uint8_t hdr_buf[BlobFileHeader::SIZE];
            hdr.serialize(hdr_buf);
            write_atomic_split(id_to_path(id), hdr_buf, BlobFileHeader::SIZE, content.data(), content.size());
        }

        // ── GC worker ────────────────────────────────────────────────────────

        void gc_loop() {
            while (running.load(std::memory_order_relaxed)) {
                uint64_t id = 0;
                {
                    std::unique_lock lk(del_mtx);
                    del_cv.wait_for(lk, std::chrono::milliseconds(200), [&]{
                        return !del_queue.empty()
                            || !running.load(std::memory_order_relaxed);
                    });
                    if (del_queue.empty()) continue;
                    id = del_queue.front();
                    del_queue.pop_front();
                }

                auto src = id_to_path(id);
                auto dst = src;
                dst += ".del";

                if (rename_file(src, dst)) {
                    delete_file(dst);
                }
                // If rename fails (file already gone), ignore silently
            }
        }
    };

    // ============================================================================
    // BlobManager — path helper (public)
    // ============================================================================

    std::filesystem::path BlobManager::blob_path(uint64_t blob_id) const {
        return impl_->id_to_path(blob_id);
    }

    uint64_t BlobManager::threshold() const noexcept {
        return impl_->threshold;
    }

    // ============================================================================
    // Factory / lifecycle
    // ============================================================================

    BlobManager::BlobManager(std::unique_ptr<Impl> impl)
        : impl_(std::move(impl)) {}

    BlobManager::~BlobManager() { close(); }

    std::unique_ptr<BlobManager> BlobManager::create(
            std::filesystem::path blobs_dir, uint64_t threshold_bytes) {
        auto impl         = std::make_unique<Impl>();
        impl->blobs_dir   = std::move(blobs_dir);
        impl->threshold   = threshold_bytes;
        return std::unique_ptr<BlobManager>(new BlobManager(std::move(impl)));
    }

    void BlobManager::start() {
        // Ensure directory exists
        std::filesystem::create_directories(impl_->blobs_dir);

        // Cleanup leftover .blob.del files from previous crash
        impl_->startup_cleanup();

        // Start GC worker
        impl_->running.store(true, std::memory_order_relaxed);
        impl_->gc_thr = std::thread([this]{ impl_->gc_loop(); });
    }

    void BlobManager::close() {
        if (!impl_) return;
        impl_->running.store(false, std::memory_order_relaxed);
        impl_->del_cv.notify_all();
        if (impl_->gc_thr.joinable()) impl_->gc_thr.join();
    }

    // ============================================================================
    // write
    // ============================================================================

    void BlobManager::write(uint64_t blob_id, std::span<const uint8_t> content) {
        // Idempotent: if the file already exists (e.g. Replica re-receive after
        // reconnect), skip silently.
        auto p = impl_->id_to_path(blob_id);
        if (std::filesystem::exists(p)) return;

        impl_->do_write(blob_id, content);
    }

    // ============================================================================
    // read
    // ============================================================================

    std::vector<uint8_t> BlobManager::read(uint64_t blob_id) const {
        auto p   = impl_->id_to_path(blob_id);
        auto raw = read_file(p); // throws on I/O error

        if (raw.size() < BlobFileHeader::SIZE)
            throw std::runtime_error("BlobManager: file too small: " + p.string());

        BlobFileHeader hdr{};
        if (!decode_blob_header(raw.data(), hdr))
            throw std::runtime_error("BlobManager: header corrupt: " + p.string());

        if (raw.size() < BlobFileHeader::SIZE + hdr.total_size)
            throw std::runtime_error("BlobManager: content truncated: " + p.string());

        return std::vector<uint8_t>(
            raw.begin() + static_cast<ptrdiff_t>(BlobFileHeader::SIZE),
            raw.begin() + static_cast<ptrdiff_t>(BlobFileHeader::SIZE + hdr.total_size));
    }

    // ============================================================================
    // schedule_delete
    // ============================================================================

    void BlobManager::schedule_delete(uint64_t blob_id) {
        std::lock_guard lk(impl_->del_mtx);
        impl_->del_queue.push_back(blob_id);
        impl_->del_cv.notify_one();
    }

    // ============================================================================
    // scan_orphans
    // ============================================================================

    void BlobManager::scan_orphans(std::function<bool(uint64_t)> is_referenced) {
        std::error_code ec;
        for (auto& entry : std::filesystem::recursive_directory_iterator(
                               impl_->blobs_dir, ec)) {
            if (ec) break;
            if (entry.path().extension() != ".blob") continue;

            uint64_t id = parse_blob_id_from_stem(entry.path().stem().string());
            if (id == 0) continue;

            if (!is_referenced(id)) {
                schedule_delete(id);
            }
        }
    }

} // namespace akkaradb::engine::blob
