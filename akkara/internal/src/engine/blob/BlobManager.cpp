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

#include <zstd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
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
    std::wstring to_wpath(const std::filesystem::path& p) {
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
        void write_atomic_split(const std::filesystem::path& path, const uint8_t* hdr, size_t hdr_len, const uint8_t* content, size_t content_len) {
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
            };
            write_region(hdr, hdr_len);
            write_region(content, content_len);
            FlushFileBuffers(h);
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

            LARGE_INTEGER sz{}; if (!GetFileSizeEx(h, &sz)) {
                CloseHandle(h);
                throw std::runtime_error("BlobManager: GetFileSizeEx failed: " + path.string());
            } std::vector<uint8_t> buf(static_cast<size_t>(sz.QuadPart));

            DWORD nread = 0;
            size_t offset = 0;
            while (offset < buf.size()) {
                DWORD chunk = (buf.size() - offset > 0x40000000u)
                    ? 0x40000000u
                    : static_cast<DWORD>(buf.size() - offset);
                if (!ReadFile(h, buf.data() + offset, chunk, &nread, nullptr) || nread == 0) {
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

const off_t sz = ::lseek(fd, 0, SEEK_END);
if (sz < 0) {
    ::close(fd);
    throw std::runtime_error("BlobManager: lseek failed: " + path.string());
}
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
        Codec codec = Codec::None;

        std::atomic<bool>       running { false };
        std::thread             gc_thr;

        // Serialises concurrent writes of the SAME blob_id (reconnect / retry path).
        // During normal operation every blob_id is unique, so this mutex is uncontended.
        std::mutex write_mu;

        std::mutex              del_mtx;
        std::condition_variable del_cv;
        std::vector<uint64_t> del_queue; // blob_ids to delete

        // ── Stats counters (relaxed atomics — zero overhead on write path) ──
        std::atomic<uint64_t> blobs_written_{0};
        std::atomic<uint64_t> bytes_uncompressed_{0};
        std::atomic<uint64_t> bytes_on_disk_{0};
        std::atomic<uint64_t> blobs_deleted_{0};
        std::atomic<uint64_t> gc_cycles_{0};

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

        void do_write(uint64_t id, std::span<const uint8_t> content, const std::filesystem::path& path) {
            // Optionally compress the content with Zstd.
            std::vector<uint8_t> compressed_buf;
            const uint8_t* payload_data = content.data();
            size_t payload_len = content.size();
            uint32_t compressed_sz = 0;
            engine::Codec actual_codec = engine::Codec::None;

            if (codec == engine::Codec::Zstd && !content.empty()) {
                const size_t bound = ZSTD_compressBound(content.size());
                compressed_buf.resize(bound);
                const size_t cz = ZSTD_compress(compressed_buf.data(), bound, content.data(), content.size(), ZSTD_CLEVEL_DEFAULT);
                // Accept compressed output only when:
                //   • no error
                //   • actually smaller than the original (incompressible data can expand)
                //   • fits in uint32_t (blobs > 4 GiB after compression would overflow)
                if (!ZSTD_isError(cz) && cz < content.size() && cz <= UINT32_MAX) {
                    compressed_buf.resize(cz);
                    compressed_sz = static_cast<uint32_t>(cz);
                    payload_data = compressed_buf.data();
                    payload_len = cz;
                    actual_codec = engine::Codec::Zstd;
                }
                // else: fall through to uncompressed write
            }

            // total_size always holds the UNCOMPRESSED content size so the reader
            // can allocate the decompression buffer without reading the payload first.
            auto hdr = BlobFileHeader::build(id, content.size(), actual_codec, compressed_sz);
            uint8_t hdr_buf[BlobFileHeader::SIZE];
            hdr.serialize(hdr_buf);
            write_atomic_split(path, hdr_buf, BlobFileHeader::SIZE, payload_data, payload_len);

            blobs_written_.fetch_add(1, std::memory_order_relaxed);
            bytes_uncompressed_.fetch_add(content.size(), std::memory_order_relaxed);
            bytes_on_disk_.fetch_add(BlobFileHeader::SIZE + payload_len, std::memory_order_relaxed);
        }

        // ── Snapshot ─────────────────────────────────────────────────────────

        [[nodiscard]] BlobManager::BlobSnapshot snapshot() const noexcept {
            return BlobManager::BlobSnapshot{
                blobs_written_.load(std::memory_order_relaxed),
                bytes_uncompressed_.load(std::memory_order_relaxed),
                bytes_on_disk_.load(std::memory_order_relaxed),
                blobs_deleted_.load(std::memory_order_relaxed),
                gc_cycles_.load(std::memory_order_relaxed),
            };
        }

        // ── GC worker ────────────────────────────────────────────────────────

        void gc_loop() {
            std::vector<uint64_t> local_batch;
            while (running.load(std::memory_order_relaxed)) {
                {
                    std::unique_lock lk(del_mtx);
                    del_cv.wait_for(lk, std::chrono::milliseconds(200), [&]{
                        return !del_queue.empty() || !running.load(std::memory_order_acquire);
                                    }
                    );
                    if (del_queue.empty()) continue;
                    // Drain entire queue in one swap — lock released before file I/O.
                    local_batch.swap(del_queue);
                }

                // Process the batch outside the lock so schedule_delete() is never
                // blocked by disk rename/unlink latency.
                uint64_t deleted_count = 0;
                for (uint64_t id : local_batch) {
                    auto src = id_to_path(id);
                    auto dst = src;
                    dst += ".del";
                    if (rename_file(src, dst)) {
                        delete_file(dst);
                        ++deleted_count;
                    }
                    // If rename fails (file already gone), ignore silently
                }
                if (deleted_count > 0) {
                    blobs_deleted_.fetch_add(deleted_count, std::memory_order_relaxed);
                }
                gc_cycles_.fetch_add(1, std::memory_order_relaxed);
                local_batch.clear();
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

    BlobManager::BlobSnapshot BlobManager::snapshot() const noexcept {
        return impl_->snapshot();
    }

    // ============================================================================
    // Factory / lifecycle
    // ============================================================================

    BlobManager::BlobManager(std::unique_ptr<Impl> impl)
        : impl_(std::move(impl)) {}

    BlobManager::~BlobManager() { close(); }

    std::unique_ptr<BlobManager> BlobManager::create(std::filesystem::path blobs_dir, uint64_t threshold_bytes, Codec codec) {
        auto impl = std::make_unique<Impl>();
        impl->blobs_dir   = std::move(blobs_dir);
        impl->threshold   = threshold_bytes;
        impl->codec = codec;
        return std::unique_ptr<BlobManager>(new BlobManager(std::move(impl)));
    }

    void BlobManager::start() {
        std::filesystem::create_directories(impl_->blobs_dir);

        // Pre-create all 256 subdirectories (00..ff) so write_atomic_split()
        // never needs create_directories on the hot write path.
        char sub[3];
        for (int i = 0; i < 256; ++i) {
            std::snprintf(sub, sizeof(sub), "%02x", i);
            std::filesystem::create_directories(impl_->blobs_dir / sub);
        }

        impl_->startup_cleanup();

        // Start GC worker
        impl_->running.store(true, std::memory_order_release);
        impl_->gc_thr = std::thread([this] { impl_->gc_loop(); });
    }

    void BlobManager::close() {
        if (!impl_) return;
        impl_->running.store(false, std::memory_order_release);
        impl_->del_cv.notify_all();
        if (impl_->gc_thr.joinable()) impl_->gc_thr.join();
    }

    // ============================================================================
    // write
    // ============================================================================

    void BlobManager::write(uint64_t blob_id, std::span<const uint8_t> content) {
        auto p = impl_->id_to_path(blob_id);

        // Fast path: file already exists — no work needed.
        // Safe to check without a lock; the file can only transition
        // non-existent → existent (never the reverse at this call site).
        if (std::filesystem::exists(p)) return;

        // Slow path: acquire lock then re-check to close the TOCTOU window.
        // Two threads (e.g. replica reconnect) could both see exists=false above
        // and race to write {id}.blob.tmp — the lock ensures only one proceeds.
        // Normal operation (unique blob_ids per write) never contends here.
        std::lock_guard lk(impl_->write_mu);
        if (std::filesystem::exists(p)) return;

        impl_->do_write(blob_id, content, p);
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

        const uint8_t* payload = raw.data() + BlobFileHeader::SIZE;
        const size_t payload_len = raw.size() - BlobFileHeader::SIZE;

        // Compressed blob: hdr.flags == Zstd and compressed_size > 0
        if (hdr.flags == static_cast<uint16_t>(engine::Codec::Zstd) && hdr.compressed_size > 0) {
            if (payload_len < hdr.compressed_size) throw std::runtime_error("BlobManager: compressed payload truncated: " + p.string());

            std::vector<uint8_t> decompressed(static_cast<size_t>(hdr.total_size));
            const size_t result = ZSTD_decompress(decompressed.data(), decompressed.size(), payload, hdr.compressed_size);
            if (ZSTD_isError(result))
                throw std::runtime_error(std::string("BlobManager: Zstd decompress failed: ") + ZSTD_getErrorName(result));
            if (result != hdr.total_size) throw std::runtime_error("BlobManager: decompressed size mismatch: " + p.string());
            return decompressed;
        }

        // Uncompressed blob (flags == None, or legacy files with reserved=0)
        if (payload_len < hdr.total_size) throw std::runtime_error("BlobManager: content truncated: " + p.string());

        // Reuse the already-allocated `raw` buffer — shift payload down and trim.
        // Avoids a second allocation + copy of potentially large uncompressed data.
        std::memmove(raw.data(), raw.data() + BlobFileHeader::SIZE, hdr.total_size);
        raw.resize(hdr.total_size);
        return raw;
    }

    std::vector<uint8_t> BlobManager::read(uint64_t blob_id, uint32_t expected_checksum) const {
        auto content = read(blob_id);
        const uint32_t actual = core::CRC32C::compute(content.data(), content.size());
        if (actual != expected_checksum) {
            auto hex32 = [](uint32_t v) {
                char buf[9];
                std::snprintf(buf, sizeof(buf), "%08x", v);
                return std::string(buf);
            };
            throw std::runtime_error(
                "BlobManager: content CRC32C mismatch for blob " + std::to_string(blob_id) + " (expected=0x" + hex32(expected_checksum) + " actual=0x" + hex32(
                    actual
                ) + ")"
            );
        }
        return content;
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
        // Collect all orphan IDs first, then enqueue with a single lock acquisition.
        // Avoids repeated lock/unlock + notify_one per file during large scans.
        std::vector<uint64_t> orphans;
        std::error_code ec;
        for (auto& entry : std::filesystem::recursive_directory_iterator(
                 impl_->blobs_dir, ec)) {
            if (ec) break;
            if (entry.path().extension() != ".blob") continue;

            uint64_t id = parse_blob_id_from_stem(entry.path().stem().string());
            if (id == 0) continue;

            if (!is_referenced(id)) {
                orphans.push_back(id);
            }
        }

        if (!orphans.empty()) {
            std::lock_guard lk(impl_->del_mtx);
            impl_->del_queue.insert(impl_->del_queue.end(),
                                    orphans.begin(), orphans.end());
            impl_->del_cv.notify_one();
        }
    }

} // namespace akkaradb::engine::blob
