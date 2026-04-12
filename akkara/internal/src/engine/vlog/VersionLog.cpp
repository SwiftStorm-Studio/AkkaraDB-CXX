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

// internal/src/engine/vlog/VersionLog.cpp
#include "engine/vlog/VersionLog.hpp"
#include "core/CRC32C.hpp"
#include "core/record/AKHdr32.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#ifdef _WIN32
#  include <io.h>       // _fileno, _commit
#else
#  include <unistd.h>   // fdatasync, fileno
#endif

namespace akkaradb::engine::vlog {
    namespace fs = std::filesystem;

    // ============================================================================
    // On-disk format constants
    // ============================================================================

    static constexpr uint32_t AKVLOG_MAGIC   = 0x41564C47u; // "AVLG"
    static constexpr uint16_t AKVLOG_VERSION = 0x0001u;

    // File header: 32 bytes, written once at file start.
    #pragma pack(push, 1)
    struct AkvlogFileHeader {
        uint32_t magic;
        uint16_t version;
        uint8_t  reserved[26];
    };
    #pragma pack(pop)
    static_assert(sizeof(AkvlogFileHeader) == 32);

    // Fixed-size prefix of every entry (before key/value payload and trailing CRC).
    // Total: 4+8+8+8+1+8+2+4 = 43 bytes.
    #pragma pack(push, 1)
    struct AkvlogEntryHeader {
        uint32_t entry_len;       // total bytes of this entry (all fields + CRC)
        uint64_t seq;
        uint64_t source_node_id;
        uint64_t timestamp_ns;
        uint8_t  flags;
        uint64_t key_fp64;        // SipHash fingerprint (for recovery indexing)
        uint16_t key_len;
        uint32_t value_len;
    };
    #pragma pack(pop)
    static_assert(sizeof(AkvlogEntryHeader) == 43);

    static constexpr size_t ENTRY_HDR_SIZE = sizeof(AkvlogEntryHeader); // 43
    static constexpr size_t CRC_SIZE       = sizeof(uint32_t);          //  4

    // Minimum entry size: header + 0-byte key + 0-byte value + CRC
    static constexpr size_t MIN_ENTRY_SIZE = ENTRY_HDR_SIZE + CRC_SIZE; // 47

    // Sanity cap: no single VersionLog entry should exceed 32 MiB
    static constexpr size_t MAX_ENTRY_SIZE = 32u * 1024u * 1024u;

    // ============================================================================
    // Platform fdatasync helper
    // ============================================================================

    static void do_fdatasync(FILE* f) {
        #ifdef _WIN32
        _commit(_fileno(f));
        #else
        fdatasync(fileno(f));
        #endif
    }

    // ============================================================================
    // VersionLog::Impl
    // ============================================================================

    class VersionLog::Impl {
        public:
            VersionLogOptions opts_;

            // ── Combined lock ────────────────────────────────────────────────────
            // Protects BOTH the file handle and the in-memory index.
            mutable std::mutex mu_;

            // ── In-memory index ───────────────────────────────────────────────────
            // Maps key_bytes → versions sorted ascending by seq.
            // Uses a transparent hasher so get_at() / history() can look up with
            // std::string_view without constructing a heap-allocated std::string.
            struct StringViewHash {
                using is_transparent = void;
                size_t operator()(std::string_view sv) const noexcept { return std::hash<std::string_view>{}(sv); }
                size_t operator()(const std::string& s) const noexcept { return std::hash<std::string_view>{}(s); }
            };

            std::unordered_map<std::string, std::vector<VersionEntry>, StringViewHash, std::equal_to<>> index_;

            // ── File handle (append-only) ─────────────────────────────────────────
            FILE* file_ = nullptr;

            // ── Reusable serialization buffer ─────────────────────────────────────
            std::vector<uint8_t> write_buf_;

            // ── Helpers ───────────────────────────────────────────────────────────

            /// Serializes one entry into write_buf_ and writes it to `f`.
            /// Checks fwrite return value and throws on I/O error.
            /// Called from both write_entry() (file_) and write_snapshot() (tmp file).
            void serialize_and_write(
                FILE* f,
                const uint8_t* key_data,
                size_t key_len,
                uint64_t seq,
                uint64_t source_node_id,
                uint64_t timestamp_ns,
                uint8_t flags,
                const uint8_t* value_data,
                size_t value_len
            ) {
                const size_t total = ENTRY_HDR_SIZE + key_len + value_len + CRC_SIZE;
                if (total > MAX_ENTRY_SIZE) { throw std::invalid_argument("VersionLog: entry too large"); }
                const uint32_t e_len = static_cast<uint32_t>(total);

                write_buf_.resize(e_len);
                uint8_t* p = write_buf_.data();

                AkvlogEntryHeader& hdr = *reinterpret_cast<AkvlogEntryHeader*>(p);
                hdr.entry_len = e_len;
                hdr.seq = seq;
                hdr.source_node_id = source_node_id;
                hdr.timestamp_ns = timestamp_ns;
                hdr.flags = flags;
                hdr.key_fp64 = core::AKHdr32::compute_key_fp64(key_data, key_len);
                hdr.key_len = static_cast<uint16_t>(key_len);
                hdr.value_len = static_cast<uint32_t>(value_len);
                p += ENTRY_HDR_SIZE;

                if (key_len > 0) {
                    std::memcpy(p, key_data, key_len);
                    p += key_len;
                }
                if (value_len > 0) {
                    std::memcpy(p, value_data, value_len);
                    p += value_len;
                }

                // CRC32C over all bytes with the trailing CRC field zeroed
                std::memset(p, 0, CRC_SIZE);
                const uint32_t crc = core::CRC32C::compute(write_buf_.data(), e_len - CRC_SIZE);
                std::memcpy(p, &crc, CRC_SIZE);

                if (fwrite(write_buf_.data(), 1, e_len, f) != e_len) { throw std::runtime_error("VersionLog: fwrite failed (disk full?)"); }
            }

            /// Writes one entry to file_ with the configured sync policy.
            /// Must be called with mu_ held and file_ non-null.
            void write_entry(
                std::span<const uint8_t> key,
                uint64_t seq,
                uint64_t source_node_id,
                uint64_t timestamp_ns,
                uint8_t flags,
                std::span<const uint8_t> value
            ) {
                serialize_and_write(file_, key.data(), key.size(), seq, source_node_id, timestamp_ns, flags, value.data(), value.size());

                if (opts_.sync_mode == VLogSyncMode::Sync) {
                    fflush(file_);
                    do_fdatasync(file_);
                }
                else if (opts_.sync_mode == VLogSyncMode::Async) { fflush(file_); }
                // VLogSyncMode::Off: no flush; caller or close() handles it
            }

            /// Opens the log file for recovery, rebuilds index_, then
            /// opens (or creates) it in append mode for subsequent writes.
            void open_or_create() {
                const auto& path = opts_.log_path;

                if (fs::exists(path)) {
                    FILE* rf = nullptr;
#ifdef _WIN32
                    rf = _wfopen(path.wstring().c_str(), L"rb");
#else
                    rf = fopen(path.string().c_str(), "rb");
#endif
                    if (rf) {
                        recover(rf);
                        fclose(rf);
                    }
                }

#ifdef _WIN32
                file_ = _wfopen(path.wstring().c_str(), L"ab");
#else
                file_ = fopen(path.string().c_str(), "ab");
#endif
                if (!file_) {
                    throw std::runtime_error("VersionLog: cannot open log file: " + path.string());
                }

                // Write file header only when creating a brand-new file
                if (!fs::exists(path) || fs::file_size(path) == 0) {
                    AkvlogFileHeader hdr{};
                    hdr.magic   = AKVLOG_MAGIC;
                    hdr.version = AKVLOG_VERSION;
                    std::memset(hdr.reserved, 0, sizeof(hdr.reserved));
                    if (fwrite(&hdr, sizeof(hdr), 1, file_) != 1) { throw std::runtime_error("VersionLog: failed to write file header"); }
                    // Always flush the file header regardless of sync_mode
                    fflush(file_);
                    do_fdatasync(file_);
                }
            }

            /// Reads all entries from rf and inserts them into index_.
            /// Silently skips corrupt entries (CRC mismatch, truncated data).
            void recover(FILE* rf) {
                AkvlogFileHeader hdr{};
                if (fread(&hdr, sizeof(hdr), 1, rf) != 1) return;
                if (hdr.magic != AKVLOG_MAGIC) return;

                std::vector<uint8_t> buf;

                while (true) {
                    uint32_t entry_len = 0;
                    if (fread(&entry_len, sizeof(entry_len), 1, rf) != 1) break;
                    if (entry_len < MIN_ENTRY_SIZE || entry_len > MAX_ENTRY_SIZE) break;

                    buf.resize(entry_len);
                    std::memcpy(buf.data(), &entry_len, sizeof(entry_len));

                    const size_t rest = entry_len - sizeof(entry_len);
                    if (fread(buf.data() + sizeof(entry_len), 1, rest, rf) != rest) break;

                    // Verify CRC32C
                    uint32_t stored_crc = 0;
                    std::memcpy(&stored_crc, buf.data() + entry_len - CRC_SIZE, CRC_SIZE);
                    std::memset(buf.data() + entry_len - CRC_SIZE, 0, CRC_SIZE);

                    const uint32_t computed_crc =
                        core::CRC32C::compute(buf.data(), entry_len - CRC_SIZE);
                    if (computed_crc != stored_crc) continue; // partial-write / corrupt

                    if (buf.size() < ENTRY_HDR_SIZE) continue;
                    const AkvlogEntryHeader& ehdr = *reinterpret_cast<const AkvlogEntryHeader*>(buf.data());

                    const uint16_t key_len = ehdr.key_len;
                    const uint32_t value_len = ehdr.value_len;

                    // Field sizes must agree with entry_len
                    if (ENTRY_HDR_SIZE + key_len + value_len + CRC_SIZE != entry_len) continue;

                    const uint8_t* p = buf.data() + ENTRY_HDR_SIZE;
                    std::string key_str(reinterpret_cast<const char*>(p), key_len);
                    p += key_len;

                    VersionEntry ve;
                    ve.seq = ehdr.seq;
                    ve.source_node_id = ehdr.source_node_id;
                    ve.timestamp_ns = ehdr.timestamp_ns;
                    ve.flags = ehdr.flags;
                    ve.value.assign(p, p + value_len);

                    insert_sorted(key_str, std::move(ve));
                }
            }

            /// Inserts ve into index_[key] keeping ascending seq order.
            void insert_sorted(const std::string& key, VersionEntry ve) {
                auto& versions = index_[key];
                if (versions.empty() || versions.back().seq <= ve.seq) {
                    versions.push_back(std::move(ve));
                } else {
                    // Out-of-order (should not occur in normal operation)
                    auto pos = std::lower_bound(
                        versions.begin(), versions.end(), ve.seq,
                        [](const VersionEntry& e, uint64_t s) { return e.seq < s; });
                    versions.insert(pos, std::move(ve));
                }
            }

            /// Writes a pruned snapshot to wf (all entries with seq >= threshold).
            /// Populates new_index with the kept entries. Must be called with mu_ held.
            size_t write_snapshot(FILE* wf, uint64_t seq_threshold, std::unordered_map<std::string, std::vector<VersionEntry>>& new_index) {
                AkvlogFileHeader file_hdr{};
                file_hdr.magic = AKVLOG_MAGIC;
                file_hdr.version = AKVLOG_VERSION;
                std::memset(file_hdr.reserved, 0, sizeof(file_hdr.reserved));
                if (fwrite(&file_hdr, sizeof(file_hdr), 1, wf) != 1) { throw std::runtime_error("VersionLog::prune_before: failed to write file header"); }

                size_t removed = 0;

                for (const auto& [key_str, versions] : index_) {
                    std::vector<VersionEntry> kept;
                    kept.reserve(versions.size());
                    for (const auto& ve : versions) {
                        if (ve.seq < seq_threshold) { ++removed; }
                        else { kept.push_back(ve); }
                    }
                    if (kept.empty()) continue;

                    for (const auto& ve : kept) {
                        serialize_and_write(
                            wf,
                            reinterpret_cast<const uint8_t*>(key_str.data()),
                            key_str.size(),
                            ve.seq,
                            ve.source_node_id,
                            ve.timestamp_ns,
                            ve.flags,
                            ve.value.data(),
                            ve.value.size()
                        );
                    }

                    new_index[key_str] = std::move(kept);
                }

                fflush(wf);
                do_fdatasync(wf); // always sync the pruned file for correctness
                return removed;
            }
    };

    // ============================================================================
    // Factory
    // ============================================================================

    std::unique_ptr<VersionLog> VersionLog::create(VersionLogOptions opts) {
        auto vl = std::unique_ptr<VersionLog>(new VersionLog());
        vl->impl_ = std::make_unique<Impl>();
        vl->impl_->opts_ = std::move(opts);
        vl->impl_->write_buf_.reserve(4096);
        vl->impl_->open_or_create();
        return vl;
    }

    VersionLog::~VersionLog() { close(); }

    // ============================================================================
    // append
    // ============================================================================

    void VersionLog::append(std::span<const uint8_t> key,
                            uint64_t seq,
                            uint64_t source_node_id,
                            uint64_t timestamp_ns,
                            uint8_t  flags,
                            std::span<const uint8_t> value) {
        if (!impl_) return;

        std::lock_guard lock(impl_->mu_);
        if (!impl_->file_) return; // closed — guard against use-after-close

        impl_->write_entry(key, seq, source_node_id, timestamp_ns, flags, value);

        // Build key_str here (after write succeeds) for index insertion.
        // std::string is required for operator[] on insertion; the read paths
        // (get_at / history) use string_view via the transparent hasher.
        const std::string key_str(reinterpret_cast<const char*>(key.data()), key.size());

        VersionEntry ve;
        ve.seq = seq;
        ve.source_node_id = source_node_id;
        ve.timestamp_ns = timestamp_ns;
        ve.flags = flags;
        ve.value.assign(value.begin(), value.end());

        impl_->insert_sorted(key_str, std::move(ve));
    }

    // ============================================================================
    // get_at
    // ============================================================================

    std::optional<VersionEntry>
    VersionLog::get_at(std::span<const uint8_t> key, uint64_t at_seq) const {
        if (!impl_) return std::nullopt;

        std::lock_guard lock(impl_->mu_);
        // string_view lookup — no heap allocation (transparent hasher).
        const std::string_view key_sv(reinterpret_cast<const char*>(key.data()), key.size());

        const auto it = impl_->index_.find(key_sv);
        if (it == impl_->index_.end()) return std::nullopt;

        const auto& versions = it->second;
        const auto pos = std::upper_bound(
            versions.begin(), versions.end(), at_seq,
            [](uint64_t s, const VersionEntry& e) { return s < e.seq; });

        if (pos == versions.begin()) return std::nullopt;
        return *std::prev(pos);
    }

    // ============================================================================
    // history
    // ============================================================================

    std::vector<VersionEntry>
    VersionLog::history(std::span<const uint8_t> key) const {
        if (!impl_) return {};

        std::lock_guard lock(impl_->mu_);
        // string_view lookup — no heap allocation (transparent hasher).
        const std::string_view key_sv(reinterpret_cast<const char*>(key.data()), key.size());

        const auto it = impl_->index_.find(key_sv);
        if (it == impl_->index_.end()) return {};
        return it->second; // copy (ascending by seq)
    }

    // ============================================================================
    // collect_rollback_targets
    // ============================================================================

    std::vector<std::pair<std::vector<uint8_t>, std::optional<VersionEntry>>>
    VersionLog::collect_rollback_targets(uint64_t target_seq) const {
        if (!impl_) return {};

        std::lock_guard lock(impl_->mu_);

        std::vector<std::pair<std::vector<uint8_t>, std::optional<VersionEntry>>> result;

        for (const auto& [key_str, versions] : impl_->index_) {
            if (versions.empty() || versions.back().seq <= target_seq) continue;

            std::vector<uint8_t> key_bytes(key_str.begin(), key_str.end());

            const auto pos = std::upper_bound(
                versions.begin(), versions.end(), target_seq,
                [](uint64_t s, const VersionEntry& e) { return s < e.seq; });

            std::optional<VersionEntry> prev;
            if (pos != versions.begin()) prev = *std::prev(pos);

            result.emplace_back(std::move(key_bytes), std::move(prev));
        }

        return result;
    }

    // ============================================================================
    // prune_before
    // ============================================================================

    size_t VersionLog::prune_before(uint64_t seq_threshold) {
        if (!impl_) return 0;

        std::lock_guard lock(impl_->mu_);

        // Quick check: is there actually anything to prune?
        bool has_old = false;
        for (const auto& versions : impl_->index_ | std::views::values) {
            if (!versions.empty() && versions.front().seq < seq_threshold) {
                has_old = true;
                break;
            }
        }
        if (!has_old) return 0;

        const auto& path = impl_->opts_.log_path;
        const auto tmp_path = fs::path(path.string() + ".tmp");

        FILE* wf = nullptr;
        #ifdef _WIN32
        wf = _wfopen(tmp_path.wstring().c_str(), L"wb");
        #else
        wf = fopen(tmp_path.string().c_str(), "wb");
        #endif
        if (!wf) { throw std::runtime_error("VersionLog::prune_before: cannot open temp file: " + tmp_path.string()); }

        std::unordered_map<std::string, std::vector<VersionEntry>> new_index;
        size_t removed = 0;
        try { removed = impl_->write_snapshot(wf, seq_threshold, new_index); }
        catch (...) {
            fclose(wf);
            fs::remove(tmp_path);
            throw;
        }
        fclose(wf);

        // Close current append handle
        if (impl_->file_) {
            fflush(impl_->file_);
            fclose(impl_->file_);
            impl_->file_ = nullptr;
        }

        // Atomic rename: tmp → main file
        std::error_code ec;
        fs::rename(tmp_path, path, ec);
        if (ec) {
            // Best-effort: try to reopen original so the log stays usable
            #ifdef _WIN32
            impl_->file_ = _wfopen(path.wstring().c_str(), L"ab");
            #else
            impl_->file_ = fopen(path.string().c_str(), "ab");
            #endif
            throw std::runtime_error("VersionLog::prune_before: rename failed: " + ec.message());
        }

        impl_->index_ = std::move(new_index);

        // Reopen append handle on the pruned file
        #ifdef _WIN32
        impl_->file_ = _wfopen(path.wstring().c_str(), L"ab");
        #else
        impl_->file_ = fopen(path.string().c_str(), "ab");
        #endif
        if (!impl_->file_) { throw std::runtime_error("VersionLog::prune_before: cannot reopen log after prune: " + path.string()); }

        return removed;
    }

    // ============================================================================
    // close
    // ============================================================================

    void VersionLog::close() {
        if (!impl_) return;
        std::lock_guard lock(impl_->mu_);
        if (impl_->file_) {
            fflush(impl_->file_);
            fclose(impl_->file_);
            impl_->file_ = nullptr;
        }
    }

} // namespace akkaradb::engine::vlog
