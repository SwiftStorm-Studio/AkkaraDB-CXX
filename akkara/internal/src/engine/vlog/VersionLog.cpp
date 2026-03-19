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

            // ── Combined lock (① mutex consolidation) ────────────────────────
            // Protects BOTH the file handle and the in-memory index.
            // write_entry() and index update are always performed under the same
            // lock, eliminating the race window that existed when two separate
            // mutexes were held in sequence.
            mutable std::mutex mu_;

            // ── In-memory index ───────────────────────────────────────────────
            // Maps key_bytes (as std::string) → versions sorted ascending by seq.
            std::unordered_map<std::string, std::vector<VersionEntry>> index_;

            // ── File handle (append-only) ─────────────────────────────────────
            FILE* file_ = nullptr;

            // ── Reusable serialization buffer ────────────────────────────────
            std::vector<uint8_t> write_buf_;

            // ── Helpers ───────────────────────────────────────────────────────

            /// Opens the log file for recovery (read-only), rebuilds index_, then
            /// opens (or creates) it in append mode for subsequent writes.
            void open_or_create() {
                const auto& path = opts_.log_path;

                if (fs::exists(path)) {
                    // Recovery pass: read-only handle
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

                // Append-mode handle for all subsequent writes
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
                    fwrite(&hdr, sizeof(hdr), 1, file_);
                    // Always flush the file header regardless of sync_mode;
                    // this is one-time initialisation, not a hot path.
                    fflush(file_);
                    do_fdatasync(file_);
                }
            }

            /// Reads all entries from rf and inserts them into index_.
            /// Silently skips corrupt entries (CRC mismatch, truncated data).
            void recover(FILE* rf) {
                // Validate file header
                AkvlogFileHeader hdr{};
                if (fread(&hdr, sizeof(hdr), 1, rf) != 1) return;
                if (hdr.magic != AKVLOG_MAGIC)              return;

                std::vector<uint8_t> buf;

                while (true) {
                    // Read entry_len (the total size of this entry)
                    uint32_t entry_len = 0;
                    if (fread(&entry_len, sizeof(entry_len), 1, rf) != 1) break;
                    if (entry_len < MIN_ENTRY_SIZE || entry_len > MAX_ENTRY_SIZE) break;

                    // Read the rest of the entry into buf (reused across iterations)
                    buf.resize(entry_len);
                    std::memcpy(buf.data(), &entry_len, sizeof(entry_len));

                    const size_t rest = entry_len - sizeof(entry_len);
                    if (fread(buf.data() + sizeof(entry_len), 1, rest, rf) != rest) break;

                    // Verify CRC32C
                    // CRC was computed over buf[0..entry_len-CRC_SIZE-1] with the
                    // trailing CRC field zeroed, so restore that for verification.
                    uint32_t stored_crc = 0;
                    std::memcpy(&stored_crc,
                                buf.data() + entry_len - CRC_SIZE,
                                CRC_SIZE);
                    std::memset(buf.data() + entry_len - CRC_SIZE, 0, CRC_SIZE);

                    const uint32_t computed_crc =
                        core::CRC32C::compute(buf.data(), entry_len - CRC_SIZE);
                    if (computed_crc != stored_crc) continue; // partial-write / corrupt

                    // Parse entry header via struct (buf[0..ENTRY_HDR_SIZE-1])
                    if (buf.size() < ENTRY_HDR_SIZE) continue;
                    const AkvlogEntryHeader& ehdr = *reinterpret_cast<const AkvlogEntryHeader*>(buf.data());

                    const uint64_t seq = ehdr.seq;
                    const uint64_t source_node_id = ehdr.source_node_id;
                    const uint64_t timestamp_ns = ehdr.timestamp_ns;
                    const uint8_t flags = ehdr.flags;
                    const uint16_t key_len = ehdr.key_len;
                    const uint32_t value_len = ehdr.value_len;

                    // Sanity: field sizes must agree with entry_len
                    const uint8_t* p = buf.data() + ENTRY_HDR_SIZE;
                    if (ENTRY_HDR_SIZE + key_len + value_len + CRC_SIZE != entry_len) continue;

                    std::string key_str(reinterpret_cast<const char*>(p), key_len);
                    p += key_len;
                    std::vector<uint8_t> value(p, p + value_len);

                    // Insert into in-memory index (maintain ascending seq order)
                    VersionEntry ve;
                    ve.seq            = seq;
                    ve.source_node_id = source_node_id;
                    ve.timestamp_ns   = timestamp_ns;
                    ve.flags          = flags;
                    ve.value          = std::move(value);

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

            /// Serializes one entry into write_buf_ and writes it to file_.
            /// Must be called with mu_ held.
            void write_entry(std::span<const uint8_t> key,
                             uint64_t seq,
                             uint64_t source_node_id,
                             uint64_t timestamp_ns,
                             uint8_t  flags,
                             std::span<const uint8_t> value) {
                const uint32_t entry_len = static_cast<uint32_t>(ENTRY_HDR_SIZE + key.size() + value.size() + CRC_SIZE);

                write_buf_.resize(entry_len);
                uint8_t* p = write_buf_.data();

                // Serialize header via struct — compiler verifies field layout
                AkvlogEntryHeader& hdr = *reinterpret_cast<AkvlogEntryHeader*>(p);
                hdr.entry_len = entry_len;
                hdr.seq = seq;
                hdr.source_node_id = source_node_id;
                hdr.timestamp_ns = timestamp_ns;
                hdr.flags = flags;
                hdr.key_fp64 = core::AKHdr32::compute_key_fp64(key.data(), key.size());
                hdr.key_len = static_cast<uint16_t>(key.size());
                hdr.value_len = static_cast<uint32_t>(value.size());
                p += ENTRY_HDR_SIZE;

                // Payload
                if (!key.empty()) {
                    std::memcpy(p, key.data(), key.size());
                    p += key.size();
                }
                if (!value.empty()) {
                    std::memcpy(p, value.data(), value.size());
                    p += value.size();
                }

                // CRC32C over [buf[0]..buf[entry_len-CRC_SIZE-1]] with CRC field = 0
                std::memset(p, 0, CRC_SIZE);
                const uint32_t crc =
                    core::CRC32C::compute(write_buf_.data(), entry_len - CRC_SIZE);
                std::memcpy(p, &crc, CRC_SIZE);

                fwrite(write_buf_.data(), 1, entry_len, file_);

                // Sync mode: flush + optional fdatasync
                if (opts_.sync_mode == VLogSyncMode::Sync) {
                    fflush(file_);
                    do_fdatasync(file_);
                }
                else if (opts_.sync_mode == VLogSyncMode::Async) { fflush(file_); }
                // VLogSyncMode::Off: no flush; caller or close() handles it
            }

            /// Writes a file header to wf, then all currently live index_ entries,
            /// each serialised in the standard format.  Used by prune_before().
            /// Must be called with mu_ held.
            size_t write_snapshot(FILE* wf, uint64_t seq_threshold, std::unordered_map<std::string, std::vector<VersionEntry>>& new_index) {
                // File header
                AkvlogFileHeader hdr{};
                hdr.magic = AKVLOG_MAGIC;
                hdr.version = AKVLOG_VERSION;
                std::memset(hdr.reserved, 0, sizeof(hdr.reserved));
                fwrite(&hdr, sizeof(hdr), 1, wf);

                size_t removed = 0;

                for (const auto& [key_str, versions] : index_) {
                    std::vector<VersionEntry> kept;
                    kept.reserve(versions.size());
                    for (const auto& ve : versions) {
                        if (ve.seq < seq_threshold) { ++removed; }
                        else { kept.push_back(ve); }
                    }
                    if (kept.empty()) continue;

                    // Write each kept entry to wf
                    for (const auto& ve : kept) {
                        const uint32_t k_len = static_cast<uint32_t>(key_str.size());
                        const uint32_t v_len = static_cast<uint32_t>(ve.value.size());
                        const uint32_t e_len = static_cast<uint32_t>(ENTRY_HDR_SIZE + k_len + v_len + CRC_SIZE);

                        write_buf_.resize(e_len);
                        uint8_t* p = write_buf_.data();

                        AkvlogEntryHeader& ehdr = *reinterpret_cast<AkvlogEntryHeader*>(p);
                        ehdr.entry_len = e_len;
                        ehdr.seq = ve.seq;
                        ehdr.source_node_id = ve.source_node_id;
                        ehdr.timestamp_ns = ve.timestamp_ns;
                        ehdr.flags = ve.flags;
                        ehdr.key_fp64 = core::AKHdr32::compute_key_fp64(reinterpret_cast<const uint8_t*>(key_str.data()), key_str.size());
                        ehdr.key_len = static_cast<uint16_t>(k_len);
                        ehdr.value_len = v_len;
                        p += ENTRY_HDR_SIZE;

                        if (k_len > 0) {
                            std::memcpy(p, key_str.data(), k_len);
                            p += k_len;
                        }
                        if (v_len > 0) {
                            std::memcpy(p, ve.value.data(), v_len);
                            p += v_len;
                        }
                        std::memset(p, 0, CRC_SIZE);
                        const uint32_t crc = core::CRC32C::compute(write_buf_.data(), e_len - CRC_SIZE);
                        std::memcpy(p, &crc, CRC_SIZE);

                        fwrite(write_buf_.data(), 1, e_len, wf);
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
        auto vl    = std::unique_ptr<VersionLog>(new VersionLog());
        vl->impl_  = std::make_unique<Impl>();
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

        // ① Single mutex: file write and index update are now atomic together.
        std::lock_guard lock(impl_->mu_);

        impl_->write_entry(key, seq, source_node_id, timestamp_ns, flags, value);

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
        const std::string key_str(reinterpret_cast<const char*>(key.data()), key.size());

        const auto it = impl_->index_.find(key_str);
        if (it == impl_->index_.end()) return std::nullopt;

        const auto& versions = it->second;
        // Binary search: find the first entry with seq > at_seq, then step back.
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
        const std::string key_str(reinterpret_cast<const char*>(key.data()), key.size());

        const auto it = impl_->index_.find(key_str);
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

            // This key has at least one write after target_seq → needs rollback.
            std::vector<uint8_t> key_bytes(key_str.begin(), key_str.end());

            // Find the state at target_seq (last entry with seq <= target_seq).
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
    // prune_before  (③)
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

        // Open temp file
        FILE* wf = nullptr;
        #ifdef _WIN32
        wf = _wfopen(tmp_path.wstring().c_str(), L"wb");
        #else
        wf = fopen(tmp_path.string().c_str(), "wb");
        #endif
        if (!wf) { throw std::runtime_error("VersionLog::prune_before: cannot open temp file: " + tmp_path.string()); }

        // Build new index and write snapshot to tmp file
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
            // Best-effort: try to reopen original file so the log stays usable
            #ifdef _WIN32
            impl_->file_ = _wfopen(path.wstring().c_str(), L"ab");
            #else
            impl_->file_ = fopen(path.string().c_str(), "ab");
            #endif
            throw std::runtime_error("VersionLog::prune_before: rename failed: " + ec.message());
        }

        // Swap in the pruned index
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
