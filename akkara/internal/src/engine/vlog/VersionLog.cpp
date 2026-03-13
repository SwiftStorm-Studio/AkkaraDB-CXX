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
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

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
    // VersionLog::Impl
    // ============================================================================

    class VersionLog::Impl {
        public:
            VersionLogOptions opts_;

            // ── In-memory index ───────────────────────────────────────────────
            // Maps key_bytes (as std::string) → versions sorted ascending by seq.
            mutable std::mutex               index_mutex_;
            std::unordered_map<std::string, std::vector<VersionEntry>> index_;

            // ── File handle (append-only) ─────────────────────────────────────
            std::mutex file_mutex_;
            FILE*      file_ = nullptr;

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
                    fflush(file_);
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

                    // Parse entry header (starts at buf[sizeof(entry_len)])
                    const uint8_t* p = buf.data() + sizeof(entry_len);

                    uint64_t seq = 0, source_node_id = 0, timestamp_ns = 0, key_fp64 = 0;
                    uint16_t key_len = 0;
                    uint32_t value_len = 0;
                    uint8_t  flags = 0;

                    std::memcpy(&seq,            p, 8); p += 8;
                    std::memcpy(&source_node_id, p, 8); p += 8;
                    std::memcpy(&timestamp_ns,   p, 8); p += 8;
                    flags = *p++;
                    std::memcpy(&key_fp64,  p, 8); p += 8;
                    std::memcpy(&key_len,   p, 2); p += 2;
                    std::memcpy(&value_len, p, 4); p += 4;

                    // Sanity: parsed sizes must agree with entry_len
                    const size_t consumed = static_cast<size_t>(p - buf.data());
                    if (consumed + key_len + value_len + CRC_SIZE != entry_len) continue;

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
            /// Must be called with file_mutex_ held.
            void write_entry(std::span<const uint8_t> key,
                             uint64_t seq,
                             uint64_t source_node_id,
                             uint64_t timestamp_ns,
                             uint8_t  flags,
                             std::span<const uint8_t> value) {
                const uint32_t key_len   = static_cast<uint32_t>(key.size());
                const uint32_t value_len = static_cast<uint32_t>(value.size());
                const uint32_t entry_len =
                    static_cast<uint32_t>(ENTRY_HDR_SIZE + key.size() + value.size() + CRC_SIZE);

                write_buf_.resize(entry_len);
                uint8_t* p = write_buf_.data();

                const uint64_t key_fp64 =
                    core::AKHdr32::compute_key_fp64(key.data(), key.size());

                // Serialize header
                std::memcpy(p, &entry_len,      4); p += 4;
                std::memcpy(p, &seq,             8); p += 8;
                std::memcpy(p, &source_node_id, 8); p += 8;
                std::memcpy(p, &timestamp_ns,   8); p += 8;
                *p++ = flags;
                std::memcpy(p, &key_fp64,        8); p += 8;
                const uint16_t k16 = static_cast<uint16_t>(key.size());
                std::memcpy(p, &k16,             2); p += 2;
                std::memcpy(p, &value_len,       4); p += 4;

                // Payload
                if (key_len   > 0) { std::memcpy(p, key.data(),   key_len);   p += key_len;   }
                if (value_len > 0) { std::memcpy(p, value.data(), value_len); p += value_len; }

                // CRC32C over [buf[0]..buf[entry_len-CRC_SIZE-1]] with CRC field = 0
                std::memset(p, 0, CRC_SIZE);
                const uint32_t crc =
                    core::CRC32C::compute(write_buf_.data(), entry_len - CRC_SIZE);
                std::memcpy(p, &crc, CRC_SIZE);

                fwrite(write_buf_.data(), 1, entry_len, file_);
                fflush(file_);
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

        // Persist first: if we crash after the file write but before the index
        // update, recovery from the file will re-populate the index correctly.
        {
            std::lock_guard file_lock(impl_->file_mutex_);
            impl_->write_entry(key, seq, source_node_id, timestamp_ns, flags, value);
        }

        // Update in-memory index
        {
            std::lock_guard idx_lock(impl_->index_mutex_);
            const std::string key_str(reinterpret_cast<const char*>(key.data()), key.size());

            VersionEntry ve;
            ve.seq            = seq;
            ve.source_node_id = source_node_id;
            ve.timestamp_ns   = timestamp_ns;
            ve.flags          = flags;
            ve.value.assign(value.begin(), value.end());

            impl_->insert_sorted(key_str, std::move(ve));
        }
    }

    // ============================================================================
    // get_at
    // ============================================================================

    std::optional<VersionEntry>
    VersionLog::get_at(std::span<const uint8_t> key, uint64_t at_seq) const {
        if (!impl_) return std::nullopt;

        std::lock_guard lock(impl_->index_mutex_);
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

        std::lock_guard lock(impl_->index_mutex_);
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

        std::lock_guard lock(impl_->index_mutex_);

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
    // close
    // ============================================================================

    void VersionLog::close() {
        if (!impl_) return;
        std::lock_guard lock(impl_->file_mutex_);
        if (impl_->file_) {
            fflush(impl_->file_);
            fclose(impl_->file_);
            impl_->file_ = nullptr;
        }
    }

} // namespace akkaradb::engine::vlog
