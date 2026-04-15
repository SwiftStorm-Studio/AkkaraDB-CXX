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

// internal/include/engine/vlog/VersionLog.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace akkaradb::engine::vlog {

    // ============================================================================
    // Constants
    // ============================================================================

    /// Sentinel source_node_id for rollback-generated writes.
    inline constexpr uint64_t ROLLBACK_NODE = UINT64_MAX;

    /// Flag value recorded in VersionLog entries produced by rollback operations.
    /// Not written to WAL or MemTable — VersionLog-internal only.
    inline constexpr uint8_t VLOG_FLAG_ROLLBACK = 0x04;

    // ============================================================================
    // VLogSyncMode
    // ============================================================================

    /**
     * Controls when written entries are fsynced to storage.
     *
     *   Sync  — fdatasync() after every write_entry() call.
     *           Strongest durability; highest latency.
     *   Async — fflush() only; OS page-cache writeback handles persistence.
     *           Low latency; a crash within the OS flush window loses recent entries.
     *           Safe at DB level: missing entries are re-derived from WAL on next open.
     *   Off   — No flush at all; data lives in libc stdio buffer until close() / prune().
     *           Use only for non-critical history or test scenarios.
     */
    enum class VLogSyncMode : uint8_t {
        Sync = 0, Async = 1, Off = 2,
    };

    // ============================================================================
    // VersionLogOptions
    // ============================================================================

    struct VersionLogOptions {
        /// Full path to the version log file (e.g. data_dir / "history.akvlog").
        std::filesystem::path log_path;

        /// Fsync policy for write_entry(). Default: Async (matches WAL default).
        VLogSyncMode sync_mode = VLogSyncMode::Async;
    };

    // ============================================================================
    // VersionEntry — one historical write event for a key
    // ============================================================================

    // C4251: value (std::vector<uint8_t>) is a private-member-of-exported-struct —
    // safe to suppress because both the DLL and its consumers share the same CRT.
    #ifdef _MSC_VER
    #pragma warning(push)
    #pragma warning(disable: 4251)
    #endif
    struct AKDB_API VersionEntry {
        uint64_t seq            = 0;
        uint64_t source_node_id = 0;    ///< 0=system/unknown, UINT64_MAX=rollback
        uint64_t timestamp_ns   = 0;    ///< wall-clock nanoseconds since epoch

        /// Flags: 0x00=NORMAL, 0x01=TOMBSTONE, 0x02=BLOB, 0x04=ROLLBACK
        uint8_t  flags          = 0;

        /// Stored value bytes.
        /// - NORMAL: inline value bytes
        /// - BLOB:   20-byte BlobRef (blob_id:8 + total_size:8 + checksum:4)
        /// - TOMBSTONE: empty
        std::vector<uint8_t> value;
    };
    #ifdef _MSC_VER
    #pragma warning(pop)
    #endif

    // ============================================================================
    // VersionLog
    // ============================================================================

    /**
     * VersionLog — per-key version history, persisted to a .akvlog file.
     *
     * Records every put/remove operation with its seq, source node, timestamp,
     * and stored value (inline or BlobRef). Enables:
     *   - Point-in-time reads:  get_at(key, at_seq)
     *   - Full history:         history(key)
     *   - Full-DB rollback:     collect_rollback_targets(target_seq)
     *
     * .akvlog on-disk format:
     *   [32-byte file header: magic "AVLG" + version]
     *   [entry]*
     *
     * Per entry:
     *   entry_len[4]        total bytes (including this field and trailing CRC)
     *   seq[8]
     *   source_node_id[8]
     *   timestamp_ns[8]
     *   flags[1]
     *   key_fp64[8]         SipHash fingerprint (for fast recovery scan)
     *   key_len[2]
     *   value_len[4]
     *   key[key_len]
     *   value[value_len]
     *   crc32c[4]           CRC32C of the full entry with crc32c field = 0
     *
     * Thread-safety: All public methods are thread-safe.
     */
    class VersionLog {
        public:
            /**
             * Opens (or creates) a VersionLog at opts.log_path.
             * If the file already exists, recovers the in-memory index from it.
             *
             * @throws std::runtime_error if the file cannot be opened.
             */
            [[nodiscard]] static std::unique_ptr<VersionLog> create(VersionLogOptions opts);

            ~VersionLog();

            VersionLog(const VersionLog&) = delete;
            VersionLog& operator=(const VersionLog&) = delete;
            VersionLog(VersionLog&&) = delete;
            VersionLog& operator=(VersionLog&&) = delete;

            // ── Write path ────────────────────────────────────────────────────

            /**
             * Appends a version entry to the log and updates the in-memory index.
             * Thread-safe: multiple shards may call concurrently.
             */
            void append(std::span<const uint8_t> key,
                        uint64_t seq,
                        uint64_t source_node_id,
                        uint64_t timestamp_ns,
                        uint8_t  flags,
                        std::span<const uint8_t> value);

            // ── Query path ────────────────────────────────────────────────────

            /**
             * Returns the latest VersionEntry for key with seq ≤ at_seq,
             * or nullopt if no such entry exists.
             */
            [[nodiscard]] std::optional<VersionEntry>
                get_at(std::span<const uint8_t> key, uint64_t at_seq) const;

            /**
             * Returns all VersionEntries for key, sorted ascending by seq.
             * Empty if the key was never written.
             */
            [[nodiscard]] std::vector<VersionEntry>
                history(std::span<const uint8_t> key) const;

            // ── Rollback support ──────────────────────────────────────────────

            /**
             * Identifies all keys that have at least one VersionEntry with
             * seq > target_seq, and for each returns the key bytes paired with
             * the "previous state" (max entry with seq ≤ target_seq, or nullopt
             * if the key did not exist at target_seq).
             *
             * Used by AkkEngine::rollback_to() to determine what to restore.
             */
            [[nodiscard]]
            std::vector<std::pair<std::vector<uint8_t>, std::optional<VersionEntry>>>
                collect_rollback_targets(uint64_t target_seq) const;

            // ── Compaction / pruning ──────────────────────────────────────────

            /**
             * Removes all history entries whose seq is strictly less than
             * seq_threshold.  Entries at or above the threshold are kept.
             *
             * Implementation: rewrites the live entries to a side-car temp file
             * (.akvlog.tmp), then atomically renames it over the main file and
             * rebuilds the in-memory index.  The operation holds the combined
             * lock throughout, so concurrent reads block until prune completes.
             *
             * @param seq_threshold  Entries with seq < seq_threshold are discarded.
             * @return               Number of entries removed across all keys.
             */
            size_t prune_before(uint64_t seq_threshold);

            // ── Lifecycle ─────────────────────────────────────────────────────

            /// Flushes and closes the file handle. Idempotent.
            void close();

        private:
            VersionLog() = default;

            class Impl;
            std::unique_ptr<Impl> impl_;
    };

} // namespace akkaradb::engine::vlog
