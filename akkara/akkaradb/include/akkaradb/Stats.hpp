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

// akkaradb/Stats.hpp
// Point-in-time engine observability snapshot returned by AkkEngine::stats().
// All counters are accumulated since engine open(); call stats() at any time.
#pragma once

#include "akkaradb/Export.hpp"

#include <cstdint>
#include <vector>

namespace akkaradb::engine {

    // ============================================================================
    // LevelStats — per-SST-level snapshot (index 0 = L0, 1 = L1, ...)
    // ============================================================================

    struct LevelStats {
        int      level        = 0;  ///< 0 = L0, 1 = L1, …
        size_t   file_count   = 0;  ///< Number of SST files at this level
        uint64_t bytes        = 0;  ///< Total on-disk size of all files at this level
        uint64_t budget_bytes = 0;  ///< Byte budget for this level (0 = L0 is count-based)
    };

    // ============================================================================
    // EngineStats — comprehensive point-in-time snapshot of engine health & usage
    //
    // Counters are accumulated since engine open().
    // All values are approximate (relaxed atomics; no global barrier on stats()).
    // ============================================================================

    // C4251: std::vector<> members — safe to suppress; DLL and consumers share CRT.
    #ifdef _MSC_VER
    #pragma warning(push)
    #pragma warning(disable: 4251)
    #endif

    struct AKDB_API EngineStats {

        // ── Identity ─────────────────────────────────────────────────────────────
        /// Current write sequence number (total puts + removes since open).
        uint64_t current_seq = 0;
        /// Persistent node identity from data_dir/node.id.  0 = ephemeral.
        uint64_t node_id     = 0;

        // ── Engine-level operation counts ─────────────────────────────────────────
        /// Total AkkEngine::put() calls (both inline and blob path).
        uint64_t puts_total        = 0;
        /// Total AkkEngine::remove() calls.
        uint64_t removes_total     = 0;
        /// Total AkkEngine::get() + get_into() calls.
        uint64_t gets_total        = 0;
        /// Gets resolved from MemTable (active map or immutables).
        uint64_t gets_memtable_hit = 0;
        /// Gets that fell through to SST and found a result there.
        uint64_t gets_sst_hit      = 0;
        /// Gets not found in any storage layer (key absent or tombstoned).
        uint64_t gets_miss         = 0;
        /// Total AkkEngine::exists() calls.
        uint64_t exists_total      = 0;
        /// Total AkkEngine::scan() calls.
        uint64_t scans_total       = 0;
        /// put() calls that took the blob path (value >= blob_threshold_bytes).
        uint64_t blob_puts_total   = 0;

        // ── MemTable ─────────────────────────────────────────────────────────────

        struct MemTableStats {
            /// Number of shards (always a power-of-2 in [2, 256]).
            uint32_t shard_count              = 0;
            /// Flush trigger: each shard flushes when it exceeds this many bytes.
            uint64_t threshold_bytes_per_shard = 0;
            /// Approximate current in-memory bytes (active + immutables, all shards).
            uint64_t approx_bytes             = 0;
            /// Records inserted since open() (includes WAL recovery replays).
            uint64_t puts_applied             = 0;
            /// Tombstones inserted since open() (includes WAL recovery replays).
            uint64_t removes_applied          = 0;
            /// Number of shard seal→SST flush cycles completed.
            uint64_t flushes_completed        = 0;
            /// Total bytes passed to the SST flush callback across all flushes.
            uint64_t bytes_flushed            = 0;
        } memtable;

        // ── WAL ──────────────────────────────────────────────────────────────────

        struct WalStats {
            /// True when wal_enabled == true in AkkEngineOptions.
            bool     enabled           = false;
            /// Number of WAL shards (power-of-2 in [1, 64]).
            uint32_t shard_count       = 0;
            /// Total WAL entries written to disk (puts + deletes, all shards).
            uint64_t entries_written   = 0;
            /// Total raw bytes written to WAL files (batch headers + entry payloads).
            uint64_t bytes_written     = 0;
            /// Number of batch write operations (one batch = one file_.write() call).
            uint64_t batches_flushed   = 0;
            /// Number of fdatasync() calls across all shards.
            uint64_t syncs_executed    = 0;
            /// Number of segment file rotations (new .akwal file created) across all shards.
            uint64_t segment_rotations = 0;
        } wal;

        // ── Blob ─────────────────────────────────────────────────────────────────

        struct BlobStats {
            /// True when blob_enabled == true and WAL is active.
            bool     enabled             = false;
            /// Values >= threshold_bytes are externalized to .blob files.
            uint64_t threshold_bytes     = 0;
            /// Total .blob files written since open().
            uint64_t blobs_written       = 0;
            /// Total uncompressed content size across all blobs written.
            uint64_t bytes_uncompressed  = 0;
            /// Total stored size on disk (< bytes_uncompressed when Zstd is active).
            uint64_t bytes_on_disk       = 0;
            /// Total .blob files deleted by the GC worker since open().
            uint64_t blobs_deleted       = 0;
            /// Number of GC worker wake-up cycles that processed at least one deletion.
            uint64_t gc_cycles           = 0;
        } blob;

        // ── SST ──────────────────────────────────────────────────────────────────

        struct SstStats {
            /// True when SST is active (wal_enabled + manifest_enabled + data_dir set).
            bool     enabled               = false;
            /// Per-level snapshot; index 0 = L0.
            std::vector<LevelStats> levels;
            /// Total SST files across all levels.
            size_t   file_count            = 0;
            /// Total SST bytes on disk across all levels.
            uint64_t bytes                 = 0;
            /// L0 file count shortcut (same as levels[0].file_count when !levels.empty()).
            size_t   l0_file_count         = 0;
            /// True if a compaction task is queued or running right now.
            bool     compaction_pending    = false;
            /// Total compaction cycles completed since open().
            uint64_t compactions_completed = 0;
            /// Total SST input files consumed by compaction since open().
            uint64_t files_compacted       = 0;
            /// Total input bytes read by all compaction cycles since open().
            uint64_t bytes_compacted_in    = 0;
            /// Total output bytes written by all compaction cycles since open().
            uint64_t bytes_compacted_out   = 0;
            /// Times a put() was blocked waiting for L0 to drain below the stall threshold.
            uint64_t l0_stalls             = 0;
        } sst;

        // ── Version Log ──────────────────────────────────────────────────────────

        struct VLogStats {
            /// True when version_log_enabled == true in AkkEngineOptions.
            bool enabled = false;
        } vlog;
    };

    #ifdef _MSC_VER
    #pragma warning(pop)
    #endif

} // namespace akkaradb::engine
