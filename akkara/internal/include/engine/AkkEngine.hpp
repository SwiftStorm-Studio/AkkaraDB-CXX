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

// internal/include/engine/AkkEngine.hpp
#pragma once

#include "engine/memtable/MemTable.hpp"
#include "engine/vlog/VersionLog.hpp"
#include "engine/wal/WalWriter.hpp"
#include "engine/Compression.hpp"
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace akkaradb::engine {
    // SyncMode lives in the wal namespace; re-export for user convenience.
    using wal::SyncMode;

    // VLogSyncMode re-exported for user convenience.
    using vlog::VLogSyncMode;

    // VersionEntry re-exported from vlog namespace for convenience.
    using VersionEntry = vlog::VersionEntry;

    // ============================================================================
    // AkkEngineOptions
    // ============================================================================

    /**
     * AkkEngineOptions - Top-level configuration for AkkEngine.
     *
     * Covers the full operating range from pure in-memory cache to
     * durable network-facing database. Fields are grouped by concern.
     *
     * Typical configurations:
     *
     *   // Embedded cache (no persistence, no WAL)
     *   AkkEngineOptions opts;
     *   opts.wal_enabled = false;
     *   opts.memtable.threshold_bytes_per_shard = 32ULL << 20; // 32 MiB
     *
     *   // WAL-only cache (no large-value blobs, no manifest)
     *   AkkEngineOptions opts;
     *   opts.wal.wal_dir      = "/fast/wal";
     *   opts.blob_enabled     = false;
     *   opts.manifest_enabled = false;
     *
     *   // Local durable KV store
     *   AkkEngineOptions opts;
     *   opts.data_dir         = "/var/lib/akkaradb";
     *   opts.wal.sync_mode    = SyncMode::Sync;
     *
     *   // High-throughput network DB (async WAL, large memtable)
     *   AkkEngineOptions opts;
     *   opts.data_dir              = "/data/akkaradb";
     *   opts.wal.sync_mode         = SyncMode::Async;
     *   opts.wal.group_n           = 1024;
     *   opts.wal.group_micros      = 200;
     *   opts.memtable.threshold_bytes_per_shard = 128ULL << 20; // 128 MiB
     */
    struct AkkEngineOptions {
        // ── Storage ──────────────────────────────────────────────────────────

        /**
         * Root directory for all persistent data.
         *
         * Used as the base for component-specific subdirectories when their
         * individual paths are not set explicitly:
         *   - WAL:      data_dir / "wal"           (unless wal.wal_dir is set)
         *   - Blob:     data_dir / "blobs"          (unless blob_dir is set)
         *   - Manifest: data_dir / "manifest.akmf"  (unless manifest_path is set)
         *
         * Required when wal_enabled == true and any component path is empty.
         * Safe to leave empty when wal_enabled == false, or when all active
         * component paths are set explicitly.
         */
        std::filesystem::path data_dir;

        // ── Component toggles ─────────────────────────────────────────────────

        /**
         * When false, the WAL is completely disabled. No WAL files are created
         * and it is safe to leave data_dir (and wal.wal_dir) empty.
         * Default: true
         */
        bool wal_enabled = true;

        /**
         * When false, the BlobManager is not started. All values are stored
         * inline in the MemTable regardless of size (no externalization to
         * .blob files). Suitable when values are always small.
         * Ignored when wal_enabled == false.
         * Default: true
         */
        bool blob_enabled = true;

        /**
         * When false, the Manifest is not created or written. WAL compaction
         * checkpoints are skipped. Suitable for ephemeral or cache-only setups
         * where WAL truncation is not needed.
         * Ignored when wal_enabled == false.
         * Default: true
         */
        bool manifest_enabled = true;

        // ── Component-specific paths ──────────────────────────────────────────

        /**
         * Directory for .blob files (large externalized values).
         * Auto-set to data_dir / "blobs" when empty.
         * Only used when blob_enabled == true.
         */
        std::filesystem::path blob_dir;

        /**
         * Full path to the manifest file.
         * Auto-set to data_dir / "manifest.akmf" when empty.
         * Only used when manifest_enabled == true.
         */
        std::filesystem::path manifest_path;

        // ── Concurrency ───────────────────────────────────────────────────────

        /**
         * Expected number of concurrent writer threads.
         *
         * When > 0 and the respective shard_count is 0 (auto), shard counts
         * for MemTable and WAL are derived from this value targeting ~80%
         * collision-free probability (birthday-paradox formula):
         *
         *   S = next_pow2( ceil( N(N-1) * 2.25 ) )
         *
         * Approximate results:
         *   writer_threads = 2  →  8  MemTable shards,  8  WAL shards
         *   writer_threads = 4  →  32 MemTable shards,  32 WAL shards
         *   writer_threads = 8  →  128 MemTable shards, 64 WAL shards (capped)
         *
         * Caps: MemTable ≤ 256, WAL ≤ 64.
         * 0 = use hardware_concurrency-based auto (existing behaviour).
         */
        uint32_t writer_threads = 0;

        // ── MemTable ─────────────────────────────────────────────────────────

        /**
         * MemTable tuning. shard_count defaults to 0 (auto) and
         * threshold_bytes_per_shard defaults to 64 MiB.
         * on_flush is wired internally by AkkEngine and should be left null here.
         */
        memtable::MemTable::Options memtable;

        // ── WAL ──────────────────────────────────────────────────────────────

        /**
         * Fine-grained WAL tuning (sync policy, batch sizes, segment rotation).
         * wal.sync_mode controls durability vs. throughput trade-off.
         * wal.wal_dir is auto-set to data_dir / "wal" if left empty.
         * Ignored when wal_enabled == false.
         */
        wal::WalOptions wal;

        // ── Blob ─────────────────────────────────────────────────────────────

        /**
         * Size threshold above which values are externalized to .blob files.
         * Values >= blob_threshold_bytes are stored as BlobRefs in WAL/MemTable;
         * smaller values are stored inline.
         * Default: 16 KiB (matches BlobManager::DEFAULT_THRESHOLD).
         * Only meaningful when blob_enabled == true and wal_enabled == true.
         */
        uint64_t blob_threshold_bytes = 16ULL * 1024;

        // ── Memory ───────────────────────────────────────────────────────────

        /**
         * Soft cap on total in-memory bytes (MemTable active + immutables).
         * When exceeded, write pressure is applied (future: backpressure / eviction).
         * 0 = unlimited.
         * Default: 0
         * NOTE: Not yet enforced — reserved for Phase 5 backpressure implementation.
         */
        size_t max_memory_bytes = 0;

        // ── Version Log ──────────────────────────────────────────────────────

        /**
         * When true, every put() and remove() is also recorded in a per-key
         * version log (.akvlog file).  This enables:
         *   - get_at(key, seq)   — point-in-time reads
         *   - history(key)       — full write history with node attribution
         *   - rollback_to(seq)   — restore the entire DB to a past state
         *   - rollback_key(...)  — restore a single key to a past state
         *
         * The log is persisted to:
         *   data_dir / "history.akvlog"   (auto-derived from data_dir)
         *
         * Default: false  (no overhead when version history is not needed)
         */
        bool version_log_enabled = false;

        /**
         * Fsync policy for version log writes.
         *
         *   Sync  — fdatasync() after every append().  Maximum durability.
         *   Async — fflush() only (OS handles writeback).  Low overhead.
         *           Safe: if the OS loses the latest vlog entries before a crash,
         *           they are re-derived from WAL on the next open() call.
         *   Off   — No stdio flush.  For non-critical / test scenarios.
         *
         * Only meaningful when version_log_enabled == true.
         * Default: Async
         */
        VLogSyncMode version_log_sync_mode = VLogSyncMode::Async;

        // ── SST ──────────────────────────────────────────────────────────────

        /**
         * Maximum number of L0 SST files before triggering L0 → L1 compaction.
         * Only meaningful when wal_enabled and manifest_enabled are both true
         * and data_dir is non-empty.
         * Default: 4
         */
        size_t max_l0_sst_files = 4;

        /**
         * Bloom filter bits per key for SST files.
         * Higher values reduce false-positive rates at the cost of more memory.
         * 0 = disable Bloom filter.
         * Default: 10 (≈1% false-positive rate)
         */
        size_t sst_bloom_bits_per_key = 10;

        /**
         * Maximum number of SST levels (L0 + up to max_sst_levels-1 sorted levels).
         * Level budgets: L1 = sst_l1_max_bytes, L(n) = L1 * multiplier^(n-1).
         * Default: 7  (supports up to ~64PB before the deepest level is exhausted)
         */
        int max_sst_levels = 7;

        /**
         * Total byte budget for L1 SST files.
         * L2 = sst_l1_max_bytes * sst_level_size_multiplier, etc.
         * Default: 64 MiB
         */
        uint64_t sst_l1_max_bytes = 64ULL * 1024 * 1024;

        /**
         * Size multiplier between consecutive SST levels.
         * Default: 10.0  (L1=64MB, L2=640MB, L3=6.4GB, ...)
         */
        double sst_level_size_multiplier = 10.0;

        /**
         * Maximum size of a single output SST file produced during compaction.
         * Merged output is split into multiple files of at most this size.
         * Default: 64 MiB
         */
        uint64_t sst_target_file_size = 64ULL * 1024 * 1024;

        /**
         * When true, a record fetched from SST on a MemTable miss is promoted
         * back into the active MemTable (bypassing WAL).  Subsequent reads for
         * the same hot key are served directly from memory without touching SST.
         *
         * Trade-offs:
         *   + Repeated reads of cold keys become O(1) after the first SST hit.
         *   - Each promoted record consumes MemTable space and may induce an
         *     extra shard seal/flush cycle (write amplification on the read path).
         *   - Blob-externalised values (FLAG_BLOB) are NOT promoted; only the
         *     inline BlobRef (20 B) would be written, not the value itself.
         *
         * Recommended for read-heavy workloads with repeated key access patterns.
         * Default: false
         */
        bool sst_promote_reads = false;

        // ── Compression ───────────────────────────────────────────────────────

        /**
         * Compression codec for SST data sections (index and bloom filter are always
         * stored uncompressed for fast open-time loading).
         *
         *   Codec::None — No compression (default). Maximum read speed, full backward
         *                 compatibility with existing .aksst files.
         *   Codec::Zstd — Zstandard compression. Typically 50-70% size reduction for
         *                 KV workloads. Decompression happens once on SST open();
         *                 subsequent reads are served from the in-memory buffer.
         *
         * Each file is self-describing (codec stored in SSTFileHeader::flags), so
         * compressed and uncompressed files can coexist in the same database.
         * Default: Codec::None
         */
        Codec sst_codec = Codec::None;

        /**
         * Compression codec for .blob files (large externalized values).
         *
         *   Codec::None — No compression (default).
         *   Codec::Zstd — Zstandard compression applied to blob content before writing.
         *                 The checksum stored in BlobRef is computed over the UNCOMPRESSED
         *                 content, so it remains valid regardless of codec.
         *
         * Each blob file is self-describing (BlobFileHeader::flags stores the codec),
         * so old uncompressed blobs and new compressed blobs can coexist.
         * Only meaningful when blob_enabled == true.
         * Default: Codec::None
         */
        Codec blob_codec = Codec::None;

        // ── Write backpressure ────────────────────────────────────────────────

        /**
         * L0 write-stall threshold: put() blocks when the L0 SST file count
         * reaches this value.  0 = auto (max_l0_sst_files * 2).
         * Set to INT_MAX to disable stalling entirely.
         * Default: 0 (auto)
         */
        int sst_l0_stall_files = 0;

        // ── Version log retention ─────────────────────────────────────────────

        /**
         * When true, prune_before() is called on close() to trim the VersionLog.
         * Keeps only the last version_log_keep_seqs sequence numbers of history.
         * Only meaningful when version_log_enabled == true.
         * Default: true
         */
        bool version_log_prune_on_close = true;

        /**
         * Number of sequence numbers of history to retain when pruning.
         * Entries with seq < (last_seq - version_log_keep_seqs) are removed.
         * Default: 1,000,000
         */
        uint64_t version_log_keep_seqs = 1'000'000;

        // ── API Server ────────────────────────────────────────────────────

        /**
         * Transport backends available for the public data API.
         * Add one or more to ApiOptions::backends to enable simultaneous access.
         */
        enum class ApiBackend : uint8_t {
            Http = 0,
            ///< HTTP/1.1 REST  (POST /v1/put, GET /v1/get, ...)
            Tcp = 1,
            ///< AkkaraDB binary TCP protocol
            // Grpc = 2,  // TODO Phase 6
        };

        /**
         * Configuration for the public data API server.
         *
         * Master switch: enabled = false → nothing starts (backends is ignored).
         * Backends list: which transports to start; both may run simultaneously.
         *
         * Example — HTTP only (default when enabled):
         *   opts.api.enabled  = true;
         *
         * Example — binary TCP only:
         *   opts.api.enabled  = true;
         *   opts.api.backends = { AkkEngineOptions::ApiBackend::Tcp };
         *
         * Example — both:
         *   opts.api.enabled  = true;
         *   opts.api.backends = { AkkEngineOptions::ApiBackend::Http,
         *                         AkkEngineOptions::ApiBackend::Tcp };
         */
        struct ApiOptions {
            bool enabled = false;
            std::vector<ApiBackend> backends = {ApiBackend::Http};

            uint16_t http_port = 7070;
            uint16_t tcp_port = 7071;
            // uint16_t              grpc_port      = 7072;  // TODO Phase 6

            size_t worker_threads = 4; ///< Shared across all backends
        } api;
    };

    // ============================================================================
    // AkkEngine
    // ============================================================================

    /**
     * AkkEngine - Core storage engine: MemTable + WAL + (future) SSTable compaction.
     *
     * Lifecycle:
     *   1. Build an AkkEngineOptions struct.
     *   2. Call AkkEngine::open(options) — recovers WAL, wires MemTable flush → SST.
     *   3. Use put / get / remove / range_scan.
     *   4. Destroy (or call close()) to flush and shut down cleanly.
     *
     * Thread-safety: All public methods are thread-safe.
     *
     * Note: Implementation is pending. This header establishes the public contract
     * so callers and tests can be written against it while the Impl is built.
     */
    class AkkEngine {
        public:
            // ── Range scan iterator ───────────────────────────────────────────

            /**
             * Forward iterator over a key range returned by AkkEngine::scan().
             *
             * Merges live MemTable records and SST records in lexicographic order.
             * Tombstones are suppressed; only live (key, value) pairs are yielded.
             * Blob-externalised values are resolved via BlobManager automatically.
             *
             * Usage:
             *   auto it = eng->scan(start, end);
             *   while (it.has_next()) {
             *       auto [key, val] = *it.next();
             *       // process...
             *   }
             */
            class ScanIterator {
                public:
                    ~ScanIterator();
                    ScanIterator(ScanIterator&&) noexcept;
                    ScanIterator& operator=(ScanIterator&&) noexcept;
                    ScanIterator(const ScanIterator&) = delete;
                    ScanIterator& operator=(const ScanIterator&) = delete;

                    /// Returns true if next() will return a non-null value.
                    [[nodiscard]] bool has_next() const noexcept;

                    /// Returns the next {key, value} pair, or nullopt when exhausted.
                    [[nodiscard]] std::optional<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> next();

                private:
                    friend class AkkEngine;
                    class Impl;
                    explicit ScanIterator(std::unique_ptr<Impl> impl);
                    std::unique_ptr<Impl> impl_;
            };

            /**
             * Opens (or creates) an engine instance from the given options.
             *
             * - Fills empty component paths from data_dir.
             * - When wal_enabled: replays WAL into MemTable, then starts WAL writer.
             * - When blob_enabled: starts BlobManager (large-value externalization).
             * - When manifest_enabled: starts Manifest (WAL checkpoint tracking).
             * - When !wal_enabled: starts with an empty MemTable.
             *
             * @throws std::runtime_error if required paths cannot be created or WAL replay fails
             */
            [[nodiscard]] static std::unique_ptr<AkkEngine> open(AkkEngineOptions options);

            ~AkkEngine();

            AkkEngine(const AkkEngine&) = delete;
            AkkEngine& operator=(const AkkEngine&) = delete;
            AkkEngine(AkkEngine&&) = delete;
            AkkEngine& operator=(AkkEngine&&) = delete;

            // ── Write path ────────────────────────────────────────────────────

            /**
             * Inserts or updates a key-value pair.
             *
             * Write order: WAL append → MemTable put.
             * When wal.sync_mode == Sync, blocks until fdatasync() completes.
             */
            void put(std::span<const uint8_t> key, std::span<const uint8_t> value);

            /**
             * Inserts a tombstone (logical delete).
             *
             * The key is not immediately removed from memory; it will be compacted
             * away during SSTable compaction (future).
             */
            void remove(std::span<const uint8_t> key);

            // ── Read path ─────────────────────────────────────────────────────

            /**
             * Point lookup: MemTable → (future) SSTable.
             *
             * @return Value bytes, or std::nullopt if the key is not found or is deleted.
             */
            [[nodiscard]] std::optional<std::vector<uint8_t>> get(std::span<const uint8_t> key) const;

            /**
             * Existence check — no value copy, no blob I/O.
             *
             * Significantly faster than get() when the stored value is large or
             * externalized to a .blob file: BlobManager::read() is skipped entirely.
             * For inline values the saving is the value memcpy.
             *
             * Search order: MemTable → SST (same as get()).
             * A tombstone at any level returns false immediately.
             *
             * @return true  if the key exists as a live (non-tombstone) record.
             *         false if absent or deleted.
             */
            [[nodiscard]] bool exists(std::span<const uint8_t> key) const;

            /**
             * Counts live records in [start_key, end_key).
             *
             * Performs the full MemTable + SST merge with tombstone suppression,
             * but skips value allocation and blob I/O on every record.
             *
             * @param start_key Inclusive lower bound (empty = beginning of keyspace).
             * @param end_key   Exclusive upper bound (empty = end of keyspace).
             * @return          Number of live key-value pairs in the range.
             */
            [[nodiscard]] size_t count(std::span<const uint8_t> start_key = {}, std::span<const uint8_t> end_key = {}) const;

            /**
             * Zero-allocation point lookup for hot paths.
             *
             * Copies value bytes directly into the caller-supplied buffer.
             * When out.capacity() >= value_size, vector::assign() is an in-place memcpy:
             * no heap allocation on the hot path. Pre-reserve out before the loop:
             *
             *   std::vector<uint8_t> buf;
             *   buf.reserve(expected_value_size);
             *   for (...) eng->get_into(key, buf);
             *
             * Blob values (blob_enabled == true): this function falls back to the
             * standard get() path and writes the result into out.
             *
             * @param key Key to look up
             * @param out Receives value bytes on success; capacity is reused.
             * @return true if the key exists and is not deleted, false otherwise.
             */
            [[nodiscard]] bool get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const;

            // ── Version history ───────────────────────────────────────────────

            /**
             * Returns the value that key held at the given sequence number, i.e.
             * the latest write with seq ≤ at_seq.
             *
             * Requires version_log_enabled == true.
             * If FLAG_BLOB was set at that seq, the blob is read from BlobManager.
             *
             * @return Value bytes, or nullopt if the key was not found / was deleted.
             */
            [[nodiscard]] std::optional<std::vector<uint8_t>> get_at(std::span<const uint8_t> key, uint64_t at_seq) const;

            /**
             * Returns the full write history for a key, sorted ascending by seq.
             * Each entry includes: seq, source_node_id, timestamp_ns, flags, value.
             *
             * Requires version_log_enabled == true.
             * Returns empty vector when version logging is disabled or the key
             * was never written.
             */
            [[nodiscard]] std::vector<VersionEntry> history(std::span<const uint8_t> key) const;

            /**
             * Rolls back the entire database to the state it was in at target_seq.
             *
             * All keys that were written or deleted after target_seq are restored
             * to their value at target_seq (or removed if they did not exist then).
             * The rollback writes are themselves logged as new WAL + VersionLog
             * entries (attributed to source_node_id = ROLLBACK_NODE).
             *
             * Requires version_log_enabled == true.
             */
            void rollback_to(uint64_t target_seq);

            /**
             * Rolls back a single key to the state it was in at target_seq.
             *
             * Requires version_log_enabled == true.
             */
            void rollback_key(std::span<const uint8_t> key, uint64_t target_seq);

            // ── Range scan ────────────────────────────────────────────────────

            /**
             * Returns a forward iterator over [start_key, end_key) in lexicographic
             * order, merging live MemTable and SST records.
             *
             * Snapshot semantics: reflects the state at the moment of the call.
             * Tombstones are hidden; only live key-value pairs are yielded.
             * Blob values are resolved transparently.
             *
             * @param start_key  Inclusive lower bound (empty = beginning of keyspace).
             * @param end_key    Exclusive upper bound (empty = end of keyspace).
             */
            [[nodiscard]] ScanIterator scan(std::span<const uint8_t> start_key = {}, std::span<const uint8_t> end_key = {}) const;

            // ── Durability ────────────────────────────────────────────────────

            /**
             * Forces all pending WAL writes to be fdatasync'd.
             * No-op when wal_enabled == false.
             * Useful after a bulk load with wal.sync_mode == Async or Off.
             */
            void force_sync();

            /**
             * Flushes all MemTable shards and blocks until complete.
             * Intended for graceful shutdown or checkpoint creation.
             */
            void force_flush();

            /**
             * Closes the engine: force_flush, force_sync, then shuts down all threads.
             * Idempotent — safe to call multiple times.
             * The destructor calls close() automatically.
             */
            void close();

        private:
            AkkEngine() = default;

            class Impl;
            std::unique_ptr<Impl> impl_;
    };

} // namespace akkaradb::engine
