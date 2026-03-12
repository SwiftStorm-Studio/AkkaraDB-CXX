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
#include "engine/wal/WalWriter.hpp"
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>

namespace akkaradb::engine {
    // SyncMode lives in the wal namespace; re-export for user convenience.
    using wal::SyncMode;

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
