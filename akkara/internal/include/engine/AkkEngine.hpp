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
    // ============================================================================
    // SyncMode
    // ============================================================================

    /**
     * SyncMode - Controls how writes are persisted to the WAL.
     *
     * Only meaningful when AkkEngineOptions::wal_enabled == true.
     */
    enum class SyncMode : uint8_t {
        /**
         * fdatasync() after every write batch.
         * Maximum durability: survives process crash and OS crash.
         * Default.
         */
        Sync,

        /**
         * Background flusher writes without blocking the caller.
         * Higher throughput; durability guaranteed only after the next Sync
         * or an explicit force_sync() call.
         * Survives process crash if the OS flushes its page cache.
         */
        Async,

        /**
         * WAL writes are disabled entirely (wal_enabled is ignored).
         * Data lives only in memory; all writes are lost on process exit.
         * Intended for pure in-memory cache use cases.
         */
        Off,
    };

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
     *   opts.sync_mode   = SyncMode::Off;
     *   opts.memtable.threshold_bytes_per_shard = 32ULL << 20; // 32 MiB
     *
     *   // Local durable KV store
     *   AkkEngineOptions opts;
     *   opts.data_dir  = "/var/lib/akkaradb";
     *   opts.sync_mode = SyncMode::Sync;
     *
     *   // High-throughput network DB (async WAL, large memtable)
     *   AkkEngineOptions opts;
     *   opts.data_dir  = "/data/akkaradb";
     *   opts.sync_mode = SyncMode::Async;
     *   opts.memtable.threshold_bytes_per_shard = 128ULL << 20; // 128 MiB
     *   opts.wal.group_n      = 1024;
     *   opts.wal.group_micros = 200;
     */
    struct AkkEngineOptions {
        // ── Storage ──────────────────────────────────────────────────────────

        /**
         * Root directory for all persistent data.
         * - WAL files go to  data_dir / "wal"  (unless wal.wal_dir is set explicitly).
         * - SSTable files will go to  data_dir / "sst"  (future).
         *
         * If wal_enabled == false this field is unused.
         * Required when wal_enabled == true.
         */
        std::filesystem::path data_dir;

        // ── Durability ───────────────────────────────────────────────────────

        /**
         * When false the WAL is completely disabled.
         * Equivalent to sync_mode == SyncMode::Off but also skips WAL
         * file creation, making it safe to leave data_dir empty.
         * Default: true
         */
        bool wal_enabled = true;

        /**
         * Controls how writes are persisted. See SyncMode enum for details.
         * Ignored when wal_enabled == false.
         * Default: SyncMode::Sync
         */
        SyncMode sync_mode = SyncMode::Sync;

        // ── MemTable ─────────────────────────────────────────────────────────

        /**
         * MemTable tuning. shard_count defaults to 0 (auto) and
         * threshold_bytes_per_shard defaults to 64 MiB.
         * on_flush is wired internally by AkkEngine and should be left null here.
         */
        memtable::MemTable::Options memtable;

        // ── WAL ──────────────────────────────────────────────────────────────

        /**
         * Fine-grained WAL tuning (batch sizes, segment rotation, etc.).
         * wal.wal_dir is auto-set to data_dir / "wal" if left empty.
         * wal.fast_mode is overridden by sync_mode: SyncMode::Async → fast_mode=true,
         * SyncMode::Sync → fast_mode=false.
         * Ignored when wal_enabled == false.
         */
        wal::WalOptions wal;

        // ── Memory ───────────────────────────────────────────────────────────

        /**
         * Soft cap on total in-memory bytes (MemTable active + immutables).
         * When exceeded, write pressure is applied (future: backpressure / eviction).
         * 0 = unlimited.
         * Default: 0
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
             * - Applies defaults (wal.wal_dir, fast_mode from sync_mode).
             * - When wal_enabled: replays WAL into MemTable, then starts writer.
             * - When !wal_enabled: starts with an empty MemTable.
             *
             * @throws std::runtime_error if data_dir cannot be created or WAL replay fails
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
             * When sync_mode == Sync, blocks until fdatasync() completes.
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
             * Useful after a bulk load with sync_mode == Async.
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
