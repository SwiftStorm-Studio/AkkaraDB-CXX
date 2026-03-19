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

// akkaradb/src/AkkaraDB.cpp
#include "akkaradb/AkkaraDB.hpp"
#include "engine/AkkEngine.hpp"  // transitively includes WalWriter.hpp + VersionLog.hpp

namespace akkaradb {

    // ── StartupMode → AkkEngineOptions preset ────────────────────────────────

    static engine::AkkEngineOptions make_engine_options(
        const std::filesystem::path& data_dir,
        StartupMode                  mode,
        const AkkaraDB::Options::Overrides& ov)
    {
        engine::AkkEngineOptions opts;
        opts.data_dir = data_dir;

        switch (mode) {

        case StartupMode::ULTRA_FAST:
            // Pure in-memory — no disk I/O at all.
            opts.wal_enabled      = false;
            opts.blob_enabled     = false;
            opts.manifest_enabled = false;
            opts.memtable.threshold_bytes_per_shard = 256ULL * 1024 * 1024; // 256 MiB
            break;

        case StartupMode::FAST:
            // Async WAL (data survives clean shutdown, not hard crashes).
            // Larger MemTable, read-through SST promotion for hot-key acceleration.
            opts.wal.sync_mode   = engine::SyncMode::Async;
            opts.wal.group_n     = 512;
            opts.wal.group_micros = 100;
            opts.sst_promote_reads           = true;
            opts.sst_bloom_bits_per_key      = 10;
            opts.memtable.threshold_bytes_per_shard = 128ULL * 1024 * 1024; // 128 MiB
            break;

        case StartupMode::NORMAL:
            // AkkEngine defaults: async WAL, 64 MiB MemTable shards, bloom enabled.
            break;

        case StartupMode::DURABLE:
            // Sync WAL (fdatasync on every write) + full version history.
            opts.wal.sync_mode         = engine::SyncMode::Sync;
            opts.version_log_enabled   = true;
            opts.version_log_sync_mode = engine::VLogSyncMode::Sync;
            break;
        }

        // Apply overrides on top of the preset.
        if (ov.memtable_threshold_per_shard)
            opts.memtable.threshold_bytes_per_shard = *ov.memtable_threshold_per_shard;
        if (ov.version_log_enabled)
            opts.version_log_enabled = *ov.version_log_enabled;
        if (ov.sst_codec)
            opts.sst_codec = *ov.sst_codec;
        if (ov.blob_codec)
            opts.blob_codec = *ov.blob_codec;
        if (ov.blob_threshold_bytes)
            opts.blob_threshold_bytes = *ov.blob_threshold_bytes;
        if (ov.sst_promote_reads)
            opts.sst_promote_reads = *ov.sst_promote_reads;
        if (ov.sst_bloom_bits_per_key)
            opts.sst_bloom_bits_per_key = *ov.sst_bloom_bits_per_key;
        if (ov.max_l0_sst_files)
            opts.max_l0_sst_files = *ov.max_l0_sst_files;

        return opts;
    }

    // ── AkkaraDB::open() ─────────────────────────────────────────────────────

    std::unique_ptr<AkkaraDB> AkkaraDB::open(std::filesystem::path data_dir, StartupMode mode) {
        return open(Options{.data_dir = std::move(data_dir), .mode = mode});
    }

    std::unique_ptr<AkkaraDB> AkkaraDB::open(Options opts) {
        auto db = std::unique_ptr<AkkaraDB>(new AkkaraDB());
        auto engine_opts = make_engine_options(opts.data_dir, opts.mode, opts.overrides);
        db->engine_ = engine::AkkEngine::open(std::move(engine_opts));
        return db;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    AkkaraDB::~AkkaraDB() { close(); }

    void AkkaraDB::close() {
        if (engine_) {
            engine_->close();
            engine_.reset();
        }
    }

    // ── Engine access ─────────────────────────────────────────────────────────

    engine::AkkEngine& AkkaraDB::engine() noexcept {
        return *engine_;
    }

    const engine::AkkEngine& AkkaraDB::engine() const noexcept {
        return *engine_;
    }

} // namespace akkaradb
