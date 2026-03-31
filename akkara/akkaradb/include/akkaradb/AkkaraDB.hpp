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

// akkaradb/AkkaraDB.hpp
#pragma once

#include "Export.hpp"
#include "PackedTable.hpp"
#include "engine/AkkEngine.hpp"
#include <filesystem>
#include <memory>
#include <string>

namespace akkaradb {

    // =========================================================================
    // StartupMode — preset operating profiles
    // =========================================================================

    /**
     * Operating mode presets for AkkaraDB::open().
     *
     * Each preset configures the underlying AkkEngine with settings tuned for
     * a particular use-case. Individual settings can still be overridden via
     * AkkaraDB::Options::overrides after the preset is applied.
     *
     *   ULTRA_FAST — Pure in-memory.  No WAL, no blobs, no persistence.
     *                Data is lost on process exit.  Maximum throughput/latency.
     *                Typical use: cache, session store, transient processing.
     *
     *   FAST       — Async WAL (fire-and-forget).  Data survives normal shutdowns
     *                but not hard crashes.  Read-through SST promotion enabled,
     *                larger MemTable.
     *                Typical use: high-throughput write-heavy workloads, message
     *                queues, event streams where occasional data loss is tolerable.
     *
     *   NORMAL     — Default AkkEngine settings.  Async WAL, bloom filters,
     *                conservative memory limits.
     *                Typical use: general-purpose embedded KV store.
     *
     *   DURABLE    — Sync WAL (fdatasync on every write).  Version log enabled
     *                for full write history.  Safest against hard crashes.
     *                Typical use: financial records, audit logs, primary source
     *                of truth databases.
     */
    enum class StartupMode {
        ULTRA_FAST,
        FAST,
        NORMAL,
        DURABLE,
    };

    // =========================================================================
    // AkkaraDB — high-level engine wrapper
    // =========================================================================

    /**
     * AkkaraDB — Entry point for the AkkaraDB high-level C++ API.
     *
     * Wraps AkkEngine with:
     *   - StartupMode presets that cover common operating profiles.
     *   - A table() factory that returns namespace-isolated PackedTable<T, ID>
     *     instances sharing the same storage engine.
     *   - Direct engine access for cases requiring low-level control.
     *
     * Lifecycle:
     *   1. auto db = AkkaraDB::open("/var/lib/myapp/db");
     *   2. auto users = db->table<User, uint64_t>("users", serialize, deserialize);
     *   3. users->put(42, {.name = "Alice"});
     *   4. auto alice = users->get(42);
     *   5. db->close();  // or let db go out of scope
     *
     * Thread-safety: AkkaraDB::engine() IS thread-safe.
     *   PackedTable is NOT thread-safe — use one per thread or synchronize.
     */
    class AKDB_API AkkaraDB {
        public:
        // ── Options ──────────────────────────────────────────────────────────

        /**
         * Options for AkkaraDB::open().
         *
         * The mode preset is applied first; then the overrides struct is merged on
         * top, allowing targeted per-field customization without overriding the
         * entire engine configuration.
         *
         * Override fields with std::optional — only set fields override the preset:
         *
         *   AkkaraDB::Options opts;
         *   opts.data_dir = "/data/db";
         *   opts.mode     = StartupMode::FAST;
         *   opts.overrides.memtable_threshold_per_shard = 256ULL << 20; // 256 MiB
         *   auto db = AkkaraDB::open(std::move(opts));
         */
        struct Options {
            std::filesystem::path data_dir;
            StartupMode           mode = StartupMode::NORMAL;

            struct Overrides {
                /// Per-shard MemTable flush threshold.  0 = use mode default.
                std::optional<size_t>                               memtable_threshold_per_shard;
                /// Force-enable or force-disable version logging.
                std::optional<bool>                                 version_log_enabled;
                /// SST compression codec.
                std::optional<engine::Codec>                        sst_codec;
                /// Blob compression codec.
                std::optional<engine::Codec>                        blob_codec;
                /// Externalize values >= this size to blob files.
                std::optional<uint64_t>                             blob_threshold_bytes;
                /// Promote SST-fetched records into MemTable for hot-key acceleration.
                std::optional<bool>                                 sst_promote_reads;
                /// Bloom filter bits per key (0 = disabled).
                std::optional<size_t>                               sst_bloom_bits_per_key;
                /// Maximum L0 SST files before compaction triggers.
                std::optional<size_t>                               max_l0_sst_files;
            } overrides;
        };

        // ── Factory ──────────────────────────────────────────────────────────

        /**
         * Opens (or creates) a database at the given directory using the specified mode.
         *
         * @throws std::runtime_error if the directory cannot be created or WAL replay fails.
         */
        [[nodiscard]] static std::unique_ptr<AkkaraDB>
        open(std::filesystem::path data_dir,
             StartupMode           mode = StartupMode::NORMAL);

        /**
         * Opens a database using a full Options bundle.
         */
        [[nodiscard]] static std::unique_ptr<AkkaraDB> open(Options opts);

        ~AkkaraDB();
        AkkaraDB(const AkkaraDB&)             = delete;
        AkkaraDB& operator=(const AkkaraDB&)  = delete;
        AkkaraDB(AkkaraDB&&)                  = delete;
        AkkaraDB& operator=(AkkaraDB&&)       = delete;

        /**
         * Flushes and shuts down the engine cleanly.
         * The destructor calls close() automatically.
         */
        void close();

        // ── Engine access ─────────────────────────────────────────────────────

        /**
         * Returns a reference to the underlying AkkEngine for low-level access.
         *
         * The returned reference is valid for the lifetime of this AkkaraDB instance.
         */
        [[nodiscard]] engine::AkkEngine&       engine() noexcept;
        [[nodiscard]] const engine::AkkEngine& engine() const noexcept;

        // ── Table factory ─────────────────────────────────────────────────────

        /**
         * Opens a typed table backed by BinPack serialization.
         *
         * PrimaryKeyPtr is a member object pointer that identifies the primary
         * key field. The entity type and key type are inferred automatically:
         *
         *   auto users = db->table<&User::id>("users");
         *
         * Secondary indexes are added via the fluent .index<>() builder:
         *
         *   auto users = db->table<&User::id>("users")
         *                    .index<&User::email>()
         *                    .index<&User::age>();
         *
         * Serialization is zero-config: any aggregate struct whose fields are
         * supported by BinPack (primitives, std::string, std::optional<T>,
         * std::vector<T>, enum, nested aggregates, ...) works without any
         * specialization. For custom types, specialize TypeAdapter<T>.
         *
         * The returned PackedTable holds a raw pointer to engine(); ensure this
         * AkkaraDB instance outlives the table.
         */
        template <auto PrimaryKeyPtr>
        [[nodiscard]] PackedTable<PrimaryKeyPtr> table(std::string name)
        {
            using Table = PackedTable<PrimaryKeyPtr>;
            Table t;
            t.engine_ = &engine();
            t.table_name_ = std::move(name);
            t.pk_prefix_ = Table::make_table_prefix(t.table_name_);
            return t;
        }

    private:
        AkkaraDB() = default;

        std::unique_ptr<engine::AkkEngine> engine_;
    };

} // namespace akkaradb
