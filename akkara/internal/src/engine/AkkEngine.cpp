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

// internal/src/engine/AkkEngine.cpp
#include "engine/AkkEngine.hpp"

#include "engine/blob/BlobFraming.hpp"
#include "engine/blob/BlobManager.hpp"
#include "engine/cluster/ClusterConfig.hpp"
#include "engine/cluster/ClusterManager.hpp"
#include "engine/cluster/ReplicationClient.hpp"
#include "engine/cluster/ReplicationServer.hpp"
#include "engine/manifest/Manifest.hpp"
#include "engine/wal/WalRecovery.hpp"
#include "core/CRC32C.hpp"
#include "core/record/AKHdr32.hpp"

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <unordered_set>
#include <vector>

namespace akkaradb::engine {
    namespace fs = std::filesystem;

    // ============================================================================
    // Internal helpers
    // ============================================================================

    namespace {
        /**
         * Loads a persistent 64-bit node ID from data_dir/node.id.
         * If the file is missing or corrupt, generates a random ID and saves it.
         */
        uint64_t load_or_create_node_id(const fs::path& data_dir) {
            auto id_path = data_dir / "node.id";

            // Try to read existing ID
            {
                std::ifstream f(id_path, std::ios::binary);
                if (f) {
                    uint64_t id = 0;
                    f.read(reinterpret_cast<char*>(&id), sizeof(id));
                    if (f && f.gcount() == static_cast<std::streamsize>(sizeof(id)) && id != 0) return id;
                }
            }

            // Generate a random ID and persist it
            std::random_device rd;
            std::mt19937_64 rng(rd());
            uint64_t id = rng();
            if (id == 0) id = 1; // avoid sentinel 0

            std::ofstream f(id_path, std::ios::binary | std::ios::trunc);
            if (f) f.write(reinterpret_cast<const char*>(&id), sizeof(id));
            return id;
        }
    } // anonymous namespace

    // ============================================================================
    // AkkEngine::Impl
    // ============================================================================

    class AkkEngine::Impl {
        public:
            // ── Options ───────────────────────────────────────────────────────────
            AkkEngineOptions opts_;

            // ── Lifecycle state ───────────────────────────────────────────────────
            std::atomic<bool> closed_{false};

            // ── Core storage (always present) ─────────────────────────────────────
            std::unique_ptr<memtable::MemTable> memtable_;

            // ── Persistent storage (present when wal_enabled) ─────────────────────
            std::unique_ptr<wal::WalWriter> wal_writer_; ///< null if !wal_enabled
            std::unique_ptr<blob::BlobManager> blob_manager_; ///< null if !blob_enabled
            std::unique_ptr<manifest::Manifest> manifest_; ///< null if !manifest_enabled

            // ── Cluster (present when cluster.akcc found in data_dir) ─────────────
            std::unique_ptr<cluster::ClusterManager> cluster_mgr_;
            std::unique_ptr<cluster::ReplicationServer> repl_server_; ///< Primary only
            std::unique_ptr<cluster::ReplicationClient> repl_client_; ///< Replica only

            // ── Replica blob tracking ─────────────────────────────────────────────
            // Tracks seqs for which a ReplBlobPut arrived (blob_callback) but the
            // corresponding ReplEntry (apply_callback) has not yet been processed.
            // Since blob_callback always fires before apply_callback on the same TCP
            // stream, we can determine FLAG_BLOB reliably without changing the wire protocol.
            std::mutex pending_blob_mu_;
            std::unordered_set<uint64_t> pending_blob_seqs_;

            // =========================================================================
            // apply_record — internal write path used by the Replica apply_callback.
            //
            // Writes a record (put OR tombstone, depending on val being empty) into
            // the local WAL + MemTable, using the seq number assigned by the Primary.
            // The MemTable internally advances seq_gen_ to max(seq_gen_, seq+1) so the
            // local counter stays correctly positioned after replication.
            // =========================================================================
            void apply_record(uint64_t seq, std::span<const uint8_t> key, std::span<const uint8_t> val, uint8_t flags = core::AKHdr32::FLAG_NORMAL) {
                const uint64_t fp64 = core::AKHdr32::compute_key_fp64(key.data(), key.size());
                const uint64_t mk = core::AKHdr32::build_mini_key(key.data(), key.size());

                if (val.empty()) {
                    // Tombstone (Primary shipped a remove)
                    if (wal_writer_) wal_writer_->append_delete(key, seq, fp64, mk);
                    memtable_->remove(key, seq);
                }
                else {
                    // Put (may be an inline value or a 20-byte BlobRef).
                    // flags carries FLAG_BLOB when the Replica received a ReplBlobPut
                    // for this seq before the ReplEntry arrived (TCP ordering guarantee).
                    if (wal_writer_) wal_writer_->append_put(key, val, seq, fp64, mk, flags);
                    memtable_->put(key, val, seq, flags, fp64);
                }
            }

            // =========================================================================
            // wire_replica_callbacks — registers blob + apply callbacks on repl_client_.
            //
            // Blob callback: fired when a ReplBlobPut arrives (before the WAL entry).
            //   Writes the blob file via BlobManager.write() (idempotent on retry).
            //
            // Apply callback: fired when a ReplEntry arrives.
            //   Writes the WAL entry + MemTable record using the Primary-assigned seq.
            // =========================================================================
            void wire_replica_callbacks() {
                if (!repl_client_) return;

                repl_client_->set_blob_callback(
                    [this](uint64_t blob_id, std::span<const uint8_t> content) {
                        if (blob_manager_) blob_manager_->write(blob_id, content);
                        // blob_id == seq (Primary uses seq as blob_id in ship_blob).
                        // Record it so apply_callback can set FLAG_BLOB on the matching entry.
                        std::lock_guard lock(pending_blob_mu_);
                        pending_blob_seqs_.insert(blob_id);
                    }
                );

                repl_client_->set_apply_callback(
                    [this](uint64_t seq, wal::WalEntryType /*wal_type*/, std::span<const uint8_t> key, std::span<const uint8_t> val) {
                        uint8_t flags = core::AKHdr32::FLAG_NORMAL;
                        {
                            std::lock_guard lock(pending_blob_mu_);
                            if (pending_blob_seqs_.erase(seq)) { flags = core::AKHdr32::FLAG_BLOB; }
                        }
                        apply_record(seq, key, val, flags);
                    }
                );
            }

            // =========================================================================
            // setup_cluster — creates and starts ReplicationServer (Primary) or
            //                 ReplicationClient (Replica) based on the elected role.
            // =========================================================================
            void setup_cluster(const cluster::ClusterConfig& cfg, uint64_t self_node_id) {
                const auto role = cluster_mgr_->role();

                if (role == cluster::NodeRole::Primary) {
                    // Determine the replication port from ClusterConfig
                    const cluster::NodeInfo* self = cfg.find_by_id(self_node_id);
                    const uint16_t repl_port = self ? self->repl_port : 0;

                    // sync_repl=true mirrors WAL sync semantics (ships + waits for ACK)
                    const bool sync_repl = (opts_.wal.sync_mode == SyncMode::Sync);

                    repl_server_ = cluster::ReplicationServer::create(
                        repl_port,
                        self_node_id,
                        [this]() -> uint64_t { return memtable_->last_seq(); },
                        sync_repl
                    );
                    repl_server_->start();
                }
                else if (role == cluster::NodeRole::Replica) {
                    repl_client_ = cluster::ReplicationClient::create(
                        cluster_mgr_->primary_host(),
                        cluster_mgr_->primary_repl_port(),
                        self_node_id,
                        [this]() -> uint64_t { return memtable_->last_seq(); }
                    );
                    wire_replica_callbacks();
                    repl_client_->start();
                }
                // NodeRole::Standalone — nothing to start
            }
    };

    // ============================================================================
    // AkkEngine::open
    // ============================================================================

    std::unique_ptr<AkkEngine> AkkEngine::open(AkkEngineOptions options) {
        // ── Validate / fill defaults ─────────────────────────────────────────
        // WAL is active whenever wal_enabled == true.
        // SyncMode::Off (wal.sync_mode) means "write to WAL but never fdatasync".
        // To disable WAL entirely, set wal_enabled = false.
        const bool persistent = options.wal_enabled;

        if (persistent) {
            // Determine which active components still need their path derived from data_dir.
            const bool wal_needs_data = options.wal.wal_dir.empty();
            const bool blob_needs_data = options.blob_enabled && options.blob_dir.empty();
            const bool manifest_needs_data = options.manifest_enabled && options.manifest_path.empty();

            if (options.data_dir.empty() && (wal_needs_data || blob_needs_data || manifest_needs_data)) {
                std::string msg = "AkkEngine: data_dir required for:";
                if (wal_needs_data) msg += " wal";
                if (blob_needs_data) msg += " blob";
                if (manifest_needs_data) msg += " manifest";
                throw std::runtime_error(msg);
            }

            // Fill component paths from data_dir where not explicitly set.
            if (wal_needs_data) options.wal.wal_dir = options.data_dir / "wal";
            if (blob_needs_data) options.blob_dir = options.data_dir / "blobs";
            if (manifest_needs_data) options.manifest_path = options.data_dir / "manifest.akmf";
        }

        // ── Allocate objects ─────────────────────────────────────────────────
        auto eng = std::unique_ptr<AkkEngine>(new AkkEngine());
        auto impl = std::make_unique<Impl>();
        impl->opts_ = options;

        // ── Create directories ───────────────────────────────────────────────
        if (persistent) {
            if (!options.data_dir.empty())    fs::create_directories(options.data_dir);
            fs::create_directories(options.wal.wal_dir);
            if (options.blob_enabled)         fs::create_directories(options.blob_dir);
            if (options.manifest_enabled && !options.manifest_path.empty()) fs::create_directories(options.manifest_path.parent_path());
        }

        // ── MemTable (flush callback wired after recovery) ───────────────────
        {
            memtable::MemTable::Options mt_opts = options.memtable;
            mt_opts.on_flush = nullptr; // installed after recovery
            impl->memtable_ = memtable::MemTable::create(mt_opts);
        }

        // ── BlobManager ──────────────────────────────────────────────────────
        if (persistent && options.blob_enabled) {
            impl->blob_manager_ = blob::BlobManager::create(options.blob_dir, options.blob_threshold_bytes);
            impl->blob_manager_->start(); // starts GC thread + startup cleanup
        }

        // ── Manifest ─────────────────────────────────────────────────────────
        if (persistent && options.manifest_enabled) {
            const bool fast_manifest = (options.wal.sync_mode == SyncMode::Async || options.wal.sync_mode == SyncMode::Off);
            impl->manifest_ = manifest::Manifest::create(options.manifest_path, fast_manifest);
            impl->manifest_->start();
        }

        // ── WAL Recovery → MemTable ──────────────────────────────────────────
        // Replay all WAL shards in global write order.  Each record is applied
        // directly into the MemTable.  The MemTable advances its seq_gen_ to
        // max(seq_gen_, seq+1) on every put/remove, so after replay the sequence
        // counter is correctly positioned for the next write.
        if (persistent) {
            auto& mt = *impl->memtable_;

            auto recovery = wal::WalRecovery::create();
            auto result = recovery->replay(
                options.wal.wal_dir,

                // Record handler: put or remove.
                // Pass the original flags (FLAG_BLOB, etc.) stored in the WAL header
                // so the MemTable correctly reconstructs the record type after restart.
                [&](const wal::WalRecordOpRef& ref) {
                    if (ref.is_tombstone()) { mt.remove(ref.key(), ref.seq()); }
                    else { mt.put(ref.key(), ref.value(), ref.seq(), ref.header().flags); }
                },

                // Commit handler: informational only at this stage
                [&](uint64_t /*seq*/, uint64_t /*ts*/) {},

                // Checkpoint handler: optional
                nullptr
            );

            // Soft-tolerate shard errors: valid data before corruption is replayed.
            (void)result;
        }

        // ── BlobManager orphan scan ──────────────────────────────────────────
        // After WAL recovery, any blob whose WAL entry did not survive is an
        // orphan.  For Phase 2+3+4 (no SST yet) we conservatively keep all blobs
        // on startup; full orphan detection requires scanning MemTable records
        // and is deferred until SST compaction is implemented.
        if (persistent && impl->blob_manager_) { impl->blob_manager_->scan_orphans([](uint64_t) { return true; }); }

        // ── WAL Writer ───────────────────────────────────────────────────────
        // Started after recovery so the first seq allocated by the writer is
        // strictly greater than the highest seq replayed.
        if (persistent) { impl->wal_writer_ = wal::WalWriter::create(options.wal); }

        // ── MemTable flush callback ──────────────────────────────────────────
        // SST writing is not yet implemented (Phase 5).
        //
        // Intentionally leaving on_flush = null so the MemTable never seals
        // and discards records.  A no-op callback would silently drop flushed
        // records from memory mid-run (WAL still has them, but get() would
        // return nullopt until the next restart + recovery).
        //
        // With null callback the MemTable holds everything in memory.
        // WAL provides crash-durability; data survives restart via replay.
        // Memory growth is bounded by max_memory_bytes (backpressure: future).
        //
        // TODO (Phase 5): set_flush_callback → write SST, truncate WAL.

        // ── Cluster setup ────────────────────────────────────────────────────
        // If data_dir/cluster.akcc exists, initialise ClusterManager and elect
        // a role (Primary / Replica).  Then start the appropriate replication
        // component (ReplicationServer or ReplicationClient).
        if (persistent && !options.data_dir.empty()) {
            const auto cluster_cfg_path = options.data_dir / "cluster.akcc";
            if (fs::exists(cluster_cfg_path)) {
                auto cfg = cluster::ClusterConfig::load(cluster_cfg_path);
                uint64_t self_nid = load_or_create_node_id(options.data_dir);

                impl->cluster_mgr_ = cluster::ClusterManager::create(options.data_dir, cfg, self_nid);

                // Register a role-change callback for future failover support.
                // (Full dynamic role switching is deferred to Phase 4+.)
                impl->cluster_mgr_->set_role_change_callback(
                    [](cluster::NodeRole /*new_role*/) {
                        // TODO (Phase 4): Dynamically tear down / create
                        //                 ReplicationServer or ReplicationClient
                        //                 when the Primary fails and a Replica
                        //                 wins the re-election.
                    }
                );

                impl->cluster_mgr_->start(); // performs role election
                impl->setup_cluster(cfg, self_nid);
            }
            // No cluster.akcc → Standalone mode; no further cluster setup needed.
        }

        eng->impl_ = std::move(impl);
        return eng;
    }

    // ============================================================================
    // Destructor
    // ============================================================================

    AkkEngine::~AkkEngine() { if (impl_) close(); }

    // ============================================================================
    // put
    // ============================================================================

    void AkkEngine::put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        if (!impl_ || impl_->closed_.load(std::memory_order_relaxed)) throw std::runtime_error("AkkEngine: engine is closed");

        // Allocate a globally-unique sequence number.  On Primary/Standalone,
        // this is the authoritative seq.  On Replica, direct puts are allowed
        // (for local-only operations) but the seq is local, not coordinated.
        const uint64_t seq = impl_->memtable_->next_seq();
        const uint64_t fp64 = core::AKHdr32::compute_key_fp64(key.data(), key.size());
        const uint64_t mk = core::AKHdr32::build_mini_key(key.data(), key.size());

        // ── BLOB path ─────────────────────────────────────────────────────────
        // Values at or above the threshold are externalized to .blob files.
        // The WAL / MemTable stores a compact 20-byte BlobRef in their place:
        //   [blob_id:u64][total_size:u64][checksum:u32]
        //
        // Ordering guarantee:
        //   ship_blob()  must be called BEFORE  ship()  so that the Replica
        //   already has the blob file when it applies the WAL entry.
        //   TCP ordering on the replication stream ensures delivery order.
        if (impl_->blob_manager_ && value.size() >= impl_->blob_manager_->threshold()) {
            // 1. Ship raw blob content to Replicas first (if Primary)
            if (impl_->repl_server_) { impl_->repl_server_->ship_blob(seq, value); }

            // 2. Write blob file locally (seq == WAL seq == blob_id)
            impl_->blob_manager_->write(seq, value);

            // 3. Build 20-byte BlobRef: [blob_id:8][total_size:8][checksum:4]
            const uint32_t cksum = core::CRC32C::compute(value.data(), value.size());
            uint8_t ref_buf[blob::BLOB_REF_SIZE];
            blob::encode_blob_ref(ref_buf, seq, value.size(), cksum);
            const auto ref_span = std::span<const uint8_t>(ref_buf, blob::BLOB_REF_SIZE);

            // 4. WAL + MemTable store the BlobRef (inline, 20 bytes), tagged FLAG_BLOB
            //    so get() can identify them reliably without heuristics.
            if (impl_->wal_writer_) impl_->wal_writer_->append_put(key, ref_span, seq, fp64, mk, core::AKHdr32::FLAG_BLOB);
            impl_->memtable_->put(key, ref_span, seq, core::AKHdr32::FLAG_BLOB, fp64);

            // 5. Ship WAL entry (carrying BlobRef) to Replicas
            if (impl_->repl_server_) impl_->repl_server_->ship(seq, wal::WalEntryType::Record, key, ref_span);

            return;
        }

        // ── Inline path ───────────────────────────────────────────────────────
        if (impl_->wal_writer_) impl_->wal_writer_->append_put(key, value, seq, fp64, mk);
        impl_->memtable_->put(key, value, seq, core::AKHdr32::FLAG_NORMAL, fp64);

        if (impl_->repl_server_) impl_->repl_server_->ship(seq, wal::WalEntryType::Record, key, value);
    }

    // ============================================================================
    // remove
    // ============================================================================

    void AkkEngine::remove(std::span<const uint8_t> key) {
        if (!impl_ || impl_->closed_.load(std::memory_order_relaxed)) throw std::runtime_error("AkkEngine: engine is closed");

        // If the key has a BLOB ref, schedule the blob for GC before removing the
        // MemTable entry (the only reference to the blob_id).
        if (impl_->blob_manager_) {
            auto existing = impl_->memtable_->get(key);
            if (existing && !existing->is_tombstone()) {
                if (existing->flags() & core::AKHdr32::FLAG_BLOB) {
                    const blob::BlobRef ref = blob::decode_blob_ref(existing->value().data());
                    impl_->blob_manager_->schedule_delete(ref.blob_id);
                }
            }
        }

        const uint64_t seq = impl_->memtable_->next_seq();
        const uint64_t fp64 = core::AKHdr32::compute_key_fp64(key.data(), key.size());
        const uint64_t mk = core::AKHdr32::build_mini_key(key.data(), key.size());

        if (impl_->wal_writer_) impl_->wal_writer_->append_delete(key, seq, fp64, mk);
        impl_->memtable_->remove(key, seq);

        // Ship tombstone to Replicas: empty val signals deletion.
        if (impl_->repl_server_) impl_->repl_server_->ship(seq, wal::WalEntryType::Record, key, {});
    }

    // ============================================================================
    // get
    // ============================================================================

    std::optional<std::vector<uint8_t>> AkkEngine::get(std::span<const uint8_t> key) const {
        if (!impl_ || impl_->closed_.load(std::memory_order_relaxed)) throw std::runtime_error("AkkEngine: engine is closed");

        // Point lookup: MemTable (active + immutables, newest-first)
        const auto rec = impl_->memtable_->get(key);
        if (!rec) return std::nullopt;
        if (rec->is_tombstone()) return std::nullopt;

        const auto stored_val = rec->value();

        // ── BLOB dereference ──────────────────────────────────────────────────
        // FLAG_BLOB is set in both the WAL header and the MemRecord flags when
        // put() externalises a large value.  It survives WAL recovery because
        // open() passes ref.header().flags when replaying into the MemTable.
        // This is fully reliable: no size heuristic, no false positives.
        if (impl_->blob_manager_ && (rec->flags() & core::AKHdr32::FLAG_BLOB)) {
            const blob::BlobRef ref = blob::decode_blob_ref(stored_val.data());
            return impl_->blob_manager_->read(ref.blob_id);
        }

        // ── Inline value ──────────────────────────────────────────────────────
        return std::vector<uint8_t>(stored_val.begin(), stored_val.end());
    }

    // ============================================================================
    // force_sync
    // ============================================================================

    void AkkEngine::force_sync() {
        if (!impl_) return;
        if (impl_->wal_writer_) impl_->wal_writer_->force_sync();
    }

    // ============================================================================
    // force_flush
    // ============================================================================

    void AkkEngine::force_flush() {
        if (!impl_) return;
        impl_->memtable_->force_flush();
    }

    // ============================================================================
    // close
    // ============================================================================

    void AkkEngine::close() {
        if (!impl_) return;

        // Idempotent: only the first caller proceeds.
        auto expected = false;
        if (!impl_->closed_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return;

        // ── Shutdown order ──────────────────────────────────────────────────
        //
        //  1. Stop Replica client first: no more incoming replicated writes.
        //  2. Flush MemTable: all pending data written to flush callback.
        //     The flush callback (manifest_->checkpoint) fires here — manifest_
        //     is still alive at this point.
        //  3. Sync WAL: guarantees all WAL entries are fdatasync'd.
        //  4. Record shutdown checkpoint in Manifest, then close Manifest.
        //  5. Stop Primary replication server.
        //  6. Stop ClusterManager (releases PRIMARY.lock if Primary).
        //  7. Stop BlobManager GC thread.

        // 1. Replica client
        if (impl_->repl_client_) {
            impl_->repl_client_->close();
            impl_->repl_client_.reset();
        }

        // 2. MemTable flush
        impl_->memtable_->force_flush();

        // 3. WAL sync
        if (impl_->wal_writer_) {
            impl_->wal_writer_->force_sync();
            impl_->wal_writer_->close();
            impl_->wal_writer_.reset();
        }

        // 4. Manifest: final checkpoint + close
        if (impl_->manifest_) {
            impl_->manifest_->checkpoint(
                /*name=*/std::optional<std::string>("shutdown"),
                         /*stripe=*/
                         std::nullopt,
                         /*last_seq=*/
                         impl_->memtable_->last_seq()
            );
            impl_->manifest_->close();
            impl_->manifest_.reset();
        }

        // 5. Primary replication server
        if (impl_->repl_server_) {
            impl_->repl_server_->close();
            impl_->repl_server_.reset();
        }

        // 6. Cluster manager (releases file lock)
        if (impl_->cluster_mgr_) {
            impl_->cluster_mgr_->close();
            impl_->cluster_mgr_.reset();
        }

        // 7. BlobManager GC
        if (impl_->blob_manager_) {
            impl_->blob_manager_->close();
            impl_->blob_manager_.reset();
        }
    }
} // namespace akkaradb::engine
