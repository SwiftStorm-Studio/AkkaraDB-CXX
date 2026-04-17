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
#include "engine/server/AkkApiServer.hpp"
#include "engine/cluster/ClusterConfig.hpp"
#include "engine/cluster/ClusterManager.hpp"
#include "engine/cluster/ReplicationClient.hpp"
#include "engine/cluster/ReplicationServer.hpp"
#include "engine/manifest/Manifest.hpp"
#include "engine/sstable/SSTManager.hpp"
#include "engine/vlog/VersionLog.hpp"
#include "engine/wal/WalRecovery.hpp"
#include "core/CRC32C.hpp"
#include "core/record/AKHdr32.hpp"

#include <atomic>
#include <ctime>
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
    // AkkEngine::ScanIterator::Impl
    // ============================================================================

    class AkkEngine::ScanIterator::Impl {
        public:
            Impl(memtable::MemTable::RangeIterator mt_iter, sst::SSTManager::Iterator sst_iter, blob::BlobManager* blob_mgr)
                : mt_iter_(std::move(mt_iter)), sst_iter_(std::move(sst_iter)), blob_mgr_(blob_mgr) {
                advance_mt();
                advance_sst();
                pre_compute();
            }

            [[nodiscard]] bool has_next() const noexcept { return pending_.has_value(); }

            std::optional<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> next() {
                auto result = std::move(pending_);
                pre_compute();
                return result;
            }

        private:
            memtable::MemTable::RangeIterator mt_iter_;
            std::optional<core::MemRecord> mt_cur_; ///< peeked MemTable record
            sst::SSTManager::Iterator sst_iter_; ///< lazy k-way merge over SST
            std::optional<core::MemRecord> sst_cur_; ///< peeked SST record
            blob::BlobManager* blob_mgr_ = nullptr;

            std::optional<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> pending_;

            void advance_mt() { mt_cur_ = mt_iter_.has_next() ? mt_iter_.next() : std::optional<core::MemRecord>{}; }
            void advance_sst() { sst_cur_ = sst_iter_.has_next() ? sst_iter_.next() : std::optional<core::MemRecord>{}; }

            void pre_compute() { pending_ = pick_one(); }

            std::optional<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> pick_one() {
                while (mt_cur_.has_value() || sst_cur_.has_value()) {
                    const bool mt_ok = mt_cur_.has_value();
                    const bool sst_ok = sst_cur_.has_value();

                    // Compare keys to determine which source wins.
                    int cmp = 0;
                    if (mt_ok && sst_ok) {
                        const auto mk = mt_cur_->key();
                        const auto sk = sst_cur_->key();
                        cmp = std::ranges::lexicographical_compare(mk, sk) ? -1 : std::ranges::lexicographical_compare(sk, mk) ? 1 : 0;
                    }
                    else { cmp = mt_ok ? -1 : 1; }

                    auto process = [&](core::MemRecord rec) -> std::optional<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> {
                        if (rec.is_tombstone()) return std::nullopt;
                        const auto key_sp = rec.key();
                        std::vector<uint8_t> key_vec(key_sp.begin(), key_sp.end());
                        const auto val_sp = rec.value();
                        if (blob_mgr_ && (rec.flags() & core::AKHdr32::FLAG_BLOB)) {
                            const blob::BlobRef ref = blob::decode_blob_ref(val_sp.data());
                            auto blob_val = blob_mgr_->read(ref.blob_id, ref.checksum);
                            if (blob_val.empty()) return std::nullopt;
                            return std::make_pair(std::move(key_vec), std::move(blob_val));
                        }
                        return std::make_pair(std::move(key_vec), std::vector<uint8_t>(val_sp.begin(), val_sp.end()));
                    };

                    if (cmp <= 0) {
                        // MemTable wins (or tie → MemTable is newer)
                        core::MemRecord rec = std::move(*mt_cur_);
                        advance_mt();
                        if (cmp == 0) advance_sst(); // skip matching SST entry
                        if (auto r = process(std::move(rec))) return r;
                    }
                    else {
                        // SST wins
                        core::MemRecord rec = std::move(*sst_cur_);
                        advance_sst();
                        if (auto r = process(std::move(rec))) return r;
                    }
                }
                return std::nullopt;
            }
    };

    // ============================================================================
    // ScanIterator methods
    // ============================================================================

    AkkEngine::ScanIterator::ScanIterator(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

    AkkEngine::ScanIterator::~ScanIterator() = default;

    AkkEngine::ScanIterator::ScanIterator(ScanIterator&&) noexcept = default;
    AkkEngine::ScanIterator& AkkEngine::ScanIterator::operator=(ScanIterator&&) noexcept = default;

    bool AkkEngine::ScanIterator::has_next() const noexcept { return impl_ && impl_->has_next(); }

    std::optional<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> AkkEngine::ScanIterator::next() {
        if (!impl_) return std::nullopt;
        return impl_->next();
    }

    // ── writer_threads → shard count ─────────────────────────────────────────
    // Birthday-paradox formula targeting ~80% collision-free probability.
    //   P(no collision) ≈ e^{-N(N-1)/(2S)} ≥ 0.80
    //   → S ≥ N(N-1) / (2 ln 1.25) ≈ N(N-1) × 2.25
    // Returns the next power-of-2 at or above that bound (no cap — caller applies own cap).
    static uint32_t shards_for_threads(uint32_t t) noexcept {
        if (t <= 1) return 2;
        const uint32_t raw = (t * (t - 1u) * 9u + 3u) / 4u; // ceil(N(N-1) × 2.25)
        uint32_t p = 2;
        while (p < raw) p <<= 1;
        return p;
    }

    // ============================================================================
    // Internal helpers
    // ============================================================================

    namespace {
        /// Returns the current wall-clock time in nanoseconds since epoch.
        /// Uses timespec_get (C17) which on MSVC calls GetSystemTimeAsFileTime (~10 ns)
        /// instead of system_clock::now() → GetSystemTimePreciseAsFileTime (~50 ns).
        /// Millisecond resolution is sufficient for VersionLog timestamps; seq provides ordering.
        uint64_t now_ns() noexcept {
            struct timespec ts{};
            timespec_get(&ts, TIME_UTC);
            return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL + static_cast<uint64_t>(ts.tv_nsec);
        }

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

            // ── Node identity ─────────────────────────────────────────────────────
            // 0 = non-persistent / non-cluster mode.
            // Set during open() from data_dir/node.id when data_dir is non-empty.
            uint64_t node_id_ = 0;

            // ── SST (present when wal_enabled + manifest_enabled + data_dir set) ────
            std::unique_ptr<sst::SSTManager> sst_manager_;

            // ── Version log (present when version_log_enabled) ────────────────────
            std::unique_ptr<vlog::VersionLog> version_log_;

            // ── Cluster (present when cluster.akcc found in data_dir) ─────────────
            std::unique_ptr<cluster::ClusterManager> cluster_mgr_;
            std::unique_ptr<cluster::ReplicationServer> repl_server_; ///< Primary only
            std::unique_ptr<cluster::ReplicationClient> repl_client_; ///< Replica only

            // ── API Server (present when options.api.enabled) ─────────────────────
            std::unique_ptr<server::AkkApiServer> api_server_;

            // ── Replica blob tracking ─────────────────────────────────────────────
            // Tracks seqs for which a ReplBlobPut arrived (blob_callback) but the
            // corresponding ReplEntry (apply_callback) has not yet been processed.
            // Since blob_callback always fires before apply_callback on the same TCP
            // stream, we can determine FLAG_BLOB reliably without changing the wire protocol.
            std::mutex pending_blob_mu_;
            std::unordered_set<uint64_t> pending_blob_seqs_;

            // ── Engine-level stats counters (relaxed atomics, zero write overhead) ──
            std::atomic<uint64_t> puts_total_{0};
            std::atomic<uint64_t> removes_total_{0};
            std::atomic<uint64_t> gets_total_{0};
            std::atomic<uint64_t> gets_mt_hit_{0};
            std::atomic<uint64_t> gets_sst_hit_{0};
            std::atomic<uint64_t> gets_miss_{0};
            std::atomic<uint64_t> exists_total_{0};
            std::atomic<uint64_t> scans_total_{0};
            std::atomic<uint64_t> blob_puts_total_{0};

            // =========================================================================
            // apply_record — internal write path used by the Replica apply_callback.
            //
            // Writes a record (put OR tombstone, depending on val being empty) into
            // the local WAL + MemTable, using the seq number assigned by the Primary.
            // The MemTable internally advances seq_gen_ to max(seq_gen_, seq+1) so the
            // local counter stays correctly positioned after replication.
            // =========================================================================
            // source_node_id: ID of the originating node (0 when not known, e.g. non-cluster replica applies).
            void apply_record(
                uint64_t seq,
                std::span<const uint8_t> key,
                std::span<const uint8_t> val,
                uint8_t flags = core::AKHdr32::FLAG_NORMAL,
                uint64_t source_node_id = 0
            ) {
                const uint64_t fp64 = core::AKHdr32::compute_key_fp64(key.data(), key.size());
                const uint64_t mk = core::AKHdr32::build_mini_key(key.data(), key.size());

                if (val.empty()) {
                    // Tombstone (Primary shipped a remove)
                    if (wal_writer_) wal_writer_->append_delete(key, seq, fp64, mk);
                    memtable_->remove(key, seq);
                    if (version_log_) version_log_->append(key, seq, source_node_id, now_ns(), core::AKHdr32::FLAG_TOMBSTONE, {});
                }
                else {
                    // Put (may be an inline value or a 20-byte BlobRef).
                    // flags carries FLAG_BLOB when the Replica received a ReplBlobPut
                    // for this seq before the ReplEntry arrived (TCP ordering guarantee).
                    if (wal_writer_) wal_writer_->append_put(key, val, seq, fp64, mk, flags);
                    memtable_->put(key, val, seq, flags, fp64, mk);
                    if (version_log_) version_log_->append(key, seq, source_node_id, now_ns(), flags, val);
                }
                // Seq comes from the Primary — advance local counter to stay in sync.
                memtable_->advance_seq(seq);
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

        // ── Node identity ────────────────────────────────────────────────────
        // Always load (or create) a stable node ID when data_dir is available.
        // Used in VersionLog entries for source attribution.
        if (persistent && !options.data_dir.empty()) { impl->node_id_ = load_or_create_node_id(options.data_dir); }

        // ── writer_threads → shard count ────────────────────────────────────
        // When writer_threads > 0 and shard_count is left at 0 (auto),
        // derive shard counts targeting ~80% collision-free probability.
        memtable::MemTable::Options mt_opts = options.memtable;
        wal::WalOptions wal_opts = options.wal;
        if (options.writer_threads > 0) {
            const uint32_t s = shards_for_threads(options.writer_threads);
            if (mt_opts.shard_count == 0) mt_opts.shard_count = std::min(s, 256u);
            if (wal_opts.shard_count == 0) wal_opts.shard_count = std::min(s, 64u);
        }

        // ── MemTable (flush callback wired after recovery) ───────────────────
        {
            mt_opts.on_flush = nullptr; // installed after recovery
            impl->memtable_ = memtable::MemTable::create(mt_opts);
        }

        // ── BlobManager ──────────────────────────────────────────────────────
        if (persistent && options.blob_enabled) {
            impl->blob_manager_ = blob::BlobManager::create(options.blob_dir, options.blob_threshold_bytes, options.blob_codec);
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
                    mt.advance_seq(ref.seq());
                },

                // Commit handler: informational only at this stage
                [&](uint64_t /*seq*/, uint64_t /*ts*/) {},

                // Checkpoint handler: optional
                nullptr
            );

            // Soft-tolerate shard errors: valid data before corruption is replayed.
            (void)result;
        }


        // ── WAL Writer ───────────────────────────────────────────────────────
        // Started after recovery so the first seq allocated by the writer is
        // strictly greater than the highest seq replayed.
        if (persistent) { impl->wal_writer_ = wal::WalWriter::create(wal_opts); }

        // ── Version Log ──────────────────────────────────────────────────────
        // Opened after WAL recovery so that any previously logged history is
        // recovered into the in-memory index before new writes are accepted.
        if (persistent && options.version_log_enabled && !options.data_dir.empty()) {
            vlog::VersionLogOptions vl_opts;
            vl_opts.log_path  = options.data_dir / "history.akvlog";
            vl_opts.sync_mode = options.version_log_sync_mode;
            impl->version_log_ = vlog::VersionLog::create(std::move(vl_opts));
        }

        // ── SST Manager (Phase 5) ────────────────────────────────────────────
        // Wire MemTable flush → SSTManager::flush() when all persistence
        // components are enabled and a data directory is available.
        if (persistent && options.manifest_enabled && !options.data_dir.empty()) {
            sst::SSTManager::Options sst_opts;
            sst_opts.sst_dir = options.data_dir / "sst";
            sst_opts.max_l0_files = static_cast<int>(options.max_l0_sst_files);
            sst_opts.max_levels = options.max_sst_levels;
            sst_opts.l1_max_bytes = options.sst_l1_max_bytes;
            sst_opts.level_size_multiplier = options.sst_level_size_multiplier;
            sst_opts.target_file_size_bytes = options.sst_target_file_size;
            sst_opts.bloom_bits_per_key = options.sst_bloom_bits_per_key;
            sst_opts.preload_sst_data = options.sst_preload_data;
            sst_opts.index_stride = sst::INDEX_STRIDE;
            sst_opts.codec = options.sst_codec;
            sst_opts.l0_stall_files = options.sst_l0_stall_files;

            impl->sst_manager_ = sst::SSTManager::create(sst_opts, impl->manifest_.get());
            impl->sst_manager_->recover(); // load live SST files from Manifest

            // MemTable flush callback: per-shard flusher threads call this
            // with a sorted batch of MemRecords.  SSTManager::flush() writes
            // the batch to a new L0 SST file and triggers compaction if needed.
            impl->memtable_->set_flush_callback(
                [sst_mgr = impl->sst_manager_.get()](std::span<const core::MemRecord> records) { if (!records.empty()) sst_mgr->flush(records); }
            );
        }

        // ── BlobManager orphan scan ──────────────────────────────────────────
        // Build the live blob reference set by scanning:
        //   • MemTable  — WAL-recovered, non-tombstone FLAG_BLOB entries
        //   • SST files — all FLAG_BLOB entries (over-inclusive: if a key was
        //                  tombstoned in MemTable but a blob record survives in
        //                  an older SST layer we keep it alive.  It will be
        //                  collected after compaction propagates the tombstone.)
        // Any blob_id absent from both sources was orphaned by a previous crash
        // (WAL record lost) and is safe to schedule for deletion now.
        if (persistent && impl->blob_manager_) {
            std::unordered_set<uint64_t> referenced;

            const auto collect_blob_refs = [&](const core::MemRecord& rec) {
                if (rec.flags() & core::AKHdr32::FLAG_BLOB) {
                    const auto val = rec.value();
                    if (val.size() >= blob::BLOB_REF_SIZE) { referenced.insert(blob::decode_blob_ref(val.data()).blob_id); }
                }
            };

            // MemTable: non-tombstone records only (tombstones carry no blob ref)
            {
                auto it = impl->memtable_->iterator(memtable::MemTable::KeyRange{});
                while (it.has_next()) {
                    const auto rec = it.next();
                    if (!rec->is_tombstone()) collect_blob_refs(*rec);
                }
            }

            // SST: all records from all levels (over-inclusive, see above)
            if (impl->sst_manager_) {
                auto it = impl->sst_manager_->scan_iter({}, {});
                while (it.has_next()) collect_blob_refs(it.next());
            }

            impl->blob_manager_->scan_orphans([&referenced](uint64_t id) { return referenced.contains(id); });
        }

        // ── Cluster setup ────────────────────────────────────────────────────
        // If data_dir/cluster.akcc exists, initialise ClusterManager and elect
        // a role (Primary / Replica).  Then start the appropriate replication
        // component (ReplicationServer or ReplicationClient).
        if (persistent && !options.data_dir.empty()) {
            const auto cluster_cfg_path = options.data_dir / "cluster.akcc";
            if (fs::exists(cluster_cfg_path)) {
                auto cfg = cluster::ClusterConfig::load(cluster_cfg_path);
                // node_id_ was already loaded above; reuse it for cluster setup.
                uint64_t self_nid = impl->node_id_;

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

        // ── API Server ───────────────────────────────────────────────────────
        if (options.api.enabled && !options.api.backends.empty()) {
            impl->api_server_ = server::AkkApiServer::create(*eng, options.api);
            impl->api_server_->start();
        }

        eng->impl_ = std::move(impl);
        return eng;
    }

    // ============================================================================
    // Destructor
    // ============================================================================

    AkkEngine::AkkEngine() = default; // defined here so Impl is complete when unique_ptr<Impl> is instantiated
    AkkEngine::~AkkEngine() { if (impl_) close(); }

    // ============================================================================
    // put
    // ============================================================================

    void AkkEngine::put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        if (!impl_ || impl_->closed_.load(std::memory_order_relaxed)) throw std::runtime_error("AkkEngine: engine is closed");
        impl_->puts_total_.fetch_add(1, std::memory_order_relaxed);

        // ── L0 write backpressure ─────────────────────────────────────────────
        // Block until compaction drains L0 below the stall threshold.
        // This prevents L0 from growing unboundedly when compaction lags behind.
        if (impl_->sst_manager_) impl_->sst_manager_->stall_if_l0_full();

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
            impl_->blob_puts_total_.fetch_add(1, std::memory_order_relaxed);
            impl_->blob_manager_->write(seq, value);

            // 3. Build 20-byte BlobRef: [blob_id:8][total_size:8][checksum:4]
            const uint32_t cksum = core::CRC32C::compute(value.data(), value.size());
            uint8_t ref_buf[blob::BLOB_REF_SIZE];
            blob::encode_blob_ref(ref_buf, seq, value.size(), cksum);
            const auto ref_span = std::span<const uint8_t>(ref_buf, blob::BLOB_REF_SIZE);

            // 4. WAL + MemTable store the BlobRef (inline, 20 bytes), tagged FLAG_BLOB
            //    so get() can identify them reliably without heuristics.
            if (impl_->wal_writer_) impl_->wal_writer_->append_put(key, ref_span, seq, fp64, mk, core::AKHdr32::FLAG_BLOB);
            impl_->memtable_->put(key, ref_span, seq, core::AKHdr32::FLAG_BLOB, fp64, mk);

            // 5. Record in VersionLog (stores the 20-byte BlobRef, FLAG_BLOB)
            if (impl_->version_log_) impl_->version_log_->append(key, seq, impl_->node_id_, now_ns(), core::AKHdr32::FLAG_BLOB, ref_span);

            // 6. Ship WAL entry (carrying BlobRef) to Replicas
            if (impl_->repl_server_) impl_->repl_server_->ship(seq, wal::WalEntryType::Record, key, ref_span);

            return;
        }

        // ── Inline path ───────────────────────────────────────────────────────
        if (impl_->wal_writer_) impl_->wal_writer_->append_put(key, value, seq, fp64, mk);
        impl_->memtable_->put(key, value, seq, core::AKHdr32::FLAG_NORMAL, fp64, mk);
        if (impl_->version_log_) impl_->version_log_->append(key, seq, impl_->node_id_, now_ns(), core::AKHdr32::FLAG_NORMAL, value);

        if (impl_->repl_server_) impl_->repl_server_->ship(seq, wal::WalEntryType::Record, key, value);
    }

    // ============================================================================
    // remove
    // ============================================================================

    void AkkEngine::remove(std::span<const uint8_t> key) {
        if (!impl_ || impl_->closed_.load(std::memory_order_relaxed)) throw std::runtime_error("AkkEngine: engine is closed");
        impl_->removes_total_.fetch_add(1, std::memory_order_relaxed);

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
        if (impl_->version_log_) impl_->version_log_->append(key, seq, impl_->node_id_, now_ns(), core::AKHdr32::FLAG_TOMBSTONE, {});

        // Ship tombstone to Replicas: empty val signals deletion.
        if (impl_->repl_server_) impl_->repl_server_->ship(seq, wal::WalEntryType::Record, key, {});
    }

    // ============================================================================
    // get
    // ============================================================================

    std::optional<std::vector<uint8_t>> AkkEngine::get(std::span<const uint8_t> key) const {
        if (!impl_ || impl_->closed_.load(std::memory_order_relaxed)) throw std::runtime_error("AkkEngine: engine is closed");
        impl_->gets_total_.fetch_add(1, std::memory_order_relaxed);

        // ── MemTable lookup (active + immutables, newest-first) ──────────────
        const auto rec = impl_->memtable_->get(key);
        if (rec) {
            if (rec->is_tombstone()) {
                impl_->gets_miss_.fetch_add(1, std::memory_order_relaxed);
                return std::nullopt;
            }
            impl_->gets_mt_hit_.fetch_add(1, std::memory_order_relaxed);

            const auto stored_val = rec->value();

            // FLAG_BLOB: externalised large value — dereference via BlobManager.
            if (impl_->blob_manager_ && (rec->flags() & core::AKHdr32::FLAG_BLOB)) {
                const blob::BlobRef ref = blob::decode_blob_ref(stored_val.data());
                return impl_->blob_manager_->read(ref.blob_id, ref.checksum);
            }

            return std::vector<uint8_t>(stored_val.begin(), stored_val.end());
        }

        // ── SST fallthrough (Phase 5) ─────────────────────────────────────────
        // MemTable miss: search SST files (L0 newest-first, then L1).
        if (impl_->sst_manager_) {
            const auto sst_rec = impl_->sst_manager_->get(key);
            if (sst_rec) {
                if (sst_rec->is_tombstone()) {
                    impl_->gets_miss_.fetch_add(1, std::memory_order_relaxed);
                    return std::nullopt;
                }
                impl_->gets_sst_hit_.fetch_add(1, std::memory_order_relaxed);

                const auto stored_val = sst_rec->value();

                // ── SST read-through promotion ────────────────────────────────
                // Re-insert into MemTable (no WAL) so the next read of this key
                // is served from memory.  Blob records are promoted as-is (the
                // inline BlobRef bytes are still correct); the BlobManager entry
                // remains alive independently.
                if (impl_->opts_.sst_promote_reads) {
                    impl_->memtable_->put(key, stored_val, sst_rec->seq(), sst_rec->flags());
                    impl_->memtable_->advance_seq(sst_rec->seq());
                }

                if (impl_->blob_manager_ && (sst_rec->flags() & core::AKHdr32::FLAG_BLOB)) {
                    const blob::BlobRef ref = blob::decode_blob_ref(stored_val.data());
                    return impl_->blob_manager_->read(ref.blob_id, ref.checksum);
                }

                return std::vector<uint8_t>(stored_val.begin(), stored_val.end());
            }
        }

        impl_->gets_miss_.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
    }

    // ============================================================================
    // get_into
    // ============================================================================

    bool AkkEngine::get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
        if (!impl_ || impl_->closed_.load(std::memory_order_relaxed)) throw std::runtime_error("AkkEngine: engine is closed");

        // ── Fast path: no blob manager ───────────────────────────────────────
        // When blob_enabled == false (or wal_enabled == false), no FLAG_BLOB records
        // exist in MemTable or SST.  Skip the flags check entirely and copy value
        // bytes directly into out — no intermediate MemRecord allocation.
        if (!impl_->blob_manager_) {
            // Skip the MemTable lookup (which computes SipHash + acquires a shard lock)
            // when the MemTable is known to be empty — mirrors the approx_size() guard
            // in exists().  approx_size() is a relaxed atomic load; the worst case is a
            // spurious MemTable check when a concurrent put races with the load.
            if (impl_->memtable_->approx_size() > 0) {
                const auto mt = impl_->memtable_->get_into(key, out);
                if (mt.has_value()) return *mt; // true=found, false=tombstone
            }

            // MemTable miss (or empty): try SST via the lock-free get_into() path.
            // SSTManager::get_into() mirrors contains(): atomic snapshot + precomputed
            // bloom hash, writes value bytes directly into `out` — no intermediate
            // MemRecord, no hidden-pointer ABI overhead, no block-cache string alloc.
            if (impl_->sst_manager_) {
                const auto r = impl_->sst_manager_->get_into(key, out);
                if (r.has_value()) {
                    if (!*r) return false; // tombstone

                    // Read-through promotion needs seq/flags — fall back to get().
                    if (impl_->opts_.sst_promote_reads) {
                        const auto sst_rec = impl_->sst_manager_->get(key);
                        if (sst_rec) {
                            impl_->memtable_->put(key, sst_rec->value(), sst_rec->seq(), sst_rec->flags());
                            impl_->memtable_->advance_seq(sst_rec->seq());
                        }
                    }

                    return true;
                }
            }
            return false;
        }

        // ── Slow path: blob manager active ───────────────────────────────────
        // FLAG_BLOB values need flag inspection — fall back to get() which handles them.
        const auto v = get(key);
        if (!v) return false;
        out = *v;
        return true;
    }

    // ============================================================================
    // exists
    // ============================================================================

    bool AkkEngine::exists(std::span<const uint8_t> key) const {
        if (!impl_ || impl_->closed_.load(std::memory_order_relaxed)) throw std::runtime_error("AkkEngine: engine is closed");
        impl_->exists_total_.fetch_add(1, std::memory_order_relaxed);

        // MemTable: skip entirely when empty (e.g. SST-only read after reopen with no writes).
        // approx_size() is a relaxed atomic load — safe and cheap.
        if (impl_->memtable_->approx_size() > 0) {
            const auto mt = impl_->memtable_->contains(key);
            if (mt.has_value()) return *mt;
        }

        // SST: same 3-way semantics
        if (impl_->sst_manager_) {
            const auto sst = impl_->sst_manager_->contains(key);
            if (sst.has_value()) return *sst;
        }

        return false;
    }

    // ============================================================================
    // count
    // ============================================================================

    size_t AkkEngine::count(std::span<const uint8_t> start_key, std::span<const uint8_t> end_key) const {
        if (!impl_ || impl_->closed_.load(std::memory_order_relaxed)) throw std::runtime_error("AkkEngine: engine is closed");

        // Build MemTable range iterator
        const std::vector<uint8_t> start_vec(start_key.begin(), start_key.end());
        const std::vector<uint8_t> end_vec(end_key.begin(), end_key.end());
        auto mt_iter = impl_->memtable_->iterator(memtable::MemTable::KeyRange{start_vec, end_vec});

        // SST lazy iterator — no intermediate vector; blob I/O skipped (nullptr blob_mgr)
        sst::SSTManager::Iterator sst_iter;
        if (impl_->sst_manager_) sst_iter = impl_->sst_manager_->scan_iter(start_key, end_key);

        auto iter_impl = std::make_unique<ScanIterator::Impl>(std::move(mt_iter), std::move(sst_iter), nullptr);

        ScanIterator it(std::move(iter_impl));
        size_t n = 0;
        while (it.has_next()) {
            (void)it.next();
            ++n;
        }
        return n;
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

        // 0. API Server: stop accepting new connections before flushing
        if (impl_->api_server_) {
            impl_->api_server_->close();
            impl_->api_server_.reset();
        }

        // 1. Replica client
        if (impl_->repl_client_) {
            impl_->repl_client_->close();
            impl_->repl_client_.reset();
        }

        // 2. MemTable flush → SST (all shards)
        impl_->memtable_->force_flush();

        // 2b. SST manager: stop background compaction thread, then release.
        //     Done after force_flush() so the flush callback has finished.
        if (impl_->sst_manager_) impl_->sst_manager_->shutdown();
        impl_->sst_manager_.reset();

        // 3. WAL sync + optional truncation
        if (impl_->wal_writer_) {
            impl_->wal_writer_->force_sync();

            // If SST was active, all MemTable data has been persisted to SST.
            // Truncate the WAL so the next startup does not replay stale entries.
            // (Manifest tracks live SSTs; data is recovered from SST on next open.)
            if (impl_->opts_.manifest_enabled && !impl_->opts_.data_dir.empty()) { impl_->wal_writer_->truncate(); }

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

        // 8. Version log: auto-prune then close
        if (impl_->version_log_) {
            if (impl_->opts_.version_log_prune_on_close) {
                const uint64_t last = impl_->memtable_->last_seq();
                const uint64_t keep = impl_->opts_.version_log_keep_seqs;
                if (last > keep) {
                    impl_->version_log_->prune_before(last - keep);
                }
            }
            impl_->version_log_->close();
            impl_->version_log_.reset();
        }
    }

    // ============================================================================
    // get_at
    // ============================================================================

    std::optional<std::vector<uint8_t>> AkkEngine::get_at(std::span<const uint8_t> key, uint64_t at_seq) const {
        if (!impl_ || impl_->closed_.load(std::memory_order_relaxed)) throw std::runtime_error("AkkEngine: engine is closed");
        if (!impl_->version_log_) return std::nullopt;

        const auto entry = impl_->version_log_->get_at(key, at_seq);
        if (!entry) return std::nullopt;
        if (entry->flags & core::AKHdr32::FLAG_TOMBSTONE) return std::nullopt;

        // Resolve BLOB reference to actual value bytes
        if (impl_->blob_manager_ && (entry->flags & core::AKHdr32::FLAG_BLOB)) {
            const blob::BlobRef ref = blob::decode_blob_ref(entry->value.data());
            return impl_->blob_manager_->read(ref.blob_id, ref.checksum);
        }

        return entry->value;
    }

    // ============================================================================
    // history
    // ============================================================================

    std::vector<VersionEntry> AkkEngine::history(std::span<const uint8_t> key) const {
        if (!impl_ || impl_->closed_.load(std::memory_order_relaxed)) throw std::runtime_error("AkkEngine: engine is closed");
        if (!impl_->version_log_) return {};
        return impl_->version_log_->history(key);
    }

    // ============================================================================
    // rollback helpers
    // ============================================================================

    namespace {
        // Resolves a VersionEntry's value, reading blobs if needed.
        // Returns nullopt for tombstones / missing entries.
        std::optional<std::vector<uint8_t>> resolve_ventry_value(const vlog::VersionEntry& entry, blob::BlobManager* blob_mgr) {
            if (entry.flags & core::AKHdr32::FLAG_TOMBSTONE) return std::nullopt;
            if (blob_mgr && (entry.flags & core::AKHdr32::FLAG_BLOB)) {
                const blob::BlobRef ref = blob::decode_blob_ref(entry.value.data());
                return blob_mgr->read(ref.blob_id, ref.checksum);
            }
            return entry.value;
        }
    } // anonymous namespace (inner, extends the outer one declared above)

    // Internal helper: restores a single key to its state at the previous
    // version entry (or removes it if prev is nullopt / tombstone).
    // Writes via the normal put/remove path so WAL + VersionLog are updated.
    static void do_rollback_key(AkkEngine* eng, std::span<const uint8_t> key, const std::optional<vlog::VersionEntry>& prev, blob::BlobManager* blob_mgr) {
        if (!prev.has_value() || (prev->flags & core::AKHdr32::FLAG_TOMBSTONE)) {
            // Key did not exist (or was deleted) at target_seq → remove it now.
            eng->remove(key);
        }
        else {
            // Restore the previous value.
            auto val = resolve_ventry_value(*prev, blob_mgr);
            if (!val.has_value()) {
                eng->remove(key); // blob lost → treat as deleted
            }
            else { eng->put(key, std::span<const uint8_t>(val->data(), val->size())); }
        }
    }

    // ============================================================================
    // rollback_to
    // ============================================================================

    void AkkEngine::rollback_to(uint64_t target_seq) {
        if (!impl_ || impl_->closed_.load(std::memory_order_relaxed)) throw std::runtime_error("AkkEngine: engine is closed");
        if (!impl_->version_log_) throw std::runtime_error("AkkEngine: version_log_enabled must be true for rollback");

        const auto targets = impl_->version_log_->collect_rollback_targets(target_seq);
        for (const auto& [key_bytes, prev] : targets) {
            const auto key = std::span<const uint8_t>(key_bytes.data(), key_bytes.size());
            do_rollback_key(this, key, prev, impl_->blob_manager_.get());
        }
    }

    // ============================================================================
    // rollback_key
    // ============================================================================

    void AkkEngine::rollback_key(std::span<const uint8_t> key, uint64_t target_seq) {
        if (!impl_ || impl_->closed_.load(std::memory_order_relaxed)) throw std::runtime_error("AkkEngine: engine is closed");
        if (!impl_->version_log_) throw std::runtime_error("AkkEngine: version_log_enabled must be true for rollback");

        const auto prev = impl_->version_log_->get_at(key, target_seq);
        do_rollback_key(this, key, prev, impl_->blob_manager_.get());
    }

    // ============================================================================
    // scan
    // ============================================================================

    AkkEngine::ScanIterator AkkEngine::scan(
        std::span<const uint8_t> start_key,
        std::span<const uint8_t> end_key
    ) const {
        if (!impl_ || impl_->closed_.load(std::memory_order_relaxed))
            throw std::runtime_error("AkkEngine: engine is closed");
        impl_->scans_total_.fetch_add(1, std::memory_order_relaxed);

        // ── 1. MemTable snapshot iterator ─────────────────────────────────────
        memtable::MemTable::KeyRange mt_range;
        if (!start_key.empty())
            mt_range.start.assign(start_key.begin(), start_key.end());
        if (!end_key.empty())
            mt_range.end.assign(end_key.begin(), end_key.end());

        auto mt_iter = impl_->memtable_->iterator(mt_range);

        // ── 2. SST lazy scan iterator (no intermediate vector allocation) ────────
        sst::SSTManager::Iterator sst_iter;
        if (impl_->sst_manager_) { sst_iter = impl_->sst_manager_->scan_iter(start_key, end_key); }

        // ── 3. Build merge iterator ───────────────────────────────────────────
        auto impl = std::make_unique<ScanIterator::Impl>(
            std::move(mt_iter),
            std::move(sst_iter),
            impl_->blob_manager_.get()
        );
        return ScanIterator(std::move(impl));
    }

    // ============================================================================
    // stats
    // ============================================================================

    EngineStats AkkEngine::stats() const noexcept {
        if (!impl_) return {};

        EngineStats s;

        // ── Identity ──────────────────────────────────────────────────────────
        s.current_seq = impl_->memtable_->last_seq();
        s.node_id     = impl_->node_id_;

        // ── Engine-level operation counts ─────────────────────────────────────
        s.puts_total        = impl_->puts_total_.load(std::memory_order_relaxed);
        s.removes_total     = impl_->removes_total_.load(std::memory_order_relaxed);
        s.gets_total        = impl_->gets_total_.load(std::memory_order_relaxed);
        s.gets_memtable_hit = impl_->gets_mt_hit_.load(std::memory_order_relaxed);
        s.gets_sst_hit      = impl_->gets_sst_hit_.load(std::memory_order_relaxed);
        s.gets_miss         = impl_->gets_miss_.load(std::memory_order_relaxed);
        s.exists_total      = impl_->exists_total_.load(std::memory_order_relaxed);
        s.scans_total       = impl_->scans_total_.load(std::memory_order_relaxed);
        s.blob_puts_total   = impl_->blob_puts_total_.load(std::memory_order_relaxed);

        // ── MemTable ──────────────────────────────────────────────────────────
        {
            const auto snap = impl_->memtable_->snapshot();
            s.memtable.shard_count               = snap.shard_count;
            s.memtable.threshold_bytes_per_shard  = snap.threshold_bytes_per_shard;
            s.memtable.approx_bytes              = snap.approx_bytes;
            s.memtable.puts_applied              = snap.puts_applied;
            s.memtable.removes_applied           = snap.removes_applied;
            s.memtable.flushes_completed         = snap.flushes_completed;
            s.memtable.bytes_flushed             = snap.bytes_flushed;
        }

        // ── WAL ───────────────────────────────────────────────────────────────
        s.wal.enabled = impl_->opts_.wal_enabled;
        if (impl_->wal_writer_) {
            const auto snap = impl_->wal_writer_->snapshot();
            s.wal.shard_count       = snap.shard_count;
            s.wal.entries_written   = snap.entries_written;
            s.wal.bytes_written     = snap.bytes_written;
            s.wal.batches_flushed   = snap.batches_flushed;
            s.wal.syncs_executed    = snap.syncs_executed;
            s.wal.segment_rotations = snap.segment_rotations;
        }

        // ── Blob ──────────────────────────────────────────────────────────────
        s.blob.enabled = impl_->opts_.blob_enabled && impl_->opts_.wal_enabled;
        if (impl_->blob_manager_) {
            s.blob.threshold_bytes = impl_->blob_manager_->threshold();
            const auto snap = impl_->blob_manager_->snapshot();
            s.blob.blobs_written      = snap.blobs_written;
            s.blob.bytes_uncompressed = snap.bytes_uncompressed;
            s.blob.bytes_on_disk      = snap.bytes_on_disk;
            s.blob.blobs_deleted      = snap.blobs_deleted;
            s.blob.gc_cycles          = snap.gc_cycles;
        }

        // ── SST ───────────────────────────────────────────────────────────────
        s.sst.enabled = (impl_->sst_manager_ != nullptr);
        if (impl_->sst_manager_) {
            const auto levels = impl_->sst_manager_->level_stats();
            s.sst.levels.reserve(levels.size());
            uint64_t total_bytes = 0;
            size_t total_files = 0;
            for (const auto& lv : levels) {
                LevelStats ls;
                ls.level        = lv.level;
                ls.file_count   = lv.file_count;
                ls.bytes        = lv.bytes;
                ls.budget_bytes = lv.budget_bytes;
                s.sst.levels.push_back(ls);
                total_bytes += lv.bytes;
                total_files += lv.file_count;
            }
            s.sst.file_count         = total_files;
            s.sst.bytes              = total_bytes;
            s.sst.l0_file_count      = levels.empty() ? 0 : levels[0].file_count;
            s.sst.compaction_pending = impl_->sst_manager_->compaction_pending();

            const auto csnap = impl_->sst_manager_->compaction_snapshot();
            s.sst.compactions_completed = csnap.compactions_completed;
            s.sst.files_compacted       = csnap.files_compacted;
            s.sst.bytes_compacted_in    = csnap.bytes_compacted_in;
            s.sst.bytes_compacted_out   = csnap.bytes_compacted_out;
            s.sst.l0_stalls             = csnap.l0_stalls;
        }

        // ── Version Log ───────────────────────────────────────────────────────
        s.vlog.enabled = impl_->opts_.version_log_enabled;

        return s;
    }

} // namespace akkaradb::engine
