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

// internal/src/engine/sstable/SSTManager.cpp
#include "engine/sstable/SSTManager.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <format>
#include <queue>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_set>

namespace akkaradb::engine::sst {

    namespace fs = std::filesystem;

    // ============================================================================
    // Internal helpers
    // ============================================================================

    namespace {
        std::string bytes_to_hex(std::span<const uint8_t> data) {
            static constexpr char kHex[] = "0123456789abcdef";
            std::string result;
            result.reserve(data.size() * 2);
            for (uint8_t b : data) {
                result += kHex[b >> 4];
                result += kHex[b & 0x0fu];
            }
            return result;
        }

        /// Priority-queue entry for k-way merge during compaction.
        struct MergeEntry {
            core::MemRecord rec;
            size_t source_idx;
        };

        /// Min-heap: smallest key first; equal keys → highest seq first.
        struct MergeEntryGreater {
            bool operator()(const MergeEntry& a, const MergeEntry& b) const noexcept {
                const int cmp = a.rec.compare_key(b.rec);
                if (cmp != 0) return cmp > 0;
                return a.rec.seq() < b.rec.seq();
            }
        };

        /// Approximate byte size of one record in an SST data section.
        inline uint64_t record_disk_size(const core::MemRecord& r) noexcept {
            // AKHdr32 (32) + key + value + crc32c (4)
            return 36u + r.key().size() + r.value().size();
        }

        /// Returns true if [a_first, a_last] overlaps with [b_first, b_last].
        inline bool key_ranges_overlap(
            std::span<const uint8_t> a_first,
            std::span<const uint8_t> a_last,
            std::span<const uint8_t> b_first,
            std::span<const uint8_t> b_last
        ) noexcept {
            // Overlap iff NOT (a_last < b_first OR b_last < a_first)
            const bool a_before_b = std::lexicographical_compare(a_last.begin(), a_last.end(), b_first.begin(), b_first.end());
            if (a_before_b) return false;
            const bool b_before_a = std::lexicographical_compare(b_last.begin(), b_last.end(), a_first.begin(), a_first.end());
            return !b_before_a;
        }
    } // anonymous namespace

    // ============================================================================
    // Construction / destruction
    // ============================================================================

    SSTManager::SSTManager(Options opts, manifest::Manifest* manifest)
        : opts_(std::move(opts)), manifest_(manifest) {
        // Initialize level vectors.
        levels_.resize(static_cast<size_t>(opts_.max_levels));
        // Publish initial (empty) snapshot so contains() never observes nullptr.
        levels_snap_.store(std::make_shared<const LevelsData>(levels_), std::memory_order_release);

        // Initialize cache shard capacities.
        if (opts_.sst_cache_capacity > 0) {
            const size_t per_shard = std::max(size_t{1}, opts_.sst_cache_capacity / kCacheShards);
            for (auto& shard : cache_shards_) shard.capacity = per_shard;
        }

        // Start the background compaction thread pool.
        // Threads wait on compact_cv_ — safe to start before recover().
        const int n = std::max(1, opts_.compact_threads);
        compact_pool_.reserve(static_cast<size_t>(n));
        for (int i = 0; i < n; ++i) { compact_pool_.emplace_back([this] { compaction_loop(); }); }
    }

    SSTManager::~SSTManager() {
        // shutdown() should already have been called by AkkEngine::close().
        // Belt-and-suspenders: ensure all threads are joined.
        if (!shutting_down_.load(std::memory_order_relaxed)) {
            shutting_down_.store(true, std::memory_order_relaxed);
            compact_cv_.notify_all();
        }
        for (auto& t : compact_pool_) { if (t.joinable()) t.join(); }
    }

    std::unique_ptr<SSTManager> SSTManager::create(Options opts,
                                                    manifest::Manifest* manifest) {
        fs::create_directories(opts.sst_dir);
        return std::unique_ptr<SSTManager>(new SSTManager(std::move(opts), manifest));
    }

    // ============================================================================
    // shutdown
    // ============================================================================

    void SSTManager::shutdown() {
        shutting_down_.store(true, std::memory_order_relaxed);
        compact_cv_.notify_all();
        // Wake any put() threads stalled on L0 backpressure.
        {
            std::lock_guard lk(l0_stall_mu_);
        }
        l0_stall_cv_.notify_all();
        for (auto& t : compact_pool_) { if (t.joinable()) t.join(); }
    }

    // ============================================================================
    // publish_snapshot_locked  (must be called under unique_lock(sst_mu_))
    // ============================================================================

    void SSTManager::publish_snapshot_locked() noexcept {
        // Copy levels_ into a new heap-allocated snapshot and publish atomically.
        // Callers holding an old snapshot (via atomic load) remain unaffected —
        // their shared_ptr keeps the old LevelsData alive until they drop it.
        levels_snap_.store(std::make_shared<const LevelsData>(levels_), std::memory_order_release);
    }

    // ============================================================================
    // stall_if_l0_full
    // ============================================================================

    void SSTManager::stall_if_l0_full() {
        const int threshold = (opts_.l0_stall_files > 0) ? opts_.l0_stall_files : opts_.max_l0_files * 2;
        if (threshold >= INT_MAX) return;

        // Fast path: L0 is not full.
        {
            std::shared_lock sst_lk(sst_mu_);
            if (static_cast<int>(levels_[0].size()) < threshold) return;
        }

        // Slow path: wait for compaction to drain L0.
        std::unique_lock lk(l0_stall_mu_);
        l0_stall_cv_.wait(
            lk,
            [this, threshold] {
                if (shutting_down_.load(std::memory_order_relaxed)) return true;
                std::shared_lock sst_lk(sst_mu_);
                return static_cast<int>(levels_[0].size()) < threshold;
            }
        );
    }

    // ============================================================================
    // scan
    // ============================================================================

    std::vector<core::MemRecord> SSTManager::scan(std::span<const uint8_t> start_key, std::span<const uint8_t> end_key) const {
        // Take a snapshot of all readers under a brief shared lock.
        // The readers are kept alive by shared_ptr even if compaction runs.
        std::vector<std::shared_ptr<SSTReader>> snapshot;
        {
            std::shared_lock lock(sst_mu_);
            for (const auto& meta : levels_[0]) { if (meta.reader) snapshot.push_back(meta.reader); }
            for (int lvl = 1; lvl < opts_.max_levels; ++lvl) {
                for (const auto& meta : levels_[static_cast<size_t>(lvl)]) {
                    if (!meta.reader) continue;
                    // Quick range filter using file metadata.
                    if (!start_key.empty() && !meta.last_key.empty() && std::lexicographical_compare(
                        meta.last_key.begin(),
                        meta.last_key.end(),
                        start_key.begin(),
                        start_key.end()
                    )) continue;
                    if (!end_key.empty() && !meta.first_key.empty() && !std::lexicographical_compare(
                        meta.first_key.begin(),
                        meta.first_key.end(),
                        end_key.begin(),
                        end_key.end()
                    )) continue;
                    snapshot.push_back(meta.reader);
                }
            }
        }

        if (snapshot.empty()) return {};

        // Build per-reader scan iterators (I/O happens outside the lock).
        std::vector<SSTReader::Iterator> iters;
        iters.reserve(snapshot.size());
        for (const auto& rdr : snapshot) { iters.push_back(rdr->scan(start_key, end_key)); }

        // k-way merge: smallest key first; equal keys → highest seq first.
        std::priority_queue<MergeEntry, std::vector<MergeEntry>, MergeEntryGreater> heap;
        for (size_t i = 0; i < iters.size(); ++i) { if (iters[i].has_next()) heap.push({iters[i].next(), i}); }

        std::vector<core::MemRecord> result;
        std::vector<uint8_t> prev_key;

        while (!heap.empty()) {
            core::MemRecord rec = heap.top().rec;
            const size_t idx = heap.top().source_idx;
            heap.pop();

            if (iters[idx].has_next()) heap.push({iters[idx].next(), idx});

            // Skip lower-seq duplicates of the same key.
            const auto k = rec.key();
            if (!prev_key.empty() && prev_key.size() == k.size() && std::equal(prev_key.begin(), prev_key.end(), k.begin())) continue;
            prev_key.assign(k.begin(), k.end());

            result.push_back(std::move(rec));
        }

        return result;
    }

    // ============================================================================
    // make_sst_path
    // ============================================================================

    fs::path SSTManager::make_sst_path(int level) {
        const uint64_t id = next_sst_id_.fetch_add(1, std::memory_order_relaxed);
        const auto filename = std::format("L{}_{:016x}.aksst", level, id);
        return opts_.sst_dir / filename;
    }

    // ============================================================================
    // make_meta
    // ============================================================================

    // static
    SSTManager::SSTMeta SSTManager::make_meta(const SSTWriter::Result& res,
                                               int level,
                                               const std::string& filename,
                                               uint64_t file_size_bytes
    ) {
        SSTMeta m;
        m.path = res.path;
        m.filename = filename;
        m.level = level;
        m.entry_count = res.entry_count;
        m.file_size_bytes = file_size_bytes;
        m.min_seq = res.min_seq;
        m.max_seq = res.max_seq;
        m.first_key = res.first_key;
        m.last_key = res.last_key;
        m.reader = nullptr; // caller sets this
        return m;
    }

    // ============================================================================
    // level_budget
    // ============================================================================

    uint64_t SSTManager::level_budget(int n) const {
        if (n <= 0) return 0; // L0 uses file-count trigger
        // L1 = l1_max_bytes; L2 = l1_max_bytes * multiplier; ...
        return static_cast<uint64_t>(static_cast<double>(opts_.l1_max_bytes) * std::pow(opts_.level_size_multiplier, static_cast<double>(n - 1)));
    }

    // ============================================================================
    // recover
    // ============================================================================

    void SSTManager::recover() {
        if (!manifest_) return;

        const auto live = manifest_->live_sst();
        if (live.empty()) return;

        // Temporary per-level vectors before sorting.
        std::vector<std::vector<SSTMeta>> tmp(static_cast<size_t>(opts_.max_levels));
        uint64_t max_id = 0;

        for (const auto& filename : live) {
            // Expected format: "L{level}_{id:016x}.aksst" (total >= 22 chars)
            if (filename.size() < 22 || filename[0] != 'L' || filename[2] != '_') {
                std::fprintf(stderr,
                             "[SSTManager] recover: skipping unknown file: %s\n",
                             filename.c_str());
                continue;
            }

            const int level = filename[1] - '0';
            if (level < 0 || level >= opts_.max_levels) {
                std::fprintf(stderr, "[SSTManager] recover: invalid level %d in: %s\n", level, filename.c_str());
                continue;
            }

            uint64_t id = 0;
            try {
                id = std::stoull(filename.substr(3, 16), nullptr, 16);
            } catch (...) {
                std::fprintf(stderr,
                             "[SSTManager] recover: cannot parse ID in: %s\n",
                             filename.c_str());
                continue;
            }
            if (id > max_id) max_id = id;

            const auto path = opts_.sst_dir / filename;
            auto reader = SSTReader::open(path, opts_.preload_sst_data);
            if (!reader) {
                std::fprintf(stderr,
                             "[SSTManager] recover: cannot open %s — skipping\n",
                             filename.c_str());
                continue;
            }

            uint64_t fsize = 0;
            std::error_code ec;
            fsize = fs::file_size(path, ec);
            if (ec) fsize = 0;

            SSTMeta meta;
            meta.path = path;
            meta.filename = filename;
            meta.level = level;
            meta.entry_count = reader->header().entry_count;
            meta.file_size_bytes = fsize;
            meta.min_seq = reader->header().min_seq;
            meta.max_seq = reader->header().max_seq;
            meta.first_key = std::vector<uint8_t>(reader->first_key().begin(), reader->first_key().end());
            meta.last_key = std::vector<uint8_t>(reader->last_key().begin(), reader->last_key().end());
            meta.reader = std::shared_ptr<SSTReader>(std::move(reader));

            tmp[static_cast<size_t>(level)].push_back(std::move(meta));
        }

        // L0: newest-first = lexicographically largest filename first.
        std::sort(
            tmp[0].begin(),
            tmp[0].end(),
            [](const SSTMeta& a, const SSTMeta& b) {
                      return a.filename > b.filename;
                  });

        // L1+: first_key ascending (non-overlapping, globally sorted per level).
        for (int lvl = 1; lvl < opts_.max_levels; ++lvl) {
            std::sort(
                tmp[static_cast<size_t>(lvl)].begin(),
                tmp[static_cast<size_t>(lvl)].end(),
                [](const SSTMeta& a, const SSTMeta& b) { return a.first_key < b.first_key; }
            );
        }

        // Advance next_sst_id_ past all recovered files.
        next_sst_id_.store(max_id + 1, std::memory_order_relaxed);

        {
            std::unique_lock lock(sst_mu_);
            levels_ = std::move(tmp);
            publish_snapshot_locked();
        }

        // ── Orphan scan ───────────────────────────────────────────────────────
        // Delete any .aksst files in sst_dir that are NOT in the live set.
        // These are remnants of interrupted compactions (output files written to
        // disk but CompactionCommit not yet flushed) or interrupted L0 flushes
        // (file written but sst_seal not yet flushed).  They are safe to delete
        // because they are not referenced by any manifest record.
        std::error_code dir_ec;
        if (fs::exists(opts_.sst_dir, dir_ec) && !dir_ec) {
            // Build live filename set from the freshly loaded levels_.
            std::unordered_set<std::string> live_names;
            {
                std::shared_lock lock(sst_mu_);
                for (const auto& level : levels_) { for (const auto& m : level) live_names.insert(m.filename); }
            }

            for (const auto& entry : fs::directory_iterator(opts_.sst_dir, dir_ec)) {
                if (dir_ec) break;
                if (entry.path().extension() != ".aksst") continue;
                const auto fn = entry.path().filename().string();
                if (live_names.find(fn) == live_names.end()) {
                    std::error_code rm_ec;
                    fs::remove(entry.path(), rm_ec);
                    std::fprintf(stderr, "[SSTManager] recover: deleted orphan SST %s%s\n", fn.c_str(), rm_ec ? " (remove failed)" : "");
                }
            }
        }
    }

    // ============================================================================
    // flush
    // ============================================================================

    uint64_t SSTManager::flush(std::vector<core::MemRecord> sorted_records) {
        if (sorted_records.empty()) return 0;

        // ── Write L0 SST file (no locks held during I/O) ─────────────────────
        SSTWriter::Options wo;
        wo.level              = 0;
        wo.bloom_bits_per_key = opts_.bloom_bits_per_key;
        wo.index_stride       = opts_.index_stride;
        wo.codec = opts_.codec;

        const auto path     = make_sst_path(0);
        const auto filename = path.filename().string();
        const auto res = SSTWriter::write(path, sorted_records, wo);

        if (manifest_) {
            const auto fk_hex = res.first_key.empty()
                ? std::optional<std::string>{}
                : std::optional<std::string>(bytes_to_hex(res.first_key));
            const auto lk_hex = res.last_key.empty()
                ? std::optional<std::string>{}
                : std::optional<std::string>(bytes_to_hex(res.last_key));
            manifest_->sst_seal(0, filename, res.entry_count, fk_hex, lk_hex);
        }

        auto reader = SSTReader::open(path, opts_.preload_sst_data);
        if (!reader) {
            throw std::runtime_error(
                std::format("[SSTManager] flush: cannot re-open SST after write: {}",
                            path.string()));
        }

        uint64_t fsize = 0;
        std::error_code ec;
        fsize = fs::file_size(path, ec);
        if (ec) fsize = 0;

        auto meta = make_meta(res, 0, filename, fsize);
        meta.reader = std::shared_ptr<SSTReader>(std::move(reader));

        // ── Prepend to L0 (newest file = front) ──────────────────────────────
        {
            std::unique_lock lock(sst_mu_);
            levels_[0].insert(levels_[0].begin(), std::move(meta));
            publish_snapshot_locked();
        }

        // ── Wake background compaction pool (non-blocking) ───────────────────
        // notify_all: with multiple worker threads all should check for new work.
        {
            std::lock_guard lk(compact_notify_mu_);
            compact_requested_ = true;
        }
        compact_cv_.notify_all();

        return res.max_seq;
    }

    // ============================================================================
    // get
    // ============================================================================

    std::optional<core::MemRecord>
    SSTManager::get(std::span<const uint8_t> key) const {
        // ── Block cache lookup ────────────────────────────────────────────────
        const size_t sidx = cache_shard_idx(key);
        auto& shard = cache_shards_[sidx];
        const std::string key_str(reinterpret_cast<const char*>(key.data()), key.size());
        {
            std::lock_guard lk(shard.mu);
            if (auto cached = shard.cache_lookup(key_str)) return cached;
        }

        std::shared_lock lock(sst_mu_);

        // ── L0: newest-first linear scan (files may overlap) ─────────────────
        for (const auto& meta : levels_[0]) {
            if (!meta.reader) continue;
            auto rec = meta.reader->get(key);
            if (rec.has_value()) {
                lock.unlock();
                std::lock_guard lk(shard.mu);
                shard.cache_put(key_str, *rec);
                return rec;
            }
        }

        // ── L1+: binary search (non-overlapping, first_key sorted) ───────────
        for (int lvl = 1; lvl < opts_.max_levels; ++lvl) {
            const auto& level = levels_[static_cast<size_t>(lvl)];
            if (level.empty()) continue;

            // Find the first file whose last_key >= key (could contain our key).
            auto it = std::lower_bound(
                level.begin(),
                level.end(),
                key,
                [](const SSTMeta& m, std::span<const uint8_t> k) {
                    return std::lexicographical_compare(m.last_key.begin(), m.last_key.end(), k.begin(), k.end());
                }
            );

            if (it == level.end()) continue;
            if (!it->reader) continue;

            // Confirm key is within [first_key, last_key] before doing I/O.
            if (!it->reader->key_in_range(key)) continue;

            auto rec = it->reader->get(key);
            if (rec.has_value()) {
                lock.unlock();
                std::lock_guard lk(shard.mu);
                shard.cache_put(key_str, *rec);
                return rec;
            }
        }

        return std::nullopt;
    }

    // ============================================================================
    // SSTManager::contains
    // ============================================================================

    std::optional<bool> SSTManager::contains(std::span<const uint8_t> key) const {
        // ── Lock-free read via atomic snapshot ───────────────────────────────
        // Acquires the current levels snapshot without taking sst_mu_.
        // The shared_ptr keeps all SSTReader objects (and for compressed SSTs,
        // their in-memory data sections) alive for the duration of this call
        // even if compaction concurrently publishes a new snapshot.
        const auto snap = levels_snap_.load(std::memory_order_acquire);
        if (!snap || snap->empty()) return std::nullopt;
        const auto& levels = *snap;
        const int nlevels = static_cast<int>(levels.size());

        // Compute the bloom hash once here, then pass to every file's contains_fast().
        // Avoids recomputing bloom_fast_hash64 for each L0 file (multiple when L0 grows)
        // and for each L1+ candidate.  For BLOOM_TYPE_BLOCKED_FAST files this eliminates
        // (N_files - 1) redundant hash computations per exists() call.
        const uint64_t bloom_h = bloom_fast_hash64(key.data(), key.size());

        // ── L0: newest-first (files may overlap — check all until a hit) ──────
        // key_in_range intentionally omitted: bloom provides fast rejection
        // (~5 ns with BLOOM_TYPE_BLOCKED_FAST), saving the extra range cmp.
        for (const auto& meta : levels[0]) {
            if (!meta.reader) continue;
            auto r = meta.reader->contains_fast(key, bloom_h);
            if (r.has_value()) return r;
        }

        // ── L1+: non-overlapping, binary search per level ─────────────────────
        for (int lvl = 1; lvl < nlevels; ++lvl) {
            const auto& level = levels[static_cast<size_t>(lvl)];
            if (level.empty()) continue;

            // Find the first file whose last_key >= key.
            auto it = std::lower_bound(
                level.begin(),
                level.end(),
                key,
                [](const SSTMeta& m, std::span<const uint8_t> k) {
                    return std::lexicographical_compare(m.last_key.begin(), m.last_key.end(), k.begin(), k.end());
                }
            );
            if (it == level.end() || !it->reader) continue;
            if (!it->reader->key_in_range(key)) continue;

            auto r = it->reader->contains_fast(key, bloom_h);
            if (r.has_value()) return r;
        }

        return std::nullopt;
    }

    // ============================================================================
    // level_file_count / level_bytes
    // ============================================================================

    size_t SSTManager::level_file_count(int level) const {
        if (level < 0 || level >= opts_.max_levels) return 0;
        std::shared_lock lock(sst_mu_);
        return levels_[static_cast<size_t>(level)].size();
    }

    uint64_t SSTManager::level_bytes(int level) const {
        if (level < 0 || level >= opts_.max_levels) return 0;
        std::shared_lock lock(sst_mu_);
        uint64_t total = 0;
        for (const auto& m : levels_[static_cast<size_t>(level)]) total += m.file_size_bytes;
        return total;
    }

    // ============================================================================
    // compaction_loop  (one instance per pool thread)
    // ============================================================================

    void SSTManager::compaction_loop() {
        while (true) {
            // ── Wait for work ─────────────────────────────────────────────────
            {
                std::unique_lock lk(compact_notify_mu_);
                compact_cv_.wait(lk, [this] { return compact_requested_ || shutting_down_.load(std::memory_order_relaxed); });
                // Don't clear compact_requested_ here: other threads in the pool
                // also need to see it.  Each thread checks whether there is actually
                // work available (via pick_and_claim_work) and loops until idle.
            }

            if (shutting_down_.load(std::memory_order_relaxed)) break;

            // ── Drain: run all compaction tasks available to this thread ──────
            while (!shutting_down_.load(std::memory_order_relaxed)) {
                auto work = pick_and_claim_work();
                if (!work) {
                    // No non-conflicting work right now; another thread may have
                    // claimed it.  Clear the flag only when the pool is fully idle.
                    {
                        std::lock_guard lk(compact_notify_mu_);
                        // Only clear if nothing else is pending
                        bool pool_idle;
                        {
                            std::lock_guard busy_lk(compact_busy_mu_);
                            pool_idle = compact_busy_srcs_.empty();
                        }
                        if (pool_idle) compact_requested_ = false;
                    }
                    break;
                }
                run_compaction(std::move(*work));

                // After completing a compaction, cascade: notify others in case
                // the finished level now exceeds its budget.
                compact_cv_.notify_all();
            }
        }
    }

    // ============================================================================
    // pick_compaction_work  (requires sst_mu_ shared + compact_busy_mu_ held)
    // ============================================================================

    std::optional<SSTManager::CompactionWork> SSTManager::pick_compaction_work(const std::set<int>& busy_srcs) const {
        // ── Priority 1: L0 file count trigger ────────────────────────────────
        if (busy_srcs.find(0) == busy_srcs.end() && static_cast<int>(levels_[0].size()) >= opts_.max_l0_files) {
            CompactionWork w;
            w.src_level = 0;
            w.dst_level = 1;
            w.is_bottom = (1 == opts_.max_levels - 1);

            // All L0 files + all L1 files (L0 can overlap the full key range).
            w.inputs.insert(w.inputs.end(), levels_[0].begin(), levels_[0].end());
            if (opts_.max_levels > 1) { w.inputs.insert(w.inputs.end(), levels_[1].begin(), levels_[1].end()); }
            return w;
        }

        // ── Priority 2: size-based trigger for L1+ ───────────────────────────
        for (int lvl = 1; lvl < opts_.max_levels - 1; ++lvl) {
            // Skip levels already being compacted by another thread
            if (busy_srcs.find(lvl) != busy_srcs.end()) continue;

            const auto& level = levels_[static_cast<size_t>(lvl)];
            if (level.empty()) continue;

            uint64_t total = 0;
            for (const auto& m : level) total += m.file_size_bytes;
            if (total <= level_budget(lvl)) continue;

            // Pick the oldest file (lowest max_seq) from this level.
            const auto pick_it = std::min_element(level.begin(), level.end(), [](const SSTMeta& a, const SSTMeta& b) { return a.max_seq < b.max_seq; });

            CompactionWork w;
            w.src_level = lvl;
            w.dst_level = lvl + 1;
            w.is_bottom = (lvl + 1 == opts_.max_levels - 1);

            w.inputs.push_back(*pick_it);

            // Find overlapping files in dst_level.
            const auto& next_level = levels_[static_cast<size_t>(lvl + 1)];
            for (const auto& m : next_level) {
                if (key_ranges_overlap(pick_it->first_key, pick_it->last_key, m.first_key, m.last_key)) { w.inputs.push_back(m); }
            }

            return w;
        }

        return std::nullopt;
    }

    // ============================================================================
    // pick_and_claim_work  (atomically picks + marks src_level busy)
    // ============================================================================

    std::optional<SSTManager::CompactionWork> SSTManager::pick_and_claim_work() {
        std::shared_lock sst_lock(sst_mu_);
        std::lock_guard busy_lock(compact_busy_mu_);

        auto work = pick_compaction_work(compact_busy_srcs_);
        if (!work) return std::nullopt;

        // Claim: mark this src_level so other pool threads skip it
        compact_busy_srcs_.insert(work->src_level);
        return work;
    }

    // ============================================================================
    // write_output_files
    // ============================================================================

    std::vector<SSTManager::SSTMeta> SSTManager::write_output_files(std::vector<core::MemRecord> merged, int dst_level) {
        std::vector<SSTMeta> output_metas;
        if (merged.empty()) return output_metas;

        SSTWriter::Options wo;
        wo.level = dst_level;
        wo.bloom_bits_per_key = opts_.bloom_bits_per_key;
        wo.index_stride = opts_.index_stride;
        wo.codec = opts_.codec;

        // Split merged vector into chunks of at most target_file_size_bytes.
        size_t chunk_start = 0;

        while (chunk_start < merged.size()) {
            uint64_t accumulated = 0;
            size_t chunk_end = chunk_start;

            while (chunk_end < merged.size()) {
                accumulated += record_disk_size(merged[chunk_end]);
                ++chunk_end;
                if (accumulated >= opts_.target_file_size_bytes) break;
            }

            // Write this chunk.
            std::vector<core::MemRecord> chunk(
                std::make_move_iterator(merged.begin() + static_cast<ptrdiff_t>(chunk_start)),
                std::make_move_iterator(merged.begin() + static_cast<ptrdiff_t>(chunk_end))
            );

            const auto path = make_sst_path(dst_level);
            const auto filename = path.filename().string();
            const auto res = SSTWriter::write(path, chunk, wo);

            // Note: sst_seal is NOT called here for compaction outputs.
            // The caller (run_compaction) will issue a single atomic
            // compaction_commit() that adds all outputs and removes all inputs.

            auto reader = SSTReader::open(path, opts_.preload_sst_data);
            if (!reader) { throw std::runtime_error(std::format("[SSTManager] compact: cannot re-open output file: {}", path.string())); }

            uint64_t fsize = 0;
            std::error_code ec;
            fsize = fs::file_size(path, ec);
            if (ec) fsize = 0;

            auto meta = make_meta(res, dst_level, filename, fsize);
            meta.reader = std::shared_ptr<SSTReader>(std::move(reader));
            output_metas.push_back(std::move(meta));

            chunk_start = chunk_end;
        }

        return output_metas;
    }

    // ============================================================================
    // run_compaction
    // ============================================================================

    void SSTManager::run_compaction(CompactionWork work) {
        if (work.inputs.empty()) {
            // Release claim (src_level was marked busy in pick_and_claim_work)
            {
                std::lock_guard lk(compact_busy_mu_);
                compact_busy_srcs_.erase(work.src_level);
            }
            return;
        }

        const int src = work.src_level;
        const int dst = work.dst_level;

        // ── 1. Collect input filenames for manifest ───────────────────────────
        std::vector<std::string> input_filenames;
        input_filenames.reserve(work.inputs.size());
        for (const auto& m : work.inputs) input_filenames.push_back(m.filename);

        // ── 2. Record compaction start ────────────────────────────────────────
        if (manifest_) manifest_->compaction_start(src, input_filenames);

        // ── 3. Open scan iterators ────────────────────────────────────────────
        std::vector<SSTReader::Iterator> iters;
        iters.reserve(work.inputs.size());
        for (const auto& m : work.inputs) {
            if (!m.reader) continue;
            iters.push_back(m.reader->scan({}, {}));
        }

        // ── 4. k-way merge ───────────────────────────────────────────────────
        std::priority_queue<MergeEntry,
                            std::vector<MergeEntry>,
                            MergeEntryGreater> heap;

        for (size_t i = 0; i < iters.size(); ++i) {
            if (iters[i].has_next()) { heap.push({iters[i].next(), i}); }
        }

        std::vector<core::MemRecord> merged;
        std::vector<uint8_t>         prev_key;

        while (!heap.empty()) {
            core::MemRecord rec = heap.top().rec;
            const size_t    idx = heap.top().source_idx;
            heap.pop();

            if (iters[idx].has_next()) { heap.push({iters[idx].next(), idx}); }

            // Skip lower-seq duplicates.
            {
                const auto k = rec.key();
                if (!prev_key.empty() &&
                    prev_key.size() == k.size() &&
                    std::equal(prev_key.begin(), prev_key.end(), k.begin())) { continue; }
                prev_key.assign(k.begin(), k.end());
            }

            // Drop tombstones only at the bottom level.
            if (work.is_bottom && rec.is_tombstone()) continue;

            merged.push_back(std::move(rec));
        }

        // ── 5. Build compacted filename set ───────────────────────────────────
        std::unordered_set<std::string> compacted_set;
        compacted_set.reserve(work.inputs.size());
        for (const auto& m : work.inputs) compacted_set.insert(m.filename);

        // ── 6. Write output files (split at target_file_size_bytes) ──────────
        // Note: write_output_files handles empty merged (returns empty vector).
        auto output_metas = write_output_files(std::move(merged), dst);

        // ── 7. Atomic manifest commit ─────────────────────────────────────────
        // Single CRC-protected record: adds all outputs, removes all inputs.
        // If the process is killed before this write completes, replay sees no
        // CompactionCommit → input files remain live, output files become orphans
        // (cleaned by recover()'s orphan scan on next startup).
        if (manifest_) {
            std::vector<std::string> output_filenames;
            output_filenames.reserve(output_metas.size());
            for (const auto& m : output_metas) output_filenames.push_back(m.filename);

            manifest_->compaction_commit(output_filenames, input_filenames);
        }

        // ── 8. Atomically update levels_ ─────────────────────────────────────
        {
            std::unique_lock lock(sst_mu_);

            // Remove compacted files from src level (L0) and dst level (L1+).
            auto& src_vec = levels_[static_cast<size_t>(src)];
            src_vec.erase(std::remove_if(src_vec.begin(), src_vec.end(), [&](const SSTMeta& m) { return compacted_set.count(m.filename) > 0; }), src_vec.end());

            auto& dst_vec = levels_[static_cast<size_t>(dst)];
            dst_vec.erase(
                std::remove_if(
                    dst_vec.begin(),
                    dst_vec.end(),
                    [&](const SSTMeta& m) {
                                   return compacted_set.count(m.filename) > 0;
                               }),
                dst_vec.end()
            );

            // Insert new output files and re-sort by first_key (dst level L1+).
            for (auto& m : output_metas) dst_vec.push_back(std::move(m));

            if (dst > 0) { std::sort(dst_vec.begin(), dst_vec.end(), [](const SSTMeta& a, const SSTMeta& b) { return a.first_key < b.first_key; }); }

            // Publish updated snapshot so lock-free contains() sees the new state.
            publish_snapshot_locked();
        }

        // ── 9. Notify stall CV + clear block cache ────────────────────────────
        // L0 files were removed: wake any put() threads blocked in stall_if_l0_full().
        // Acquire l0_stall_mu_ momentarily to ensure the notify is not missed by a
        // thread between its predicate check and its wait() call.
        {
            std::lock_guard stall_lk(l0_stall_mu_);
        }
        l0_stall_cv_.notify_all();

        // Invalidate the block cache: compacted files were replaced with new
        // output files, so any cached entries may reference stale SST data.
        cache_clear_all();

        // ── 10. Release src_level busy claim ──────────────────────────────────
        // Done before I/O so other threads can pick new work for this level
        // immediately (the level_ state has already been updated under sst_mu_).
        {
            std::lock_guard lk(compact_busy_mu_);
            compact_busy_srcs_.erase(work.src_level);
        }

        // ── 11. Delete old files from disk ─────────────────────────────────────
        // Windows: close all handles before deleting.
        iters.clear();
        for (auto& m : work.inputs) m.reader.reset();

        for (const auto& m : work.inputs) {
            std::error_code ec;
            fs::remove(m.path, ec);
            if (ec) {
                std::fprintf(stderr,
                             "[SSTManager] compact: failed to delete %s: %s\n",
                             m.path.string().c_str(),
                             ec.message().c_str()
                );
            }
        }
    }

} // namespace akkaradb::engine::sst
