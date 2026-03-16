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

        // Start background compaction thread immediately.
        // The thread waits on compact_cv_ — safe to start before recover().
        compact_thread_ = std::thread([this] { compaction_loop(); });
    }

    SSTManager::~SSTManager() {
        // shutdown() should already have been called by AkkEngine::close().
        // Belt-and-suspenders: ensure thread is joined.
        if (!shutting_down_.load(std::memory_order_relaxed)) {
            shutting_down_.store(true, std::memory_order_relaxed);
            compact_cv_.notify_all();
        }
        if (compact_thread_.joinable()) compact_thread_.join();
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
        if (compact_thread_.joinable()) compact_thread_.join();
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
            auto reader = SSTReader::open(path);
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

        std::unique_lock lock(sst_mu_);
        levels_ = std::move(tmp);
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

        auto reader = SSTReader::open(path);
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
        }

        // ── Wake background compaction thread (non-blocking) ─────────────────
        {
            std::lock_guard lk(compact_notify_mu_);
            compact_requested_ = true;
        }
        compact_cv_.notify_one();

        return res.max_seq;
    }

    // ============================================================================
    // get
    // ============================================================================

    std::optional<core::MemRecord>
    SSTManager::get(std::span<const uint8_t> key) const {
        std::shared_lock lock(sst_mu_);

        // ── L0: newest-first linear scan (files may overlap) ─────────────────
        for (const auto& meta : levels_[0]) {
            if (!meta.reader) continue;
            auto rec = meta.reader->get(key);
            if (rec.has_value()) return rec;
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
            if (rec.has_value()) return rec;
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
    // compaction_loop  (background thread)
    // ============================================================================

    void SSTManager::compaction_loop() {
        while (true) {
            // ── Wait for work ─────────────────────────────────────────────────
            {
                std::unique_lock lk(compact_notify_mu_);
                compact_cv_.wait(lk, [this] { return compact_requested_ || shutting_down_.load(std::memory_order_relaxed); });
                compact_requested_ = false;
            }

            if (shutting_down_.load(std::memory_order_relaxed)) break;

            // ── Drain: run cascading compactions until nothing left to do ─────
            while (!shutting_down_.load(std::memory_order_relaxed)) {
                auto work = pick_compaction_work();
                if (!work) break;
                run_compaction(std::move(*work));
            }
        }
    }

    // ============================================================================
    // pick_compaction_work
    // ============================================================================

    std::optional<SSTManager::CompactionWork> SSTManager::pick_compaction_work() const {
        std::shared_lock lock(sst_mu_);

        // ── Priority 1: L0 file count trigger ────────────────────────────────
        if (static_cast<int>(levels_[0].size()) >= opts_.max_l0_files) {
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
    // write_output_files
    // ============================================================================

    std::vector<SSTManager::SSTMeta> SSTManager::write_output_files(std::vector<core::MemRecord> merged, int dst_level) {
        std::vector<SSTMeta> output_metas;
        if (merged.empty()) return output_metas;

        SSTWriter::Options wo;
        wo.level = dst_level;
        wo.bloom_bits_per_key = opts_.bloom_bits_per_key;
        wo.index_stride = opts_.index_stride;

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

            if (manifest_) {
                const auto fk_hex = res.first_key.empty() ? std::optional<std::string>{} : std::optional<std::string>(bytes_to_hex(res.first_key));
                const auto lk_hex = res.last_key.empty() ? std::optional<std::string>{} : std::optional<std::string>(bytes_to_hex(res.last_key));
                manifest_->sst_seal(dst_level, filename, res.entry_count, fk_hex, lk_hex);
            }

            auto reader = SSTReader::open(path);
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
        if (work.inputs.empty()) return;

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

        // ── 7. Update manifest ────────────────────────────────────────────────
        if (manifest_) {
            const std::string output_summary = output_metas.empty() ? std::string{} : output_metas.front().filename; // primary output name for logging

            const auto fk_hex = output_metas.empty() || output_metas.front().first_key.empty()
                                    ? std::optional<std::string>{}
                                    : std::optional<std::string>(bytes_to_hex(output_metas.front().first_key));
            const auto lk_hex = output_metas.empty() || output_metas.back().last_key.empty()
                                    ? std::optional<std::string>{}
                                    : std::optional<std::string>(bytes_to_hex(output_metas.back().last_key));

            uint64_t total_entries = 0;
            for (const auto& m : output_metas) total_entries += m.entry_count;

            manifest_->compaction_end(src, output_summary, input_filenames, total_entries, fk_hex, lk_hex);
            for (const auto& fn : input_filenames) manifest_->sst_delete(fn);
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
        }

        // ── 9. Delete old files from disk ─────────────────────────────────────
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
