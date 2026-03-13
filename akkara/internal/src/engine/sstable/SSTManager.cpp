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

// internal/src/engine/sst/SSTManager.cpp
#include "engine/sstable/SSTManager.hpp"

#include <algorithm>
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
        /// Hex-encodes a byte span into a lowercase hex string.
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
            size_t          source_idx; ///< index into the iterator array
        };

        /// Min-heap comparator: smallest key first; same key → highest seq first.
        /// Returns true when a should have LOWER priority than b (i.e., a is "larger").
        struct MergeEntryGreater {
            bool operator()(const MergeEntry& a, const MergeEntry& b) const noexcept {
                const int cmp = a.rec.compare_key(b.rec);
                if (cmp != 0) return cmp > 0;    // larger key → lower priority
                return a.rec.seq() < b.rec.seq(); // lower seq  → lower priority
            }
        };
    } // anonymous namespace

    // ============================================================================
    // Construction / destruction
    // ============================================================================

    SSTManager::SSTManager(Options opts, manifest::Manifest* manifest)
        : opts_(std::move(opts))
        , manifest_(manifest) {}

    SSTManager::~SSTManager() = default;

    std::unique_ptr<SSTManager> SSTManager::create(Options opts,
                                                    manifest::Manifest* manifest) {
        fs::create_directories(opts.sst_dir);
        return std::unique_ptr<SSTManager>(new SSTManager(std::move(opts), manifest));
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
                                               const std::string& filename) {
        SSTMeta m;
        m.path        = res.path;
        m.filename    = filename;
        m.level       = level;
        m.entry_count = res.entry_count;
        m.min_seq     = res.min_seq;
        m.max_seq     = res.max_seq;
        m.first_key   = res.first_key;
        m.last_key    = res.last_key;
        m.reader      = nullptr; // caller sets this
        return m;
    }

    // ============================================================================
    // recover
    // ============================================================================

    void SSTManager::recover() {
        if (!manifest_) return;

        const auto live = manifest_->live_sst();
        if (live.empty()) return;

        std::vector<SSTMeta> new_l0;
        std::vector<SSTMeta> new_l1;
        uint64_t max_id = 0;

        for (const auto& filename : live) {
            // Expected format: "L{level}_{id:016x}.aksst"  (total >= 22 chars)
            // E.g.: "L0_000000000000000a.aksst"
            if (filename.size() < 22 || filename[0] != 'L' || filename[2] != '_') {
                std::fprintf(stderr,
                             "[SSTManager] recover: skipping unknown file: %s\n",
                             filename.c_str());
                continue;
            }

            const int level = filename[1] - '0';
            if (level < 0 || level > 7) {
                std::fprintf(stderr,
                             "[SSTManager] recover: invalid level in: %s\n",
                             filename.c_str());
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

            SSTMeta meta;
            meta.path        = path;
            meta.filename    = filename;
            meta.level       = level;
            meta.entry_count = reader->header().entry_count;
            meta.min_seq     = reader->header().min_seq;
            meta.max_seq     = reader->header().max_seq;
            meta.first_key   = std::vector<uint8_t>(reader->first_key().begin(),
                                                     reader->first_key().end());
            meta.last_key    = std::vector<uint8_t>(reader->last_key().begin(),
                                                     reader->last_key().end());
            meta.reader      = std::shared_ptr<SSTReader>(std::move(reader));

            if (level == 0) {
                new_l0.push_back(std::move(meta));
            } else {
                new_l1.push_back(std::move(meta));
            }
        }

        // L0: newest-first = lexicographically largest filename first
        // (filenames have hex IDs that sort correctly as strings).
        std::sort(new_l0.begin(), new_l0.end(),
                  [](const SSTMeta& a, const SSTMeta& b) {
                      return a.filename > b.filename;
                  });

        // L1: sorted by first_key ascending (non-overlapping, globally sorted).
        std::sort(new_l1.begin(), new_l1.end(),
                  [](const SSTMeta& a, const SSTMeta& b) {
                      return a.first_key < b.first_key;
                  });

        // Advance next_sst_id_ past all recovered files.
        next_sst_id_.store(max_id + 1, std::memory_order_relaxed);

        std::unique_lock lock(sst_mu_);
        l0_files_ = std::move(new_l0);
        l1_files_ = std::move(new_l1);
    }

    // ============================================================================
    // flush
    // ============================================================================

    uint64_t SSTManager::flush(std::vector<core::MemRecord> sorted_records) {
        if (sorted_records.empty()) return 0;

        // Build write options.
        SSTWriter::Options wo;
        wo.level              = 0;
        wo.bloom_bits_per_key = opts_.bloom_bits_per_key;
        wo.index_stride       = opts_.index_stride;

        // Allocate a unique path before touching any lock.
        const auto path     = make_sst_path(0);
        const auto filename = path.filename().string();

        // Write the SST file (pure I/O, no locks held).
        const auto res = SSTWriter::write(path, sorted_records, wo);

        // Log to manifest (manifest is independently thread-safe).
        if (manifest_) {
            const auto fk_hex = res.first_key.empty()
                ? std::optional<std::string>{}
                : std::optional<std::string>(bytes_to_hex(res.first_key));
            const auto lk_hex = res.last_key.empty()
                ? std::optional<std::string>{}
                : std::optional<std::string>(bytes_to_hex(res.last_key));
            manifest_->sst_seal(0, filename, res.entry_count, fk_hex, lk_hex);
        }

        // Open a reader for the newly written file.
        auto reader = SSTReader::open(path);
        if (!reader) {
            throw std::runtime_error(
                std::format("[SSTManager] flush: cannot re-open SST after write: {}",
                            path.string()));
        }

        // Build SSTMeta.
        auto meta    = make_meta(res, 0, filename);
        meta.reader  = std::shared_ptr<SSTReader>(std::move(reader));

        // Prepend to l0_files_ (newest file = front).
        {
            std::unique_lock lock(sst_mu_);
            l0_files_.insert(l0_files_.begin(), std::move(meta));
        }

        // Trigger compaction if L0 has grown too large.
        maybe_compact();

        return res.max_seq;
    }

    // ============================================================================
    // get
    // ============================================================================

    std::optional<core::MemRecord>
    SSTManager::get(std::span<const uint8_t> key) const {
        std::shared_lock lock(sst_mu_);

        // Search L0 newest-first (files may have overlapping key ranges).
        for (const auto& meta : l0_files_) {
            if (!meta.reader) continue;
            auto rec = meta.reader->get(key);
            if (rec.has_value()) return rec;
        }

        // Search L1 (at most one file in Phase 5).
        for (const auto& meta : l1_files_) {
            if (!meta.reader) continue;
            auto rec = meta.reader->get(key);
            if (rec.has_value()) return rec;
        }

        return std::nullopt;
    }

    // ============================================================================
    // maybe_compact
    // ============================================================================

    void SSTManager::maybe_compact() {
        // Quick check without compaction lock.
        {
            std::shared_lock lock(sst_mu_);
            if (static_cast<int>(l0_files_.size()) < opts_.max_l0_files) return;
        }

        // Try to acquire the compaction mutex without blocking.
        // If another compaction is already running, skip — it will handle the backlog.
        std::unique_lock<std::mutex> compact_lock(compact_mu_, std::try_to_lock);
        if (!compact_lock.owns_lock()) return;

        // Re-check under the compaction lock: another thread might have compacted
        // between our quick check and acquiring compact_mu_.
        {
            std::shared_lock lock(sst_mu_);
            if (static_cast<int>(l0_files_.size()) < opts_.max_l0_files) return;
        }

        compact_l0_to_l1_locked();
    }

    // ============================================================================
    // compact_l0_to_l1_locked
    //
    // Caller must hold compact_mu_.
    // ============================================================================

    void SSTManager::compact_l0_to_l1_locked() {
        // ── 1. Snapshot input files (brief shared lock) ──────────────────────────
        std::vector<SSTMeta> inputs;
        {
            std::shared_lock lock(sst_mu_);
            inputs.insert(inputs.end(), l0_files_.begin(), l0_files_.end());
            inputs.insert(inputs.end(), l1_files_.begin(), l1_files_.end());
        }
        if (inputs.empty()) return;

        // ── 2. Collect input filenames for manifest ──────────────────────────────
        std::vector<std::string> input_filenames;
        input_filenames.reserve(inputs.size());
        for (const auto& m : inputs) input_filenames.push_back(m.filename);

        // ── 3. Record compaction start in manifest ───────────────────────────────
        if (manifest_) manifest_->compaction_start(0, input_filenames);

        // ── 4. Open iterators for all input files ────────────────────────────────
        std::vector<SSTReader::Iterator> iters;
        iters.reserve(inputs.size());
        for (const auto& m : inputs) {
            if (!m.reader) continue;
            iters.push_back(m.reader->scan({}, {}));
        }

        // ── 5. k-way merge using a min-heap ──────────────────────────────────────
        //
        // std::greater<MergeEntry> produces a min-heap (uses MergeEntry::operator<).
        // Top element = smallest key; equal keys → highest seq on top.
        std::priority_queue<MergeEntry,
                            std::vector<MergeEntry>,
                            MergeEntryGreater> heap;

        for (size_t i = 0; i < iters.size(); ++i) {
            if (iters[i].has_next()) {
                heap.push({iters[i].next(), i});
            }
        }

        std::vector<core::MemRecord> merged;
        std::vector<uint8_t>         prev_key;

        while (!heap.empty()) {
            // Copy out the top record and its iterator index, then pop.
            core::MemRecord rec = heap.top().rec;
            const size_t    idx = heap.top().source_idx;
            heap.pop();

            // Advance the source iterator and push its next record.
            if (iters[idx].has_next()) {
                heap.push({iters[idx].next(), idx});
            }

            // Skip lower-seq duplicates of the same key.
            // The heap already surfaces the highest-seq record first for each key.
            {
                const auto k = rec.key();
                if (!prev_key.empty() &&
                    prev_key.size() == k.size() &&
                    std::equal(prev_key.begin(), prev_key.end(), k.begin())) {
                    continue; // duplicate key — lower-seq version; skip
                }
                prev_key.assign(k.begin(), k.end());
            }

            // At L1 (bottom level), tombstones are dropped entirely.
            if (rec.is_tombstone()) continue;

            merged.push_back(std::move(rec));
        }

        // ── 6. Build set of compacted filenames for in-memory state update ───────
        std::unordered_set<std::string> compacted_set;
        compacted_set.reserve(inputs.size());
        for (const auto& m : inputs) compacted_set.insert(m.filename);

        // ── 7. Handle empty merge result (all tombstones / all inputs empty) ─────
        if (merged.empty()) {
            if (manifest_) {
                manifest_->compaction_end(0, "", input_filenames, 0,
                                          std::nullopt, std::nullopt);
                for (const auto& fn : input_filenames) manifest_->sst_delete(fn);
            }

            {
                std::unique_lock lock(sst_mu_);
                l0_files_.erase(
                    std::remove_if(l0_files_.begin(), l0_files_.end(),
                                   [&](const SSTMeta& m) {
                                       return compacted_set.count(m.filename) > 0;
                                   }),
                    l0_files_.end());
                l1_files_.clear(); // all L1 was in inputs
            }

            // Windows: close all file handles before deleting.
            iters.clear();
            for (auto& m : inputs) m.reader.reset();

            for (const auto& m : inputs) {
                std::error_code ec;
                fs::remove(m.path, ec);
            }
            return;
        }

        // ── 8. Write the new L1 SST ──────────────────────────────────────────────
        SSTWriter::Options wo;
        wo.level              = 1;
        wo.bloom_bits_per_key = opts_.bloom_bits_per_key;
        wo.index_stride       = opts_.index_stride;

        const auto new_path     = make_sst_path(1);
        const auto new_filename = new_path.filename().string();
        const auto res          = SSTWriter::write(new_path, merged, wo);

        // ── 9. Update manifest ───────────────────────────────────────────────────
        if (manifest_) {
            const auto fk_hex = res.first_key.empty()
                ? std::optional<std::string>{}
                : std::optional<std::string>(bytes_to_hex(res.first_key));
            const auto lk_hex = res.last_key.empty()
                ? std::optional<std::string>{}
                : std::optional<std::string>(bytes_to_hex(res.last_key));

            manifest_->sst_seal(1, new_filename, res.entry_count, fk_hex, lk_hex);
            manifest_->compaction_end(0, new_filename, input_filenames,
                                      res.entry_count, fk_hex, lk_hex);
            for (const auto& fn : input_filenames) manifest_->sst_delete(fn);
        }

        // ── 10. Open a reader for the new L1 file ────────────────────────────────
        auto new_reader = SSTReader::open(new_path);
        if (!new_reader) {
            throw std::runtime_error(
                std::format("[SSTManager] compact: cannot re-open new L1: {}",
                            new_path.string()));
        }
        SSTMeta new_meta   = make_meta(res, 1, new_filename);
        new_meta.reader    = std::shared_ptr<SSTReader>(std::move(new_reader));

        // ── 11. Atomically swap in the new L1, remove compacted L0/L1 ───────────
        //
        // Only remove the files that were part of the compaction snapshot.
        // Any new L0 files added to l0_files_ during compaction are preserved.
        std::vector<fs::path> to_delete;
        to_delete.reserve(inputs.size());
        for (const auto& m : inputs) to_delete.push_back(m.path);

        {
            std::unique_lock lock(sst_mu_);

            // Remove only compacted L0 files; leave any new ones.
            l0_files_.erase(
                std::remove_if(l0_files_.begin(), l0_files_.end(),
                               [&](const SSTMeta& m) {
                                   return compacted_set.count(m.filename) > 0;
                               }),
                l0_files_.end());

            // Replace L1 entirely (all L1 files were included in the compaction).
            l1_files_.clear();
            l1_files_.push_back(std::move(new_meta));
        }

        // ── 12. Delete old SST files from disk ───────────────────────────────────
        // Windows: close all file handles (iterators + readers) before deleting.
        iters.clear();
        for (auto& m : inputs) m.reader.reset();

        for (const auto& p : to_delete) {
            std::error_code ec;
            fs::remove(p, ec);
            if (ec) {
                std::fprintf(stderr,
                             "[SSTManager] compact: failed to delete %s: %s\n",
                             p.string().c_str(), ec.message().c_str());
            }
        }
    }

    // ============================================================================
    // l0_count / l1_count
    // ============================================================================

    size_t SSTManager::l0_count() const {
        std::shared_lock lock(sst_mu_);
        return l0_files_.size();
    }

    size_t SSTManager::l1_count() const {
        std::shared_lock lock(sst_mu_);
        return l1_files_.size();
    }

} // namespace akkaradb::engine::sst
