/*
* AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/src/engine/AkkEngine.cpp
#include "engine/AkkEngine.hpp"
#include "engine/wal/WalOp.hpp"
#include "format-akk/AkkBlockPacker.hpp"
#include "format-akk/AkkBlockUnpacker.hpp"
#include "format-akk/parity/NoParityCoder.hpp"
#include "core/record/AKHdr32.hpp"
#include <algorithm>
#include <fstream>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>

#include "engine/wal/WalRecovery.hpp"

namespace akkaradb::engine {
    namespace {
        /**
         * Converts key to hex string (first 16 bytes).
         */
        std::string to_hex(std::span<const uint8_t> key) {
            std::stringstream ss;
            ss << std::hex << std::setfill('0');

            const size_t max_bytes = (std::min)(key.size(), size_t{16});
            for (size_t i = 0; i < max_bytes; ++i) { ss << std::setw(2) << static_cast<int>(key[i]); }

            return ss.str();
        }

        /**
         * Returns the appropriate parity coder for m parity lanes.
         */
        std::shared_ptr<format::ParityCoder> make_parity_coder(size_t m) {
            using namespace format::akk;
            switch (m) {
                case 0: return format::NoParityCoder::create();
                case 1: return format::XorParityCoder::create();
                case 2: return format::DualXorParityCoder::create();
                default: return format::NoParityCoder::create(); //RSParityCoder::create(m);
            }
        }
    } // anonymous namespace

    // ==================== AkkEngine ====================

    std::unique_ptr<AkkEngine> AkkEngine::open(const Options& opts) {
        // Create directories
        std::filesystem::create_directories(opts.base_dir);
        const auto sst_dir = opts.base_dir / "sst";
        const auto l0_dir = sst_dir / "L0";
        std::filesystem::create_directories(l0_dir);

        // Create buffer pool (as shared_ptr)
        auto buffer_pool = std::shared_ptr<core::BufferPool>(core::BufferPool::create(32 * 1024, 4096, opts.buffer_pool_size).release());

        // Create manifest (as shared_ptr)
        const auto manifest_path = opts.base_dir / "manifest.akmf";
        auto manifest = std::shared_ptr<manifest::Manifest>(manifest::Manifest::create(manifest_path, true).release());

        // Start Manifest
        manifest->start();

        // Create compactor (as shared_ptr)
        auto compactor = std::shared_ptr<sstable::SSTCompactor>(
            sstable::SSTCompactor::create(sst_dir, manifest, buffer_pool, opts.max_sst_per_level).release()
        );

        // Create MemTable with flush callback (will be set later)
        auto memtable = memtable::MemTable::create(
            opts.memtable_shard_count,
            opts.memtable_threshold_bytes / opts.memtable_shard_count,
            nullptr // Callback set after AkkEngine construction
        );

        // Create WAL
        const auto wal_path = opts.base_dir / "wal.akwal";
        auto wal = wal::WalWriter::create(wal_path, opts.wal_group_n, opts.wal_group_micros, opts.wal_fast_mode);

        // Recover from WAL
        if (std::filesystem::exists(wal_path)) {
            wal::WalRecovery::replay(
                wal_path,
                [&](auto key, auto value, uint64_t seq) { memtable->put(key, value, seq); },
                [&](auto key, uint64_t seq) { memtable->remove(key, seq); },
                nullptr // No checkpoint handler
            );

            // Note: recovery result can be logged
        }

        // Construct AkkEngine
        auto db = std::unique_ptr<AkkEngine>(new AkkEngine(std::move(opts), std::move(memtable), std::move(wal), manifest, compactor, buffer_pool));

        // Wire the flush callback now that db is fully constructed.
        // This cannot be done earlier because the lambda captures db.get(),
        // which is only valid after the AkkEngine object exists.
        db->memtable_->set_flush_callback([raw = db.get()](std::vector<core::MemRecord> batch) { raw->on_flush(std::move(batch)); });

        // --- Stripe layer setup ---
        const auto lane_dir = db->opts_.base_dir / "lanes";

        db->stripe_store_ = StripeStore::create(
            lane_dir,
            db->opts_.stripe_k,
            db->opts_.stripe_m,
            format::akk::AkkBlockPacker::BLOCK_SIZE,
            db->opts_.stripe_fast_mode
        );

        db->stripe_writer_ = format::akk::AkkStripeWriter::create(
            db->opts_.stripe_k,
            db->opts_.stripe_m,
            make_parity_coder(db->opts_.stripe_m),
            [raw_store = db->stripe_store_.get()](format::StripeWriter::Stripe s) { raw_store->on_stripe_ready(std::move(s)); },
            db->opts_.stripe_flush_policy
        );

        // Stripe recovery: restore last_sealed position from lane file sizes.
        {
            const auto rec = db->stripe_store_->recover();
            if (rec.truncated_tail) {
                // Discard any partial trailing block by truncating all lanes to
                // the last fully-written stripe.
                const int64_t keep = rec.last_sealed >= 0
                                         ? rec.last_sealed + 1
                                         : 0;
                db->stripe_store_->truncate(keep);
            }
            if (rec.last_sealed >= 0) {
                manifest->checkpoint(std::optional("stripeRecover"), std::optional(rec.last_sealed), std::optional(db->memtable_->last_seq()));
            }
        }

        // Optional stripe reader for get() fallback.
        if (db->opts_.use_stripe_for_read) {
            db->stripe_reader_ = format::akk::AkkStripeReader::create(
                db->opts_.stripe_k,
                db->opts_.stripe_m,
                format::akk::AkkBlockPacker::BLOCK_SIZE,
                make_parity_coder(db->opts_.stripe_m)
            );
            db->block_unpacker_ = format::akk::AkkBlockUnpacker::create();
        }

        // Load existing SSTables
        db->rebuild_readers();

        // Compact any levels that exceeded threshold from a previous run.
        try { while (db->compactor_->compact_one()) { db->rebuild_readers(); } }
        catch (...) {}

        return db;
    }

    AkkEngine::AkkEngine(
        Options opts,
        std::unique_ptr<memtable::MemTable> memtable,
        std::unique_ptr<wal::WalWriter> wal,
        std::shared_ptr<manifest::Manifest> manifest,
        std::shared_ptr<sstable::SSTCompactor> compactor,
        std::shared_ptr<core::BufferPool> buffer_pool
    )
        : opts_{std::move(opts)},
          memtable_{std::move(memtable)},
          wal_{std::move(wal)},
          manifest_{std::move(manifest)},
          compactor_{std::move(compactor)},
          buffer_pool_{std::move(buffer_pool)} {
        // Start background compaction thread (skipped if explicitly disabled).
        compact_running_ = !opts_.disable_background_compaction;
        last_compact_time_ = std::chrono::steady_clock::now();
        if (compact_running_) {
            compact_thread_ = std::thread(
                [this]() {
                    using Clock = std::chrono::steady_clock;
                    using Ms = std::chrono::milliseconds;

                    while (true) {
                        std::unique_lock lock{compact_mutex_};

                        // Determine how long to wait.
                        // Wake up when ANY of these is true:
                        //   (a) shutdown requested
                        //   (b) idle trigger: compact_pending_ AND (now - last_flush_time_) >= idle_ms
                        //   (c) force trigger: (now - last_compact_time_) >= force_ms (if force_ms > 0)
                        const bool force_enabled = opts_.compact_force_ms > 0;

                        // Wait with a timeout so we can re-evaluate the force trigger periodically.
                        // Max sleep = min(idle_ms, force_ms) to avoid missing deadlines.
                        const auto sleep_ms = [&]() -> Ms {
                            auto ms = Ms(
                                opts_.compact_idle_ms > 0
                                    ? opts_.compact_idle_ms
                                    : 100
                            );
                            if (force_enabled) { ms = (std::min)(ms, Ms(opts_.compact_force_ms)); }
                            return ms;
                        }();

                        compact_cv_.wait_for(lock, sleep_ms, [this]() { return !compact_running_; });

                        if (!compact_running_) { break; }

                        const auto now = Clock::now();

                        // Check force trigger first (runs even without a recent flush).
                        const bool force_due = force_enabled && (now - last_compact_time_) >= Ms(opts_.compact_force_ms);

                        // Check idle trigger.
                        const bool idle_configured = opts_.compact_idle_ms > 0;
                        const bool idle_due = compact_pending_ && (!idle_configured || (now - last_flush_time_) >= Ms(opts_.compact_idle_ms));

                        if (!force_due && !idle_due) {
                            continue; // Nothing to do yet; go back to sleep.
                        }

                        compact_pending_ = false;
                        lock.unlock();

                        // Compact one level, then immediately re-enter the loop.
                        // If more levels need work the force/idle check will trigger again right away.
                        try {
                            // Release readers so Windows can delete input SST files.
                            {
                                std::unique_lock rlock{readers_mutex_};
                                readers_.clear();
                            }
                            const bool did_work = compactor_->compact_one();
                            rebuild_readers();

                            std::unique_lock relock{compact_mutex_};
                            last_compact_time_ = Clock::now();
                            if (did_work) {
                                // More levels may need compaction; mark pending so the
                                // idle countdown resets to 0 and we compact again immediately.
                                compact_pending_ = true;
                                last_flush_time_ = Clock::now();
                            }
                        }
                        catch (...) {}
                    }
                }
            );
        } // end if (compact_running_)
    }

    AkkEngine::~AkkEngine() {
        // close() may have already joined the thread; guard with joinable().
        if (compact_thread_.joinable()) {
            {
                std::unique_lock lock{compact_mutex_};
                compact_running_ = false;
                compact_pending_ = false;
            }
            compact_cv_.notify_one();
            compact_thread_.join();
        }
    }

    uint64_t AkkEngine::put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        const uint64_t seq = memtable_->next_seq();

        // Write to WAL first (durable before apply)
        auto op = wal::WalOp::put(key, value, seq);
        wal_->append(op);

        // Apply to MemTable
        memtable_->put(key, value, seq);

        return seq;
    }

    uint64_t AkkEngine::del(std::span<const uint8_t> key) {
        const uint64_t seq = memtable_->next_seq();

        // Write to WAL first
        auto op = wal::WalOp::del(key, seq);
        wal_->append(op);

        // Apply tombstone to MemTable
        memtable_->remove(key, seq);

        return seq;
    }

    std::optional<std::vector<uint8_t>> AkkEngine::get(std::span<const uint8_t> key) {
        // 1. Check MemTable (fast path)
        if (auto record = memtable_->get(key)) {
            if (record->is_tombstone()) { return std::nullopt; }

            auto value_span = record->value();
            return std::vector(value_span.begin(), value_span.end());
        }

        // 2. Check SSTables (newest first)
        {
            std::shared_lock lock{readers_mutex_};
            for (auto& reader : readers_) {
                auto record_opt = reader->get(key);
                if (record_opt.has_value()) {
                    auto value_span = record_opt->value();
                    return std::vector(value_span.begin(), value_span.end());
                }
            }
        }

        // 3. Stripe fallback (optional; only when use_stripe_for_read = true)
        if (stripe_reader_&& stripe_store_ && block_unpacker_) {
            const int64_t total_stripes = stripe_store_->last_sealed_stripe() + 1;
            int64_t best_seq = -1;
            bool best_tombstone = false;
            std::vector<uint8_t> best_value;

            for (int64_t si = 0; si < total_stripes; ++si) {
                auto raw_blocks = stripe_store_->read_stripe_blocks(si);
                if (raw_blocks.empty()) break;

                // Build BufferView spans for the reader
                std::vector<core::BufferView> views;
                views.reserve(raw_blocks.size());
                for (const auto& b : raw_blocks) views.push_back(b.view());

                auto result = stripe_reader_->read_stripe(views);
                if (!result) continue; // unrecoverable; skip

                for (const auto& data_block : result->data_blocks) {
                    auto cursor = block_unpacker_->cursor(data_block.view());
                    while (cursor->has_next()) {
                        auto entry_opt = cursor->try_next();
                        if (!entry_opt) break;
                        const auto& entry = *entry_opt;
                        if (entry.compare_key(key) != 0) continue;

                        const int64_t seq = static_cast<int64_t>(entry.seq());
                        if (seq > best_seq) {
                            best_seq = seq;
                            best_tombstone = entry.is_tombstone();
                            if (!best_tombstone) {
                                const auto v = entry.value();
                                best_value.assign(v.begin(), v.end());
                            }
                            else { best_value.clear(); }
                        }
                        else if (seq == best_seq && entry.is_tombstone()) {
                            // Tie-break: tombstone wins
                            best_tombstone = true;
                            best_value.clear();
                        }
                    }
                }
            }

            if (best_seq >= 0) {
                if (best_tombstone) return std::nullopt;
                return best_value;
            }
        }

        return std::nullopt;
    }

    bool AkkEngine::compare_and_swap(std::span<const uint8_t> key, uint64_t expected_seq, const std::optional<std::span<const uint8_t>>& new_value) {
        // Get current record
        auto current = memtable_->get(key);

        if (!current) {
            return false; // Key doesn't exist
        }

        if (current->seq() != expected_seq) {
            return false; // Sequence mismatch
        }

        // Perform update
        const uint64_t new_seq = memtable_->next_seq();

        if (new_value.has_value()) {
            // Update
            auto op = wal::WalOp::put(key, *new_value, new_seq);
            wal_->append(op);
            memtable_->put(key, *new_value, new_seq);
        }
        else {
            // Delete
            auto op = wal::WalOp::del(key, new_seq);
            wal_->append(op);
            memtable_->remove(key, new_seq);
        }

        return true;
    }

    std::vector<core::MemRecord> AkkEngine::range(std::span<const uint8_t> start_key, std::optional<std::span<const uint8_t>> end_key) {
        memtable::MemTable::KeyRange mem_range{
            std::vector(start_key.begin(), start_key.end()),
            end_key.has_value()
                ? std::vector(end_key->begin(), end_key->end())
                : std::vector<uint8_t>{}
        };

        auto mem_iter = memtable_->iterator(mem_range);

        std::vector<sstable::SSTableReader::RangeIterator> sst_iterators;
        {
            std::shared_lock lock{readers_mutex_};
            for (auto& reader : readers_) { sst_iterators.push_back(reader->range_iter(start_key, end_key)); }
        }

        struct Entry {
            core::MemRecord record;
            size_t source_idx;

            bool operator>(const Entry& other) const {
                const int cmp = record.compare_key(other.record);
                if (cmp != 0) return cmp > 0;
                return record.seq() < other.record.seq();
            }
        };

        std::priority_queue<Entry, std::vector<Entry>, std::greater<>> pq;

        if (mem_iter.has_next()) { if (auto rec = mem_iter.next()) { pq.push(Entry{std::move(*rec), 0}); } }
        for (size_t i = 0; i < sst_iterators.size(); ++i) {
            if (sst_iterators[i].has_next()) { if (auto rec = sst_iterators[i].next()) { pq.push(Entry{core::MemRecord::from_view(*rec), i + 1}); } }
        }

        std::vector<core::MemRecord> result;
        std::optional<std::vector<uint8_t>> last_key;

        while (!pq.empty()) {
            auto [record, source_idx] = pq.top();
            pq.pop();

            if (source_idx == 0) { if (mem_iter.has_next()) { if (auto rec = mem_iter.next()) { pq.push(Entry{std::move(*rec), 0}); } } }
            else {
                auto& iter = sst_iterators[source_idx - 1];
                if (iter.has_next()) { if (auto rec = iter.next()) { pq.push(Entry{core::MemRecord::from_view(*rec), source_idx}); } }
            }

            auto key_span = record.key();
            std::vector cur_key(key_span.begin(), key_span.end());
            if (last_key.has_value() && *last_key == cur_key) continue;
            last_key = cur_key;

            if (record.is_tombstone()) continue;

            result.push_back(std::move(record));
        }

        return result;
    }

    void AkkEngine::flush() {
        memtable_->force_flush();
        wal_->force_sync();

        // Seal any pending partial stripe, then force all lane files to disk.
        if (stripe_writer_) { stripe_writer_->flush(); }
        if (stripe_store_) { stripe_store_->force(); }

        manifest_->checkpoint(
            std::optional<std::string>("flush"),
            stripe_store_
                ? std::optional<int64_t>(stripe_store_->last_sealed_stripe())
                : std::nullopt,
            std::optional(memtable_->last_seq())
        );
    }

    void AkkEngine::close() {
        flush();

        // Stop the background compaction thread before running the final compaction.
        {
            std::unique_lock lock{compact_mutex_};
            compact_running_ = false;
            compact_pending_ = false;
        }
        compact_cv_.notify_one();
        if (compact_thread_.joinable()) { compact_thread_.join(); }

        // Run a full compaction pass on close so the DB is tidy for next open.
        try {
            {
                std::unique_lock rlock{readers_mutex_};
                readers_.clear();
            }
            while (compactor_->compact_one()) {}
            rebuild_readers();
        }
        catch (...) {}

        if (stripe_store_) { stripe_store_->close(); }
        if (wal_) { wal_->close(); }
        if (manifest_) { manifest_->close(); }
    }

    uint64_t AkkEngine::last_seq() const { return memtable_->last_seq(); }

    void AkkEngine::notify_flush_done() {
        {
            std::unique_lock lock{compact_mutex_};
            compact_pending_ = true;
            last_flush_time_ = std::chrono::steady_clock::now();
        }
        compact_cv_.notify_one();
    }

    void AkkEngine::rebuild_readers() {
        std::unique_lock lock{readers_mutex_};
        readers_.clear();

        const auto sst_dir = opts_.base_dir / "sst";

        if (!std::filesystem::exists(sst_dir)) { return; }

        // Collect all .sst files from all levels
        std::vector<std::filesystem::path> sst_files;

        for (const auto& entry : std::filesystem::recursive_directory_iterator(sst_dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".aksst") { sst_files.push_back(entry.path()); }
        }

        // Sort by level (L0 < L1 < L2...) then by mtime (newest first within level)
        std::ranges::sort(
            sst_files,
            [](const auto& a, const auto& b) {
                auto parse_level = [](const std::filesystem::path& p) -> int {
                    auto parent = p.parent_path().filename().string();
                    if (parent.size() >= 2 && parent[0] == 'L') {
                        try { return std::stoi(parent.substr(1)); }
                        catch (...) { return INT_MAX; }
                    }
                    return INT_MAX;
                };

                const int level_a = parse_level(a);
                const int level_b = parse_level(b);

                if (level_a != level_b) { return level_a < level_b; }

                // Within same level, newer first
                return std::filesystem::last_write_time(a) > std::filesystem::last_write_time(b);
            }
        );

        // Open all SSTables
        for (const auto& path : sst_files) {
            try {
                auto reader = sstable::SSTableReader::open(path, false);
                readers_.push_back(std::move(reader));
            }
            catch (...) {
                // Skip corrupted SSTables
            }
        }
    }

    void AkkEngine::on_flush(std::vector<core::MemRecord> batch) {
        if (batch.empty()) {
            manifest_->checkpoint(std::optional<std::string>("memFlush-empty"), std::nullopt, std::nullopt);
            return;
        }

        // Sort by key
        std::ranges::sort(batch, [](const auto& a, const auto& b) { return a.compare_key(b) < 0; });

        // Generate filename
        const auto l0_dir = opts_.base_dir / "sst" / "L0";
        std::filesystem::create_directories(l0_dir);

        const auto now = std::chrono::system_clock::now().time_since_epoch().count();
        std::stringstream ss;
        ss << "L0_" << now << ".aksst";
        auto sst_path = l0_dir / ss.str();

        // Write SSTable
        auto writer = sstable::SSTableWriter::create(sst_path, buffer_pool_, batch.size(), opts_.bloom_fp_rate);

        std::optional<std::string> first_hex;
        std::optional<std::string> last_hex;

        for (const auto& record : batch) {
            writer->write(record);

            auto key_span = record.key();
            if (!first_hex.has_value()) { first_hex = to_hex(key_span); }
            last_hex = to_hex(key_span);
        }

        auto seal_result = writer->seal();
        writer->close();

        // Pack records into stripe blocks (best-effort; stripe layer is optional).
        // Only runs if a stripe writer is configured.
        if (stripe_writer_) {
            try {
                auto packer = format::akk::AkkBlockPacker::create(
                    [this](core::OwnedBuffer block) { stripe_writer_->add_block(std::move(block)); },
                    buffer_pool_
                );

                packer->begin_block();
                for (const auto& record : batch) {
                    const bool ok = packer->try_append(record);
                    if (!ok) {
                        packer->end_block();
                        packer->begin_block();
                        // A single record must always fit in a fresh block.
                        packer->try_append(record);
                    }
                }
                packer->end_block();
                // Do not force-flush here; partial stripes stay pending and
                // will be sealed when k blocks accumulate or flush() is called.
            }
            catch (...) {
                // Stripe packing is non-fatal; SST already persisted above.
            }
        }

        // Record in manifest
        manifest_->sst_seal(
            0,
            // L0
            "L0/" + sst_path.filename().string(),
            seal_result.entries,
            first_hex,
            last_hex
        );

        // Make the new SST immediately visible to readers.
        rebuild_readers();

        // Notify compaction thread that a flush occurred (idle-based scheduling).
        notify_flush_done();

        // Checkpoint
        manifest_->checkpoint(std::optional<std::string>("memFlush"), std::nullopt, std::optional<uint64_t>(batch.back().seq()));
    }
} // namespace akkaradb::engine