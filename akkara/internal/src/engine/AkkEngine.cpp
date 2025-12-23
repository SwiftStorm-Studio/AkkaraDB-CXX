// internal/src/engine/AkkEngine.cpp
#include "engine/AkkEngine.hpp"
#include "engine/wal/WalOp.hpp"
#include <algorithm>
#include <fstream>

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
    } // anonymous namespace

    // ==================== AkkEngine ====================

    std::unique_ptr<AkkEngine> AkkEngine::open(const Options& opts) {
        // Create directories
        std::filesystem::create_directories(opts.base_dir);
        const auto sst_dir = opts.base_dir / "sst";
        const auto l0_dir = sst_dir / "L0";
        std::filesystem::create_directories(l0_dir);

        // Create buffer pool (as shared_ptr)
        auto buffer_pool = std::shared_ptr<core::BufferPool>(
            core::BufferPool::create(32 * 1024, 4096, opts.buffer_pool_size).release()
        );

        // Create manifest (as shared_ptr)
        const auto manifest_path = opts.base_dir / "manifest.akmf";
        auto manifest = std::shared_ptr<manifest::Manifest>(
            manifest::Manifest::create(manifest_path, true).release()
        );

        // Replay manifest
        manifest->replay();
        manifest->start();

        // Create compactor (as shared_ptr)
        auto compactor = std::shared_ptr<sstable::SSTCompactor>(
            sstable::SSTCompactor::create(
                sst_dir,
                manifest,
                buffer_pool,
                opts.max_sst_per_level
            ).release()
        );

        // Create MemTable with flush callback (will be set later)
        auto memtable = memtable::MemTable::create(
            opts.memtable_shard_count,
            opts.memtable_threshold_bytes / opts.memtable_shard_count,
            nullptr // Callback set after AkkEngine construction
        );

        // Create WAL
        const auto wal_path = opts.base_dir / "wal.akwal";
        auto wal = wal::WalWriter::create(
            wal_path,
            opts.wal_group_n,
            opts.wal_group_micros,
            opts.wal_fast_mode
        );

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
        auto db = std::unique_ptr<AkkEngine>(new AkkEngine(
            std::move(opts),
            std::move(memtable),
            std::move(wal),
            manifest,
            compactor,
            buffer_pool
        ));

        // Load existing SSTables
        db->rebuild_readers();

        return db;
    }

    AkkEngine::AkkEngine(
        Options opts,
        std::unique_ptr<memtable::MemTable> memtable,
        std::unique_ptr<wal::WalWriter> wal,
        std::shared_ptr<manifest::Manifest> manifest,
        std::shared_ptr<sstable::SSTCompactor> compactor,
        std::shared_ptr<core::BufferPool> buffer_pool
    ) : opts_{std::move(opts)}
        , memtable_{std::move(memtable)}
        , wal_{std::move(wal)}
        , manifest_{std::move(manifest)}
        , compactor_{std::move(compactor)}
        , buffer_pool_{std::move(buffer_pool)} {}

    AkkEngine::~AkkEngine() { close(); }

    uint64_t AkkEngine::put(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value
    ) {
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

    std::optional<std::vector<uint8_t>> AkkEngine::get(
        std::span<const uint8_t> key
    ) {
        // 1. Check MemTable (fast path)
        if (auto record = memtable_->get(key)) {
            if (record->is_tombstone()) { return std::nullopt; }

            auto value_span = record->value();
            return std::vector<uint8_t>(value_span.begin(), value_span.end());
        }

        // 2. Check SSTables (newest first)
        std::lock_guard lock{readers_mutex_};

        for (auto& reader : readers_) {
            // Use get() which returns std::optional<RecordView>
            auto record_opt = reader->get(key);
            if (record_opt.has_value()) {
                auto value_span = record_opt->value();
                return std::vector<uint8_t>(value_span.begin(), value_span.end());
            }
        }

        return std::nullopt;
    }

    bool AkkEngine::compare_and_swap(
        std::span<const uint8_t> key,
        uint64_t expected_seq,
        const std::optional<std::span<const uint8_t>>& new_value
    ) {
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

    std::vector<core::MemRecord> AkkEngine::range(
        std::span<const uint8_t> start_key,
        std::optional<std::span<const uint8_t>> end_key
    ) {
        std::vector<core::MemRecord> result;

        // Create range from MemTable
        memtable::MemTable::KeyRange mem_range{
            std::vector(start_key.begin(), start_key.end()),
            end_key.has_value()
                ? std::vector(end_key->begin(), end_key->end())
                : std::vector<uint8_t>{} // ← empty vector instead of nullopt
        };

        auto mem_records = memtable_->range(mem_range);

        // Create iterators from SSTables
        std::vector<sstable::SSTableReader::RangeIterator> sst_iterators;
        {
            std::lock_guard lock{readers_mutex_};
            for (auto& reader : readers_) { sst_iterators.push_back(reader->range_iter(start_key, end_key)); }
        }

        // K-way merge
        struct Entry {
            core::MemRecord record;
            size_t source_idx; // 0 = MemTable, 1+ = SSTable index

            bool operator>(const Entry& other) const {
                const int cmp = record.compare_key(other.record);
                if (cmp != 0) { return cmp > 0; }
                // If keys equal, prioritize higher seq
                return record.seq() < other.record.seq();
            }
        };

        std::priority_queue<Entry, std::vector<Entry>, std::greater<>> pq;

        // Initialize with first from each source
        size_t mem_idx = 0;
        if (mem_idx < mem_records.size()) { pq.push(Entry{mem_records[mem_idx], 0}); }

        for (size_t i = 0; i < sst_iterators.size(); ++i) {
            if (sst_iterators[i].has_next()) {
                auto record_opt = sst_iterators[i].next();
                if (record_opt.has_value()) {
                    auto mem_record = core::MemRecord::from_view(*record_opt);
                    pq.push(Entry{std::move(mem_record), i + 1});
                }
            }
        }

        std::optional<std::vector<uint8_t>> last_key;

        while (!pq.empty()) {
            auto entry = pq.top(); // ← structured binding避ける（コピーエラー回避）
            pq.pop();

            // Advance source
            if (entry.source_idx == 0) {
                // MemTable
                ++mem_idx;
                if (mem_idx < mem_records.size()) { pq.push(Entry{mem_records[mem_idx], 0}); }
            }
            else {
                // SSTable
                auto& iter = sst_iterators[entry.source_idx - 1];
                if (iter.has_next()) {
                    auto record_opt = iter.next();
                    if (record_opt.has_value()) {
                        auto mem_record = core::MemRecord::from_view(*record_opt);
                        pq.push(Entry{std::move(mem_record), entry.source_idx});
                    }
                }
            }

            // Dedup: skip if same key as last
            auto current_key_span = entry.record.key();
            std::vector current_key(current_key_span.begin(), current_key_span.end());

            if (last_key.has_value() && *last_key == current_key) { continue; }

            last_key = current_key;

            // Skip tombstones
            if (entry.record.is_tombstone()) { continue; }

            result.push_back(std::move(entry.record));
        }

        return result;
    }

    void AkkEngine::flush() {
        memtable_->force_flush();
        wal_->force_sync();

        manifest_->checkpoint(
            std::optional<std::string>("flush"),
            std::nullopt,
            std::optional(memtable_->last_seq())
        );
    }

    void AkkEngine::close() {
        flush();

        if (wal_) { wal_->close(); }

        if (manifest_) { manifest_->close(); }
    }

    uint64_t AkkEngine::last_seq() const { return memtable_->last_seq(); }

    void AkkEngine::rebuild_readers() {
        std::lock_guard lock{readers_mutex_};
        readers_.clear();

        const auto sst_dir = opts_.base_dir / "sst";

        if (!std::filesystem::exists(sst_dir)) { return; }

        // Collect all .sst files from all levels
        std::vector<std::filesystem::path> sst_files;

        for (const auto& entry : std::filesystem::recursive_directory_iterator(sst_dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".sst") { sst_files.push_back(entry.path()); }
        }

        // Sort by level (L0 < L1 < L2...) then by mtime (newest first within level)
        std::sort(sst_files.begin(), sst_files.end(), [](const auto& a, const auto& b) {
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
        });

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
            manifest_->checkpoint(
                std::optional<std::string>("memFlush-empty"),
                std::nullopt,
                std::nullopt
            );
            return;
        }

        // Sort by key
        std::sort(batch.begin(), batch.end(), [](const auto& a, const auto& b) { return a.compare_key(b) < 0; });

        // Generate filename
        const auto l0_dir = opts_.base_dir / "sst" / "L0";
        std::filesystem::create_directories(l0_dir);

        const auto now = std::chrono::system_clock::now().time_since_epoch().count();
        std::stringstream ss;
        ss << "L0_" << now << ".sst";
        auto sst_path = l0_dir / ss.str();

        // Write SSTable
        auto writer = sstable::SSTableWriter::create(
            sst_path,
            buffer_pool_,
            batch.size(),
            opts_.bloom_fp_rate
        );

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

        // Record in manifest
        manifest_->sst_seal(
            0, // L0
            "L0/" + sst_path.filename().string(),
            seal_result.entries,
            first_hex,
            last_hex
        );

        // Run compaction
        try { compactor_->compact(); }
        catch (...) {
            // Compaction failure is non-fatal
        }

        // Rebuild readers
        rebuild_readers();

        // Checkpoint
        manifest_->checkpoint(
            std::optional<std::string>("memFlush"),
            std::nullopt,
            std::optional<uint64_t>(batch.back().seq())
        );
    }
} // namespace akkaradb::engine