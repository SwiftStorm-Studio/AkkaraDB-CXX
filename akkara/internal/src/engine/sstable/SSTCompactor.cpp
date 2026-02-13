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

// internal/src/engine/sstable/SSTCompactor.cpp
#include "engine/sstable/SSTCompactor.hpp"
#include <algorithm>
#include <queue>
#include <chrono>
#include <random>
#include <iomanip>
#include <sstream>
#include <utility>

namespace akkaradb::engine::sstable {
    namespace {
        /**
         * Heap entry for K-way merge.
         */
        struct HeapEntry {
            core::MemRecord record;
            size_t reader_idx;

            bool operator>(const HeapEntry& other) const {
                // First compare by key
                const int key_cmp = record.compare_key(other.record);
                if (key_cmp != 0) {
                    return key_cmp > 0; // Min-heap by key
                }

                // If keys equal, prioritize higher seq (newer)
                return record.seq() < other.record.seq();
            }
        };

        /**
         * Gets current time in milliseconds.
         */
        uint64_t now_millis() { return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count(); }

        /**
         * Generates UUID-like suffix.
         */
        std::string generate_suffix() {
            std::random_device rd;
            std::mt19937_64 gen(rd());
            std::uniform_int_distribution<uint64_t> dis;

            std::stringstream ss;
            ss << std::hex << std::setfill('0') << std::setw(8) << (dis(gen) & 0xFFFFFFFF);
            return ss.str();
        }
    } // anonymous namespace

    // ==================== SSTCompactor ====================

    std::unique_ptr<SSTCompactor> SSTCompactor::create(
        const std::filesystem::path& base_dir,
        std::shared_ptr<manifest::Manifest> manifest,
        std::shared_ptr<core::BufferPool> buffer_pool,
        size_t max_per_level,
        uint64_t ttl_millis,
        SeqClockFn seq_clock
    ) {
        if (max_per_level == 0) { throw std::invalid_argument("SSTCompactor: max_per_level must be > 0"); }

        return std::unique_ptr<SSTCompactor>(
            new SSTCompactor(base_dir, std::move(manifest), std::move(buffer_pool), max_per_level, ttl_millis, std::move(seq_clock))
        );
    }

    SSTCompactor::SSTCompactor(
        std::filesystem::path base_dir,
        std::shared_ptr<manifest::Manifest> manifest,
        std::shared_ptr<core::BufferPool> buffer_pool,
        size_t max_per_level,
        uint64_t ttl_millis,
        SeqClockFn seq_clock
    )
        : base_dir_{std::move(base_dir)},
          manifest_{std::move(manifest)},
          buffer_pool_{std::move(buffer_pool)},
          max_per_level_{max_per_level},
          ttl_millis_{ttl_millis},
          seq_clock_{std::move(seq_clock)} {}

    SSTCompactor::~SSTCompactor() = default;

    bool SSTCompactor::compact_one() {
        if (!std::filesystem::exists(base_dir_)) { std::filesystem::create_directories(base_dir_); }

        auto levels = existing_levels();
        for (int level : levels) {
            auto files = list_sst_files(level);
            if (files.size() > max_per_level_) {
                compact_level(level);
                return true; // Compacted one level; caller should re-schedule if needed.
            }
        }
        return false; // Nothing to compact.
    }

    void SSTCompactor::compact() {
        // Create base directory if not exists
        if (!std::filesystem::exists(base_dir_)) { std::filesystem::create_directories(base_dir_); }

        // Compact levels until no level exceeds threshold
        while (true) {
            auto levels = existing_levels();
            if (levels.empty()) { break; }

            // Find first level exceeding threshold
            bool found = false;
            for (int level : levels) {
                auto files = list_sst_files(level);
                if (files.size() > max_per_level_) {
                    compact_level(level);
                    found = true;
                    break;
                }
            }

            if (!found) { break; }
        }
    }

    void SSTCompactor::compact_level(int level) {
        auto current_files = list_sst_files(level);
        if (current_files.size() <= max_per_level_) { return; }

        const int next_level = level + 1;
        const auto next_level_path = level_path(next_level);
        std::filesystem::create_directories(next_level_path);

        auto next_files = list_sst_files(next_level);

        // Merge current level + next level files
        std::vector<std::filesystem::path> all_inputs;
        all_inputs.insert(all_inputs.end(), current_files.begin(), current_files.end());
        all_inputs.insert(all_inputs.end(), next_files.begin(), next_files.end());

        if (all_inputs.empty()) { return; }

        // Output file
        const auto output = next_level_path / new_file_name(next_level);

        // Check if this is bottom level
        auto levels = existing_levels();
        const bool is_bottom = std::ranges::none_of(levels, [next_level](int l) { return l > next_level; });

        // Record compaction start in manifest
        std::vector<std::string> input_rel_paths;
        for (const auto& path : all_inputs) { input_rel_paths.push_back(std::filesystem::relative(path, base_dir_).string()); }
        manifest_->compaction_start(level, input_rel_paths);

        // Compact
        auto [entries, first_key_hex, last_key_hex] = compact_into(all_inputs, output, is_bottom);

        // Delete input files
        for (const auto& path : all_inputs) { std::filesystem::remove(path); }

        // Record compaction end in manifest
        const auto output_rel = std::filesystem::relative(output, base_dir_).string();
        manifest_->compaction_end(next_level, output_rel, input_rel_paths, entries, first_key_hex, last_key_hex);
    }

    SSTCompactor::CompactResult SSTCompactor::compact_into(
        const std::vector<std::filesystem::path>& inputs,
        const std::filesystem::path& output,
        bool is_bottom_level
    ) {
        // Open all input SSTables
        std::vector<std::unique_ptr<SSTableReader>> readers;
        uint64_t expected_entries = 0;

        for (const auto& path : inputs) {
            auto reader = SSTableReader::open(path, false);
            expected_entries += reader->entries();
            readers.push_back(std::move(reader));
        }

        // Merge
        auto merged = merge(readers, is_bottom_level);

        // Write to output (create non-const copy)
        auto output_path = output;
        auto writer = SSTableWriter::create(output_path, buffer_pool_, expected_entries);

        std::optional<std::string> first_hex;
        std::optional<std::string> last_hex;

        for (const auto& record : merged) {
            writer->write(record);

            auto key_span = record.key();
            if (!first_hex.has_value()) { first_hex = first_key_hex(key_span); }
            last_hex = first_key_hex(key_span);
        }

        auto seal_result = writer->seal();
        writer->close();

        return CompactResult{seal_result.entries, first_hex, last_hex};
    }

    std::vector<core::MemRecord> SSTCompactor::merge(std::vector<std::unique_ptr<SSTableReader>>& readers, bool is_bottom_level) {
        std::vector<core::MemRecord> result;

        if (readers.empty()) { return result; }

        // Create iterators
        std::vector<SSTableReader::RangeIterator> iterators;
        for (auto& reader : readers) { iterators.push_back(reader->range_iter({}, std::nullopt)); }

        // K-way merge using priority queue
        std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>> pq;

        // Initialize heap with first record from each iterator
        for (size_t i = 0; i < iterators.size(); ++i) {
            if (iterators[i].has_next()) {
                auto record_opt = iterators[i].next();
                if (record_opt.has_value()) {
                    auto mem_record = core::MemRecord::from_view(*record_opt);
                    pq.push(HeapEntry{std::move(mem_record), i});
                }
            }
        }

        const uint64_t now = now_millis();

        // Process heap
        while (!pq.empty()) {
            // Pop minimum
            auto first = pq.top();
            pq.pop();

            // Collect all records with same key
            std::vector<HeapEntry> same_key;
            same_key.push_back(std::move(first));

            while (!pq.empty()) {
                auto& peek = pq.top();
                if (same_key[0].record.compare_key(peek.record) == 0) {
                    same_key.push_back(std::move(const_cast<HeapEntry&>(peek)));
                    pq.pop();
                }
                else { break; }
            }

            // Advance iterators for consumed records
            for (auto& [record, reader_idx] : same_key) {
                if (iterators[reader_idx].has_next()) {
                    auto record_opt = iterators[reader_idx].next();
                    if (record_opt.has_value()) {
                        auto mem_record = core::MemRecord::from_view(*record_opt);
                        pq.push(HeapEntry{std::move(mem_record), reader_idx});
                    }
                }
            }

            // Find winner (highest seq, tombstone wins on tie)
            core::MemRecord* winner = &same_key[0].record;

            for (size_t i = 1; i < same_key.size(); ++i) {
                auto& candidate = same_key[i].record;

                if (candidate.seq() > winner->seq()) { winner = &candidate; }
                else if (candidate.seq() == winner->seq()) {
                    // On tie, tombstone wins
                    if (candidate.is_tombstone() && !winner->is_tombstone()) { winner = &candidate; }
                }
            }

            // Check if we should drop tombstone
            if (winner->is_tombstone()) {
                if (should_drop_tombstone(*winner, now, is_bottom_level)) {
                    continue; // Skip this tombstone
                }
            }

            // Add winner to result
            result.push_back(*winner);
        }

        return result;
    }

    bool SSTCompactor::should_drop_tombstone(const core::MemRecord& record, uint64_t now_millis, bool is_bottom_level) const {
        if (!record.is_tombstone()) { return false; }

        // Can only drop at bottom level
        if (!is_bottom_level) { return false; }

        // Try to get deletion timestamp
        std::optional<uint64_t> delete_at;

        // Check if value contains timestamp (8 bytes)
        auto value_span = record.value();
        if (value_span.size() == 8) {
            uint64_t ts;
            std::memcpy(&ts, value_span.data(), 8);
            delete_at = ts;
        }

        // Fallback to seq clock
        if (!delete_at.has_value() && seq_clock_) { delete_at = seq_clock_(record.seq()); }

        // Check TTL
        if (delete_at.has_value()) { return (now_millis - *delete_at) >= ttl_millis_; }

        return false;
    }

    std::vector<int> SSTCompactor::existing_levels() const {
        std::vector<int> levels;
        levels.push_back(0); // L0 always exists conceptually

        if (!std::filesystem::exists(base_dir_)) { return levels; }

        for (const auto& entry : std::filesystem::directory_iterator(base_dir_)) {
            if (entry.is_directory()) {
                const auto name = entry.path().filename().string();
                if (name.size() >= 2 && name[0] == 'L') {
                    try {
                        int level = std::stoi(name.substr(1));
                        levels.push_back(level);
                    }
                    catch (...) {
                        // Ignore invalid names
                    }
                }
            }
        }

        std::ranges::sort(levels);
        levels.erase(std::ranges::unique(levels).begin(), levels.end());

        return levels;
    }

    std::vector<std::filesystem::path> SSTCompactor::list_sst_files(int level) const {
        const auto level_dir = level_path(level);

        if (!std::filesystem::exists(level_dir) || !std::filesystem::is_directory(level_dir)) { return {}; }

        std::vector<std::filesystem::path> files;

        for (const auto& entry : std::filesystem::directory_iterator(level_dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".aksst") { files.push_back(entry.path()); }
        }

        // Sort by filename
        std::ranges::sort(files, [](const auto& a, const auto& b) { return a.filename().string() < b.filename().string(); });

        return files;
    }

    std::string SSTCompactor::first_key_hex(std::span<const uint8_t> key) {
        std::stringstream ss;
        ss << std::hex << std::setfill('0');

        const size_t max_bytes = (std::min)(key.size(), size_t{16});
        for (size_t i = 0; i < max_bytes; ++i) { ss << std::setw(2) << static_cast<int>(key[i]); }

        return ss.str();
    }

    std::string SSTCompactor::new_file_name(int level) {
        const uint64_t ts = now_millis();
        const auto suffix = generate_suffix();

        std::stringstream ss;
        ss << "L" << level << "_" << ts << "_" << suffix << ".aksst";
        return ss.str();
    }

    std::filesystem::path SSTCompactor::level_path(int level) const {
        std::stringstream ss;
        ss << "L" << level;
        return base_dir_ / ss.str();
    }
} // namespace akkaradb::engine::sstable