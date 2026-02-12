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

// internal/src/engine/sstable/SSTableReader.cpp
#include "engine/sstable/SSTableReader.hpp"
#include <algorithm>
#include <stdexcept>
#include <utility>

namespace akkaradb::engine::sstable {
    // ==================== BlockCache ====================

    BlockCache::BlockCache(size_t max_blocks) : max_blocks_{max_blocks} {}

    std::shared_ptr<core::OwnedBuffer> BlockCache::get(uint64_t offset) {
        std::lock_guard lock{mutex_};

        auto it = cache_.find(offset);
        if (it == cache_.end()) { return nullptr; }

        // Move to front (MRU) - O(1)
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second.lru_it);

        return it->second.buffer;
    }

    void BlockCache::put(uint64_t offset, std::shared_ptr<core::OwnedBuffer> buffer) {
        std::lock_guard lock{mutex_};

        // Check if already exists
        auto it = cache_.find(offset);
        if (it != cache_.end()) {
            // Update and move to front
            it->second.buffer = std::move(buffer);
            lru_list_.splice(lru_list_.begin(), lru_list_, it->second.lru_it);
            return;
        }

        // Evict LRU if at capacity - O(1)
        if (cache_.size() >= max_blocks_) {
            auto lru_offset = lru_list_.back();
            cache_.erase(lru_offset);
            lru_list_.pop_back();
        }

        // Insert at front (MRU)
        lru_list_.push_front(offset);
        cache_[offset] = Entry{std::move(buffer), lru_list_.begin()};
    }

    void BlockCache::clear() {
        std::lock_guard lock{mutex_};
        cache_.clear();
        lru_list_.clear();
    }

    // ==================== SSTableReader ====================

    std::unique_ptr<SSTableReader> SSTableReader::open(
        const std::filesystem::path& file_path,
        bool verify_footer_crc
    ) {
        // Open file
        std::ifstream file(file_path, std::ios::binary);
        if (!file) { throw std::runtime_error("SSTableReader: failed to open file"); }

        // Get file size
        file.seekg(0, std::ios::end);
        const uint64_t file_size = file.tellg();

        if (file_size < AKSSFooter::SIZE) { throw std::runtime_error("SSTableReader: file too small for footer"); }

        // Read footer
        file.seekg(file_size - AKSSFooter::SIZE);
        auto footer_buf = core::OwnedBuffer::allocate(AKSSFooter::SIZE);
        file.read(reinterpret_cast<char*>(footer_buf.view().data()), AKSSFooter::SIZE);

        auto footer = verify_footer_crc
                          ? AKSSFooter::read_from_file(footer_buf.view(), file_path, true)
                          : AKSSFooter::read_from(footer_buf.view());

        // Read IndexBlock
        const uint64_t index_end = footer.bloom_off > 0 ? footer.bloom_off : (file_size - AKSSFooter::SIZE);
        const uint64_t index_len = index_end - footer.index_off;

        if (index_len < 16 || index_len > (1ULL << 31)) { throw std::runtime_error("SSTableReader: invalid index length"); }

        file.seekg(footer.index_off);
        auto index_buf = core::OwnedBuffer::allocate(index_len);
        file.read(reinterpret_cast<char*>(index_buf.view().data()), index_len);

        auto index = IndexBlock::read_from(index_buf.view());

        // Read BloomFilter (optional)
        std::optional<BloomFilter> bloom;
        if (footer.bloom_off > 0) {
            const uint64_t bloom_len = (file_size - AKSSFooter::SIZE) - footer.bloom_off;

            if (bloom_len >= BloomFilter::HEADER_SIZE) {
                file.seekg(footer.bloom_off);
                auto bloom_buf = core::OwnedBuffer::allocate(bloom_len);
                file.read(reinterpret_cast<char*>(bloom_buf.view().data()), bloom_len);

                bloom = BloomFilter::read_from(bloom_buf.view());
            }
        }

        file.close();

        return std::unique_ptr<SSTableReader>(new SSTableReader(
            file_path, footer, std::move(index), std::move(bloom)
        ));
    }

    SSTableReader::SSTableReader(
        std::filesystem::path file_path,
        const AKSSFooter::Footer& footer,
        IndexBlock index,
        std::optional<BloomFilter> bloom
    ) : file_path_{std::move(file_path)}
        , footer_{footer}
        , index_{std::move(index)}
        , bloom_{std::move(bloom)}
        , unpacker_{format::akk::AkkBlockUnpacker::create()}
        , cache_{512} {
        file_.open(file_path_, std::ios::binary);
        if (!file_) { throw std::runtime_error("SSTableReader: failed to reopen file"); }

        file_.seekg(0, std::ios::end);
        file_size_ = static_cast<uint64_t>(file_.tellg());
    }

    SSTableReader::~SSTableReader() { close(); }

    std::optional<core::RecordView> SSTableReader::get(std::span<const uint8_t> key) {
        // Fast Bloom reject
        if (bloom_.has_value() && !bloom_->might_contain(key)) { return std::nullopt; }

        // Locate candidate block via index
        const int64_t block_off = index_.lookup(key);
        if (block_off < 0) { return std::nullopt; }

        // Load block (zero-copy via shared_ptr)
        auto block_ptr = load_block(static_cast<uint64_t>(block_off));

        // Search in block (zero-copy)
        return find_in_block(block_ptr->view(), key);
    }

    std::optional<std::vector<uint8_t>> SSTableReader::get_copy(std::span<const uint8_t> key) {
        auto record_opt = get(key);
        if (!record_opt) { return std::nullopt; }

        auto value_span = record_opt->value();
        return std::vector(value_span.begin(), value_span.end());
    }

    SSTableReader::RangeIterator::RangeIterator(
        SSTableReader* reader,
        std::span<const uint8_t> start_key,
        std::optional<std::span<const uint8_t>> end_key
    ) : reader_{reader}
        , start_key_(start_key.begin(), start_key.end())
        , end_key_(end_key.has_value()
                       ? std::optional(std::vector(end_key->begin(), end_key->end()))
                       : std::nullopt)
        , current_entry_idx_{0} {
        // Find starting block
        const int64_t start_block_off = reader_->index_.lookup(start_key);
        if (start_block_off < 0) {
            current_entry_idx_ = reader_->index_.entries().size();
            return;
        }

        const auto& entries = reader_->index_.entries();
        auto entry_it = std::ranges::find_if(entries, [start_block_off](const IndexBlock::Entry& e) {
            return static_cast<int64_t>(e.block_offset) == start_block_off;
        });

        if (entry_it != entries.end()) {
            current_entry_idx_ = std::distance(entries.begin(), entry_it);
            current_block_ = reader_->load_block(entry_it->block_offset);
            current_cursor_ = reader_->unpacker_->cursor(current_block_->view());
        }
        else { current_entry_idx_ = entries.size(); }
    }

    bool SSTableReader::RangeIterator::has_next() const { return current_entry_idx_ < reader_->index_.entries().size(); }

    std::optional<core::RecordView> SSTableReader::RangeIterator::next() {
        if (!has_next()) { return std::nullopt; }

        while (true) {
            // Try current cursor
            if (current_cursor_ && current_cursor_->has_next()) {
                auto record_opt = current_cursor_->try_next();
                if (!record_opt) {
                    break; // Advance to next block
                }

                auto& record = *record_opt;
                auto rec_key = record.key();

                // Check start boundary
                if (std::ranges::lexicographical_compare(rec_key, start_key_)) { continue; }

                // Check end boundary
                if (end_key_.has_value()) {
                    if (!std::ranges::lexicographical_compare(rec_key, *end_key_)) {
                        current_entry_idx_ = reader_->index_.entries().size();
                        return std::nullopt;
                    }
                }

                return record;
            }

            // Advance to next block
            ++current_entry_idx_;
            if (!has_next()) { return std::nullopt; }

            const auto& entries = reader_->index_.entries();
            auto offset = entries[current_entry_idx_].block_offset;
            current_block_ = reader_->load_block(offset);
            current_cursor_ = reader_->unpacker_->cursor(current_block_->view());
        }

        return std::nullopt;
    }

    SSTableReader::RangeIterator SSTableReader::range_iter(
        std::span<const uint8_t> start_key,
        const std::optional<std::span<const uint8_t>>& end_key
    ) { return RangeIterator{this, start_key, end_key}; }

    std::vector<SSTableReader::RangeRecord> SSTableReader::range(
        std::span<const uint8_t> start_key,
        const std::optional<std::span<const uint8_t>>& end_key
    ) {
        std::vector<RangeRecord> results;

        auto iter = range_iter(start_key, end_key);
        while (iter.has_next()) {
            auto record_opt = iter.next();
            if (!record_opt) break;

            auto& record = *record_opt;
            auto rec_key = record.key();
            auto rec_value = record.value();

            results.push_back(RangeRecord{
                std::vector(rec_key.begin(), rec_key.end()),
                std::vector(rec_value.begin(), rec_value.end()),
                record.header()
            });
        }

        return results;
    }

    void SSTableReader::close() {
        if (file_.is_open()) { file_.close(); }
        cache_.clear();
    }

    std::shared_ptr<core::OwnedBuffer> SSTableReader::load_block(uint64_t offset) {
        // Check cache (zero-copy via shared_ptr)
        if (auto cached = cache_.get(offset)) { return cached; }

        // Read from file
        constexpr size_t BLOCK_SIZE = 32 * 1024;
        auto block_buf = core::OwnedBuffer::allocate(BLOCK_SIZE);

        file_.seekg(offset);
        file_.read(
            reinterpret_cast<char*>(block_buf.view().data()),
            BLOCK_SIZE
        );

        if (!file_) { throw std::runtime_error("SSTableReader: failed to read block"); }

        // Validate
        if (!unpacker_->validate(block_buf.view())) { throw std::runtime_error("SSTableReader: block validation failed"); }

        // Wrap in shared_ptr and cache
        auto block_ptr = std::make_shared<core::OwnedBuffer>(std::move(block_buf));
        cache_.put(offset, block_ptr);

        return block_ptr;
    }

    std::optional<core::RecordView> SSTableReader::find_in_block(
        core::BufferView block,
        std::span<const uint8_t> key
    ) {
        auto cursor = unpacker_->cursor(block);

        while (cursor->has_next()) {
            auto record_opt = cursor->try_next();
            if (!record_opt) break;

            auto& record = *record_opt;
            auto rec_key = record.key();

            if (std::ranges::equal(rec_key, key)) { return record; }
        }

        return std::nullopt;
    }
} // namespace akkaradb::engine::sstable