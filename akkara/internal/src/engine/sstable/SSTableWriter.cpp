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

// internal/src/engine/sstable/SSTableWriter.cpp
#include "engine/sstable/SSTableWriter.hpp"
#include "engine/sstable/AKSSFooter.hpp"

namespace akkaradb::engine::sstable {
    std::unique_ptr<SSTableWriter> SSTableWriter::create(
        std::filesystem::path& file_path,
        std::shared_ptr<core::BufferPool> buffer_pool,
        uint64_t expected_entries,
        double bloom_fp_rate
    ) { return std::unique_ptr<SSTableWriter>(new SSTableWriter(file_path, std::move(buffer_pool), expected_entries, bloom_fp_rate)); }

    SSTableWriter::SSTableWriter(
        std::filesystem::path& file_path,
        std::shared_ptr<core::BufferPool> buffer_pool,
        uint64_t expected_entries,
        double bloom_fp_rate
    )
        : file_path_{file_path},
          buffer_pool_{std::move(buffer_pool)},
          total_entries_{0},
          index_builder_{256},
          bloom_builder_{BloomFilter::Builder::create(expected_entries, bloom_fp_rate)} {
        // Create parent directory
        if (file_path_.has_parent_path()) { std::filesystem::create_directories(file_path_.parent_path()); }

        // Open file
        file_.open(file_path_, std::ios::binary | std::ios::trunc);
        if (!file_) { throw std::runtime_error("SSTableWriter: failed to create file"); }

        // Create packer with callback that returns buffer to pool after writing
        packer_ = format::akk::AkkBlockPacker::create([this](core::OwnedBuffer block) { this->on_block_ready(std::move(block)); }, buffer_pool_);

        packer_->begin_block();
    }

    SSTableWriter::~SSTableWriter() {
        try { close(); }
        catch (...) {
            // Destructor must not throw
        }
    }

    void SSTableWriter::write(const core::MemRecord& record) {
        // Try to append to current block
        if (!packer_->try_append(record)) {
            // Block full, seal and retry
            packer_->end_block();
            packer_->begin_block();

            if (!packer_->try_append(record)) { throw std::runtime_error("SSTableWriter: record too large for block"); }
        }

        // Capture first key of current block
        if (pending_first_key_.empty()) {
            auto key_span = record.key();
            pending_first_key_.assign(key_span.begin(), key_span.end());
        }

        // Add to bloom filter
        bloom_builder_.add_key(record.key());

        ++total_entries_;
    }

    void SSTableWriter::write_all(const std::vector<core::MemRecord>& records) { for (const auto& record : records) { write(record); } }

    SSTableWriter::SealResult SSTableWriter::seal() {
        // Flush any pending block
        packer_->flush();

        // Record current position for index
        file_.flush();
        const auto index_off = static_cast<uint64_t>(file_.tellp());
        if (file_.fail()) { throw std::runtime_error("SSTableWriter: I/O error before writing index"); }

        // Write index block
        auto index_data = index_builder_.build();
        file_.write(reinterpret_cast<const char*>(index_data.data()), static_cast<std::streamsize>(index_data.size()));
        if (file_.fail()) { throw std::runtime_error("SSTableWriter: I/O error writing index block (disk full?)"); }

        // Record current position for bloom
        file_.flush();
        const auto bloom_off = static_cast<uint64_t>(file_.tellp());
        if (file_.fail()) { throw std::runtime_error("SSTableWriter: I/O error before writing bloom filter"); }

        // Write bloom filter
        auto bloom = bloom_builder_.build();
        auto bloom_data = bloom.serialize();
        file_.write(reinterpret_cast<const char*>(bloom_data.data()), static_cast<std::streamsize>(bloom_data.size()));
        if (file_.fail()) { throw std::runtime_error("SSTableWriter: I/O error writing bloom filter (disk full?)"); }

        // Write footer via AKSSFooter to guarantee layout matches the spec:
        // [magic:4][version:1][pad:3][index_off:8][bloom_off:8][entries:4][crc32c:4]
        const auto footer_buf = AKSSFooter::write_to_buffer(AKSSFooter::Footer{index_off, bloom_off, static_cast<uint32_t>(total_entries_)});
        file_.write(reinterpret_cast<const char*>(footer_buf.data()), static_cast<std::streamsize>(footer_buf.size()));
        if (file_.fail()) { throw std::runtime_error("SSTableWriter: I/O error writing footer (disk full?)"); }

        file_.flush();
        if (file_.fail()) { throw std::runtime_error("SSTableWriter: I/O error during final flush (disk full?)"); }

        return SealResult{.index_off = index_off, .bloom_off = bloom_off, .entries = total_entries_};
    }

    void SSTableWriter::close() { if (file_.is_open()) { file_.close(); } }

    /**
 * Callback when a block is ready from packer.
 *
 * IMPROVEMENT: After writing to file, return buffer to pool for reuse.
 * This significantly improves memory efficiency and reduces allocations.
 */
    void SSTableWriter::on_block_ready(core::OwnedBuffer block) {
        // Record block offset for index
        const auto block_offset = static_cast<uint64_t>(file_.tellp());
        if (file_.fail()) { throw std::runtime_error("SSTableWriter: I/O error getting block offset"); }

        // Add to index: first_key -> offset
        if (!pending_first_key_.empty()) {
            index_builder_.add(pending_first_key_, block_offset);
            pending_first_key_.clear();
        }

        // Write block to file
        file_.write(reinterpret_cast<const char*>(block.data()), static_cast<std::streamsize>(block.size()));
        if (file_.fail()) { throw std::runtime_error("SSTableWriter: I/O error writing data block (disk full?)"); }

        // IMPROVEMENT: Return buffer to pool for reuse instead of deallocation
        // This reduces memory churn and improves performance
        buffer_pool_->release(std::move(block));
    }
} // namespace akkaradb::engine::sstable