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
#include <stdexcept>
#include <array>

#include "engine/sstable/AKSSFooter.hpp"

namespace akkaradb::engine::sstable {
    namespace {
        /**
         * CRC32C computation.
         */
        uint32_t compute_file_crc32c(const std::filesystem::path& file_path, uint64_t file_size) {
            constexpr size_t BUFFER_SIZE = 1 << 20; // 1 MiB
            std::vector<uint8_t> buffer(BUFFER_SIZE);

            std::ifstream file(file_path, std::ios::binary);
            if (!file) { throw std::runtime_error("SSTableWriter: failed to open file for CRC"); }

            uint32_t crc = 0xFFFFFFFF;
            uint64_t total_read = 0;
            const uint64_t limit = file_size - 4; // Exclude last 4 bytes (CRC field)

            while (total_read < limit) {
                const size_t to_read = (std::min)(
                    BUFFER_SIZE,
                    limit - total_read
                );

                file.read(reinterpret_cast<char*>(buffer.data()), to_read);
                const size_t actually_read = file.gcount();

                if (actually_read == 0) { break; }

                for (size_t i = 0; i < actually_read; ++i) {
                    crc ^= buffer[i];
                    for (int j = 0; j < 8; ++j) { crc = (crc >> 1) ^ (0x82F63B78 & -(crc & 1)); }
                }

                total_read += actually_read;
            }

            return ~crc;
        }
    } // anonymous namespace

    // ==================== SSTableWriter ====================

    std::unique_ptr<SSTableWriter> SSTableWriter::create(
        std::filesystem::path& file_path,
        std::shared_ptr<core::BufferPool> buffer_pool,
        uint64_t expected_entries,
        double bloom_fp_rate
    ) {
        return std::unique_ptr<SSTableWriter>(new SSTableWriter(
            file_path, std::move(buffer_pool), expected_entries, bloom_fp_rate
        ));
    }

    SSTableWriter::SSTableWriter(
        std::filesystem::path& file_path,
        std::shared_ptr<core::BufferPool> buffer_pool,
        uint64_t expected_entries,
        double bloom_fp_rate
    ) : file_path_{file_path}
        , buffer_pool_{std::move(buffer_pool)}
        , total_entries_{0}
        , index_builder_{256}
        , bloom_builder_{BloomFilter::Builder::create(expected_entries, bloom_fp_rate)} {
        // Create parent directory
        if (file_path_.has_parent_path()) { std::filesystem::create_directories(file_path_.parent_path()); }

        // Open file
        file_.open(file_path_, std::ios::binary | std::ios::trunc);
        if (!file_) { throw std::runtime_error("SSTableWriter: failed to create file"); }

        // Create packer
        packer_ = format::akk::AkkBlockPacker::create(
            [this](core::OwnedBuffer block) { this->on_block_ready(std::move(block)); },
            buffer_pool_
        );

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

        // Add to Bloom filter
        bloom_builder_.add_key(record.key());

        ++total_entries_;
    }

    void SSTableWriter::write_all(const std::vector<core::MemRecord>& records) { for (const auto& record : records) { write(record); } }

    void SSTableWriter::on_block_ready(core::OwnedBuffer block) {
        // Get current file position (block offset)
        const uint64_t block_off = file_.tellp();

        // Write block
        auto block_view = block.view();
        file_.write(
            reinterpret_cast<const char*>(block_view.data()),
            static_cast<std::streamsize>(block_view.size())
        );

        if (!file_) { throw std::runtime_error("SSTableWriter: block write failed"); }

        // Add index entry
        if (pending_first_key_.empty()) { throw std::runtime_error("SSTableWriter: missing first key for block"); }

        index_builder_.add(
            std::span<const uint8_t>(pending_first_key_),
            block_off
        );

        pending_first_key_.clear();
    }

    SSTableWriter::SealResult SSTableWriter::seal() {
        // Flush any pending block
        packer_->flush();

        // (1) Write IndexBlock
        const uint64_t index_off = file_.tellp();
        auto index_buf = index_builder_.build();
        auto index_view = index_buf.view();
        file_.write(
            reinterpret_cast<const char*>(index_view.data()),
            index_view.size()
        );

        // (2) Write BloomFilter
        const uint64_t bloom_off = total_entries_ > 0
                                       ? static_cast<uint64_t>(file_.tellp())
                                       : 0;
        if (total_entries_ > 0) {
            auto bloom = bloom_builder_.build();
            auto bloom_buf = bloom.serialize();
            auto bloom_view = bloom_buf.view();

            // Skip tiny bloom filters (header only)
            if (bloom_view.size() >= 20) {
                file_.write(
                    reinterpret_cast<const char*>(bloom_view.data()),
                    bloom_view.size()
                );
            }
        }

        // (3) Write Footer (without CRC first)
        AKSSFooter::Footer footer{
            index_off,
            bloom_off,
            static_cast<uint32_t>(total_entries_)
        };

        auto footer_buf = AKSSFooter::write_to_buffer(footer, 0);
        auto footer_view = footer_buf.view();
        file_.write(
            reinterpret_cast<const char*>(footer_view.data()),
            footer_view.size()
        );

        file_.flush();

        // (4) Compute file CRC32C
        const uint64_t file_size = file_.tellp();
        file_.close();

        const uint32_t crc = compute_file_crc32c(file_path_, file_size);

        // (5) Update footer with CRC
        std::fstream update_file(file_path_, std::ios::binary | std::ios::in | std::ios::out);
        if (!update_file) { throw std::runtime_error("SSTableWriter: failed to reopen for CRC update"); }

        update_file.seekp(file_size - 4);
        update_file.write(reinterpret_cast<const char*>(&crc), 4);
        update_file.close();

        return SealResult{index_off, bloom_off, total_entries_};
    }

    void SSTableWriter::close() { if (file_.is_open()) { file_.close(); } }
} // namespace akkaradb::engine::sstable
