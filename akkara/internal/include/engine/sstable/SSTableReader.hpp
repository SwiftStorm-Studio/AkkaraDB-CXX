// internal/include/engine/sstable/SSTableReader.hpp
#pragma once

#include "AKSSFooter.hpp"
#include "IndexBlock.hpp"
#include "BloomFilter.hpp"
#include "format-akk/AkkBlockUnpacker.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include "core/record/RecordView.hpp"
#include <filesystem>
#include <memory>
#include <optional>
#include <fstream>
#include <map>
#include <mutex>

namespace akkaradb::engine::sstable {
    /**
     * BlockCache - Simple LRU cache for blocks (zero-copy with shared_ptr).
     *
     * Thread-safe block cache using shared_ptr to avoid expensive memcpy.
     */
    class BlockCache {
    public:
        struct CachedBlock {
            std::shared_ptr<core::OwnedBuffer> buffer;
            uint64_t access_count;
        };

        explicit BlockCache(size_t max_blocks = 512);

        std::shared_ptr<core::OwnedBuffer> get(uint64_t offset);
        void put(uint64_t offset, std::shared_ptr<core::OwnedBuffer> buffer);
        void clear();

    private:
        size_t max_blocks_;
        std::map<uint64_t, CachedBlock> cache_;
        std::mutex mutex_;
    };

    /**
     * SSTableReader - Reads records from immutable SSTable files.
     *
     * Layout:
     *   [ Data Blocks (32 KiB each) ]
     *   [ IndexBlock ('AKIX') ]
     *   [ BloomFilter ('AKBL') ]
     *   [ Footer ('AKSS', 32 bytes) ]
     *
     * Responsibilities:
     *   - Parse footer, load index/bloom into memory
     *   - Point lookup: Bloom → Index → Block → Record
     *   - Range query: Sequential block iteration
     *   - Block caching with LRU eviction (zero-copy via shared_ptr)
     *   - CRC verification on first load
     *
     * Thread-safety: Read-only operations are thread-safe.
     *                Cache uses internal locking.
     */
    class SSTableReader {
    public:
        /**
         * Opens an SSTable file.
         *
         * @param file_path Path to .sst file
         * @param verify_footer_crc If true, verify file-level CRC
         * @return Unique pointer to reader
         * @throws std::runtime_error if file invalid
         */
        [[nodiscard]] static std::unique_ptr<SSTableReader> open(
            const std::filesystem::path& file_path,
            bool verify_footer_crc = false
        );

        ~SSTableReader();

        /**
         * Point lookup.
         *
         * @param key Key to find
         * @return Value if found, nullopt otherwise
         */
        [[nodiscard]] std::optional<std::vector<uint8_t>> get(std::span<const uint8_t> key);

        /**
         * Range query.
         *
         * Returns all records in [start_key, end_key).
         *
         * @param start_key Inclusive start
         * @param end_key Exclusive end (nullopt = EOF)
         * @return Vector of (key, value, header) tuples
         */
        struct RangeRecord {
            std::vector<uint8_t> key;
            std::vector<uint8_t> value;
            core::AKHdr32 header;
        };

        [[nodiscard]] std::vector<RangeRecord> range(
            std::span<const uint8_t> start_key,
            const std::optional<std::span<const uint8_t>>& end_key = std::nullopt
        );

        /**
         * Closes file.
         */
        void close();

        /**
         * Returns footer info.
         */
        [[nodiscard]] const AKSSFooter::Footer& footer() const noexcept { return footer_; }

        /**
         * Returns entry count.
         */
        [[nodiscard]] uint32_t entries() const noexcept { return footer_.entries; }

    private:
        SSTableReader(
            std::filesystem::path file_path,
            const AKSSFooter::Footer& footer,
            IndexBlock index,
            std::optional<BloomFilter> bloom
        );

        std::shared_ptr<core::OwnedBuffer> load_block(uint64_t offset);
        std::optional<core::RecordView> find_in_block(core::BufferView block, std::span<const uint8_t> key);

        std::filesystem::path file_path_;
        std::ifstream file_;
        uint64_t file_size_;

        AKSSFooter::Footer footer_;
        IndexBlock index_;
        std::optional<BloomFilter> bloom_;

    std::unique_ptr<format::akk::AkkBlockUnpacker> unpacker_;
    BlockCache cache_;
};

} // namespace akkaradb::engine::sstable