// internal/include/engine/AkkaraDB.hpp
#pragma once

#include "memtable/MemTable.hpp"
#include "wal/WalWriter.hpp"
#include "manifest/Manifest.hpp"
#include "sstable/SSTableReader.hpp"
#include "sstable/SSTCompactor.hpp"
#include "core/buffer/BufferPool.hpp"
#include "core/record/MemRecord.hpp"
#include <filesystem>
#include <memory>
#include <vector>
#include <deque>
#include <mutex>
#include <optional>

namespace akkaradb::engine {
    /**
     * AkkaraDB - High-performance embedded key-value database.
     *
     * Main entry point combining all components:
     * - MemTable: In-memory write buffer
     * - WAL: Write-ahead log for durability
     * - SSTable: Immutable sorted tables on disk
     * - Manifest: Metadata log
     * - Compaction: Background merge of SSTables
     *
     * Write path: WAL → MemTable → SSTable (on flush)
     * Read path: MemTable → SSTables (newest first)
     *
     * Thread-safety: Thread-safe for concurrent reads/writes.
     */
    class AkkaraDB {
    public:
        /**
         * Configuration options.
         */
        struct Options {
            std::filesystem::path base_dir;

            // MemTable
            size_t memtable_shard_count = 4;
            size_t memtable_threshold_bytes = 64 * 1024 * 1024;

            // WAL
            size_t wal_group_n = 64;
            uint64_t wal_group_micros = 1000;
            bool wal_fast_mode = true;

            // SSTable
            double bloom_fp_rate = 0.01;

            // Compaction
            size_t max_sst_per_level = 4;

            // Buffer pool
            size_t buffer_pool_size = 256;
        };

        /**
         * Opens or creates a database.
         *
         * Recovery sequence:
         * 1. Load Manifest
         * 2. Replay WAL → MemTable
         * 3. Open SSTables
         * 4. Start background flusher
         *
         * @param opts Configuration options
         * @return Unique pointer to AkkaraDB instance
         * @throws std::runtime_error on failure
         */
        [[nodiscard]] static std::unique_ptr<AkkaraDB> open(const Options& opts);

        ~AkkaraDB();

        /**
         * Inserts or updates a key-value pair.
         *
         * @param key Key bytes
         * @param value Value bytes
         * @return Assigned sequence number
         */
        [[nodiscard]] uint64_t put(
            std::span<const uint8_t> key,
            std::span<const uint8_t> value
        );

        /**
         * Deletes a key (tombstone).
         *
         * @param key Key bytes
         * @return Assigned sequence number
         */
        [[nodiscard]] uint64_t del(std::span<const uint8_t> key);

        /**
         * Retrieves value for key.
         *
         * @param key Key bytes
         * @return Value if found, nullopt otherwise
         */
        [[nodiscard]] std::optional<std::vector<uint8_t>> get(
            std::span<const uint8_t> key
        );

        /**
         * Compare-and-swap operation.
         *
         * @param key Key bytes
         * @param expected_seq Expected sequence number
         * @param new_value New value (nullopt for delete)
         * @return true if successful, false if seq mismatch
         */
        [[nodiscard]] bool compare_and_swap(
            std::span<const uint8_t> key,
            uint64_t expected_seq,
            const std::optional<std::span<const uint8_t>>& new_value
        );

        /**
         * Range query [start_key, end_key).
         *
         * @param start_key Inclusive start
         * @param end_key Exclusive end (nullopt = EOF)
         * @return Vector of records
         */
        [[nodiscard]] std::vector<core::MemRecord> range(
            std::span<const uint8_t> start_key,
            std::optional<std::span<const uint8_t>> end_key = std::nullopt
        );

        /**
         * Forces flush of MemTable to SSTable.
         */
        void flush();

        /**
         * Closes database (calls flush first).
         */
        void close();

        /**
         * Returns last assigned sequence number.
         */
        [[nodiscard]] uint64_t last_seq() const;

    private:
        AkkaraDB(
            Options opts,
            std::unique_ptr<memtable::MemTable> memtable,
            std::unique_ptr<wal::WalWriter> wal,
            std::shared_ptr<manifest::Manifest> manifest,
            std::shared_ptr<sstable::SSTCompactor> compactor,
            std::shared_ptr<core::BufferPool> buffer_pool
        );

        void rebuild_readers();
        void on_flush(std::vector<core::MemRecord> batch);

        Options opts_;
        std::unique_ptr<memtable::MemTable> memtable_;
        std::unique_ptr<wal::WalWriter> wal_;
        std::shared_ptr<manifest::Manifest> manifest_;
        std::shared_ptr<sstable::SSTCompactor> compactor_;
        std::shared_ptr<core::BufferPool> buffer_pool_;

        mutable std::mutex readers_mutex_;
        std::deque<std::unique_ptr<sstable::SSTableReader>> readers_;
    };
} // namespace akkaradb::engine
