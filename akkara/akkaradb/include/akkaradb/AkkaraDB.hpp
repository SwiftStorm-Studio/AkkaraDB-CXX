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

// akkara/akkaradb/include/akkaradb/AkkaraDB.hpp
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>
#include <filesystem>

/**
 * AkkaraDB Public API
 *
 * Stable C++ interface with complete ABI isolation.
 * Thread-safe for concurrent reads/writes.
 */
namespace akkaradb {
    /**
     * Record - Public record type for range queries.
     *
     * Represents a single key-value pair with metadata.
     * Lifetime: Valid until the owning vector is destroyed.
     */
    class Record {
    public:
        ~Record() noexcept;

        // Move-only
        Record(Record&&) noexcept;
        Record& operator=(Record&&) noexcept;
        Record(const Record&) = delete;
        Record& operator=(const Record&) = delete;

        /**
         * Returns a view of the key.
         * Lifetime: Valid as long as this Record exists.
         */
        [[nodiscard]] std::span<const uint8_t> key() const noexcept;

        /**
         * Returns a view of the value.
         * Lifetime: Valid as long as this Record exists.
         */
        [[nodiscard]] std::span<const uint8_t> value() const noexcept;

        /**
         * Returns the sequence number.
         */
        [[nodiscard]] uint64_t seq() const noexcept;

        /**
         * Returns the record flags.
         */
        [[nodiscard]] uint8_t flags() const noexcept;

        /**
         * Checks if this is a tombstone (deleted record).
         */
        [[nodiscard]] bool is_tombstone() const noexcept;

        /**
         * Returns approximate size in bytes.
         */
        [[nodiscard]] size_t approx_size_bytes() const noexcept;

    private:
        friend class AkkaraDB;
        explicit Record(void* internal_record);

        class Impl;
        std::unique_ptr<Impl> impl_;
    };

    /**
     * AkkaraDB - High-performance embedded key-value database.
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
         * @param opts Configuration options
         * @return Unique pointer to AkkaraDB instance
         * @throws std::runtime_error on failure
         */
        [[nodiscard]] static std::unique_ptr<AkkaraDB> open(const Options& opts);

        /**
         * Destructor (automatically calls close).
         */
        ~AkkaraDB() noexcept;

        // Non-copyable, movable
        AkkaraDB(const AkkaraDB&) = delete;
        AkkaraDB& operator=(const AkkaraDB&) = delete;
        AkkaraDB(AkkaraDB&&) noexcept;
        AkkaraDB& operator=(AkkaraDB&&) noexcept;

        // ==================== Core Operations ====================

        /**
         * Inserts or updates a key-value pair.
         *
         * @param key Key bytes
         * @param value Value bytes
         * @return Assigned sequence number
         * @throws std::runtime_error if database is closed or WAL write fails
         */
        [[nodiscard]] uint64_t put(
            std::span<const uint8_t> key,
            std::span<const uint8_t> value
        );

        /**
         * Deletes a key (tombstone marker).
         *
         * @param key Key bytes
         * @return Assigned sequence number
         * @throws std::runtime_error if database is closed or WAL write fails
         */
        [[nodiscard]] uint64_t del(std::span<const uint8_t> key);

        /**
         * Retrieves value for key.
         *
         * @param key Key bytes
         * @return Value if found and not tombstone, nullopt otherwise
         * @throws std::runtime_error if database is closed
         */
        [[nodiscard]] std::optional<std::vector<uint8_t>> get(
            std::span<const uint8_t> key
        );

        /**
         * Compare-and-swap operation.
         *
         * @param key Key bytes
         * @param expected_seq Expected current sequence number
         * @param new_value New value (nullopt = delete)
         * @return true if successful, false if seq mismatch
         * @throws std::runtime_error if database is closed or WAL write fails
         */
        [[nodiscard]] bool compare_and_swap(
            std::span<const uint8_t> key,
            uint64_t expected_seq,
            std::optional<std::span<const uint8_t>> new_value
        );

        /**
         * Range query [start_key, end_key).
         *
         * @param start_key Inclusive start
         * @param end_key Exclusive end (nullopt = EOF)
         * @return Vector of records in sorted order
         * @throws std::runtime_error if database is closed
         */
        [[nodiscard]] std::vector<Record> range(
            std::span<const uint8_t> start_key,
            std::optional<std::span<const uint8_t>> end_key = std::nullopt
        );

        // ==================== Metadata & Control ====================

        /**
         * Returns the last assigned sequence number.
         *
         * @throws std::runtime_error if database is closed
         */
        [[nodiscard]] uint64_t last_seq() const;

        /**
         * Forces flush of MemTable to SSTable.
         *
         * @throws std::runtime_error if database is closed
         */
        void flush();

        /**
         * Closes the database.
         * Safe to call multiple times (idempotent).
         */
        void close() noexcept;

private:
    explicit AkkaraDB();

    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace akkaradb