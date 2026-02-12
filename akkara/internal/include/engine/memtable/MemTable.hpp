/*
* AkkEngine
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkEngine.
 *
 * AkkEngine is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkEngine is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkEngine.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/include/engine/memtable/MemTable.hpp
#pragma once

#include "core/record/MemRecord.hpp"
#include <memory>
#include <functional>
#include <span>
#include <vector>
#include <atomic>
#include <optional>

namespace akkaradb::engine::memtable {
    /**
     * MemTable - Sharded in-memory sorted key-value store.
     *
     * Lock-sharded TreeMap (std::map) for concurrent write performance.
     * Each shard maintains:
     * - active: Current writable map
     * - immutables: Sealed maps awaiting flush
     *
     * Design principles:
     * - Sharding: 2-8 shards based on key hash
     * - MVCC: Multiple versions per key (seq-based)
     * - Flush trigger: Per-shard byte threshold
     * - Single flusher thread: Background coalesced I/O
     *
     * Typical usage:
     * ```cpp
     * auto memtable = MemTable::create(
     *     4,  // shard_count
     *     16 * 1024 * 1024,  // threshold per shard (16 MiB)
     *     [](std::vector<MemRecord> batch) {
     *         // Flush callback
     *         write_to_sst(batch);
     *     }
     * );
     *
     * memtable->put(key, value, seq);
     * auto record = memtable->get(key);
     * ```
     *
     * Thread-safety: Fully thread-safe with per-shard locks.
     */
    class MemTable {
    public:
        /**
         * Flush callback - receives sorted MemRecord batch.
         */
        using FlushCallback = std::function<void(std::vector<core::MemRecord>)>;

        /**
         * Key range for iteration.
         */
        struct KeyRange {
            std::vector<uint8_t> start; ///< Inclusive start key
            std::vector<uint8_t> end; ///< Exclusive end key
        };

        /**
         * Streaming iterator over a key range.
         * No intermediate vector allocation.
         * Yields records in lexicographic order, highest seq first per key.
         */
        class RangeIterator {
            public:
                ~RangeIterator();
                RangeIterator(RangeIterator&&) noexcept;
                RangeIterator& operator=(RangeIterator&&) noexcept;
                RangeIterator(const RangeIterator&) = delete;
                RangeIterator& operator=(const RangeIterator&) = delete;

                [[nodiscard]] bool has_next() const noexcept;
                [[nodiscard]] std::optional<core::MemRecord> next();

            private:
                friend class MemTable;
                class Impl;
                explicit RangeIterator(std::unique_ptr<Impl> impl);
                std::unique_ptr<Impl> impl_;
        };

        /**
         * Creates a MemTable.
         *
         * @param shard_count Number of shards (2-8, typically 4)
         * @param threshold_bytes_per_shard Flush threshold per shard
         * @param on_flush Callback invoked with sorted batch
         * @return Unique pointer to MemTable
         */
        [[nodiscard]] static std::unique_ptr<MemTable> create(
            size_t shard_count = 4,
            size_t threshold_bytes_per_shard = 16 * 1024 * 1024,
            FlushCallback on_flush = nullptr
        );

        ~MemTable();

        /**
         * Inserts or updates a key-value pair.
         *
         * @param key Key bytes
         * @param value Value bytes
         * @param seq Sequence number
         */
        void put(
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint64_t seq
        );

        /**
         * Inserts a tombstone (delete marker).
         *
         * @param key Key bytes
         * @param seq Sequence number
         */
        void remove(
            std::span<const uint8_t> key,
            uint64_t seq
        );

        /**
         * Retrieves the latest version of a key.
         *
         * @param key Key bytes
         * @return MemRecord if found, nullptr otherwise
         */
        [[nodiscard]] std::shared_ptr<const core::MemRecord> get(
            std::span<const uint8_t> key
        ) const;

        /**
         * Returns the next sequence number.
         */
        [[nodiscard]] uint64_t next_seq() noexcept;

        /**
         * Returns the last assigned sequence number.
         */
        [[nodiscard]] uint64_t last_seq() const noexcept;

        /**
         * Hints that a flush should occur (async).
         */
        void flush_hint();

        /**
         * Forces immediate flush of all shards (blocking).
         */
        void force_flush();

        /**
         * Returns approximate memory usage in bytes.
         */
        [[nodiscard]] size_t approx_size() const noexcept;

        /**
         * Replaces the flush callback after construction.
         * Must be called before any writes if a null callback was passed to create().
         *
         * @param cb New flush callback (nullptr disables flushing)
         */
        void set_flush_callback(FlushCallback cb);

        /**
         * Iterator over key range (sorted, merged across shards).
         *
         * @param range Key range
         * @return Vector of MemRecords
         */
        [[nodiscard]] RangeIterator iterator(const KeyRange& range) const;

    private:
        MemTable(
            size_t shard_count,
            size_t threshold_bytes_per_shard,
            FlushCallback on_flush
        );

        class Impl;
        std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::memtable