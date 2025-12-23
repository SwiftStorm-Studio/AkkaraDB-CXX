// akkara/akkaradb/include/akkaradb/AkkaraDB.hpp
#pragma once

#include <cstdint>
#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <vector>
#include <filesystem>
#include <string_view>

#include "core/record/MemRecord.hpp"

/**
 * AkkaraDB Public API
 *
 * This is the stable public interface for C++ users.
 * Internal implementation details are completely hidden via Pimpl idiom.
 *
 * Thread-safety: All operations are thread-safe for concurrent reads/writes.
 *
 * Example usage:
 * ```cpp
 * auto db = akkaradb::AkkaraDB::open({
 *     .base_dir = "./data/mydb"
 * });
 *
 * // Put
 * std::vector<uint8_t> key = {1, 2, 3};
 * std::vector<uint8_t> value = {4, 5, 6};
 * uint64_t seq = db->put(key, value);
 *
 * // Get
 * auto result = db->get(key);
 * if (result) {
 *     // Use *result
 * }
 *
 * // Close
 * db->close();  // or let RAII handle it
 * ```
 */
namespace akkaradb {
    /**
     * AkkaraDB - High-performance embedded key-value database.
     *
     * Design principles:
     * - LSM-tree architecture (MemTable → SSTable)
     * - Write-ahead log for durability
     * - Crash-safe on single node
     * - Optional parity redundancy (XOR/Dual-XOR/Reed-Solomon)
     * - Lock-free optimizations for hot paths
     *
     * Write path: WAL → MemTable → SSTable (on flush)
     * Read path: MemTable → SSTables (newest first)
     *
     * Lifecycle:
     * 1. open() - Opens or creates a database
     * 2. put/get/delete/range operations
     * 3. close() - Flushes and releases resources (or use RAII)
     */
    class AkkaraDB {
    public:
        /**
         * Configuration options.
         *
         * All fields have sensible defaults suitable for most use cases.
         * Adjust based on workload characteristics and hardware.
         */
        struct Options {
            /// Base directory for database files
            std::filesystem::path base_dir;

            // ==================== MemTable Configuration ====================

            /// Number of MemTable shards (2-8, typically 4)
            /// Higher values improve write concurrency
            size_t memtable_shard_count = 4;

            /// MemTable flush threshold in bytes (default: 64 MiB)
            /// Larger values reduce write amplification, increase memory usage
            size_t memtable_threshold_bytes = 64 * 1024 * 1024;

            // ==================== WAL Configuration ====================

            /// WAL group commit count (default: 64)
            /// Batches multiple writes before fsync
            size_t wal_group_n = 64;

            /// WAL group commit timeout in microseconds (default: 1000 µs = 1 ms)
            /// Maximum time to wait for batch before forcing fsync
            uint64_t wal_group_micros = 1000;

            /// WAL fast mode (default: true)
            /// Uses fdatasync instead of fsync (metadata not synced)
            bool wal_fast_mode = true;

            // ==================== SSTable Configuration ====================

            /// Bloom filter false positive rate (default: 0.01 = 1%)
            /// Lower values increase memory usage
            double bloom_fp_rate = 0.01;

            // ==================== Compaction Configuration ====================

            /// Maximum SSTables per level before compaction (default: 4)
            /// Smaller values reduce read amplification, increase write amplification
            size_t max_sst_per_level = 4;

            // ==================== Buffer Pool Configuration ====================

            /// Buffer pool size (number of 32 KiB buffers) (default: 256 = 8 MiB)
            /// Larger values reduce allocation overhead
            size_t buffer_pool_size = 256;
        };

        /**
         * Opens or creates a database.
         *
         * Recovery sequence:
         * 1. Load Manifest (metadata log)
         * 2. Replay WAL → MemTable (recover uncommitted writes)
         * 3. Open SSTables (read-only sorted tables)
         * 4. Start background flusher (MemTable → SSTable)
         *
         * Directory structure created:
         * ```
         * base_dir/
         * ├── wal.akwal           # Write-ahead log
         * ├── manifest.akmf       # Manifest (metadata)
         * ├── sst/                # SSTables
         * │   ├── L0/            # Level 0 (newest)
         * │   ├── L1/            # Level 1
         * │   └── L2/            # Level 2 ...
         * └── lanes/              # Stripe lanes (optional redundancy)
         *     ├── data_0.akd
         *     └── parity_0.akp
         * ```
         *
         * @param opts Configuration options
         * @return Unique pointer to AkkaraDB instance
         * @throws std::runtime_error on failure (e.g., corrupted data, I/O error)
         */
        [[nodiscard]] static std::unique_ptr<AkkaraDB> open(const Options& opts);

        /**
         * Destructor.
         *
         * Automatically calls close() if not already closed.
         * Safe to call multiple times.
         */
        ~AkkaraDB();

        // Non-copyable, movable
        AkkaraDB(const AkkaraDB&) = delete;
        AkkaraDB& operator=(const AkkaraDB&) = delete;
        AkkaraDB(AkkaraDB&&) noexcept;
        AkkaraDB& operator=(AkkaraDB&&) noexcept;

        // ==================== Core Operations ====================

        /**
         * Inserts or updates a key-value pair.
         *
         * Write path:
         * 1. Acquire sequence number (atomic increment)
         * 2. Append to WAL (durable before apply)
         * 3. Insert into MemTable (in-memory)
         * 4. Return sequence number
         *
         * If MemTable exceeds threshold, background flusher writes to SSTable.
         *
         * Thread-safety: Fully thread-safe, lock-free in hot path.
         *
         * @param key Key bytes (any length, typically < 1 KiB)
         * @param value Value bytes (any length, typically < 1 MiB)
         * @return Assigned sequence number (monotonically increasing)
         * @throws std::runtime_error on WAL write failure
         */
        [[nodiscard]] uint64_t put(
            std::span<const uint8_t> key,
            std::span<const uint8_t> value
        );

        /**
         * Deletes a key (tombstone marker).
         *
         * Tombstones are eventually garbage collected during compaction.
         * Get operations on deleted keys return nullopt.
         *
         * Write path: Same as put(), but with tombstone flag.
         *
         * @param key Key bytes
         * @return Assigned sequence number
         * @throws std::runtime_error on WAL write failure
         */
        [[nodiscard]] uint64_t del(std::span<const uint8_t> key);

        /**
         * Retrieves value for key.
         *
         * Read path:
         * 1. Search MemTable (O(log n), fast path)
         * 2. If not found, search SSTables newest → oldest
         *    - Check Bloom filter first (skip if key absent)
         *    - Binary search in index block
         *    - Read data block
         * 3. Return value or nullopt
         *
         * Thread-safety: Fully thread-safe, concurrent reads do not block writes.
         *
         * @param key Key bytes
         * @return Value if found and not tombstone, nullopt otherwise
         */
        [[nodiscard]] std::optional<std::vector<uint8_t>> get(
            std::span<const uint8_t> key
        );

        /**
         * Compare-and-swap operation (conditional update).
         *
         * Atomically updates value only if current sequence matches expected.
         * Useful for optimistic locking / conflict detection.
         *
         * Algorithm:
         * 1. Check if current seq(key) == expected_seq
         * 2. If match:
         *    - Assign new sequence number
         *    - Apply update (or delete if new_value is nullopt)
         *    - Optionally log to WAL
         *    - Return true
         * 3. If mismatch: Return false (no-op)
         *
         * Thread-safety: Atomic operation, fully thread-safe.
         *
         * @param key Key bytes
         * @param expected_seq Expected current sequence number
         * @param new_value New value (nullopt = delete)
         * @return true if successful, false if seq mismatch
         * @throws std::runtime_error on WAL write failure (if durableCas enabled)
         */
        [[nodiscard]] bool compare_and_swap(
            std::span<const uint8_t> key,
            uint64_t expected_seq,
            std::optional<std::span<const uint8_t>> new_value
        );

        /**
         * Range query [start_key, end_key).
         *
         * Returns all non-tombstone records in lexicographic key order.
         *
         * Algorithm:
         * 1. Create iterators: MemTable + all SSTables
         * 2. K-way merge (min-heap)
         * 3. Deduplication: Keep newest version (highest seq)
         * 4. Skip tombstones
         *
         * Memory: Returns vector of MemRecords (copies).
         * For large ranges, consider pagination or streaming API (future).
         *
         * Thread-safety: Snapshot isolation - sees consistent view at call time.
         *
         * @param start_key Inclusive start (lexicographic order)
         * @param end_key Exclusive end (nullopt = scan to EOF)
         * @return Vector of records in sorted order
         */
        [[nodiscard]] std::vector<core::MemRecord> range(
            std::span<const uint8_t> start_key,
            const std::optional<std::span<const uint8_t>>& end_key = std::nullopt
        );

        // ==================== Metadata & Control ====================

        /**
         * Returns the last assigned sequence number.
         *
         * Sequence numbers are monotonically increasing (never decrease).
         * Useful for tracking write progress / change detection.
         *
         * Thread-safety: Atomic read, may return slightly stale value.
         *
         * @return Last sequence number
         */
        [[nodiscard]] uint64_t last_seq() const noexcept;

        /**
         * Forces flush of MemTable to SSTable.
         *
         * Operations:
         * 1. Seal all MemTable shards (prevent new writes to current batch)
         * 2. Write sorted batch to SSTable (Level 0)
         * 3. Force fsync on WAL
         * 4. Write checkpoint to Manifest
         *
         * Use cases:
         * - Before taking a backup
         * - Before planned shutdown
         * - After critical writes
         *
         * Note: Normally not needed - background flusher handles this automatically.
         *
         * Thread-safety: Blocks until flush completes.
         */
        void flush();

        /**
         * Closes the database.
         *
         * Operations:
         * 1. Call flush() (ensure all data is durable)
         * 2. Stop background threads (flusher, compactor)
         * 3. Close WAL, Manifest, SSTables
         * 4. Release all resources
         *
         * Safe to call multiple times (idempotent).
         * Automatically called by destructor if not explicitly closed.
         *
         * After close(), all operations (put/get/etc.) will fail.
         *
         * Thread-safety: Blocks until all operations complete.
         */
        void close();

    private:
        // Private constructor (use open() factory)
        explicit AkkaraDB();

        // Pimpl idiom: Hide internal implementation completely
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace akkaradb