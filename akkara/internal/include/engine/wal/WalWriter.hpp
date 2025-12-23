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

// internal/include/engine/wal/WalWriter.hpp
#pragma once

#include "WalOp.hpp"
#include <memory>
#include <filesystem>
#include <cstdint>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::wal {
    /**
 * WalWriter - Write-Ahead Log writer with group commit optimization.
 *
 * Follows JVM implementation exactly:
 * - Single WAL file (wal.akwal)
 * - Direct frame writes [len:u32][payload][crc32c:u32]
 * - Queue-based single-writer architecture
 * - Group commit for throughput
 *
 * Design principles:
 * - Single-writer: Dedicated flusher thread
 * - Lock-free producers: Atomic LSN + queue submission
 * - Group commit: Batch N frames or T microseconds
 * - Platform-specific fsync: fdatasync/F_FULLFSYNC/FlushFileBuffers
 *
 * Typical usage:
 * ```cpp
 * auto writer = WalWriter::create(
 *     "./data/wal.akwal",
 *     32,    // groupN
 *     500,   // groupMicros
 *     false  // fastMode
 * );
 *
 * writer->append(WalOp::put("key", "value", 1));
 * writer->append(WalOp::del("key", 2));
 * writer->flush();
 * writer->close();
 * ```
 *
 * Thread-safety: Fully thread-safe. Multiple producers, single consumer.
 */
    class WalWriter {
    public:
        /**
     * Creates a WalWriter.
     *
     * @param wal_file Path to WAL file (e.g., "./data/wal.akwal")
     * @param group_n Maximum entries per batch (default: 32)
     * @param group_micros Maximum microseconds to wait (default: 500)
     * @param fast_mode If true, append() returns immediately; if false, blocks until fsync
     * @return Unique pointer to writer
     * @throws std::runtime_error if file cannot be opened
     */
        [[nodiscard]] static std::unique_ptr<WalWriter> create(
            const std::filesystem::path& wal_file,
            size_t group_n = 32,
            size_t group_micros = 500,
            bool fast_mode = false
        );

        ~WalWriter();

        /**
     * Appends a WAL operation and returns LSN.
     *
     * In fast mode: returns immediately after queuing (~5-10 µs)
     * In durable mode: blocks until fsync completes (~50-500 µs)
     *
     * @param op Operation to append
     * @return Log Sequence Number assigned
     * @throws std::runtime_error if WAL is closed
     * @throws std::runtime_error if durable mode times out
     */
        [[nodiscard]] uint64_t append(const WalOp& op);

        /**
     * Forces all pending writes to durable storage.
     *
     * Blocks until all queued frames are written and fsync'd.
     */
        void force_sync();

        /**
     * Truncates WAL file to zero length.
     *
     * Intended after durable checkpoint when WAL is no longer needed.
     */
        void truncate();

        /**
     * Closes WAL and releases resources.
     *
     * Blocks until flusher thread completes. Idempotent.
     */
        void close();

        /**
     * Returns next LSN that will be assigned.
     */
        [[nodiscard]] uint64_t next_lsn() const noexcept;

    private:
        WalWriter(
            const std::filesystem::path& wal_file,
            size_t group_n,
            size_t group_micros,
            bool fast_mode
        );

        class Impl;
        std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::wal