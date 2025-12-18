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

// internal/include/engine/wal/WalWriter.hpp
#pragma once

#include "WalOp.hpp"
#include "format-api/FlushPolicy.hpp"
#include "format-api/ParityCoder.hpp"
#include "core/buffer/BufferPool.hpp"
#include <memory>
#include <filesystem>
#include <cstdint>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::wal {
    /**
 * WalWriter - Write-Ahead Log writer with true fsync support.
 *
 * Uses platform-specific low-level I/O for guaranteed durability:
 * - Linux: fsync(2) or fdatasync(2)
 * - macOS: fcntl(F_FULLFSYNC)
 * - Windows: FlushFileBuffers
 *
 * Design principles:
 * - Durability: True fsync on flush
 * - Group commit: Batches operations for throughput
 * - Striped storage: Optional parity for redundancy
 * - Direct I/O ready: Aligned writes for O_DIRECT (future)
 * - Single-producer: NOT thread-safe
 */
    class WalWriter {
    public:
        /**
     * Flush mode configuration.
     */
        enum class FlushMode {
            SYNC, ///< Block until fsync completes (max durability)
            ASYNC ///< Return immediately, fsync in background (fast mode)
        };

        /**
     * Sync mode configuration (POSIX-specific).
     */
        enum class SyncMode {
            FULL, ///< fsync() - sync data + metadata
            DATA_ONLY ///< fdatasync() - sync data only (faster)
        };

        [[nodiscard]] static std::unique_ptr<WalWriter> create(
            const std::filesystem::path& wal_dir,
            std::shared_ptr<core::BufferPool> buffer_pool,
            format::FlushPolicy flush_policy = format::FlushPolicy::default_policy(),
            size_t k = 4,
            size_t m = 2,
            std::shared_ptr<format::ParityCoder> parity_coder = nullptr,
            FlushMode flush_mode = FlushMode::SYNC,
            SyncMode sync_mode = SyncMode::FULL
        );

        ~WalWriter();

        void append(const WalOp& op);
        void flush();

        [[nodiscard]] uint64_t operations_written() const noexcept;
        [[nodiscard]] uint64_t blocks_written() const noexcept;
        [[nodiscard]] uint64_t stripes_written() const noexcept;
        [[nodiscard]] uint64_t bytes_written() const noexcept;
        [[nodiscard]] std::filesystem::path current_file_path() const;

    private:
        WalWriter(
            const std::filesystem::path& wal_dir,
            std::shared_ptr<core::BufferPool> buffer_pool,
            format::FlushPolicy flush_policy,
            size_t k,
            size_t m,
            std::shared_ptr<format::ParityCoder> parity_coder,
            FlushMode flush_mode,
            SyncMode sync_mode
        );

        class Impl;
        std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::wal