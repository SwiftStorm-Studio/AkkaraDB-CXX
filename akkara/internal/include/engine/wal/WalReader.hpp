/*
* AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
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

// internal/include/engine/wal/WalReader.hpp
#pragma once

#include "WalOp.hpp"
#include <memory>
#include <filesystem>
#include <optional>
#include <cstdint>

namespace akkaradb::engine::wal {
    /**
     * WalReader - Read-only iterator over WAL file operations.
     *
     * Reads a WAL file frame-by-frame, validates CRC, and yields WalOp entries.
     * Handles truncated/corrupted frames gracefully for recovery scenarios.
     *
     * Design principles:
     * - Forward-only: Single-pass iteration
     * - Error recovery: Stops at first corruption, reports position
     * - Zero-copy where possible: Reuses read buffer
     * - Stateful: Maintains current file position
     *
     * Typical usage:
     * ```cpp
     * auto reader = WalReader::open("./data/wal/wal-12345.dat");
     *
     * while (auto op_opt = reader->next()) {
     *     const auto& op = *op_opt;
     *     if (op.is_put()) {
     *         // Apply to MemTable
     *     }
     * }
     *
     * if (reader->has_error()) {
     *     std::cout << "Stopped at: " << reader->error_position() << std::endl;
     * }
     * ```
     *
     * Thread-safety: NOT thread-safe.
     */
    class WalReader {
        public:
            /**
         * Error type encountered during reading.
         */
            enum class ErrorType {
                NONE,
                ///< No error
                TRUNCATED_FRAME,
                ///< Incomplete frame at end of file
                INVALID_MAGIC,
                ///< Frame magic mismatch
                CRC_MISMATCH,
                ///< CRC validation failed
                MALFORMED_OP,
                ///< Operation deserialization failed
                IO_ERROR ///< File I/O error
            };

            /**
         * Opens a WAL file for reading.
         *
         * @param wal_file Path to WAL file
         * @return Unique pointer to reader
         * @throws std::runtime_error if file cannot be opened
         */
            [[nodiscard]] static std::unique_ptr<WalReader> open(const std::filesystem::path& wal_file);

            ~WalReader();

            /**
         * Reads the next operation from the WAL.
         *
         * @return WalOp if successful, std::nullopt if end-of-file or error
         */
            [[nodiscard]] std::optional<WalOp> next();

            /**
         * Checks if an error occurred during reading.
         */
            [[nodiscard]] bool has_error() const noexcept;

            /**
         * Returns the type of error encountered.
         */
            [[nodiscard]] ErrorType error_type() const noexcept;

            /**
         * Returns the file position where the error occurred.
         */
            [[nodiscard]] uint64_t error_position() const noexcept;

            /**
         * Returns the current file position (bytes read so far).
         */
            [[nodiscard]] uint64_t current_position() const noexcept;

            /**
         * Returns the total file size.
         */
            [[nodiscard]] uint64_t file_size() const noexcept;

            /**
         * Returns the number of operations successfully read.
         */
            [[nodiscard]] uint64_t operations_read() const noexcept;

        private:
            explicit WalReader(const std::filesystem::path& wal_file);

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::wal