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

// internal/include/format-api/BlockPacker.hpp
#pragma once

#include "core/buffer/OwnedBuffer.hpp"
#include "core/record/AKHdr32.hpp"
#include <functional>
#include <span>
#include <cstdint>

namespace akkaradb::format {
    /**
     * BlockPacker - Abstract interface for packing records into fixed-size blocks.
     *
     * A block packer accumulates records into blocks of a fixed size (typically 32 KiB).
     * When a block is full or explicitly flushed, it's finalized and passed to a callback.
     *
     * Design principles:
     * - Single-producer: NOT thread-safe, caller must serialize calls
     * - Callback-based: Emits blocks via callback for flexibility
     * - Abstract interface: Allows different packing strategies
     * - Try-based API: try_append() returns false when block is full
     *
     * Typical workflow:
     * ```cpp
     * auto packer = AkkBlockPacker::create([](OwnedBuffer block) {
     *     // Write to disk, send over network, etc.
     * }, pool);
     *
     * packer->begin_block();
     *
     * while (has_records()) {
     *     if (!packer->try_append(key, value, seq, flags, fp64, mini_key)) {
     *         packer->end_block();
     *         packer->begin_block();
     *         // Retry append
     *     }
     * }
     *
     * packer->flush();
     * ```
     *
     * Thread-safety: NOT thread-safe. Single producer only.
     */
    class BlockPacker {
        public:
            /**
         * Callback invoked when a block is ready.
         *
         * @param block Completed block (ownership transferred to callback)
         */
            using BlockReadyCallback = std::function<void(core::OwnedBuffer block)>;

            virtual ~BlockPacker() = default;

            /**
         * Begins a new block.
         *
         * If a non-empty block is currently open, it's automatically ended first.
         */
            virtual void begin_block() = 0;

            /**
         * Attempts to append a record to the current block.
         *
         * If the block doesn't have enough space, returns false without
         * modifying state. Caller should call end_block(), begin_block(),
         * then retry.
         *
         * @param key Key bytes
         * @param value Value bytes
         * @param seq Sequence number
         * @param flags Flags (FLAG_NORMAL or FLAG_TOMBSTONE)
         * @param key_fp64 SipHash-2-4 fingerprint of key
         * @param mini_key First 8 bytes of key (LE-packed)
         * @return true if appended, false if block is full
         */
            [[nodiscard]] virtual bool try_append(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags,
                uint64_t key_fp64,
                uint64_t mini_key
            ) = 0;

            /**
         * Convenience overload that computes key_fp64 and mini_key automatically.
         *
         * @param key Key bytes
         * @param value Value bytes
         * @param seq Sequence number
         * @param flags Flags
         * @return true if appended, false if block is full
         */
            [[nodiscard]] bool try_append(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags = core::AKHdr32::FLAG_NORMAL
            ) {
                const uint64_t fp64 = core::AKHdr32::compute_key_fp64(key.data(), key.size());
                const uint64_t mini = core::AKHdr32::build_mini_key(key.data(), key.size());
                return try_append(key, value, seq, flags, fp64, mini);
            }

            /**
         * Ends the current block and emits it via callback.
         *
         * No-op if no block is open or block is empty.
         */
            virtual void end_block() = 0;

            /**
         * Flushes any open block.
         *
         * Equivalent to end_block() if a block is open and non-empty.
         */
            virtual void flush() = 0;

            /**
         * Returns the block size in bytes.
         */
            [[nodiscard]] virtual size_t block_size() const noexcept = 0;

            /**
         * Returns the number of bytes remaining in the current block.
         *
         * @return Remaining space, or 0 if no block is open
         */
            [[nodiscard]] virtual size_t remaining() const noexcept = 0;

            /**
     * Returns the number of records in the current block.
     *
     * @return Record count, or 0 if no block is open
     */
            [[nodiscard]] virtual size_t record_count() const noexcept = 0;
    };
} // namespace akkaradb::format