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

// internal/include/format-akk/AkkBlockPacker.hpp
#pragma once

#include "format-api/BlockPacker.hpp"
#include "core/buffer/BufferPool.hpp"
#include <memory>

#include "core/record/MemRecord.hpp"

namespace akkaradb::format::akk {
    /**
     * AkkBlockPacker - Concrete implementation of BlockPacker for AkkEngine format.
     *
     * Packs records into 32 KiB blocks with the following layout:
     * [0..3]       payloadLen (u32 LE)
     * [4..N)       payload = repeated { AKHdr32(32B) + key + value }
     * [N..32764)   zero padding
     * [32764..32768) CRC32C (u32 LE)
     *
     * Design principles:
     * - Fixed block size: Always 32 KiB (32768 bytes)
     * - Direct write: Single-copy writes (header+key+value written once)
     * - CRC32C: Hardware-accelerated checksum over entire block
     * - Buffer pooling: Reuses buffers from BufferPool
     *
     * Memory layout example:
     * ```
     * [payloadLen=100][hdr32][key1][val1][hdr32][key2][val2]...[pad][CRC]
     *  0              4      36    41    46    78    83    ...  32764 32768
     * ```
     *
     * Thread-safety: NOT thread-safe. Single producer only.
     */
    class AkkBlockPacker : public BlockPacker {
        public:
            /**
         * Block size constant (32 KiB).
         */
            static constexpr size_t BLOCK_SIZE = 32 * 1024;

            /**
         * Overhead per block (payloadLen + CRC32C).
         */
            static constexpr size_t BLOCK_OVERHEAD = sizeof(uint32_t) * 2;

            /**
         * Maximum payload size.
         */
            static constexpr size_t MAX_PAYLOAD = BLOCK_SIZE - BLOCK_OVERHEAD;

            /**
         * Creates an AkkBlockPacker.
         *
         * @param callback Callback invoked when a block is ready
         * @param pool Buffer pool for allocating blocks
         * @return Unique pointer to packer
         */
            [[nodiscard]] static std::unique_ptr<AkkBlockPacker> create(BlockReadyCallback callback, std::shared_ptr<core::BufferPool> pool);

            ~AkkBlockPacker() override;

            void begin_block() override;

            [[nodiscard]] bool try_append(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags,
                uint64_t key_fp64,
                uint64_t mini_key
            ) override;

            bool try_append(const core::MemRecord& record);

            void end_block() override;

            void flush() override;

            [[nodiscard]] size_t block_size() const noexcept override { return BLOCK_SIZE; }

            [[nodiscard]] size_t remaining() const noexcept override;

            [[nodiscard]] size_t record_count() const noexcept override;

        private:
            AkkBlockPacker(BlockReadyCallback callback, std::shared_ptr<core::BufferPool> pool);

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::format::akk