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

// internal/include/format-api/FlushPolicy.hpp
#pragma once

#include <cstdint>

namespace akkaradb::format {
    /**
 * FlushPolicy - Configuration for group commit behavior.
 *
 * Defines when buffered blocks/entries should be flushed to durable storage.
 * Flush occurs when EITHER condition is met:
 * - max_blocks: Number of blocks/entries accumulated
 * - max_micros: Time elapsed since first block in group
 *
 * Design principles:
 * - Dual trigger: Count OR time (whichever comes first)
 * - Latency control: max_micros caps worst-case latency
 * - Throughput optimization: max_blocks enables batching
 *
 * Typical configurations:
 * ```cpp
 * // High throughput (larger batches)
 * FlushPolicy{.max_blocks = 64, .max_micros = 1000};
 *
 * // Low latency (smaller batches, faster flush)
 * FlushPolicy{.max_blocks = 16, .max_micros = 200};
 *
 * // Immediate flush (no batching)
 * FlushPolicy{.max_blocks = 1, .max_micros = 0};
 * ```
 */
    struct FlushPolicy {
        /**
     * Maximum blocks/entries before forced flush.
     *
     * Typical values: 16-64 for blocks, 32-128 for WAL entries.
     */
        size_t max_blocks;

        /**
     * Maximum microseconds before forced flush.
     *
     * Typical values: 200-1000 µs.
     * Set to 0 for immediate flush.
     */
        uint64_t max_micros;

        /**
     * Creates a default policy (32 blocks OR 500 µs).
     */
        static constexpr FlushPolicy default_policy() noexcept { return FlushPolicy{.max_blocks = 32, .max_micros = 500}; }

        /**
     * Creates an immediate flush policy (no batching).
     */
        static constexpr FlushPolicy immediate() noexcept { return FlushPolicy{.max_blocks = 1, .max_micros = 0}; }

        /**
     * Creates a high-throughput policy (larger batches).
     */
        static constexpr FlushPolicy high_throughput() noexcept { return FlushPolicy{.max_blocks = 64, .max_micros = 1000}; }

        /**
     * Creates a low-latency policy (smaller batches).
     */
        static constexpr FlushPolicy low_latency() noexcept { return FlushPolicy{.max_blocks = 16, .max_micros = 200}; }
    };
} // namespace akkaradb::format