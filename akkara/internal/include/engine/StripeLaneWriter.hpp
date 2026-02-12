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

// internal/include/engine/StripeLaneWriter.hpp
#pragma once

#include "format-api/StripeWriter.hpp"
#include "core/buffer/OwnedBuffer.hpp"
#include <filesystem>
#include <memory>
#include <vector>
#include <cstdint>

namespace akkaradb::engine {
    /**
     * StripeLaneWriter (alias: StripeStore) — persists stripe data to per-lane files.
     *
     * Bridges AkkStripeWriter's StripeReadyCallback to on-disk storage.
     * A stripe of k data + m parity blocks is distributed across k+m append-only
     * lane files ("lane_0.akl" … "lane_N.akl") inside lane_dir.
     *
     * File layout per lane:
     *   [block_0][block_1]...[block_S]   (each block is exactly block_size bytes)
     *
     * Index convention:
     *   stripe index  = block offset within a lane file
     *   byte offset   = stripe_index * block_size
     *
     * Recovery:
     *   All lane files must have identical sizes that are a multiple of block_size.
     *   If the tail is shorter or mismatched, truncated_tail = true and the caller
     *   should call truncate() to restore a consistent state.
     *
     * Thread-safety: NOT thread-safe. Single-producer assumed.
     */
    class StripeLaneWriter {
        public:
            /** Matches format::RecoveryResult but uses int64_t for signed sentinel (-1 = none). */
            struct RecoveryResult {
                int64_t last_sealed = -1; ///< Last fully-written stripe index, or -1 if empty.
                int64_t last_durable = -1; ///< Last fdatasync-confirmed stripe index, or -1.
                bool truncated_tail = false; ///< True if a partial trailing stripe was detected.
            };

            /**
             * Creates a StripeLaneWriter.
             *
             * @param lane_dir   Directory for lane files (created if absent).
             * @param k          Number of data lanes.
             * @param m          Number of parity lanes.
             * @param block_size Fixed block size in bytes (must match packer, e.g. 32 KiB).
             * @param fast_mode  When true, fsync is issued asynchronously (or skipped for
             *                   the data fsync at the expense of some durability margin).
             */
            [[nodiscard]] static std::unique_ptr<StripeLaneWriter> create(
                const std::filesystem::path& lane_dir,
                size_t k,
                size_t m,
                size_t block_size,
                bool fast_mode = false
            );

            ~StripeLaneWriter();

            // --- Write path ---

            /**
             * Called by AkkStripeWriter via StripeReadyCallback.
             * Writes data blocks to the k data lanes and parity blocks to the m parity lanes.
             * Each call advances last_sealed_stripe() by one.
             *
             * @throws std::runtime_error on I/O error.
             */
            void on_stripe_ready(format::StripeWriter::Stripe stripe);

            // --- Durability ---

            /**
             * Calls fdatasync (or FlushFileBuffers on Windows) on all lane files.
             * After this returns successfully, last_durable_stripe() == last_sealed_stripe().
             */
            void force();

            // --- Recovery ---

            /**
             * Scans lane file sizes to determine the last consistent stripe.
             * Does NOT modify any files; use truncate() to repair.
             */
            [[nodiscard]] RecoveryResult recover() const;

            /**
             * Truncates all lane files so that only [0, stripe_count) stripes remain.
             * Idempotent if the files are already shorter.
             *
             * @param stripe_count Number of stripes to keep (0 = empty lanes).
             */
            void truncate(int64_t stripe_count);

            // --- Read path (for stripe fallback in get()) ---

            /**
             * Reads all k+m blocks for a given stripe from lane files.
             * Returns an empty vector if stripe_index is out of range or I/O fails.
             *
             * @param stripe_index  Zero-based stripe index.
             */
            [[nodiscard]] std::vector<core::OwnedBuffer> read_stripe_blocks(int64_t stripe_index) const;

            // --- State ---

            /** Last fully-written stripe index (-1 if no stripes written yet). */
            [[nodiscard]] int64_t last_sealed_stripe() const noexcept;

            /** Last fdatasync-confirmed stripe index (-1 if never forced). */
            [[nodiscard]] int64_t last_durable_stripe() const noexcept;

            /** Total number of lanes (k + m). */
            [[nodiscard]] size_t total_lanes() const noexcept;

            void close();

        private:
            StripeLaneWriter(const std::filesystem::path& lane_dir, size_t k, size_t m, size_t block_size, bool fast_mode);

            class Impl;
            std::unique_ptr<Impl> impl_;
    };

    // Alias used throughout AkkEngine.cpp.
    using StripeStore = StripeLaneWriter;

} // namespace akkaradb::engine