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

// internal/include/engine/manifest/Manifest.hpp
#pragma once

#include <filesystem>
#include <string>
#include <vector>
#include <memory>
#include <cstdint>
#include <optional>

namespace akkaradb::engine::manifest {
    /**
 * Manifest - Append-only metadata log for database state.
 *
 * Records critical events:
 * - StripeCommit: Stripe write completion
 * - SSTSeal: SSTable creation
 * - Checkpoint: Durable state marker
 * - SSTDelete: SSTable removal
 * - CompactionStart/End: Compaction lifecycle
 * - Truncate: WAL truncation
 * - FormatBump: Version upgrade
 *
 * Format: [length:u32][json_payload][crc32c:u32]
 *
 * Design principles:
 * - Append-only: Never modifies existing entries
 * - Fast mode: Background batch writes with periodic fsync
 * - Rotation: Creates new file at 32 MiB threshold
 * - Recovery-friendly: JSON for debuggability
 *
 * Thread-safety: All public methods are thread-safe.
 */
    class Manifest {
    public:
        /**
     * Event types.
     */
        struct StripeCommitEvent {
            uint64_t after; ///< Stripe count after commit
            uint64_t ts; ///< Timestamp (milliseconds since epoch)
        };

        struct SSTSealEvent {
            int level; ///< SST level (0, 1, 2, ...)
            std::string file; ///< File path (e.g., "L0/sst_001.sst")
            uint64_t entries; ///< Entry count
            std::optional<std::string> first_key_hex; ///< First key (hex)
            std::optional<std::string> last_key_hex; ///< Last key (hex)
            uint64_t ts;
        };

        struct CheckpointEvent {
            std::optional<std::string> name; ///< Checkpoint name
            std::optional<uint64_t> stripe; ///< Stripe ID
            std::optional<uint64_t> last_seq; ///< Last sequence number
            uint64_t ts{};
        };

        struct CompactionStartEvent {
            int level;
            std::vector<std::string> inputs; ///< Input SST files
            uint64_t ts;
        };

        struct CompactionEndEvent {
            int level;
            std::string output; ///< Output SST file
            std::vector<std::string> inputs; ///< Input SST files (optional)
            uint64_t entries;
            std::optional<std::string> first_key_hex;
            std::optional<std::string> last_key_hex;
            uint64_t ts;
        };

        struct SSTDeleteEvent {
            std::string file;
            uint64_t ts;
        };

        /**
     * Creates a Manifest.
     *
     * @param path Manifest file path
     * @param fast_mode Enable background batching (default: true)
     * @return Unique pointer to Manifest
     */
        [[nodiscard]] static std::unique_ptr<Manifest> create(
            const std::filesystem::path& path,
            bool fast_mode = true
        );

        ~Manifest();

        /**
     * Starts background flusher (fast mode only).
     */
        void start();

        /**
     * Records stripe commit.
     */
        void advance(uint64_t new_count);

        /**
     * Records SSTable seal.
     */
        void sst_seal(
            int level,
            const std::string& file,
            uint64_t entries,
            const std::optional<std::string>& first_key_hex,
            const std::optional<std::string>& last_key_hex
        );

        /**
     * Records checkpoint.
     */
        void checkpoint(
            const std::optional<std::string>& name = std::nullopt,
            const std::optional<uint64_t>& stripe = std::nullopt,
            const std::optional<uint64_t>& last_seq = std::nullopt
        );

        /**
     * Records compaction start.
     */
        void compaction_start(int level, const std::vector<std::string>& inputs);

        /**
     * Records compaction end.
     */
        void compaction_end(
            int level,
            const std::string& output,
            const std::vector<std::string>& inputs,
            uint64_t entries,
            const std::optional<std::string>& first_key_hex,
            const std::optional<std::string>& last_key_hex
        );

        /**
     * Records SSTable deletion.
     */
        void sst_delete(const std::string& file);

        /**
     * Records truncate event.
     */
        void truncate(const std::optional<std::string>& reason = std::nullopt);

        /**
     * Replays manifest from disk.
     */
        void replay();

        /**
     * Returns stripe count.
     */
        [[nodiscard]] uint64_t stripes_written() const noexcept;

        /**
     * Returns last checkpoint event.
     */
        [[nodiscard]] std::optional<CheckpointEvent> last_checkpoint() const noexcept;

        /**
     * Returns live SST files.
     */
        [[nodiscard]] std::vector<std::string> live_sst() const;

        /**
     * Returns deleted SST files.
     */
        [[nodiscard]] std::vector<std::string> deleted_sst() const;

        /**
     * Returns all SST seal events.
     */
        [[nodiscard]] std::vector<SSTSealEvent> sst_seals() const;

        /**
     * Closes manifest (blocks until flusher completes).
     */
        void close();

    private:
        Manifest(const std::filesystem::path& path, bool fast_mode);

        class Impl;
        std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::manifest