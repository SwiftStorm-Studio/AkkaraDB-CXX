/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/include/engine/blob/BlobManager.hpp
#pragma once

#include "engine/blob/BlobFraming.hpp"
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <span>
#include <vector>

namespace akkaradb::engine::blob {

    /**
     * BlobManager — Manages externalized large-value blob files.
     *
     * Large values (>= threshold_bytes) are written to separate .blob files
     * instead of being stored inline in the WAL / MemTable.
     *
     * File layout:
     * ┌─────────────────────────────────────────────────────┐
     * │  blobs/
     * │      {blob_id >> 56 : 2-hex-digit dir}/
     * │          {blob_id : 16-hex-digit}.blob
     * │  Example:
     * │      blobs/00/0000000000000001.blob
     * │      blobs/ff/ff00000000000042.blob
     * └─────────────────────────────────────────────────────┘
     *
     * Each .blob file = BlobFileHeader (32 bytes) + raw content.
     *
     * GC flow (crash-safe):
     *  1. schedule_delete(blob_id)  → enqueues into delete_queue_
     *  2. GC worker:  rename .blob → .blob.del  (atomic on most FS)
     *  3. GC worker:  delete .blob.del
     *  Startup cleanup: deletes any leftover *.blob.del files (rename was committed).
     *
     * Blob ID generation:
     *  On start(), scans blobs/ for the maximum existing blob_id + 1.
     *  Stored as std::atomic<uint64_t>; no extra file required.
     *
     * Thread safety:
     *  write(), read(), schedule_delete() are thread-safe.
     *  start() / close() must not be called concurrently.
     */
    class BlobManager {
    public:
        static constexpr uint64_t DEFAULT_THRESHOLD = 16 * 1024; ///< 16 KiB

        /**
         * Creates (but does not start) the BlobManager.
         *
         * @param blobs_dir       Directory where blob files are stored.
         *                        Created if it does not exist.
         * @param threshold_bytes Values >= this size are stored as blobs.
         */
        [[nodiscard]] static std::unique_ptr<BlobManager> create(
            std::filesystem::path blobs_dir,
            uint64_t              threshold_bytes = DEFAULT_THRESHOLD
        );

        ~BlobManager();

        /**
         * Starts the GC worker thread and runs startup cleanup
         * (deletes leftover *.blob.del files from a previous crash).
         *
         * Must be called once before write/read/schedule_delete.
         */
        void start();

        // ── write ────────────────────────────────────────────────────────────

        /**
         * Writes content to a new blob file.
         * Assigns a fresh blob_id, writes BlobFileHeader + content atomically
         * via a tmp-file + rename.
         *
         * @return Assigned blob_id.
         */
        [[nodiscard]] uint64_t write(std::span<const uint8_t> content);

        /**
         * Writes a blob that was received from the Primary (Replica path).
         * The blob_id is supplied by the caller (from ReplBlobPut).
         * If a blob with the same id already exists, this is a no-op.
         */
        void write_remote(uint64_t blob_id, std::span<const uint8_t> content);

        // ── read ─────────────────────────────────────────────────────────────

        /**
         * Reads and verifies (header CRC) the content of a blob file.
         * Throws std::runtime_error on I/O error or corruption.
         */
        [[nodiscard]] std::vector<uint8_t> read(uint64_t blob_id) const;

        // ── delete ───────────────────────────────────────────────────────────

        /**
         * Schedules a blob for asynchronous deletion.
         * The GC worker will rename and then delete the file.
         */
        void schedule_delete(uint64_t blob_id);

        // ── GC ───────────────────────────────────────────────────────────────

        /**
         * Scans all blobs/ files and calls is_referenced(blob_id) for each.
         * Schedules deletion for any blob that is not referenced.
         * Intended to be called once during engine open (after WAL replay).
         *
         * @param is_referenced  Returns true if the blob is still needed.
         */
        void scan_orphans(std::function<bool(uint64_t)> is_referenced);

        // ── helpers ──────────────────────────────────────────────────────────

        /**
         * Returns the threshold (in bytes) above which values are externalized.
         */
        [[nodiscard]] uint64_t threshold() const noexcept;

        /**
         * Returns the absolute path for a given blob_id.
         * Format: blobs_dir / {hi8:02x} / {blob_id:016x}.blob
         */
        [[nodiscard]] std::filesystem::path blob_path(uint64_t blob_id) const;

        /** Stops the GC worker thread. */
        void close();

    private:
        struct Impl;
        std::unique_ptr<Impl> impl_;

        explicit BlobManager(std::unique_ptr<Impl> impl);
    };

} // namespace akkaradb::engine::blob
