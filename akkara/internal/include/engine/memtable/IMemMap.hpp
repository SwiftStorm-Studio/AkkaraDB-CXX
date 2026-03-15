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

// internal/include/engine/memtable/IMemMap.hpp
#pragma once

#include "core/record/MemRecord.hpp"

#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace akkaradb::engine::memtable {

    /**
     * IMemMap — Abstract ordered key-value map used by MemTable shards.
     *
     * Each shard's active write buffer is backed by a concrete implementation
     * (BPTreeMap or SkipListMap).  All implementations are single-threaded;
     * exclusion is provided by the Shard's shared_mutex.
     *
     * Key ordering: lexicographic byte order (same as MemRecord::compare_key).
     * Duplicate keys: put() overwrites — returns the displaced record.
     *
     * Implementations available:
     *   BPTreeMap   — B+ tree backed; best random-access perf; arena-allocated nodes.
     *   SkipListMap — Skip list backed; industry-standard LSM MemTable structure.
     *
     * Backend is selected at MemTable construction via MemTable::Options::backend.
     */
    class IMemMap {
        public:
            virtual ~IMemMap() = default;

            // ── Write ─────────────────────────────────────────────────────────

            /**
             * Insert or overwrite a record.
             *
             * @return The displaced (old) record on overwrite; nullopt on fresh insert.
             */
            [[nodiscard]] virtual std::optional<core::MemRecord>
                put(core::MemRecord record) = 0;

            // ── Read ──────────────────────────────────────────────────────────

            /**
             * Point lookup.
             * @return The record if found (including tombstones), nullopt if absent.
             */
            [[nodiscard]] virtual std::optional<core::MemRecord>
                find(std::span<const uint8_t> key) const = 0;

            /**
             * Zero-copy point lookup into caller-supplied buffer.
             *
             * @return
             *   nullopt → key not present; caller may continue to SST.
             *   false   → tombstone; delete is authoritative, do NOT check SST.
             *   true    → found; value bytes written to out.
             */
            [[nodiscard]] virtual std::optional<bool>
                find_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const = 0;

            // ── Capacity ──────────────────────────────────────────────────────

            [[nodiscard]] virtual bool   empty() const noexcept = 0;
            [[nodiscard]] virtual size_t size()  const noexcept = 0;

            // ── Iteration ─────────────────────────────────────────────────────

            /**
             * Append all records in sorted key order to out.
             * Used by the flusher to build an SST-ready batch.
             */
            virtual void collect_sorted(std::vector<core::MemRecord>& out) const = 0;

            /**
             * Append all records with key >= start_key in sorted order to out.
             * start_key empty → collect from the beginning of the map.
             * Used by RangeIterator to snapshot the active shard.
             */
            virtual void collect_from(std::span<const uint8_t>  start_key,
                                      std::vector<core::MemRecord>& out) const = 0;

            // ── Factory ───────────────────────────────────────────────────────

            /**
             * Create a fresh empty map of the same concrete type.
             * Called by seal_active() to reset the active shard after sealing.
             */
            [[nodiscard]] virtual std::unique_ptr<IMemMap> make_empty() const = 0;
    };

} // namespace akkaradb::engine::memtable
