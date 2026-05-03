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

// internal/include/engine/memtable/IMemTable.hpp
#pragma once

#include <cstddef>
#include <cstdint>

#include "core/Status.hpp"
#include "core/record/RecordView.hpp"
#include "core/types/ByteView.hpp"
#include "core/utils/ArenaGenerator.hpp"

using namespace akkaradb::core;

namespace akkaradb::engine {
    /**
     * @brief Abstract interface for pluggable MemTable implementations.
     *
     * IMemTable represents the in-memory mutable ordered index layer
     * positioned between WAL and SST.
     *
     * Supported responsibilities:
     * - append-style writes with sequence numbers
     * - snapshot-aware point lookup
     * - ordered iteration for flush and scans
     * - immutable freeze before SST flush
     *
     * Intended backend implementations include:
     * - Skip List
     * - B+ Tree
     * - Red-Black Tree
     *
     * Design constraints:
     * - ordered traversal is mandatory
     * - multi-version records are backend-managed
     * - tombstones are represented via record flags
     */
    class IMemTable {
        public:
            virtual ~IMemTable() = default;

            /**
             * @brief Insert or append a new record version.
             *
             * Implementations are expected to support append-style
             * version retention internally.
             *
             * Both key and value are treated as immutable binary blobs.
             * No ownership is transferred.
             *
             * @param key User key bytes.
             * @param value Value payload bytes.
             * @param seq Monotonic sequence number.
             * @param flags Record metadata flags (e.g. tombstone).
             *
             * @return Operation result.
             */
            [[nodiscard]] virtual Status put(ByteView key, ByteView value, uint64_t seq, uint8_t flags) = 0;

            /**
             * @brief Retrieve the visible version for a snapshot.
             *
             * Returns the newest version whose sequence number is
             * less than or equal to snapshot_seq.
             *
             * @param key User key.
             * @param snapshot_seq Snapshot sequence boundary.
             * @param out Output record view.
             *
             * @return true if visible record exists.
             * @return false otherwise.
             */
            [[nodiscard]] virtual bool get(ByteView key, uint64_t snapshot_seq, RecordView* out) const = 0;

            /**
             * @brief Create ordered iterator for snapshot view.
             *
             * Iteration order must be lexicographically ordered by key.
             *
             * @param snapshot_seq Snapshot sequence boundary.
             * @return Generator of visible records.
             */
            [[nodiscard]] virtual ArenaGenerator<RecordView> iterator(uint64_t snapshot_seq) const = 0;

            /**
             * @brief Freeze the MemTable into immutable state.
             *
             * After freeze():
             * - writes must be rejected
             * - reads and iteration remain valid
             *
             * Intended for SST flush handoff.
             */
            virtual void freeze() = 0;

            /**
             * @brief Current approximate memory usage in bytes.
             *
             * Used for flush threshold decisions.
             *
             * @return Memory usage.
             */
            [[nodiscard]] virtual size_t sizeBytes() const = 0;

            /**
             * @brief Total logical record count.
             *
             * Includes all retained versions managed by backend.
             *
             * @return Number of stored records.
             */
            [[nodiscard]] virtual size_t entryCount() const = 0;
    };
} // namespace akkaradb::engine
