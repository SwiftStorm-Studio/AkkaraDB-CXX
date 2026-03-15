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

// internal/include/engine/memtable/SkipListMap.hpp
#pragma once

#include "engine/memtable/IMemMap.hpp"

#include <array>
#include <random>

namespace akkaradb::engine::memtable {

    /**
     * SkipListMap — IMemMap backed by a probabilistic skip list.
     *
     * Skip lists are the industry-standard MemTable structure for LSM engines
     * (LevelDB, RocksDB, Apache Cassandra).  This implementation provides:
     *
     *   put/find : O(log n) amortized
     *   scan     : O(n) via level-0 linked list
     *   memory   : per-node heap allocation (no arena), freed on destruction
     *
     * Thread-safety: single-threaded; Shard's mutex provides exclusion.
     *
     * Parameters:
     *   MAX_HEIGHT = 12  → ideal for up to ~4M entries at branching factor p = 0.25
     *
     * Limitations:
     *   Empty-byte keys ({}) are reserved for the internal head sentinel.
     *   User keys must have length ≥ 1 (standard for all AkkaraDB callers).
     */
    class SkipListMap final : public IMemMap {
        public:
            static constexpr int MAX_HEIGHT = 12;

            SkipListMap();
            ~SkipListMap() override;

            SkipListMap(const SkipListMap&)            = delete;
            SkipListMap& operator=(const SkipListMap&) = delete;

            // ── IMemMap ───────────────────────────────────────────────────────

            std::optional<core::MemRecord> put(core::MemRecord record) override;

            std::optional<core::MemRecord> find(std::span<const uint8_t> key) const override;

            std::optional<bool> find_into(std::span<const uint8_t>  key,
                                          std::vector<uint8_t>& out) const override;

            bool   empty() const noexcept override { return size_ == 0; }
            size_t size()  const noexcept override { return size_; }

            void collect_sorted(std::vector<core::MemRecord>& out) const override;

            void collect_from(std::span<const uint8_t>     start_key,
                              std::vector<core::MemRecord>& out) const override;

            std::unique_ptr<IMemMap> make_empty() const override;

        private:
            // ── Internal node type ────────────────────────────────────────────
            //
            // Level-0 forward pointers form a sorted singly-linked list.
            // Levels 1..height-1 provide O(log n) skip-ahead during search.
            //
            // Head sentinel: a Node with an empty-byte key MemRecord (seq=0, tombstone).
            // User keys always have length ≥ 1, so the sentinel compares less than
            // every real key without any special-casing in the comparator.
            struct Node {
                core::MemRecord              record;
                int                          height;
                std::array<Node*, MAX_HEIGHT> next{};

                Node(core::MemRecord r, int h) : record(std::move(r)), height(h) {}
            };

            // ── Members ───────────────────────────────────────────────────────

            Node*        head_;    ///< Sentinel node (empty key, seq=0)
            int          height_;  ///< Current max height across all non-sentinel nodes
            size_t       size_;
            std::mt19937 rng_;

            // ── Helpers ───────────────────────────────────────────────────────

            /// Generate a random level in [1, MAX_HEIGHT] with P(level=k) = (1-p)*p^{k-1}.
            int random_height() noexcept;

            /// Walk the list top-down, filling update[i] = predecessor at level i.
            /// Returns the first node with key >= search_key (or nullptr if none).
            Node* find_predecessors(std::span<const uint8_t> key,
                                    Node* update[MAX_HEIGHT]) const noexcept;

            /// Return the first node with key >= start_key (or the first node if empty).
            Node* lower_bound_node(std::span<const uint8_t> start_key) const noexcept;
    };

} // namespace akkaradb::engine::memtable
