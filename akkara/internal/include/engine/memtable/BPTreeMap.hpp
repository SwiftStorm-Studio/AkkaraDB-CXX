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

// internal/include/engine/memtable/BPTreeMap.hpp
#pragma once

#include "engine/memtable/IMemMap.hpp"
#include "engine/memtable/BPTree.hpp"

namespace akkaradb::engine::memtable {

    /**
     * BPTreeMap — IMemMap backed by the in-process B+ tree.
     *
     * Header-only: wraps BPTree<MemRecord, monostate, Cmp> with the IMemMap API.
     *
     * Characteristics:
     *   put/find : O(log n), arena-allocated nodes (low malloc overhead)
     *   scan     : O(n), leaf-linked list traversal
     *   memory   : MonotonicArena — bulk-freed on destruction / move
     */
    class BPTreeMap final : public IMemMap {
        public:
            // ── Comparator ────────────────────────────────────────────────────
            struct Cmp {
                using is_transparent = void;
                bool operator()(const core::MemRecord& a, const core::MemRecord& b)        const noexcept { return a.compare_key(b) < 0; }
                bool operator()(const core::MemRecord& a, std::span<const uint8_t> b)       const noexcept { return a.compare_key(b) < 0; }
                bool operator()(std::span<const uint8_t> a,  const core::MemRecord& b)      const noexcept { return b.compare_key(a) > 0; }
            };
            using Tree = BPTree<core::MemRecord, std::monostate, Cmp>;

            BPTreeMap() = default;

            // ── IMemMap ───────────────────────────────────────────────────────

            std::optional<core::MemRecord> put(core::MemRecord record) override {
                return tree_.put(std::move(record), std::monostate{});
            }

            std::optional<core::MemRecord> find(std::span<const uint8_t> key) const override {
                auto it = tree_.lower_bound(key);
                if (!it.is_end() && it->first.compare_key(key) == 0) return it->first;
                return std::nullopt;
            }

            std::optional<bool> find_into(std::span<const uint8_t> key,
                                          std::vector<uint8_t>& out) const override {
                auto it = tree_.lower_bound(key);
                if (!it.is_end() && it->first.compare_key(key) == 0) {
                    if (it->first.is_tombstone()) return false;
                    const auto val = it->first.value();
                    out.assign(val.begin(), val.end());
                    return true;
                }
                return std::nullopt;
            }

            bool   empty() const noexcept override { return tree_.empty(); }
            size_t size()  const noexcept override { return tree_.size();  }

            void collect_sorted(std::vector<core::MemRecord>& out) const override {
                out.reserve(out.size() + tree_.size());
                for (auto it = tree_.begin(); !it.is_end(); ++it) out.push_back(it->first);
            }

            void collect_from(std::span<const uint8_t>     start_key,
                              std::vector<core::MemRecord>& out) const override {
                auto it = start_key.empty() ? tree_.begin() : tree_.lower_bound(start_key);
                for (; !it.is_end(); ++it) out.push_back(it->first);
            }

            std::unique_ptr<IMemMap> make_empty() const override {
                return std::make_unique<BPTreeMap>();
            }

        private:
            Tree tree_;
    };

} // namespace akkaradb::engine::memtable
