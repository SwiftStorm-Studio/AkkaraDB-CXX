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

// internal/src/engine/memtable/SkipListMap.cpp
#include "engine/memtable/SkipListMap.hpp"
#include "core/record/AKHdr32.hpp"

namespace akkaradb::engine::memtable {

    // ── Level probability ─────────────────────────────────────────────────────
    // p = 0.25 → branching factor 4 → space overhead ~ n/3 pointers
    // Threshold: advance a level when rng() < 0xFFFFFFFF * 0.25
    static constexpr uint32_t LEVEL_THRESHOLD =
        static_cast<uint32_t>(0.25 * static_cast<double>(0xFFFFFFFFu));

    // ============================================================================
    // Constructor / Destructor
    // ============================================================================

    SkipListMap::SkipListMap()
        : height_(1), size_(0), rng_(std::random_device{}())
    {
        // Create the head sentinel with an empty-byte key (length 0).
        // All user keys have length >= 1, so the sentinel always compares less.
        void* mem = arena_.allocate(sizeof(Node), alignof(Node));
        head_ = new(mem) Node(
            core::MemRecord::tombstone(std::span<const uint8_t>{}, 0, 0),
            MAX_HEIGHT
        );
    }

    SkipListMap::~SkipListMap() {
        // Walk the level-0 list and explicitly call ~Node() on every node
        // (including head_) so that each MemRecord's vector<uint8_t> is freed.
        // The arena then bulk-frees all slab blocks without per-node overhead.
        Node* cur = head_;
        while (cur) {
            Node* nxt = cur->next[0];
            cur->~Node();
            cur = nxt;
        }
        // arena_ destructor frees all slab blocks here.
    }

    // ============================================================================
    // random_height
    // ============================================================================

    int SkipListMap::random_height() noexcept {
        int h = 1;
        while (h < MAX_HEIGHT && (rng_() & 0xFFFFFFFFu) < LEVEL_THRESHOLD) ++h;
        return h;
    }

    // ============================================================================
    // find_predecessors
    // ============================================================================
    //
    // Classic skip-list descent: start at (head_, height_-1), walk right while
    // the next node's key < search_key, then descend.  Fills update[i] with the
    // predecessor at each level so the caller can splice a new node in.
    //
    // Returns the first node at level 0 with key >= search_key (may be nullptr).

    SkipListMap::Node* SkipListMap::find_predecessors(
        std::span<const uint8_t> key,
        Node* update[MAX_HEIGHT]) const noexcept
    {
        Node* cur = head_;
        for (int level = height_ - 1; level >= 0; --level) {
            while (cur->next[level] &&
                   cur->next[level]->record.compare_key(key) < 0)
            {
                cur = cur->next[level];
            }
            update[level] = cur;
        }
        return cur->next[0];
    }

    // ============================================================================
    // lower_bound_node
    // ============================================================================

    SkipListMap::Node* SkipListMap::lower_bound_node(
        std::span<const uint8_t> start_key) const noexcept
    {
        if (start_key.empty()) return head_->next[0];

        Node* cur = head_;
        for (int level = height_ - 1; level >= 0; --level) {
            while (cur->next[level] &&
                   cur->next[level]->record.compare_key(start_key) < 0)
            {
                cur = cur->next[level];
            }
        }
        return cur->next[0];
    }

    // ============================================================================
    // put
    // ============================================================================

    std::optional<core::MemRecord> SkipListMap::put(core::MemRecord record) {
        Node* update[MAX_HEIGHT] = {};
        Node* candidate = find_predecessors(record.key(), update);

        // Key already exists → overwrite in place, return displaced record.
        if (candidate && candidate->record.compare_key(record.key()) == 0) {
            auto old = std::move(candidate->record);
            candidate->record = std::move(record);
            return old;
        }

        // Fresh insert: generate height, extend list height if needed.
        const int h = random_height();
        if (h > height_) {
            // Levels [height_, h) have head_ as their only predecessor.
            for (int i = height_; i < h; ++i) update[i] = head_;
            height_ = h;
        }

        // Allocate new node from arena and splice into each level.
        void* mem = arena_.allocate(sizeof(Node), alignof(Node));
        Node* node = new(mem) Node(std::move(record), h);
        for (int i = 0; i < h; ++i) {
            node->next[i]    = update[i]->next[i];
            update[i]->next[i] = node;
        }
        ++size_;
        return std::nullopt;
    }

    // ============================================================================
    // find
    // ============================================================================

    std::optional<core::MemRecord> SkipListMap::find(
        std::span<const uint8_t> key) const
    {
        Node* candidate = lower_bound_node(key);
        if (candidate && candidate->record.compare_key(key) == 0)
            return candidate->record;
        return std::nullopt;
    }

    // ============================================================================
    // find_into
    // ============================================================================

    std::optional<bool> SkipListMap::find_into(
        std::span<const uint8_t> key,
        std::vector<uint8_t>&    out) const
    {
        Node* candidate = lower_bound_node(key);
        if (candidate && candidate->record.compare_key(key) == 0) {
            if (candidate->record.is_tombstone()) return false;
            const auto val = candidate->record.value();
            out.assign(val.begin(), val.end());
            return true;
        }
        return std::nullopt;
    }

    // ============================================================================
    // contains
    // ============================================================================

    std::optional<bool> SkipListMap::contains(std::span<const uint8_t> key) const {
        Node* candidate = lower_bound_node(key);
        if (candidate && candidate->record.compare_key(key) == 0) return !candidate->record.is_tombstone();
        return std::nullopt;
    }

    // ============================================================================
    // collect_sorted
    // ============================================================================

    void SkipListMap::collect_sorted(std::vector<core::MemRecord>& out) const {
        out.reserve(out.size() + size_);
        for (Node* cur = head_->next[0]; cur; cur = cur->next[0])
            out.push_back(cur->record);
    }

    // ============================================================================
    // collect_from
    // ============================================================================

    void SkipListMap::collect_from(std::span<const uint8_t>     start_key,
                                   std::vector<core::MemRecord>& out) const
    {
        for (Node* cur = lower_bound_node(start_key); cur; cur = cur->next[0])
            out.push_back(cur->record);
    }

    // ============================================================================
    // make_empty
    // ============================================================================

    std::unique_ptr<IMemMap> SkipListMap::make_empty() const {
        return std::make_unique<SkipListMap>();
    }

} // namespace akkaradb::engine::memtable
