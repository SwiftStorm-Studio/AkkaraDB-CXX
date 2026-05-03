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

#include "engine/memtable/SkipListMemTable.hpp"

#include <algorithm>
#include <limits>
#include <new>
#include <utility>

#include "core/record/MemHdr16.hpp"
#include "core/record/SSTHdr32.hpp"

namespace akkaradb::engine {
    namespace {
        template <typename T, typename... Args>
        [[nodiscard]] T* arena_new(BufferArena& arena, Args&&... args) {
            std::byte* mem = arena.allocate(sizeof(T), alignof(T));
            return new (mem) T(std::forward<Args>(args)...);
        }
    } // namespace

    SkipListMemTable::SkipListMemTable(
        size_t data_arena_initial_block_size,
        size_t data_arena_max_block_size,
        size_t generator_arena_initial_block_size,
        size_t generator_arena_max_block_size
    )
        : data_arena_{data_arena_initial_block_size, data_arena_max_block_size},
          generator_arena_{generator_arena_initial_block_size, generator_arena_max_block_size} {
        head_ = arena_new<Node>(data_arena_);
        head_->level = MAX_LEVEL;
        head_->key_record = nullptr;
        for (uint8_t i = 0; i < MAX_LEVEL; ++i) {
            head_->next[i].store(nullptr, std::memory_order_relaxed);
        }

        bytes_.store(sizeof(Node), std::memory_order_relaxed);
    }

    std::span<const uint8_t> SkipListMemTable::as_u8(ByteView view) noexcept {
        return {reinterpret_cast<const uint8_t*>(view.data()), view.size()};
    }

    uint64_t SkipListMemTable::next_random() noexcept {
        uint64_t x = rng_state_;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        rng_state_ = x;
        return x;
    }

    uint8_t SkipListMemTable::random_level() noexcept {
        uint8_t level = 1;
        while (level < MAX_LEVEL && ((next_random() & 0x3ULL) == 0ULL)) {
            ++level;
        }
        return level;
    }

    SkipListMemTable::Node* SkipListMemTable::new_node(const core::OwnedRecord* initial_record, uint8_t level) {
        Node* node = arena_new<Node>(data_arena_);
        node->level = level;
        node->key_record = initial_record;
        node->head.store(0, std::memory_order_relaxed);
        node->count.store(1, std::memory_order_relaxed);
        node->version.store(0, std::memory_order_relaxed);

        for (uint8_t i = 0; i < MAX_LEVEL; ++i) {
            node->next[i].store(nullptr, std::memory_order_relaxed);
        }
        for (uint8_t i = 0; i < MAX_VERSIONS_PER_KEY; ++i) {
            node->ring[i].record.store(nullptr, std::memory_order_relaxed);
        }

        node->ring[0].record.store(initial_record, std::memory_order_release);
        return node;
    }

    core::OwnedRecord* SkipListMemTable::make_record(ByteView key, ByteView value, uint64_t seq, uint8_t flags) {
        const auto key_u8 = as_u8(key);
        const auto value_u8 = as_u8(value);

        const uint64_t fp64 = key_u8.empty()
            ? 0
            : core::SSTHdr32::compute_key_fp64(key_u8.data(), key_u8.size());
        const uint64_t mini = key_u8.empty()
            ? 0
            : core::SSTHdr32::build_mini_key(key_u8.data(), key_u8.size());

        return arena_new<core::OwnedRecord>(
            data_arena_,
            core::OwnedRecord::create(key_u8, value_u8, seq, flags, data_arena_, fp64, mini)
        );
    }

    int SkipListMemTable::compare_node_key(const Node* node, std::span<const uint8_t> key) noexcept {
        return node->key_record->compare_key(key);
    }

    SkipListMemTable::Node* SkipListMemTable::find_node(
        std::span<const uint8_t> key,
        std::array<Node*, MAX_LEVEL>* update
    ) const noexcept {
        Node* current = head_;
        for (int level = static_cast<int>(MAX_LEVEL) - 1; level >= 0; --level) {
            Node* next = current->next[level].load(std::memory_order_acquire);
            while (next != nullptr && compare_node_key(next, key) < 0) {
                current = next;
                next = current->next[level].load(std::memory_order_acquire);
            }
            if (update != nullptr) {
                (*update)[static_cast<size_t>(level)] = current;
            }
        }
        return current->next[0].load(std::memory_order_acquire);
    }

    bool SkipListMemTable::visible_record(const Node* node, uint64_t snapshot_seq, RecordView* out) const noexcept {
        for (;;) {
            const uint64_t begin = node->version.load(std::memory_order_acquire);
            if ((begin & 1ULL) != 0ULL) {
                continue;
            }

            const uint8_t head = node->head.load(std::memory_order_acquire);
            const uint8_t count = node->count.load(std::memory_order_acquire);

            const core::OwnedRecord* selected = nullptr;
            for (uint8_t i = 0; i < count; ++i) {
                const uint8_t index = static_cast<uint8_t>((head - i) & (MAX_VERSIONS_PER_KEY - 1));
                const core::OwnedRecord* candidate = node->ring[index].record.load(std::memory_order_acquire);
                if (candidate != nullptr && candidate->seq() <= snapshot_seq) {
                    selected = candidate;
                    break;
                }
            }

            const uint64_t end = node->version.load(std::memory_order_acquire);
            if (begin == end && (end & 1ULL) == 0ULL) {
                if (selected == nullptr) {
                    return false;
                }
                *out = to_view(*selected);
                return true;
            }
        }
    }

    RecordView SkipListMemTable::to_view(const core::OwnedRecord& record) noexcept {
        const auto key = record.key();
        const auto value = record.value();
        return {
            key.data(),
            record.hdr.k_len,
            value.data(),
            record.hdr.v_len,
            record.hdr.seq,
            record.hdr.flags,
            record.key_fp64,
            record.mini_key
        };
    }

    Status SkipListMemTable::put(ByteView key, ByteView value, uint64_t seq, uint8_t flags) {
        if (frozen_.load(std::memory_order_acquire)) {
            return Status::Error(Status::Code::InvalidArgument, "memtable is frozen");
        }

        if (key.size() > std::numeric_limits<uint16_t>::max() ||
            value.size() > std::numeric_limits<uint16_t>::max()) {
            return Status::Error(Status::Code::InvalidArgument, "key/value too large for MemHdr16");
        }

        std::array<Node*, MAX_LEVEL> update{};
        Node* candidate = find_node(as_u8(key), &update);

        const core::OwnedRecord* record = make_record(key, value, seq, flags);
        bytes_.fetch_add(sizeof(core::OwnedRecord) + key.size() + value.size(), std::memory_order_relaxed);

        if (candidate != nullptr && compare_node_key(candidate, as_u8(key)) == 0) {
            candidate->version.fetch_add(1, std::memory_order_acq_rel); // enter write (odd)

            const uint8_t prev_head = candidate->head.load(std::memory_order_relaxed);
            const uint8_t prev_count = candidate->count.load(std::memory_order_relaxed);
            const uint8_t next_head = static_cast<uint8_t>((prev_head + 1) & (MAX_VERSIONS_PER_KEY - 1));

            candidate->ring[next_head].record.store(record, std::memory_order_release);
            if (prev_count < MAX_VERSIONS_PER_KEY) {
                candidate->count.store(static_cast<uint8_t>(prev_count + 1), std::memory_order_relaxed);
                entries_.fetch_add(1, std::memory_order_relaxed);
            }
            candidate->head.store(next_head, std::memory_order_release);
            candidate->version.fetch_add(1, std::memory_order_release); // leave write (even)
            return Status::OK();
        }

        const uint8_t level = random_level();
        Node* node = new_node(record, level);
        bytes_.fetch_add(sizeof(Node), std::memory_order_relaxed);
        entries_.fetch_add(1, std::memory_order_relaxed);

        for (uint8_t i = 0; i < level; ++i) {
            Node* next = update[i]->next[i].load(std::memory_order_relaxed);
            node->next[i].store(next, std::memory_order_relaxed);
        }

        for (int i = static_cast<int>(level) - 1; i >= 0; --i) {
            update[static_cast<size_t>(i)]->next[static_cast<size_t>(i)].store(node, std::memory_order_release);
        }

        return Status::OK();
    }

    bool SkipListMemTable::get(ByteView key, uint64_t snapshot_seq, RecordView* out) const {
        if (out == nullptr) {
            return false;
        }

        Node* node = find_node(as_u8(key), nullptr);
        if (node == nullptr || compare_node_key(node, as_u8(key)) != 0) {
            return false;
        }

        return visible_record(node, snapshot_seq, out);
    }

    ArenaGenerator<RecordView> SkipListMemTable::iterate_snapshot(uint64_t snapshot_seq) const {
        Node* current = head_->next[0].load(std::memory_order_acquire);
        while (current != nullptr) {
            RecordView visible;
            if (visible_record(current, snapshot_seq, &visible)) {
                co_yield visible;
            }
            current = current->next[0].load(std::memory_order_acquire);
        }
    }

    ArenaGenerator<RecordView> SkipListMemTable::iterator(uint64_t snapshot_seq) const {
        std::lock_guard<std::mutex> lock{generator_arena_mutex_};
        return ArenaGenerator<RecordView>::with_arena(generator_arena_, [this, snapshot_seq]() {
            return iterate_snapshot(snapshot_seq);
        });
    }

    void SkipListMemTable::freeze() {
        frozen_.store(true, std::memory_order_release);
    }

    size_t SkipListMemTable::sizeBytes() const {
        return bytes_.load(std::memory_order_acquire);
    }

    size_t SkipListMemTable::entryCount() const {
        return entries_.load(std::memory_order_acquire);
    }
} // namespace akkaradb::engine
