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

// internal/include/core/buffer/MonotonicArena.hpp
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <new>
#include <vector>

#ifdef _WIN32
#  include <malloc.h>
#else
#  include <cstdlib>
#endif

namespace akkaradb::core {
    /**
     * MonotonicArena - Bump-pointer allocator for ephemeral tree node memory.
     *
     * Design goals:
     * - Near-zero allocation overhead: O(1) bump-pointer vs O(1) avg malloc (~50–100 ns)
     * - Cache-friendly node layout: blocks are 64-byte aligned; sequential allocations
     *   stay within the same 256 KiB block → nodes touched together are near each other
     * - Move-friendly: BPTree moves its arena alongside its root pointer so immutable
     *   snapshot maps own their slab memory until the flusher drops them
     * - No individual frees: all memory reclaimed at once via reset() or destruction
     *
     * Typical usage (BPTree node lifecycle):
     *   alloc_leaf()   → arena_.allocate(sizeof(LeafNode), alignof(LeafNode))
     *                  → placement-new LeafNode{}
     *   seal_active()  → BPTree std::move → arena moves with the tree
     *   on_flushed()   → shared_ptr<const Map> drops → ~BPTree()
     *                  → destroy_tree() calls ~LeafNode() (frees vector data)
     *                  → ~MonotonicArena() frees slab blocks
     *
     * Thread-safety: None. External synchronization required (shard mutex).
     */
    class MonotonicArena {
        public:
            /// Size of each internally allocated block. 256 KiB fits ~8 BPTree leaf nodes
            /// (sizeof(LeafNode) ≈ 32 KB with LEAF_ORDER=503, K=MemRecord).
            static constexpr size_t BLOCK_SIZE = 256u * 1024u;

            MonotonicArena() = default;

            ~MonotonicArena() noexcept { free_blocks(); }

            MonotonicArena(const MonotonicArena&)            = delete;
            MonotonicArena& operator=(const MonotonicArena&) = delete;

            MonotonicArena(MonotonicArena&& o) noexcept
                : blocks_{std::move(o.blocks_)}, cur_{o.cur_}, pos_{o.pos_} {
                o.cur_ = 0;
                o.pos_ = 0;
            }

            MonotonicArena& operator=(MonotonicArena&& o) noexcept {
                if (this != &o) {
                    free_blocks();
                    blocks_ = std::move(o.blocks_);
                    cur_    = o.cur_;
                    pos_    = o.pos_;
                    o.cur_  = 0;
                    o.pos_  = 0;
                }
                return *this;
            }

            /**
             * Bump-allocate `size` bytes aligned to `align` (must be power-of-2).
             *
             * Fast path (no new block needed): ~3–5 ns
             *   - align_up + bounds check + pointer bump
             *
             * Slow path (new block): ~50–100 ns (_aligned_malloc / posix_memalign)
             *   - Occurs at most once per ~8 leaf insertions (256 KB / 32 KB per node)
             *   - Amortized over LEAF_ORDER inserts ≈ 0.1–0.2 ns per insert
             *
             * @throws std::bad_alloc on OS allocation failure.
             */
            [[nodiscard]] void* allocate(size_t size, size_t align = alignof(std::max_align_t)) {
                // Fast path: enough room in the current block.
                if (!blocks_.empty()) {
                    const size_t off = align_up(pos_, align);
                    if (off + size <= BLOCK_SIZE) {
                        pos_ = off + size;
                        return static_cast<uint8_t*>(blocks_[cur_]) + off;
                    }
                }
                // Slow path: allocate a new block (oversized if size > BLOCK_SIZE).
                push_block(std::max(size, BLOCK_SIZE));
                // After push_block: pos_ == 0, cur_ == index of new block.
                // Blocks are 64-byte aligned by the OS allocator, so align_up(0, align) == 0
                // for any align ≤ 64 (all BPTree nodes satisfy this).
                const size_t off = align_up(pos_, align);
                pos_ = off + size;
                return static_cast<uint8_t*>(blocks_[cur_]) + off;
            }

            /**
             * Rewinds all position counters to zero without freeing blocks.
             * Existing blocks are retained for reuse on the next allocation cycle.
             *
             * IMPORTANT: The caller must have already destroyed all objects whose
             * memory lives in this arena before calling reset(). Destructors are
             * NOT called by reset() — that is the responsibility of destroy_tree().
             */
            void reset() noexcept {
                cur_ = 0;
                pos_ = 0;
            }

            [[nodiscard]] bool empty() const noexcept { return blocks_.empty(); }

        private:
            static size_t align_up(size_t v, size_t a) noexcept {
                return (v + a - 1u) & ~(a - 1u);
            }

            void push_block(size_t sz) {
#ifdef _WIN32
                void* p = _aligned_malloc(sz, 64u);
                if (!p) throw std::bad_alloc{};
#else
                void* p = nullptr;
                if (::posix_memalign(&p, 64u, sz) != 0 || !p) throw std::bad_alloc{};
#endif
                blocks_.push_back(p);
                cur_ = blocks_.size() - 1u;
                pos_ = 0;
            }

            void free_blocks() noexcept {
                for (void* p : blocks_) {
#ifdef _WIN32
                    _aligned_free(p);
#else
                    ::free(p);
#endif
                }
                blocks_.clear();
                cur_ = 0;
                pos_ = 0;
            }

            std::vector<void*> blocks_;
            size_t cur_ = 0; ///< Index of the current (active) block in blocks_.
            size_t pos_ = 0; ///< Write offset within blocks_[cur_].
    };
} // namespace akkaradb::core
