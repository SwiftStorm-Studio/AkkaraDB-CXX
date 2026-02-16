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

// internal/src/core/buffer/PerThreadArena.cpp
#include "core/buffer/PerThreadArena.hpp"

#include <vector>
#include <stdexcept>

// Platform-specific includes for arena allocation
#if defined(__linux__) || defined(__APPLE__) || defined(__unix__)
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace akkaradb::core {
    namespace {
        // ============================================================================
        // Configuration Constants
        // ============================================================================

        /// Batch size for opportunistic zero-fill (number of buffers to clean at once)
        constexpr size_t BATCH_CLEAN_SIZE = 4;

        /// Minimum dirty buffers before triggering batch clean
        constexpr size_t DIRTY_THRESHOLD = 8;

        /// Minimum clean buffers to maintain (trigger batch clean if below)
        constexpr size_t CLEAN_LOW_WATERMARK = 4;

        // ============================================================================
        // Arena Allocator
        // ============================================================================

        /**
         * ArenaBlock - Contiguous memory region for bump allocation.
         *
         * Allocates a large block upfront and distributes fixed-size buffers
         * via bump pointer increment. Zero synchronization overhead.
         */
        class ArenaBlock {
            public:
                ArenaBlock(size_t block_size, size_t alignment, size_t capacity)
                    : block_size_{block_size}, capacity_{capacity} {
                    const size_t total_size = block_size * capacity;

                    #if defined(__linux__) || defined(__APPLE__) || defined(__unix__)
                    // Use mmap for large page support
                    base_ = mmap(nullptr, total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0); if (base_ == MAP_FAILED) {
                        throw std::bad_alloc{};
                    }

                    #ifdef __linux__
                    // Advise kernel to use transparent huge pages for better TLB performance
                    madvise(base_, total_size, MADV_HUGEPAGE);
                    #endif

                    #else
                    // Fallback to aligned_alloc for non-POSIX systems
                    base_ = OwnedBuffer::allocate(total_size, alignment).release();
                    if (!base_) { throw std::bad_alloc{}; }
                    #endif

                    current_ = static_cast<std::byte*>(base_);
                    end_ = current_ + total_size;
                }

                ~ArenaBlock() noexcept {
                    if (!base_) { return; }

                    #if defined(__linux__) || defined(__APPLE__) || defined(__unix__)
                    if (base_ != MAP_FAILED) {
                        const size_t total_size = block_size_ * capacity_;
                        munmap(base_, total_size);
                    }
                    #else
                    // For aligned_alloc fallback (Windows, etc)
                    // MAP_FAILED doesn't exist here, just free
                    std::free(base_);
                    #endif
                }

                // Non-copyable, non-movable
                ArenaBlock(const ArenaBlock&) = delete;
                ArenaBlock& operator=(const ArenaBlock&) = delete;
                ArenaBlock(ArenaBlock&&) = delete;
                ArenaBlock& operator=(ArenaBlock&&) = delete;

                /**
                 * Attempts to allocate one buffer from the arena.
                 * @return Pointer to buffer, or nullptr if exhausted
                 */
                [[nodiscard]] std::byte* try_allocate() noexcept {
                    std::byte* ptr = current_;
                    std::byte* next = ptr + block_size_;

                    if (next > end_) {
                        return nullptr; // Arena exhausted
                    }

                    current_ = next;
                    return ptr;
                }

                /**
                 * Checks if arena has capacity for at least one more buffer.
                 */
                [[nodiscard]] bool has_capacity() const noexcept { return (current_ + block_size_) <= end_; }

                /**
                 * Returns number of buffers allocated from this arena.
                 */
                [[nodiscard]] size_t allocated_count() const noexcept { return (current_ - static_cast<std::byte*>(base_)) / block_size_; }

            private:
                size_t block_size_;
                size_t capacity_;
                void* base_;
                std::byte* current_;
                std::byte* end_;
        };

        // ============================================================================
        // Thread-Local State
        // ============================================================================

        /**
         * ThreadLocalState - Per-thread allocation state.
         *
         * Each thread maintains:
         * - clean_list: Zero-filled buffers ready for immediate use
         * - dirty_list: Returned buffers awaiting zero-fill
         * - arena: Bump allocator for fresh allocations
         */
        struct ThreadLocalState {
            std::vector<OwnedBuffer> clean_list;
            std::vector<OwnedBuffer> dirty_list;
            std::unique_ptr<ArenaBlock> arena;

            size_t block_size;
            size_t alignment;
            size_t arena_capacity;
            size_t clean_capacity;
            size_t dirty_capacity;

            ThreadLocalState(size_t bs, size_t align, size_t ac, size_t cc, size_t dc)
                : block_size{bs}, alignment{align}, arena_capacity{ac}, clean_capacity{cc}, dirty_capacity{dc} {
                clean_list.reserve(cc);
                dirty_list.reserve(dc);
                // Arena is lazily allocated on first acquire
            }

            /**
             * Ensures arena is allocated.
             */
            void ensure_arena() { if (!arena) { arena = std::make_unique<ArenaBlock>(block_size, alignment, arena_capacity); } }

            /**
             * Attempts to allocate from arena.
             * @return Pointer to buffer, or nullptr if arena exhausted
             */
            [[nodiscard]] std::byte* try_allocate_from_arena() {
                ensure_arena();
                return arena->try_allocate();
            }

            /**
             * Performs batch zero-fill of dirty buffers.
             * @param count Number of buffers to clean (0 = clean all)
             */
            void batch_zero_fill(size_t count = 0) {
                if (dirty_list.empty()) { return; }

                const size_t to_clean = (count == 0)
                                            ? dirty_list.size()
                                            : std::min(count, dirty_list.size());

                for (size_t i = 0; i < to_clean; ++i) {
                    auto& buf = dirty_list.back();
                    buf.zero_fill();
                    clean_list.push_back(std::move(buf));
                    dirty_list.pop_back();
                }
            }

            /**
             * Checks if opportunistic batch cleaning should be triggered.
             */
            [[nodiscard]] bool should_batch_clean() const noexcept { return dirty_list.size() >= DIRTY_THRESHOLD && clean_list.size() < CLEAN_LOW_WATERMARK; }
        };
    } // anonymous namespace

    // ============================================================================
    // PerThreadArena::Impl
    // ============================================================================

    class PerThreadArena::Impl {
        public:
            Impl(size_t block_size, size_t alignment, size_t arena_capacity, size_t clean_capacity, size_t dirty_capacity, PerThreadArena* parent)
                : block_size_{block_size},
                  alignment_{alignment},
                  arena_capacity_{arena_capacity},
                  clean_capacity_{clean_capacity},
                  dirty_capacity_{dirty_capacity},
                  parent_{parent} {}

            /**
             * Gets or creates the thread-local state for current thread.
             */
            [[nodiscard]] ThreadLocalState& get_tls() noexcept {
                thread_local ThreadLocalState state{block_size_, alignment_, arena_capacity_, clean_capacity_, dirty_capacity_};
                return state;
            }

            [[nodiscard]] OwnedBuffer acquire(bool skip_zero_fill) {
                auto& tls = get_tls();

                parent_->stats_total_acquired_.fetch_add(1, std::memory_order_relaxed);

                // Fast path: Clean pool hit
                if (!tls.clean_list.empty()) {
                    parent_->stats_clean_hits_.fetch_add(1, std::memory_order_relaxed);

                    auto buf = std::move(tls.clean_list.back());
                    tls.clean_list.pop_back();

                    // Clean buffers are already zero-filled
                    if (!skip_zero_fill && buf.data()) {
                        // Already clean, but re-zero if requested for security
                        buf.zero_fill();
                    }

                    return buf;
                }

                // Medium path: Batch clean dirty buffers
                if (!tls.dirty_list.empty()) {
                    parent_->stats_dirty_hits_.fetch_add(1, std::memory_order_relaxed);
                    parent_->stats_batch_cleanings_.fetch_add(1, std::memory_order_relaxed);

                    tls.batch_zero_fill(BATCH_CLEAN_SIZE);

                    auto buf = std::move(tls.clean_list.back());
                    tls.clean_list.pop_back();

                    if (skip_zero_fill) {
                        // Skip zero-fill optimization (caller will overwrite)
                    }

                    return buf;
                }

                // Slow path: Allocate from arena
                if (auto* ptr = tls.try_allocate_from_arena(); ptr != nullptr) {
                    parent_->stats_arena_allocs_.fetch_add(1, std::memory_order_relaxed);

                    // Create OwnedBuffer without taking ownership of arena memory
                    // Note: This is a special case - the buffer's memory is owned by the arena
                    // We use a custom deleter that does nothing
                    auto buf = create_arena_buffer(ptr, block_size_);

                    if (!skip_zero_fill) { buf.zero_fill(); }

                    return buf;
                }

                // Cold path: Arena exhausted, fallback to aligned_alloc
                parent_->stats_fallback_allocs_.fetch_add(1, std::memory_order_relaxed);

                auto buf = OwnedBuffer::allocate(block_size_, alignment_);

                if (!skip_zero_fill) { buf.zero_fill(); }

                return buf;
            }

            void release(OwnedBuffer&& buffer) noexcept {
                if (buffer.empty()) { return; }

                auto& tls = get_tls();

                parent_->stats_total_released_.fetch_add(1, std::memory_order_relaxed);

                // Push to dirty queue
                if (tls.dirty_list.size() < tls.dirty_capacity) { tls.dirty_list.push_back(std::move(buffer)); }
                else {
                    // Dirty queue full, push to clean after zero-fill
                    buffer.zero_fill();
                    if (tls.clean_list.size() < tls.clean_capacity) { tls.clean_list.push_back(std::move(buffer)); }
                    // else: buffer is dropped (destructor will free)
                }

                // Opportunistic batch cleaning
                if (tls.should_batch_clean()) {
                    parent_->stats_batch_cleanings_.fetch_add(1, std::memory_order_relaxed);
                    tls.batch_zero_fill(BATCH_CLEAN_SIZE);
                }
            }

            void force_clean_all() noexcept {
                auto& tls = get_tls();
                if (!tls.dirty_list.empty()) {
                    parent_->stats_batch_cleanings_.fetch_add(1, std::memory_order_relaxed);
                    tls.batch_zero_fill(0); // Clean all
                }
            }

            [[nodiscard]] size_t current_clean() const noexcept {
                // Note: This is an approximation - can't easily sum across all threads
                // Returning 0 as placeholder (could be improved with TLS registry)
                return 0;
            }

            [[nodiscard]] size_t current_dirty() const noexcept {
                // Same limitation as current_clean()
                return 0;
            }

        private:
            size_t block_size_;
            size_t alignment_;
            size_t arena_capacity_;
            size_t clean_capacity_;
            size_t dirty_capacity_;
            PerThreadArena* parent_;

            /**
             * Creates an OwnedBuffer from arena-allocated memory.
             *
             * The buffer doesn't own the memory (arena does).
             * Uses wrap_non_owning() to prevent deallocation.
             */
            [[nodiscard]] static OwnedBuffer create_arena_buffer(std::byte* ptr, size_t size) { return OwnedBuffer::wrap_non_owning(ptr, size); }
    };

    // ============================================================================
    // PerThreadArena Public API
    // ============================================================================

    std::unique_ptr<PerThreadArena> PerThreadArena::create(
        size_t block_size,
        size_t alignment,
        size_t arena_capacity,
        size_t clean_capacity,
        size_t dirty_capacity
    ) {
        if (block_size == 0) { throw std::invalid_argument("PerThreadArena: block_size must be > 0"); }

        if (alignment == 0 || (alignment & (alignment - 1)) != 0) { throw std::invalid_argument("PerThreadArena: alignment must be power of 2"); }

        return std::unique_ptr<PerThreadArena>(new PerThreadArena(block_size, alignment, arena_capacity, clean_capacity, dirty_capacity));
    }

    PerThreadArena::PerThreadArena(size_t block_size, size_t alignment, size_t arena_capacity, size_t clean_capacity, size_t dirty_capacity)
        : block_size_{block_size}, alignment_{alignment}, arena_capacity_{arena_capacity}, clean_capacity_{clean_capacity}, dirty_capacity_{dirty_capacity} {
        impl_ = std::make_unique<Impl>(block_size, alignment, arena_capacity, clean_capacity, dirty_capacity, this);
    }

    PerThreadArena::~PerThreadArena() = default;

    OwnedBuffer PerThreadArena::acquire(bool skip_zero_fill) { return impl_->acquire(skip_zero_fill); }

    void PerThreadArena::release(OwnedBuffer&& buffer) noexcept { impl_->release(std::move(buffer)); }

    PerThreadArena::Stats PerThreadArena::stats() const noexcept {
        Stats s;
        s.total_acquired = stats_total_acquired_.load(std::memory_order_relaxed);
        s.total_released = stats_total_released_.load(std::memory_order_relaxed);
        s.clean_hits = stats_clean_hits_.load(std::memory_order_relaxed);
        s.dirty_hits = stats_dirty_hits_.load(std::memory_order_relaxed);
        s.arena_allocs = stats_arena_allocs_.load(std::memory_order_relaxed);
        s.fallback_allocs = stats_fallback_allocs_.load(std::memory_order_relaxed);
        s.batch_cleanings = stats_batch_cleanings_.load(std::memory_order_relaxed);
        s.current_clean = impl_->current_clean();
        s.current_dirty = impl_->current_dirty();
        return s;
    }

    size_t PerThreadArena::block_size() const noexcept { return block_size_; }

    size_t PerThreadArena::alignment() const noexcept { return alignment_; }

    void PerThreadArena::force_clean_all() noexcept { impl_->force_clean_all(); }
} // namespace akkaradb::core
