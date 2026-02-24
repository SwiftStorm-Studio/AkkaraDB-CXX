/*
 * AkkaraDB - Low-latency, crash-safe JVM KV store with WAL & stripe parity
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

// internal/src/engine/memtable/MemTable.cpp
#include "engine/memtable/MemTable.hpp"
#include "engine/memtable/BPTree.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <queue>
#include <ranges>
#include <shared_mutex>
#include <stdexcept>
#include <thread>

namespace akkaradb::engine::memtable {
    // ============================================================================
    // Helpers
    // ============================================================================

    namespace {
        /**
         * Selects shard index from a pre-computed key_fp64.
         * shard_count must be a power of 2.
         * Identical routing to WAL's shard_for(): shard = fp64 & (count - 1).
         */
        [[nodiscard]] inline uint32_t shard_for(uint64_t key_fp64, uint32_t shard_count) noexcept {
            return static_cast<uint32_t>(key_fp64 & (shard_count - 1u));
        }

        /**
         * Rounds n up to the next power of 2, clamped to [2, 16].
         */
        [[nodiscard]] uint32_t resolve_shard_count(size_t n) {
            if (n == 0) throw std::invalid_argument("MemTable: shard_count must be >= 1");
            if (n == 1) return 2; // minimum parallelism
            uint32_t p = 2;
            while (p < static_cast<uint32_t>(n)) p <<= 1;
            return std::min(p, 16u);
        }

        // ── Transparent comparator ────────────────────────────────────────────

        struct MemRecordCmp {
            using is_transparent = void;

            bool operator()(const core::MemRecord& a, const core::MemRecord& b) const noexcept { return a.compare_key(b) < 0; }
            bool operator()(const core::MemRecord& a, std::span<const uint8_t> b) const noexcept { return a.compare_key(b) < 0; }
            bool operator()(std::span<const uint8_t> a, const core::MemRecord& b) const noexcept { return b.compare_key(a) > 0; }
        };
    } // anonymous namespace

    // ============================================================================
    // Map alias
    // ============================================================================

    // BPTree keyed by MemRecord, value = std::monostate (key-only set).
    // MemRecordCmp is transparent so lower_bound(span<const uint8_t>) works directly.
    using Map = BPTree<core::MemRecord, std::monostate, MemRecordCmp>;

    // ============================================================================
    // Shard
    // ============================================================================

    /**
     * Shard - Single shard: one active map + zero or more immutable maps.
     *
     * Immutables are held as shared_ptr<const Map> so readers and the flusher
     * thread share the same allocation without any record copying.
     *
     * seal_active() moves the active map into a new shared_ptr and records an
     * immutable entry. The flusher receives the same shared_ptr and sorts/calls
     * the callback. on_flushed() drops the immutable entry.
     */
    class Shard {
        public:
            Shard() : approx_bytes_{0}, next_imm_id_{0}, threshold_bytes_{0} {}

            void set_threshold(size_t t) noexcept { threshold_bytes_ = t; }

            // ── Write ─────────────────────────────────────────────────────────

            /**
             * Inserts or replaces a record.
             * Returns true if the shard is now over the flush threshold.
             */
            bool put(core::MemRecord record) {
                std::unique_lock lock{mutex_};

                const size_t new_size = record.approx_size();

                // If key already exists, subtract its old approx_size before overwriting.
                // BPTree::put() handles the in-place update; we only need the size delta.
                auto it = active_.lower_bound(record.key());
                if (!it.is_end() && it->first.compare_key(record.key()) == 0) { approx_bytes_ -= it->first.approx_size(); }

                active_.put(record, std::monostate{});
                approx_bytes_ += new_size;

                return threshold_bytes_ > 0 && approx_bytes_ > threshold_bytes_;
            }

            // ── Read ──────────────────────────────────────────────────────────

            [[nodiscard]] std::shared_ptr<const core::MemRecord>
            get(std::span<const uint8_t> key) const {
                std::shared_lock lock{mutex_};

                // Active map first
                auto it = active_.lower_bound(key);
                if (!it.is_end() && it->first.compare_key(key) == 0) { return std::make_shared<const core::MemRecord>(it->first); }

                // Immutables: newest to oldest (highest id = last in deque)
                for (auto rit = immutables_.rbegin(); rit != immutables_.rend(); ++rit) {
                    const auto& map = *rit->second;
                    auto it2 = map.lower_bound(key);
                    if (!it2.is_end() && it2->first.compare_key(key) == 0) { return std::make_shared<const core::MemRecord>(it2->first); }
                }

                return nullptr;
            }

            // ── Flush lifecycle ───────────────────────────────────────────────

            /**
             * Seals the active map and moves it to immutables.
             * Returns {id, shared_ptr<const Map>}. If active is empty, returns {0, nullptr}.
             *
             * No records are copied: the flusher receives the same shared_ptr.
             */
            [[nodiscard]] std::pair<uint64_t, std::shared_ptr<const Map>> seal_active() {
                std::unique_lock lock{mutex_};
                if (active_.empty()) return {0, nullptr};

                const uint64_t id = next_imm_id_++;

                // BPTree is movable: transfer active_ into a new heap-allocated Map.
                auto imm = std::make_shared<Map>(std::move(active_));
                // active_ is now in a valid-but-empty state after move.
                approx_bytes_ = 0;

                // Store as shared_ptr<const Map> for read-only sharing with flusher.
                auto imm_const = std::shared_ptr<const Map>(imm);
                immutables_.emplace_back(id, imm_const);
                return {id, imm_const};
            }

            void on_flushed(uint64_t id) {
                std::unique_lock lock{mutex_};
                std::erase_if(immutables_, [id](const auto& p) { return p.first == id; });
            }

            // ── Range ─────────────────────────────────────────────────────────

            /**
             * Collects all maps (active + immutables) that overlap [start, end).
             * Returns shared_ptrs so callers hold the maps alive without copying.
             * The first entry is always the active map snapshot.
             */
            [[nodiscard]] std::vector<std::shared_ptr<const Map>>
            snapshot_maps() const {
                std::shared_lock lock{mutex_};
                std::vector<std::shared_ptr<const Map>> result;
                result.reserve(1 + immutables_.size());

                // Snapshot active: BPTree is non-copyable, so rebuild into a new tree.
                // This is only used for range scans (read path), not the flush hot path.
                if (!active_.empty()) {
                    auto snap = std::make_shared<Map>();
                    for (auto it = active_.begin(); !it.is_end(); ++it) { snap->put(it->first, it->second); }
                    result.push_back(std::move(snap));
                }
                for (const auto& [id, map] : immutables_) { result.push_back(map); }
                return result;
            }

            // ── Metrics ───────────────────────────────────────────────────────

            [[nodiscard]] size_t approx_bytes() const noexcept {
                std::shared_lock lock{mutex_};
                return approx_bytes_;
            }

        private:
            mutable std::shared_mutex mutex_;
            Map active_;
            std::deque<std::pair<uint64_t, std::shared_ptr<const Map>>> immutables_;
            size_t approx_bytes_;
            uint64_t next_imm_id_;
            size_t threshold_bytes_;
    };

    // ============================================================================
    // Flusher - one per shard
    // ============================================================================

    /**
     * Flusher - Background thread that consumes sealed Maps, sorts them,
     * calls FlushCallback, then notifies the shard via on_done.
     *
     * One Flusher per Shard: no cross-shard serialization.
     * The flusher receives a shared_ptr<const Map> (zero record copies).
     * Sorting happens inside the flusher thread to avoid blocking writers.
     */
    class Flusher {
        public:
            using DoneCallback = std::function<void(uint64_t id)>;

            explicit Flusher(MemTable::FlushCallback callback, DoneCallback on_done)
                : callback_{std::move(callback)}, on_done_{std::move(on_done)}, running_{true} { thread_ = std::thread([this] { run(); }); }

            ~Flusher() {
                {
                    std::unique_lock lock{mutex_};
                    running_ = false;
                }
                cv_.notify_one();
                if (thread_.joinable()) thread_.join();
            }

            Flusher(const Flusher&) = delete;
            Flusher& operator=(const Flusher&) = delete;

            void enqueue(uint64_t id, std::shared_ptr<const Map> map) {
                {
                    std::unique_lock lock{mutex_};
                    queue_.push(Item{id, std::move(map)});
                }
                cv_.notify_one();
            }

            /**
             * Blocks until the queue is empty and no callback is executing.
             */
            void drain() {
                std::unique_lock lock{mutex_};
                cv_.wait(lock, [this] { return queue_.empty() && in_flight_ == 0; });
            }

        private:
            struct Item {
                uint64_t id;
                std::shared_ptr<const Map> map;
            };

            void run() {
                while (true) {
                    std::unique_lock lock{mutex_};
                    cv_.wait(lock, [this] { return !queue_.empty() || !running_; });
                    if (!running_ && queue_.empty()) break;

                    if (!queue_.empty()) {
                        auto item = std::move(queue_.front());
                        queue_.pop();
                        ++in_flight_;
                        lock.unlock();

                        // Build sorted vector from the immutable BPTree.
                        // BPTree::Iterator::operator*() returns pair<const K&, const V&>
                        // (references into the node), so we copy the key (MemRecord) explicitly.
                        // The BPTree is already key-sorted; no additional sort needed.
                        std::vector<core::MemRecord> batch;
                        batch.reserve(item.map->size());
                        for (auto it = item.map->begin(); !it.is_end(); ++it) {
                            batch.push_back(it->first); // copy MemRecord from reference
                        }

                        if (callback_) callback_(std::move(batch));
                        if (on_done_) on_done_(item.id);

                        {
                            std::unique_lock done_lock{mutex_};
                            --in_flight_;
                        }
                        cv_.notify_all(); // wake drain()
                    }
                }
            }

            MemTable::FlushCallback callback_;
            DoneCallback on_done_;
            std::thread thread_;
            std::mutex mutex_;
            std::condition_variable cv_;
            std::queue<Item> queue_;
            bool running_;
            int in_flight_{0};
    };

    // ============================================================================
    // RangeIterator::Impl - k-way merge
    // ============================================================================

    class MemTable::RangeIterator::Impl {
        public:
            /**
             * @param maps    Snapshots of all shard maps (each already key-sorted)
             * @param range   Key range [start, end)
             */
            Impl(std::vector<std::shared_ptr<const Map>> maps, const MemTable::KeyRange& range)
                : maps_{std::move(maps)}, range_{range} {
                const auto start_span = std::span<const uint8_t>{range_.start};

                for (size_t i = 0; i < maps_.size(); ++i) {
                    const auto& m = *maps_[i];
                    auto it = range_.start.empty()
                                  ? m.begin()
                                  : m.lower_bound(start_span);
                    iters_.push_back(it);
                    if (!it.is_end() && !past_end(it->first)) { pq_.push(static_cast<uint32_t>(i)); }
                }

                prefetch();
            }

            [[nodiscard]] bool has_next() const noexcept { return next_.has_value(); }

            [[nodiscard]] std::optional<core::MemRecord> next() {
                if (!next_) return std::nullopt;
                auto result = std::move(*next_);
                next_ = std::nullopt;
                prefetch();
                return result;
            }

        private:
            [[nodiscard]] bool past_end(const core::MemRecord& rec) const noexcept {
                if (range_.end.empty()) return false;
                return !std::ranges::lexicographical_compare(rec.key(), range_.end);
            }

            struct PqCmp {
                const std::vector<Map::Iterator>* iters;

                bool operator()(uint32_t a, uint32_t b) const {
                    const auto& ra = (*iters)[a]->first;
                    const auto& rb = (*iters)[b]->first;
                    const int cmp = ra.compare_key(rb);
                    if (cmp != 0) return cmp > 0; // min-heap by key
                    return ra.seq() < rb.seq(); // tie-break: higher seq wins
                }
            };

            void prefetch() {
                while (!pq_.empty()) {
                    const uint32_t si = pq_.top();
                    pq_.pop();

                    auto& it = iters_[si];
                    core::MemRecord rec = it->first; // copy from reference pair
                    ++it;

                    // Re-push if still in range
                    if (!it.is_end() && !past_end(it->first)) { pq_.push(si); }

                    // Drain heap entries with the same key (keep highest seq)
                    while (!pq_.empty()) {
                        const uint32_t ni = pq_.top();
                        const auto& nr = iters_[ni]->first;
                        if (nr.compare_key(rec.key()) != 0) break;
                        // Same key: keep the one with higher seq
                        if (nr.seq() > rec.seq()) rec = nr;
                        pq_.pop();
                        auto& nit = iters_[ni];
                        ++nit;
                        if (!nit.is_end() && !past_end(nit->first)) { pq_.push(ni); }
                    }

                    // Skip if this key was already yielded
                    auto key_vec = std::vector<uint8_t>(rec.key().begin(), rec.key().end());
                    if (key_vec == last_key_) continue;

                    last_key_ = std::move(key_vec);
                    next_ = std::move(rec);
                    return;
                }
            }

            std::vector<std::shared_ptr<const Map>> maps_;
            MemTable::KeyRange range_;
            std::vector<Map::Iterator> iters_;

            std::priority_queue<uint32_t, std::vector<uint32_t>, PqCmp> pq_{PqCmp{&iters_}};

            std::optional<core::MemRecord> next_;
            std::vector<uint8_t> last_key_;
    };

    MemTable::RangeIterator::RangeIterator(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    MemTable::RangeIterator::~RangeIterator() = default;
    MemTable::RangeIterator::RangeIterator(RangeIterator&&) noexcept = default;
    MemTable::RangeIterator& MemTable::RangeIterator::operator=(RangeIterator&&) noexcept = default;

    bool MemTable::RangeIterator::has_next() const noexcept { return impl_ && impl_->has_next(); }

    std::optional<core::MemRecord> MemTable::RangeIterator::next() {
        if (!impl_) return std::nullopt;
        return impl_->next();
    }

    // ============================================================================
    // MemTable::Impl
    // ============================================================================

    class MemTable::Impl {
        public:
            Impl(size_t shard_count_req, size_t threshold_bytes_per_shard, FlushCallback on_flush)
                : shard_count_{resolve_shard_count(shard_count_req)}, threshold_bytes_per_shard_{threshold_bytes_per_shard}, seq_gen_{0} {
                shards_.reserve(shard_count_);
                flushers_.reserve(shard_count_);

                for (uint32_t i = 0; i < shard_count_; ++i) {
                    auto shard = std::make_unique<Shard>();
                    shard->set_threshold(threshold_bytes_per_shard_);
                    shards_.push_back(std::move(shard));
                }

                // Flushers created separately so on_flush capture is correct
                if (on_flush) { for (uint32_t i = 0; i < shard_count_; ++i) { make_flusher(i, on_flush); } }
            }

            ~Impl() {
                // Flushers join their threads in destructor; destroy before shards.
                flushers_.clear();
            }

            // ── Write path ────────────────────────────────────────────────────

            void put(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq) {
                const uint64_t fp64 = core::AKHdr32::compute_key_fp64(key.data(), key.size());
                const uint32_t si = shard_for(fp64, shard_count_);

                auto record = core::MemRecord::create(key, value, seq);
                advance_seq_gen(seq);

                if (shards_[si]->put(std::move(record))) { trigger_flush(si); }
            }

            void remove(std::span<const uint8_t> key, uint64_t seq) {
                const uint64_t fp64 = core::AKHdr32::compute_key_fp64(key.data(), key.size());
                const uint32_t si = shard_for(fp64, shard_count_);

                auto record = core::MemRecord::tombstone(key, seq);
                advance_seq_gen(seq);

                if (shards_[si]->put(std::move(record))) { trigger_flush(si); }
            }

            // ── Read path ─────────────────────────────────────────────────────

            [[nodiscard]] std::shared_ptr<const core::MemRecord>
            get(std::span<const uint8_t> key) const {
                const uint64_t fp64 = core::AKHdr32::compute_key_fp64(key.data(), key.size());
                const uint32_t si = shard_for(fp64, shard_count_);
                return shards_[si]->get(key);
            }

            [[nodiscard]] RangeIterator iterator(const KeyRange& range) const {
                // Collect snapshots from all shards
                std::vector<std::shared_ptr<const Map>> all_maps;
                for (const auto& shard : shards_) { for (auto& m : shard->snapshot_maps()) { all_maps.push_back(std::move(m)); } }
                return RangeIterator{std::make_unique<RangeIterator::Impl>(std::move(all_maps), range)};
            }

            // ── Sequence ──────────────────────────────────────────────────────

            [[nodiscard]] uint64_t next_seq() noexcept { return seq_gen_.fetch_add(1, std::memory_order_relaxed); }

            [[nodiscard]] uint64_t last_seq() const noexcept { return seq_gen_.load(std::memory_order_relaxed); }

            // ── Flush control ─────────────────────────────────────────────────

            void flush_hint() {
                for (uint32_t i = 0; i < shard_count_; ++i) { if (shards_[i]->approx_bytes() > threshold_bytes_per_shard_) { trigger_flush(i); } }
            }

            void force_flush() {
                for (uint32_t i = 0; i < shard_count_; ++i) trigger_flush(i);
                for (auto& f : flushers_) { if (f) f->drain(); }
            }

            void set_flush_callback(FlushCallback cb) {
                // Drain existing flushers first to avoid losing in-flight batches.
                for (auto& f : flushers_) { if (f) f->drain(); }
                flushers_.clear();
                flushers_.resize(shard_count_);

                if (cb) { for (uint32_t i = 0; i < shard_count_; ++i) { make_flusher(i, cb); } }
            }

            // ── Metrics ───────────────────────────────────────────────────────

            [[nodiscard]] size_t approx_size() const noexcept {
                size_t total = 0;
                for (const auto& s : shards_) total += s->approx_bytes();
                return total;
            }

        private:
            // ── Helpers ───────────────────────────────────────────────────────

            /**
             * Advances seq_gen_ to max(current, observed_seq + 1).
             * Used during WAL recovery (explicit seq values) to leave the counter
             * correctly positioned past the highest recovered sequence number.
             */
            void advance_seq_gen(uint64_t observed_seq) noexcept {
                uint64_t cur = seq_gen_.load(std::memory_order_relaxed);
                while (cur <= observed_seq) {
                    if (seq_gen_.compare_exchange_weak(cur, observed_seq + 1, std::memory_order_relaxed, std::memory_order_relaxed)) { break; }
                    // cur refreshed by CAS failure; loop retries
                }
            }

            void trigger_flush(uint32_t si) {
                if (!flushers_[si]) return;
                auto [id, map] = shards_[si]->seal_active();
                if (map) flushers_[si]->enqueue(id, std::move(map));
            }

            void make_flusher(uint32_t i, const FlushCallback& cb) {
                flushers_[i] = std::make_unique<Flusher>(cb, [this, i](uint64_t id) { shards_[i]->on_flushed(id); });
            }

            // ── Members ───────────────────────────────────────────────────────

            uint32_t shard_count_;
            size_t threshold_bytes_per_shard_;

            std::vector<std::unique_ptr<Shard>> shards_;
            std::vector<std::unique_ptr<Flusher>> flushers_;

            std::atomic<uint64_t> seq_gen_;
    };

    // ============================================================================
    // MemTable public API
    // ============================================================================

    std::unique_ptr<MemTable> MemTable::create(size_t shard_count, size_t threshold_bytes_per_shard, FlushCallback on_flush) {
        return std::unique_ptr<MemTable>(new MemTable(shard_count, threshold_bytes_per_shard, std::move(on_flush)));
    }

    MemTable::MemTable(size_t sc, size_t tb, FlushCallback of) : impl_{std::make_unique<Impl>(sc, tb, std::move(of))} {}

    MemTable::~MemTable() = default;

    void MemTable::put(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq) { impl_->put(key, value, seq); }
    void MemTable::remove(std::span<const uint8_t> key, uint64_t seq) { impl_->remove(key, seq); }

    std::shared_ptr<const core::MemRecord> MemTable::get(std::span<const uint8_t> key) const { return impl_->get(key); }
    MemTable::RangeIterator MemTable::iterator(const KeyRange& range) const { return impl_->iterator(range); }

    uint64_t MemTable::next_seq() noexcept { return impl_->next_seq(); }
    uint64_t MemTable::last_seq() const noexcept { return impl_->last_seq(); }
    void MemTable::flush_hint() { impl_->flush_hint(); }
    void MemTable::force_flush() { impl_->force_flush(); }
    size_t MemTable::approx_size() const noexcept { return impl_->approx_size(); }
    void MemTable::set_flush_callback(FlushCallback cb) { impl_->set_flush_callback(std::move(cb)); }
} // namespace akkaradb::engine::memtable
