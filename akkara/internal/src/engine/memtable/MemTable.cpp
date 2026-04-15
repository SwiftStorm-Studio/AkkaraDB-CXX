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

// internal/src/engine/memtable/MemTable.cpp
#include "engine/memtable/MemTable.hpp"
#include "engine/memtable/BPTreeMap.hpp"
#include "engine/memtable/SkipListMap.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <queue>
#include <ranges>
#include <shared_mutex>
#include <thread>
#include <utility>

namespace akkaradb::engine::memtable {
    // ============================================================================
    // Helpers
    // ============================================================================

    namespace {
        [[nodiscard]] uint32_t shard_for(uint64_t key_fp64, uint32_t shard_count) noexcept {
            return static_cast<uint32_t>(key_fp64 & (shard_count - 1u));
        }

        [[nodiscard]] uint32_t resolve_shard_count(size_t n) {
            const bool is_auto = (n == 0);
            if (n == 0) n = std::max(2u, std::thread::hardware_concurrency());
            if (n == 1) return 2;
            uint32_t p = 2;
            while (p < static_cast<uint32_t>(n)) p <<= 1;
            return std::min(p, is_auto ? 16u : 256u);
        }

        /// Factory: create an empty IMemMap of the requested backend type.
        std::unique_ptr<IMemMap> make_map(MemTable::Backend backend) {
            switch (backend) {
                case MemTable::Backend::SkipList: return std::make_unique<SkipListMap>();
                case MemTable::Backend::BPTree: [[fallthrough]];
                default: return std::make_unique<BPTreeMap>();
            }
        }
    } // anonymous namespace

    // ============================================================================
    // Immutable snapshot type
    // ============================================================================

    // Immutable snapshots are stored as sorted vectors of MemRecord.
    // Immutables are produced by seal_active() (collect_sorted) and consumed by:
    //   - Shard::get() / get_into(): binary search
    //   - RangeIterator::Impl: forward scan from a lower_bound position
    //   - Flusher: iterates and calls the FlushCallback
    using ImmVec = std::vector<core::MemRecord>;

    struct SealResult {
        uint64_t id = 0;
        std::shared_ptr<const ImmVec> vec;
        size_t bytes = 0; ///< approx_bytes captured before reset
    };

    // ============================================================================
    // Shard
    // ============================================================================

    class Shard {
        public:
            Shard() : next_imm_id_{0}, threshold_bytes_{0} {}

            void init(size_t threshold, MemTable::Backend backend) {
                threshold_bytes_ = threshold;
                backend_ = backend;
                active_ = make_map(backend_);
            }

            // ── Write ─────────────────────────────────────────────────────────

            /// Returns true when the shard exceeds its flush threshold.
            bool put(core::MemRecord record) {
                std::unique_lock lock{mutex_};

                const size_t new_size = record.approx_size();
                auto old = active_->put(std::move(record));
                if (old) approx_bytes_.fetch_sub(old->approx_size(), std::memory_order_relaxed);
                approx_bytes_.fetch_add(new_size, std::memory_order_relaxed);

                return threshold_bytes_ > 0 && approx_bytes_.load(std::memory_order_relaxed) > threshold_bytes_;
            }

            // ── Read ──────────────────────────────────────────────────────────

            [[nodiscard]] std::optional<core::MemRecord> get(std::span<const uint8_t> key) const {
                std::shared_lock lock{mutex_};

                // Active map first
                if (auto r = active_->find(key)) return r;

                // Immutables: newest to oldest (highest index = newest)
                for (auto rit = immutables_.rbegin(); rit != immutables_.rend(); ++rit) {
                    const ImmVec& vec = *rit->second;
                    auto it = std::lower_bound(
                        vec.begin(),
                        vec.end(),
                        key,
                        [](const core::MemRecord& r, std::span<const uint8_t> k) { return r.compare_key(k) < 0; }
                    );
                    if (it != vec.end() && it->compare_key(key) == 0) return *it;
                }

                return std::nullopt;
            }

            [[nodiscard]] std::optional<bool> get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
                std::shared_lock lock{mutex_};

                // Active map
                if (auto r = active_->find_into(key, out)) return r;

                // Immutables: newest to oldest
                for (auto rit = immutables_.rbegin(); rit != immutables_.rend(); ++rit) {
                    const ImmVec& vec = *rit->second;
                    auto it = std::lower_bound(
                        vec.begin(),
                        vec.end(),
                        key,
                        [](const core::MemRecord& r, std::span<const uint8_t> k) { return r.compare_key(k) < 0; }
                    );
                    if (it != vec.end() && it->compare_key(key) == 0) {
                        if (it->is_tombstone()) return false;
                        const auto val = it->value();
                        out.assign(val.begin(), val.end());
                        return true;
                    }
                }

                return std::nullopt;
            }

            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key) const {
                std::shared_lock lock{mutex_};

                // Active map — no value copy
                if (auto r = active_->contains(key)) return r;

                // Immutables: newest to oldest — just check flag
                for (auto rit = immutables_.rbegin(); rit != immutables_.rend(); ++rit) {
                    const ImmVec& vec = *rit->second;
                    auto it = std::lower_bound(
                        vec.begin(),
                        vec.end(),
                        key,
                        [](const core::MemRecord& r, std::span<const uint8_t> k) { return r.compare_key(k) < 0; }
                    );
                    if (it != vec.end() && it->compare_key(key) == 0) return !it->is_tombstone();
                }

                return std::nullopt;
            }

            // ── Flush lifecycle ───────────────────────────────────────────────

            /// Seals the active map into a sorted ImmVec.
            /// Returns {id, sorted_records_ptr, bytes}.  Empty active → {0, nullptr, 0}.
            [[nodiscard]] SealResult seal_active() {
                std::unique_lock lock{mutex_};
                if (active_->empty()) return {};

                const uint64_t id = next_imm_id_++;

                auto vec = std::make_shared<ImmVec>();
                vec->reserve(active_->size());
                active_->collect_sorted(*vec); // IMemMap guarantees sorted order

                const size_t bytes = approx_bytes_.load(std::memory_order_relaxed);
                active_ = active_->make_empty(); // reset to fresh empty map
                approx_bytes_.store(0, std::memory_order_relaxed);

                immutables_.emplace_back(id, vec);
                return {id, vec, bytes};
            }

            void on_flushed(uint64_t id) {
                std::unique_lock lock{mutex_};
                std::erase_if(immutables_, [id](const auto& p) { return p.first == id; });
            }

            // ── Range ─────────────────────────────────────────────────────────

            /// Snapshot all maps for range iteration.
            /// Returns [active_snapshot, imm0, imm1, ...] — all sorted vectors.
            [[nodiscard]] std::vector<std::shared_ptr<const ImmVec>> snapshot_sorted() const {
                std::shared_lock lock{mutex_};
                std::vector<std::shared_ptr<const ImmVec>> result;
                result.reserve(1 + immutables_.size());

                if (!active_->empty()) {
                    auto snap = std::make_shared<ImmVec>();
                    snap->reserve(active_->size());
                    active_->collect_sorted(*snap);
                    result.push_back(std::move(snap));
                }
                for (const auto& [id, vec] : immutables_) result.push_back(vec);
                return result;
            }

            // ── Metrics ───────────────────────────────────────────────────────

            [[nodiscard]] size_t approx_bytes() const noexcept { return approx_bytes_.load(std::memory_order_relaxed); }

        private:
            mutable std::shared_mutex mutex_;
            std::unique_ptr<IMemMap> active_;
            MemTable::Backend backend_ = MemTable::Backend::BPTree;
            std::deque<std::pair<uint64_t, std::shared_ptr<const ImmVec>>> immutables_;
            std::atomic<size_t> approx_bytes_{0};
            uint64_t next_imm_id_;
            size_t threshold_bytes_;
    };

    // ============================================================================
    // Flusher - one per shard
    // ============================================================================

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

            void enqueue(uint64_t id, std::shared_ptr<const ImmVec> vec) {
                {
                    std::unique_lock lock{mutex_};
                    queue_.push(Item{id, std::move(vec)});
                }
                cv_.notify_one();
            }

            void drain() {
                std::unique_lock lock{mutex_};
                cv_.wait(lock, [this] { return queue_.empty() && in_flight_ == 0; });
            }

        private:
            struct Item {
                uint64_t id;
                std::shared_ptr<const ImmVec> vec;
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

                        // Records are already sorted — pass as span (zero copy).
                        if (callback_) callback_(std::span<const core::MemRecord>(*item.vec));
                        if (on_done_) on_done_(item.id);

                        {
                            std::unique_lock done_lock{mutex_};
                            --in_flight_;
                        }
                        cv_.notify_all();
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
    // RangeIterator::Impl — k-way merge over sorted ImmVec snapshots
    // ============================================================================

    class MemTable::RangeIterator::Impl {
        public:
            Impl(std::vector<std::shared_ptr<const ImmVec>> vecs, KeyRange range)
                : vecs_{std::move(vecs)}, range_{std::move(range)} {
                const auto& start = range_.start;
                positions_.reserve(vecs_.size());

                for (const auto& vp : vecs_) {
                    const ImmVec& v = *vp;
                    size_t pos = 0;
                    if (!start.empty()) {
                        auto it = std::lower_bound(
                            v.begin(),
                            v.end(),
                            std::span<const uint8_t>{start},
                            [](const core::MemRecord& r, std::span<const uint8_t> k) { return r.compare_key(k) < 0; }
                        );
                        pos = static_cast<size_t>(it - v.begin());
                    }
                    positions_.push_back(pos);
                    if (pos < v.size() && !past_end(v[pos])) pq_.push(static_cast<uint32_t>(positions_.size() - 1));
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
                const std::vector<std::shared_ptr<const ImmVec>>* vecs;
                const std::vector<size_t>* positions;

                bool operator()(uint32_t a, uint32_t b) const {
                    const auto& ra = (*vecs)[a]->at((*positions)[a]);
                    const auto& rb = (*vecs)[b]->at((*positions)[b]);
                    const int cmp = ra.compare_key(rb);
                    if (cmp != 0) return cmp > 0; // min-heap by key
                    return ra.seq() < rb.seq(); // tie-break: higher seq first
                }
            };

            void prefetch() {
                while (!pq_.empty()) {
                    const uint32_t si = pq_.top();
                    pq_.pop();
                    const ImmVec& v = *vecs_[si];
                    size_t& pos = positions_[si];

                    core::MemRecord rec = v[pos];
                    ++pos;
                    if (pos < v.size() && !past_end(v[pos])) pq_.push(si);

                    // Drain same-key entries from heap, keep highest seq
                    while (!pq_.empty()) {
                        const uint32_t ni = pq_.top();
                        const core::MemRecord& nr = vecs_[ni]->at(positions_[ni]);
                        if (nr.compare_key(rec.key()) != 0) break;

                        if (nr.seq() > rec.seq()) rec = nr;
                        pq_.pop();
                        size_t& npos = positions_[ni];
                        ++npos;
                        if (npos < vecs_[ni]->size() && !past_end(vecs_[ni]->at(npos))) pq_.push(ni);
                    }

                    // Compare key span directly — avoids per-record vector allocation.
                    const auto k = rec.key();
                    if (k.size() == last_key_.size() && std::equal(k.begin(), k.end(), last_key_.begin())) continue;

                    last_key_.assign(k.begin(), k.end());
                    next_ = std::move(rec);
                    return;
                }
            }

            std::vector<std::shared_ptr<const ImmVec>> vecs_;
            std::vector<size_t> positions_;
            MemTable::KeyRange range_;
            std::priority_queue<uint32_t, std::vector<uint32_t>, PqCmp> pq_{PqCmp{&vecs_, &positions_}};
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
            explicit Impl(const Options& opts)
                : shard_count_{resolve_shard_count(opts.shard_count)},
                  threshold_bytes_per_shard_{opts.threshold_bytes_per_shard},
                  backend_{opts.backend},
                  seq_gen_{1} {
                shards_.reserve(shard_count_);
                flushers_.resize(shard_count_);

                for (uint32_t i = 0; i < shard_count_; ++i) {
                    auto shard = std::make_unique<Shard>();
                    shard->init(threshold_bytes_per_shard_, backend_);
                    shards_.push_back(std::move(shard));
                }

                if (opts.on_flush) { for (uint32_t i = 0; i < shard_count_; ++i) make_flusher(i, opts.on_flush); }
            }

            ~Impl() { flushers_.clear(); }

            // ── Write ─────────────────────────────────────────────────────────

            void put(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq, uint8_t flags, uint64_t precomputed_fp64) {
                puts_applied_.fetch_add(1, std::memory_order_relaxed);
                const uint64_t fp64 = precomputed_fp64 != 0 ? precomputed_fp64 : core::AKHdr32::compute_key_fp64(key.data(), key.size());
                const uint32_t si = shard_for(fp64, shard_count_);

                auto record = core::MemRecord::create(key, value, seq, flags, fp64);
                advance_seq_gen(seq);

                if (shards_[si]->put(std::move(record))) trigger_flush(si);
            }

            void remove(std::span<const uint8_t> key, uint64_t seq) {
                removes_applied_.fetch_add(1, std::memory_order_relaxed);
                const uint64_t fp64 = core::AKHdr32::compute_key_fp64(key.data(), key.size());
                const uint32_t si = shard_for(fp64, shard_count_);

                auto record = core::MemRecord::tombstone(key, seq, fp64);
                advance_seq_gen(seq);

                if (shards_[si]->put(std::move(record))) trigger_flush(si);
            }

            // ── Read ──────────────────────────────────────────────────────────

            [[nodiscard]] std::optional<core::MemRecord> get(std::span<const uint8_t> key) const {
                const uint64_t fp64 = core::AKHdr32::compute_key_fp64(key.data(), key.size());
                return shards_[shard_for(fp64, shard_count_)]->get(key);
            }

            [[nodiscard]] std::optional<bool> get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
                const uint64_t fp64 = core::AKHdr32::compute_key_fp64(key.data(), key.size());
                return shards_[shard_for(fp64, shard_count_)]->get_into(key, out);
            }

            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key) const {
                const uint64_t fp64 = core::AKHdr32::compute_key_fp64(key.data(), key.size());
                return shards_[shard_for(fp64, shard_count_)]->contains(key);
            }

            [[nodiscard]] RangeIterator iterator(const KeyRange& range) const {
                std::vector<std::shared_ptr<const ImmVec>> all_vecs;
                for (const auto& shard : shards_) for (auto& v : shard->snapshot_sorted()) all_vecs.push_back(std::move(v));
                return RangeIterator{std::make_unique<RangeIterator::Impl>(std::move(all_vecs), range)};
            }

            // ── Sequence ──────────────────────────────────────────────────────

            [[nodiscard]] uint64_t next_seq() noexcept { return seq_gen_.fetch_add(1, std::memory_order_relaxed); }
            [[nodiscard]] uint64_t last_seq() const noexcept { return seq_gen_.load(std::memory_order_relaxed); }

            // ── Flush control ─────────────────────────────────────────────────

            void flush_hint() { for (uint32_t i = 0; i < shard_count_; ++i) if (shards_[i]->approx_bytes() > threshold_bytes_per_shard_) trigger_flush(i); }

            void force_flush() {
                for (uint32_t i = 0; i < shard_count_; ++i) trigger_flush(i);
                for (auto& f : flushers_) { if (f) f->drain(); }
            }

            void set_flush_callback(const FlushCallback& cb) {
                for (auto& f : flushers_) { if (f) f->drain(); }
                flushers_.clear();
                flushers_.resize(shard_count_);
                if (cb) { for (uint32_t i = 0; i < shard_count_; ++i) make_flusher(i, cb); }
            }

            [[nodiscard]] size_t approx_size() const noexcept {
                size_t total = 0;
                for (const auto& s : shards_) total += s->approx_bytes();
                return total;
            }

            [[nodiscard]] MemTableSnapshot snapshot() const noexcept {
                size_t total_bytes = 0;
                for (const auto& s : shards_) total_bytes += s->approx_bytes();
                return MemTableSnapshot{
                    shard_count_,
                    static_cast<uint64_t>(threshold_bytes_per_shard_),
                    static_cast<uint64_t>(total_bytes),
                    puts_applied_.load(std::memory_order_relaxed),
                    removes_applied_.load(std::memory_order_relaxed),
                    flushes_completed_.load(std::memory_order_relaxed),
                    bytes_flushed_.load(std::memory_order_relaxed),
                };
            }

        private:
            void advance_seq_gen(uint64_t observed_seq) noexcept {
                uint64_t cur = seq_gen_.load(std::memory_order_relaxed);
                while (cur <= observed_seq) {
                    if (seq_gen_.compare_exchange_weak(cur, observed_seq + 1, std::memory_order_relaxed, std::memory_order_relaxed)) break;
                }
            }

            void trigger_flush(uint32_t si) {
                if (!flushers_[si]) return;
                auto [id, vec, bytes] = shards_[si]->seal_active();
                if (vec) {
                    bytes_flushed_.fetch_add(bytes, std::memory_order_relaxed);
                    flushers_[si]->enqueue(id, std::move(vec));
                }
            }

            void make_flusher(uint32_t i, const FlushCallback& cb) {
                flushers_[i] = std::make_unique<Flusher>(cb, [this, i](uint64_t id) {
                    shards_[i]->on_flushed(id);
                    flushes_completed_.fetch_add(1, std::memory_order_relaxed);
                });
            }

            uint32_t shard_count_;
            size_t threshold_bytes_per_shard_;
            MemTable::Backend backend_;

            std::vector<std::unique_ptr<Shard>> shards_;
            std::vector<std::unique_ptr<Flusher>> flushers_;

            std::atomic<uint64_t> seq_gen_;

            // ── Stats counters (relaxed atomics — zero overhead on write path) ──
            std::atomic<uint64_t> puts_applied_{0};
            std::atomic<uint64_t> removes_applied_{0};
            std::atomic<uint64_t> flushes_completed_{0};
            std::atomic<uint64_t> bytes_flushed_{0};
    };

    // ============================================================================
    // MemTable public API
    // ============================================================================

    std::unique_ptr<MemTable> MemTable::create(const Options& options) { return std::unique_ptr<MemTable>(new MemTable(options)); }

    MemTable::MemTable(const Options& options) : impl_{std::make_unique<Impl>(options)} {}
    MemTable::~MemTable() = default;

    void MemTable::put(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq, uint8_t flags, uint64_t precomputed_fp64) {
        impl_->put(key, value, seq, flags, precomputed_fp64);
    }

    void MemTable::remove(std::span<const uint8_t> key, uint64_t seq) { impl_->remove(key, seq); }

    std::optional<core::MemRecord> MemTable::get(std::span<const uint8_t> key) const { return impl_->get(key); }
    std::optional<bool> MemTable::get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const { return impl_->get_into(key, out); }
    std::optional<bool> MemTable::contains(std::span<const uint8_t> key) const { return impl_->contains(key); }
    MemTable::RangeIterator MemTable::iterator(const KeyRange& range) const { return impl_->iterator(range); }

    uint64_t MemTable::next_seq() noexcept { return impl_->next_seq(); }
    uint64_t MemTable::last_seq() const noexcept { return impl_->last_seq(); }
    void MemTable::flush_hint() { impl_->flush_hint(); }
    void MemTable::force_flush() { impl_->force_flush(); }
    size_t MemTable::approx_size() const noexcept { return impl_->approx_size(); }
    void MemTable::set_flush_callback(const FlushCallback& cb) { impl_->set_flush_callback(cb); }
    MemTable::MemTableSnapshot MemTable::snapshot() const noexcept { return impl_->snapshot(); }
} // namespace akkaradb::engine::memtable
