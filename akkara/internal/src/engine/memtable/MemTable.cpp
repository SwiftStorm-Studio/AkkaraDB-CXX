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

// internal/src/engine/memtable/MemTable.cpp
#include "engine/memtable/MemTable.hpp"
#include <map>
#include <shared_mutex>
#include <thread>
#include <queue>
#include <condition_variable>
#include <algorithm>
#include <numeric>
#include <ranges>

namespace akkaradb::engine::memtable {
    namespace {
        size_t hash_key(std::span<const uint8_t> key) noexcept {
            size_t hash = 0;
            for (const auto byte : key) { hash = hash * 31 + static_cast<size_t>(byte); }
            return hash;
        }

        struct MemRecordCmp {
            using is_transparent = void;

            bool operator()(const core::MemRecord& a, const core::MemRecord& b) const noexcept { return a.compare_key(b) < 0; }
            bool operator()(const core::MemRecord& a, std::span<const uint8_t> b) const noexcept { return a.compare_key(b) < 0; }
            bool operator()(std::span<const uint8_t> a, const core::MemRecord& b) const noexcept { return b.compare_key(a) > 0; }
        };
    }

    /**
     * Shard - Single shard with active + immutable maps.
     */
    class Shard {
        public:
            using Map = std::map<core::MemRecord, std::monostate, MemRecordCmp>;

            Shard() : approx_bytes_{0}, next_imm_id_{0} {}

            // Returns true if the shard exceeded the flush threshold after this put.
            bool put(core::MemRecord record) {
                std::unique_lock lock{mutex_};

                auto [it, inserted] = active_.emplace(record, std::monostate{});
                if (inserted) { approx_bytes_ += record.approx_size(); }
                else {
                    approx_bytes_ -= it->first.approx_size();
                    approx_bytes_ += record.approx_size();
                    auto node = active_.extract(it);
                    node.key() = std::move(record);
                    active_.insert(std::move(node));
                }

                return approx_bytes_ > threshold_bytes_;
            }

            void set_threshold(size_t threshold) noexcept { threshold_bytes_ = threshold; }

            std::shared_ptr<const core::MemRecord> get(std::span<const uint8_t> key) const {
                std::shared_lock lock{mutex_};

                auto it = active_.find(key);
                if (it != active_.end()) { return std::make_shared<const core::MemRecord>(it->first); }

                for (auto rit = immutables_.rbegin(); rit != immutables_.rend(); ++rit) {
                    auto it2 = rit->second.find(key);
                    if (it2 != rit->second.end()) { return std::make_shared<const core::MemRecord>(it2->first); }
                }

                return nullptr;
            }

            std::pair<uint64_t, std::vector<core::MemRecord>> seal_and_get() {
                std::unique_lock lock{mutex_};
                if (active_.empty()) return {0, {}};

                const uint64_t id = next_imm_id_++;
                immutables_.push_back({id, std::move(active_)});
                active_ = Map{};
                approx_bytes_ = 0;

                std::vector<core::MemRecord> result;
                result.reserve(immutables_.back().second.size());
                for (const auto& rec : immutables_.back().second | std::views::keys) { result.push_back(rec); }
                return {id, std::move(result)};
            }

            void on_flushed(uint64_t id) {
                std::unique_lock lock{mutex_};
                immutables_.erase(std::remove_if(immutables_.begin(), immutables_.end(), [id](const auto& p) { return p.first == id; }), immutables_.end());
            }

            size_t approx_bytes() const noexcept {
                std::shared_lock lock{mutex_};
                return approx_bytes_;
            }

            /**
         * Returns sorted slices from active and each immutable map within [start, end).
         * Each slice is independently sorted; caller merges via k-way merge.
         */
            std::vector<std::vector<core::MemRecord>> range_slices(const std::vector<uint8_t>& start, const std::vector<uint8_t>& end) const {
                std::shared_lock lock{mutex_};
                std::vector<std::vector<core::MemRecord>> result;

                auto start_span = std::span(start);
                auto end_span = std::span(end);

                auto collect = [&](const Map& m) {
                    std::vector<core::MemRecord> slice;
                    auto it = m.lower_bound(start_span);
                    for (; it != m.end(); ++it) {
                        const auto key = it->first.key();
                        if (!end.empty() && !std::ranges::lexicographical_compare(key, end_span)) break;
                        slice.push_back(it->first);
                    }
                    if (!slice.empty()) result.push_back(std::move(slice));
                };

                collect(active_);
                for (const auto& imm : immutables_) collect(imm.second);
                return result;
            }

        private:
            mutable std::shared_mutex mutex_;
            Map active_;
            std::deque<std::pair<uint64_t, Map>> immutables_;
            size_t approx_bytes_;
            uint64_t next_imm_id_;
            size_t threshold_bytes_{0};
    };

    /**
     * RangeIterator::Impl - K-way merge over sorted slices from all shards.
     * Yields records in lexicographic key order, highest seq first per key.
     */
    class MemTable::RangeIterator::Impl {
        public:
            explicit Impl(std::vector<std::vector<core::MemRecord>> all_slices) {
                for (auto& slice : all_slices) { if (!slice.empty()) { sources_.push_back({std::move(slice), 0}); } }
                for (size_t i = 0; i < sources_.size(); ++i) { pq_.push(i); }
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
            struct Source {
                std::vector<core::MemRecord> records;
                size_t idx;

                [[nodiscard]] const core::MemRecord& peek() const { return records[idx]; }
                [[nodiscard]] bool exhausted() const { return idx >= records.size(); }
            };

            struct PqCmp {
                const std::vector<Source>* sources;

                bool operator()(size_t a, size_t b) const {
                    const auto& ra = (*sources)[a].peek();
                    const auto& rb = (*sources)[b].peek();
                    const int cmp = ra.compare_key(rb);
                    if (cmp != 0) return cmp > 0;
                    return ra.seq() < rb.seq();
                }
            };

            void prefetch() {
                while (!pq_.empty()) {
                    const size_t si = pq_.top();
                    pq_.pop();

                    auto& src = sources_[si];
                    core::MemRecord rec = src.records[src.idx];
                    ++src.idx;
                    if (!src.exhausted()) pq_.push(si);

                    auto key_span = rec.key();
                    std::vector cur_key(key_span.begin(), key_span.end());
                    if (last_key_ == cur_key) continue;

                    last_key_ = std::move(cur_key);
                    next_ = std::move(rec);
                    return;
                }
            }

            std::vector<Source> sources_;
            std::priority_queue<size_t, std::vector<size_t>, PqCmp> pq_{PqCmp{&sources_}};
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

    /**
     * Flusher - Background thread for coalescing flush operations.
     */
    class Flusher {
        public:
            using DoneCallback = std::function<void(size_t shard_idx, uint64_t id)>;

            explicit Flusher(MemTable::FlushCallback callback, DoneCallback on_done)
                : callback_{std::move(callback)}, on_done_{std::move(on_done)}, running_{true} { thread_ = std::thread([this]() { this->run(); }); }

            ~Flusher() {
                {
                    std::unique_lock lock{mutex_};
                    running_ = false;
                }
                cv_.notify_one();
                if (thread_.joinable()) thread_.join();
            }

            void enqueue(size_t shard_idx, uint64_t id, std::vector<core::MemRecord> batch) {
                {
                    std::unique_lock lock{mutex_};
                    queue_.emplace(Item{shard_idx, id, std::move(batch)});
                }
                cv_.notify_one();
            }

            void force_flush() {
                std::unique_lock lock{mutex_};
                // Wait until queue is empty AND no callback is currently executing.
                cv_.wait(lock, [this]() { return queue_.empty() && in_flight_ == 0; });
            }

        private:
            struct Item {
                size_t shard_idx;
                uint64_t id;
                std::vector<core::MemRecord> batch;
            };

            void run() {
                while (true) {
                    std::unique_lock lock{mutex_};
                    cv_.wait(lock, [this]() { return !queue_.empty() || !running_; });
                    if (!running_ && queue_.empty()) break;
                    if (!queue_.empty()) {
                        auto item = std::move(queue_.front());
                        queue_.pop();
                        ++in_flight_;
                        lock.unlock();
                        std::ranges::sort(item.batch, [](const auto& a, const auto& b) { return a.compare_key(b) < 0; });
                        if (callback_) callback_(std::move(item.batch));
                        if (on_done_) on_done_(item.shard_idx, item.id);
                        {
                            std::unique_lock done_lock{mutex_};
                            --in_flight_;
                        }
                        // Wake force_flush() which waits on queue_.empty() && in_flight_==0.
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
            int in_flight_{0}; // Number of callbacks currently executing
    };

    /**
     * MemTable::Impl - Private implementation.
     */
    class MemTable::Impl {
        public:
            Impl(size_t shard_count, size_t threshold_bytes_per_shard, FlushCallback on_flush)
                : shard_count_{shard_count},
                  threshold_bytes_per_shard_{threshold_bytes_per_shard},
                  seq_gen_{0},
                  flusher_{
                      on_flush
                          ? std::make_unique<Flusher>(std::move(on_flush), [this](size_t shard_idx, uint64_t id) { shards_[shard_idx]->on_flushed(id); })
                          : nullptr
                  } {
                if (shard_count_ < 2 || shard_count_ > 8) throw std::invalid_argument("MemTable: shard_count must be 2-8");
                shards_.reserve(shard_count_);
                for (size_t i = 0; i < shard_count_; ++i) {
                    auto shard = std::make_unique<Shard>();
                    shard->set_threshold(threshold_bytes_per_shard_);
                    shards_.push_back(std::move(shard));
                }
            }

            void put(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq) {
                const size_t shard_idx = hash_key(key) % shard_count_;
                auto record = core::MemRecord::create(key, value, seq);
                const bool over_threshold = shards_[shard_idx]->put(std::move(record));
                advance_seq_gen(seq);
                if (over_threshold) trigger_flush(shard_idx);
            }

            void remove(std::span<const uint8_t> key, uint64_t seq) {
                const size_t shard_idx = hash_key(key) % shard_count_;
                auto record = core::MemRecord::tombstone(key, seq);
                const bool over_threshold = shards_[shard_idx]->put(std::move(record));
                advance_seq_gen(seq);
                if (over_threshold) trigger_flush(shard_idx);
            }

            std::shared_ptr<const core::MemRecord> get(std::span<const uint8_t> key) const {
                const size_t shard_idx = hash_key(key) % shard_count_;
                return shards_[shard_idx]->get(key);
            }

            uint64_t next_seq() noexcept { return seq_gen_.fetch_add(1, std::memory_order_relaxed); }
            uint64_t last_seq() const noexcept { return seq_gen_.load(std::memory_order_relaxed); }

            void flush_hint() { for (size_t i = 0; i < shard_count_; ++i) if (shards_[i]->approx_bytes() > threshold_bytes_per_shard_) trigger_flush(i); }

            void force_flush() {
                for (size_t i = 0; i < shard_count_; ++i) trigger_flush(i);
                if (flusher_) flusher_->force_flush();
            }

            size_t approx_size() const noexcept {
                size_t total = 0;
                for (const auto& shard : shards_) total += shard->approx_bytes();
                return total;
            }

            /**
             * Returns a streaming iterator over [range.start, range.end).
             * Merges slices from all shards via k-way merge without allocating a full result vector.
             */
            RangeIterator iterator(const KeyRange& range) const {
                std::vector<std::vector<core::MemRecord>> all_slices;
                for (const auto& shard : shards_) {
                    auto slices = shard->range_slices(range.start, range.end);
                    for (auto& s : slices) all_slices.push_back(std::move(s));
                }
                return RangeIterator{std::make_unique<RangeIterator::Impl>(std::move(all_slices))};
            }

            // Replaces the active Flusher.
            // If an existing Flusher is running, it is drained and destroyed first to
            // avoid losing in-flight batches.
            void set_flush_callback(FlushCallback cb) {
                if (flusher_) {
                    // Drain any pending batches through the old callback before switching.
                    flusher_->force_flush();
                    flusher_.reset();
                }
                if (cb) { flusher_ = std::make_unique<Flusher>(std::move(cb), [this](size_t shard_idx, uint64_t id) { shards_[shard_idx]->on_flushed(id); }); }
            }

        private:
            // Advances seq_gen_ to (observed_seq + 1) if the current value is lower.
            // Called on every write path so that WAL recovery (which supplies explicit seq values)
            // leaves seq_gen_ correctly positioned past the highest recovered sequence number.
            void advance_seq_gen(uint64_t observed_seq) noexcept {
                uint64_t current = seq_gen_.load(std::memory_order_relaxed);
                while (current <= observed_seq) {
                    if (seq_gen_.compare_exchange_weak(current, observed_seq + 1, std::memory_order_relaxed, std::memory_order_relaxed)) { break; }
                    // On CAS failure, current is refreshed to the latest value by compare_exchange_weak.
                }
            }

            void trigger_flush(size_t shard_idx) {
                if (!flusher_) return;
                auto [id, batch] = shards_[shard_idx]->seal_and_get();
                if (!batch.empty()) flusher_->enqueue(shard_idx, id, std::move(batch));
            }

            size_t shard_count_;
            size_t threshold_bytes_per_shard_;
            std::vector<std::unique_ptr<Shard>> shards_;
            std::atomic<uint64_t> seq_gen_;
            std::unique_ptr<Flusher> flusher_;
    };

    std::unique_ptr<MemTable> MemTable::create(size_t shard_count, size_t threshold_bytes_per_shard, FlushCallback on_flush) {
        return std::unique_ptr<MemTable>(new MemTable(shard_count, threshold_bytes_per_shard, std::move(on_flush)));
    }

    MemTable::MemTable(size_t sc, size_t tb, FlushCallback of) : impl_{std::make_unique<Impl>(sc, tb, std::move(of))} {}

    MemTable::~MemTable() = default;

    void MemTable::put(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq) { impl_->put(key, value, seq); }
    void MemTable::remove(std::span<const uint8_t> key, uint64_t seq) { impl_->remove(key, seq); }

    std::shared_ptr<const core::MemRecord> MemTable::get(std::span<const uint8_t> key) const { return impl_->get(key); }
    uint64_t MemTable::next_seq() noexcept { return impl_->next_seq(); }
    uint64_t MemTable::last_seq() const noexcept { return impl_->last_seq(); }
    void MemTable::flush_hint() { impl_->flush_hint(); }
    void MemTable::force_flush() { impl_->force_flush(); }
    size_t MemTable::approx_size() const noexcept { return impl_->approx_size(); }
    MemTable::RangeIterator MemTable::iterator(const KeyRange& range) const { return impl_->iterator(range); }

    void MemTable::set_flush_callback(FlushCallback cb) { impl_->set_flush_callback(std::move(cb)); }
} // namespace akkaradb::engine::memtable