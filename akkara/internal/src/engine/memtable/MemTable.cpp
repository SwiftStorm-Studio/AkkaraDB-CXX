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
        /**
 * Comparator for byte array keys (lexicographic).
 */
        struct ByteArrayComparator {
            bool operator()(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b) const { return std::ranges::lexicographical_compare(a, b); }
        };

        /**
 * Simple hash function for shard selection.
 */
        size_t hash_key(std::span<const uint8_t> key) noexcept {
            size_t hash = 0;
            for (const auto byte : key) { hash = hash * 31 + static_cast<size_t>(byte); }
            return hash;
        }
    } // anonymous namespace

    /**
 * Shard - Single shard with active + immutable maps.
 */
    class Shard {
    public:
        using Map = std::map<std::vector<uint8_t>, core::MemRecord, ByteArrayComparator>;

        Shard() : approx_bytes_{0} {}

        void put(const std::vector<uint8_t>& key, core::MemRecord record) {
            std::unique_lock lock{mutex_};

            auto it = active_.find(key);
            if (it != active_.end()) {
                // Replace existing
                approx_bytes_ -= it->second.approx_size();
                approx_bytes_ += record.approx_size();
                it->second = std::move(record);
            }
            else {
                // Insert new
                approx_bytes_ += record.approx_size();
                active_.emplace(key, std::move(record));
            }
        }

        std::shared_ptr<const core::MemRecord> get(const std::vector<uint8_t>& key) const {
            std::shared_lock lock{mutex_};

            // Check active first
            auto it = active_.find(key);
            if (it != active_.end()) { return std::make_shared<const core::MemRecord>(it->second); }

            // Check immutables (newest first)
            for (auto rit = immutables_.rbegin(); rit != immutables_.rend(); ++rit) {
                auto it2 = rit->find(key);
                if (it2 != rit->end()) { return std::make_shared<const core::MemRecord>(it2->second); }
            }

            return nullptr;
        }

        std::vector<core::MemRecord> seal_and_get() {
            std::unique_lock lock{mutex_};

            if (active_.empty()) { return {}; }

            // Move active to immutables
            immutables_.push_back(std::move(active_));
            active_ = Map{};
            approx_bytes_ = 0;

            // Extract all records from last immutable
            std::vector<core::MemRecord> result;
            result.reserve(immutables_.back().size());

            for (auto& v : immutables_.back() | std::views::values) { result.push_back(v); }

            // Remove the sealed map from immutables
            immutables_.pop_back();

            return result;
        }

        size_t approx_bytes() const noexcept {
            std::shared_lock lock{mutex_};
            return approx_bytes_;
        }

        std::vector<core::MemRecord> all_records() const {
            std::shared_lock lock{mutex_};

            std::vector<core::MemRecord> result;

            // Collect from immutables
            for (const auto& map : immutables_) { for (const auto& v : map | std::views::values) { result.push_back(v); } }

            // Collect from active
            for (const auto& v : active_ | std::views::values) { result.push_back(v); }

            return result;
        }

    private:
        mutable std::shared_mutex mutex_;
        Map active_;
        std::vector<Map> immutables_;
        size_t approx_bytes_;
    };

    /**
 * Flusher - Background thread for coalescing flush operations.
 */
    class Flusher {
    public:
        explicit Flusher(MemTable::FlushCallback callback) : callback_{std::move(callback)}, running_{true} {
            thread_ = std::thread([this]() { this->run(); });
        }

        ~Flusher() {
            {
                std::unique_lock lock{mutex_};
                running_ = false;
            }
            cv_.notify_one();
            if (thread_.joinable()) { thread_.join(); }
        }

        void enqueue(std::vector<core::MemRecord> batch) {
            {
                std::unique_lock lock{mutex_};
                queue_.emplace(std::move(batch));
            }
            cv_.notify_one();
        }

        void force_flush() {
            // Wait until queue is empty
            std::unique_lock lock{mutex_};
            cv_.wait(lock, [this]() { return queue_.empty(); });
        }

    private:
        void run() {
            while (true) {
                std::unique_lock lock{mutex_};
                cv_.wait(lock, [this]() { return !queue_.empty() || !running_; });

                if (!running_ && queue_.empty()) { break; }

                if (!queue_.empty()) {
                    auto batch = std::move(queue_.front());
                    queue_.pop();
                    lock.unlock();

                    // Sort batch by key
                    std::ranges::sort(batch, [](const auto& a, const auto& b) { return a.compare_key(b) < 0; });

                    // Invoke callback
                    if (callback_) { callback_(std::move(batch)); }
                }
            }
        }

        MemTable::FlushCallback callback_;
        std::thread thread_;
        std::mutex mutex_;
        std::condition_variable cv_;
        std::queue<std::vector<core::MemRecord>> queue_;
        bool running_;
    };

    /**
 * MemTable::Impl - Private implementation.
 */
    class MemTable::Impl {
    public:
        Impl(size_t shard_count, size_t threshold_bytes_per_shard, FlushCallback on_flush) : shard_count_{shard_count}
                                                                                             , threshold_bytes_per_shard_{threshold_bytes_per_shard}
                                                                                             , seq_gen_{0}
                                                                                             , flusher_{on_flush ? std::make_unique<Flusher>(std::move(on_flush)) : nullptr} {
            if (shard_count_ < 2 || shard_count_ > 8) { throw std::invalid_argument("MemTable: shard_count must be 2-8"); }

            shards_.reserve(shard_count_);
            for (size_t i = 0; i < shard_count_; ++i) { shards_.push_back(std::make_unique<Shard>()); }
        }

        void put(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq) {
            const size_t shard_idx = hash_key(key) % shard_count_;

            auto record = core::MemRecord::create(key, value, seq);
            shards_[shard_idx]->put(std::vector(key.begin(), key.end()), std::move(record));

            // Check flush threshold
            if (shards_[shard_idx]->approx_bytes() > threshold_bytes_per_shard_) { trigger_flush(shard_idx); }
        }

        void remove(std::span<const uint8_t> key, uint64_t seq) {
            const size_t shard_idx = hash_key(key) % shard_count_;

            auto record = core::MemRecord::tombstone(key, seq);
            shards_[shard_idx]->put(std::vector(key.begin(), key.end()), std::move(record));

            if (shards_[shard_idx]->approx_bytes() > threshold_bytes_per_shard_) { trigger_flush(shard_idx); }
        }

        std::shared_ptr<const core::MemRecord> get(std::span<const uint8_t> key) const {
            const size_t shard_idx = hash_key(key) % shard_count_;
            return shards_[shard_idx]->get(std::vector(key.begin(), key.end()));
        }

        uint64_t next_seq() noexcept { return seq_gen_.fetch_add(1, std::memory_order_relaxed); }

        uint64_t last_seq() const noexcept { return seq_gen_.load(std::memory_order_relaxed); }

        void flush_hint() {
            // Trigger flush for all shards exceeding threshold
            for (size_t i = 0; i < shard_count_; ++i) { if (shards_[i]->approx_bytes() > threshold_bytes_per_shard_) { trigger_flush(i); } }
        }

        void force_flush() {
            // Flush all shards
            for (size_t i = 0; i < shard_count_; ++i) { trigger_flush(i); }

            // Wait for flusher to complete
            if (flusher_) { flusher_->force_flush(); }
        }

        size_t approx_size() const noexcept {
            size_t total = 0;
            for (const auto& shard : shards_) { total += shard->approx_bytes(); }
            return total;
        }

        std::vector<core::MemRecord> range(const KeyRange& range) const {
            // Collect from all shards
            std::vector<core::MemRecord> all;

            for (const auto& shard : shards_) {
                auto records = shard->all_records();
                all.insert(all.end(), records.begin(), records.end());
            }

            // Sort by key
            std::ranges::sort(all, [](const auto& a, const auto& b) { return a.compare_key(b) < 0; });

            // Filter by range
            std::vector<core::MemRecord> result;
            for (const auto& record : all) {
                if (record.key().size() >= range.start.size() &&
                    std::equal(range.start.begin(), range.start.end(), record.key().begin())) {
                    if (record.key().size() < range.end.size() ||
                        !std::equal(range.end.begin(), range.end.end(), record.key().begin())) { result.push_back(record); }
                }
            }

            return result;
        }

    private:
        void trigger_flush(size_t shard_idx) {
            if (!flusher_) { return; }

            auto batch = shards_[shard_idx]->seal_and_get();
            if (!batch.empty()) { flusher_->enqueue(std::move(batch)); }
        }

        size_t shard_count_;
        size_t threshold_bytes_per_shard_;
        std::vector<std::unique_ptr<Shard>> shards_;
        std::atomic<uint64_t> seq_gen_;
        std::unique_ptr<Flusher> flusher_;
    };

    // ==================== MemTable Public API ====================

    std::unique_ptr<MemTable> MemTable::create(
        size_t shard_count,
        size_t threshold_bytes_per_shard,
        FlushCallback on_flush
    ) {
        return std::unique_ptr<MemTable>(new MemTable(
            shard_count, threshold_bytes_per_shard, std::move(on_flush)
        ));
    }

    MemTable::MemTable(
        size_t shard_count,
        size_t threshold_bytes_per_shard,
        FlushCallback on_flush
    ) : impl_{std::make_unique<Impl>(shard_count, threshold_bytes_per_shard, std::move(on_flush))} {}

    MemTable::~MemTable() = default;

    void MemTable::put(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq) { impl_->put(key, value, seq); }

    void MemTable::remove(std::span<const uint8_t> key, uint64_t seq) { impl_->remove(key, seq); }

    std::shared_ptr<const core::MemRecord> MemTable::get(std::span<const uint8_t> key) const { return impl_->get(key); }

    uint64_t MemTable::next_seq() noexcept { return impl_->next_seq(); }

    uint64_t MemTable::last_seq() const noexcept { return impl_->last_seq(); }

    void MemTable::flush_hint() { impl_->flush_hint(); }

    void MemTable::force_flush() { impl_->force_flush(); }

    size_t MemTable::approx_size() const noexcept { return impl_->approx_size(); }

    std::vector<core::MemRecord> MemTable::range(const KeyRange& range) const { return impl_->range(range); }
} // namespace akkaradb::engine::memtable