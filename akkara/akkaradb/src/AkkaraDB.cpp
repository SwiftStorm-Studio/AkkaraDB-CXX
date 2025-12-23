// akkara/akkaradb/src/AkkaraDB.cpp
#include <stdexcept>

#include "akkaradb/AkkaraDB.hpp"

#include "core/record/MemRecord.hpp"
#include "engine/AkkEngine.hpp"

namespace akkaradb {
    class AkkaraDB::Impl {
    public:
        explicit Impl(std::unique_ptr<engine::AkkEngine> internal_db) : internal_db_{std::move(internal_db)} {
            if (!internal_db_) { throw std::runtime_error("Internal database is null"); }
        }

        [[nodiscard]] uint64_t put(
            std::span<const uint8_t> key,
            std::span<const uint8_t> value
        ) {
            check_open();
            return internal_db_->put(key, value);
        }

        [[nodiscard]] uint64_t del(std::span<const uint8_t> key) {
            check_open();
            return internal_db_->del(key);
        }

        [[nodiscard]] std::optional<std::vector<uint8_t>> get(
            std::span<const uint8_t> key
        ) {
            check_open();
            return internal_db_->get(key);
        }

        [[nodiscard]] bool compare_and_swap(
            std::span<const uint8_t> key,
            uint64_t expected_seq,
            std::optional<std::span<const uint8_t>> new_value
        ) {
            check_open();
            return internal_db_->compare_and_swap(key, expected_seq, new_value);
        }

        [[nodiscard]] std::vector<core::MemRecord> range(
            std::span<const uint8_t> start_key,
            std::optional<std::span<const uint8_t>> end_key
        ) {
            check_open();
            return internal_db_->range(start_key, end_key);
        }

        [[nodiscard]] uint64_t last_seq() const noexcept {
            if (!internal_db_) return 0;
            return internal_db_->last_seq();
        }

        void flush() {
            check_open();
            internal_db_->flush();
        }

        void close() {
            if (internal_db_) {
                internal_db_->close();
                internal_db_.reset();
            }
        }

        [[nodiscard]] bool is_closed() const noexcept { return internal_db_ == nullptr; }

    private:
        void check_open() const { if (!internal_db_) { throw std::runtime_error("Database is closed"); } }

        std::unique_ptr<engine::AkkEngine> internal_db_;
    };

    // ==================== AkkaraDB Public API ====================

    std::unique_ptr<AkkaraDB> AkkaraDB::open(const Options& opts) {
        engine::AkkEngine::Options internal_opts{
            .base_dir = opts.base_dir,
            .memtable_shard_count = opts.memtable_shard_count,
            .memtable_threshold_bytes = opts.memtable_threshold_bytes,
            .wal_group_n = opts.wal_group_n,
            .wal_group_micros = opts.wal_group_micros,
            .wal_fast_mode = opts.wal_fast_mode,
            .bloom_fp_rate = opts.bloom_fp_rate,
            .max_sst_per_level = opts.max_sst_per_level,
            .buffer_pool_size = opts.buffer_pool_size
        };

        auto internal_db = engine::AkkEngine::open(internal_opts);
        if (!internal_db) { throw std::runtime_error("Failed to open database"); }

        auto public_db = std::unique_ptr<AkkaraDB>(new AkkaraDB());
        public_db->impl_ = std::make_unique<Impl>(std::move(internal_db));

        return public_db;
    }

    AkkaraDB::AkkaraDB() = default;

    AkkaraDB::~AkkaraDB() {
        if (impl_ && !impl_->is_closed()) {
            try { impl_->close(); }
            catch (...) {
                // Suppress exceptions in destructor
            }
        }
    }

    AkkaraDB::AkkaraDB(AkkaraDB&&) noexcept = default;
    AkkaraDB& AkkaraDB::operator=(AkkaraDB&&) noexcept = default;

    uint64_t AkkaraDB::put(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value
    ) {
        if (!impl_) { throw std::runtime_error("Database is not initialized"); }
        return impl_->put(key, value);
    }

    uint64_t AkkaraDB::del(std::span<const uint8_t> key) {
        if (!impl_) { throw std::runtime_error("Database is not initialized"); }
        return impl_->del(key);
    }

    std::optional<std::vector<uint8_t>> AkkaraDB::get(
        std::span<const uint8_t> key
    ) {
        if (!impl_) { throw std::runtime_error("Database is not initialized"); }
        return impl_->get(key);
    }

    bool AkkaraDB::compare_and_swap(
        std::span<const uint8_t> key,
        uint64_t expected_seq,
        std::optional<std::span<const uint8_t>> new_value
    ) {
        if (!impl_) { throw std::runtime_error("Database is not initialized"); }
        return impl_->compare_and_swap(key, expected_seq, new_value);
    }

    std::vector<core::MemRecord> AkkaraDB::range(
        std::span<const uint8_t> start_key,
        const std::optional<std::span<const uint8_t>>& end_key
    ) {
        if (!impl_) { throw std::runtime_error("Database is not initialized"); }
        return impl_->range(start_key, end_key);
    }

    uint64_t AkkaraDB::last_seq() const noexcept {
        if (!impl_) { return 0; }
        return impl_->last_seq();
    }

    void AkkaraDB::flush() {
        if (!impl_) { throw std::runtime_error("Database is not initialized"); }
        impl_->flush();
    }

    void AkkaraDB::close() { if (impl_) { impl_->close(); } }
} // namespace akkaradb
