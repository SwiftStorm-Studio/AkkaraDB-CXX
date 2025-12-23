/*
* AkkEngine
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkEngine.
 *
 * AkkEngine is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkEngine is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkEngine.  If not, see <https://www.gnu.org/licenses/>.
 */

// akkara/akkaradb/src/AkkaraDB.cpp
#include "akkaradb/AkkaraDB.hpp"

#include "engine/AkkEngine.hpp"
#include "core/record/MemRecord.hpp"
#include <stdexcept>
#include <utility>

namespace akkaradb {

// ==================== Record::Impl ====================

class Record::Impl {
public:
    explicit Impl(core::MemRecord internal_record)
        : internal_{std::move(internal_record)} {}

    [[nodiscard]] std::span<const uint8_t> key() const noexcept {
        return internal_.key();
    }

    [[nodiscard]] std::span<const uint8_t> value() const noexcept {
        return internal_.value();
    }

    [[nodiscard]] uint64_t seq() const noexcept {
        return internal_.seq();
    }

    [[nodiscard]] uint8_t flags() const noexcept {
        return internal_.flags();
    }

    [[nodiscard]] bool is_tombstone() const noexcept {
        return internal_.is_tombstone();
    }

    [[nodiscard]] size_t approx_size_bytes() const noexcept {
        return internal_.approx_size();
    }

private:
    core::MemRecord internal_;
};

// ==================== Record Public API ====================

Record::Record(void* internal_record)
    : impl_{std::make_unique<Impl>(
        std::move(*static_cast<core::MemRecord*>(internal_record))
    )} {}

Record::~Record() noexcept = default;

Record::Record(Record&&) noexcept = default;
Record& Record::operator=(Record&&) noexcept = default;

std::span<const uint8_t> Record::key() const noexcept {
    return impl_->key();
}

std::span<const uint8_t> Record::value() const noexcept {
    return impl_->value();
}

uint64_t Record::seq() const noexcept {
    return impl_->seq();
}

uint8_t Record::flags() const noexcept {
    return impl_->flags();
}

bool Record::is_tombstone() const noexcept {
    return impl_->is_tombstone();
}

size_t Record::approx_size_bytes() const noexcept {
    return impl_->approx_size_bytes();
}

// ==================== AkkaraDB::Impl ====================

class AkkaraDB::Impl {
public:
    explicit Impl(std::unique_ptr<engine::AkkEngine> internal_db)
        : internal_db_{std::move(internal_db)} {
        if (!internal_db_) {
            throw std::runtime_error("AkkaraDB: failed to initialize database");
        }
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

    [[nodiscard]] std::vector<Record> range(
        std::span<const uint8_t> start_key,
        std::optional<std::span<const uint8_t>> end_key
    ) {
        check_open();

        // Get internal records
        auto internal_records = internal_db_->range(start_key, end_key);

        // Convert to public Record
        std::vector<Record> public_records;
        public_records.reserve(internal_records.size());

        for (auto& internal_rec : internal_records) {
            // Use friend constructor
            public_records.emplace_back(static_cast<void*>(&internal_rec));
        }

        return public_records;
    }

    [[nodiscard]] uint64_t last_seq() const {
        check_open();
        return internal_db_->last_seq();
    }

    void flush() {
        check_open();
        internal_db_->flush();
    }

    void close() noexcept {
        if (internal_db_) {
            try {
                internal_db_->close();
            } catch (...) {
                // Suppress exceptions
            }
            internal_db_.reset();
        }
    }

    [[nodiscard]] bool is_closed() const noexcept {
        return internal_db_ == nullptr;
    }

private:
    void check_open() const {
        if (!internal_db_) {
            throw std::runtime_error("AkkaraDB: database is closed");
        }
    }

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
    if (!internal_db) {
        throw std::runtime_error("AkkaraDB: failed to open database");
    }

    auto public_db = std::unique_ptr<AkkaraDB>(new AkkaraDB());
    public_db->impl_ = std::make_unique<Impl>(std::move(internal_db));

    return public_db;
}

AkkaraDB::AkkaraDB() = default;

AkkaraDB::~AkkaraDB() noexcept {
    if (impl_) {
        impl_->close();
    }
}

AkkaraDB::AkkaraDB(AkkaraDB&&) noexcept = default;
AkkaraDB& AkkaraDB::operator=(AkkaraDB&&) noexcept = default;

uint64_t AkkaraDB::put(
    std::span<const uint8_t> key,
    std::span<const uint8_t> value
) {
    if (!impl_) {
        throw std::runtime_error("AkkaraDB: database is closed");
    }
    return impl_->put(key, value);
}

uint64_t AkkaraDB::del(std::span<const uint8_t> key) {
    if (!impl_) {
        throw std::runtime_error("AkkaraDB: database is closed");
    }
    return impl_->del(key);
}

std::optional<std::vector<uint8_t>> AkkaraDB::get(
    std::span<const uint8_t> key
) {
    if (!impl_) {
        throw std::runtime_error("AkkaraDB: database is closed");
    }
    return impl_->get(key);
}

bool AkkaraDB::compare_and_swap(
    std::span<const uint8_t> key,
    uint64_t expected_seq,
    std::optional<std::span<const uint8_t>> new_value
) {
    if (!impl_) {
        throw std::runtime_error("AkkaraDB: database is closed");
    }
    return impl_->compare_and_swap(key, expected_seq, new_value);
}

std::vector<Record> AkkaraDB::range(
    std::span<const uint8_t> start_key,
    std::optional<std::span<const uint8_t>> end_key
) {
    if (!impl_) {
        throw std::runtime_error("AkkaraDB: database is closed");
    }
    return impl_->range(start_key, end_key);
}

uint64_t AkkaraDB::last_seq() const {
    if (!impl_) {
        throw std::runtime_error("AkkaraDB: database is closed");
    }
    return impl_->last_seq();
}

void AkkaraDB::flush() {
    if (!impl_) {
        throw std::runtime_error("AkkaraDB: database is closed");
    }
    impl_->flush();
}

void AkkaraDB::close() noexcept {
    if (impl_) {
        impl_->close();
    }
}

} // namespace akkaradb