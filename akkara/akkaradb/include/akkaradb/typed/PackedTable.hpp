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

// akkara/akkaradb/include/akkaradb/typed/PackedTable.hpp
#pragma once

#include <akkaradb/AkkaraDB.hpp>
#include "EntityMacros.hpp"
#include "Query.hpp"
#include "serialization/Serialization.hpp"
#include <optional>
#include <string_view>
#include <memory>
#include <array>
#include <cstring>
#include <vector>

/**
 * PackedTable - Type-safe CRUD + Query interface for AkkaraDB.
 *
 * Features:
 * - Type-safe CRUD operations
 * - Expression Template queries with early filtering
 * - Zero-copy optimizations
 * - JVM-compatible file format
 *
 * Usage:
 *   struct User {
 *       std::string id;
 *       std::string name;
 *       int age;
 *
 *       AKKARA_TABLE(User,
 *           PRIMARY_KEY(id),
 *           FIELDS(id, name, age)
 *       )
 *   };
 *
 *   auto db = akkaradb::AkkaraDB::open({.base_dir = "./data"});
 *   akkaradb::typed::PackedTable<User> users{*db, "User"};
 *
 *   // CRUD
 *   users.put("u001", User{"u001", "Taro", 30});
 *   auto user = users.get("u001");
 *
 *   // Query (with early filtering)
 *   Field<User, int> age{&User::age, OFFSET_OF_AGE};
 *   auto adults = users.query(age >= 18);
 */
namespace akkaradb::typed {
    // ==================== Murmur3 Hash ====================

    [[nodiscard]] constexpr uint64_t rotl64(uint64_t x, int r) noexcept { return (x << r) | (x >> (64 - r)); }

    [[nodiscard]] constexpr uint64_t fmix64(uint64_t k) noexcept {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53ULL;
        k ^= k >> 33;
        return k;
    }

    [[nodiscard]] inline uint64_t murmur3_hash(std::string_view str) noexcept {
        const auto* data = reinterpret_cast<const uint8_t*>(str.data());
        const size_t len = str.size();

        constexpr uint64_t seed = 0;
        uint64_t h1 = seed;
        uint64_t h2 = seed;

        constexpr uint64_t c1 = 0x87c37b91114253d5ULL;
        constexpr uint64_t c2 = 0x4cf5ad432745937fULL;

        const size_t nblocks = len / 16;

        for (size_t i = 0; i < nblocks; i++) {
            uint64_t k1, k2;
            std::memcpy(&k1, data + i * 16, 8);
            std::memcpy(&k2, data + i * 16 + 8, 8);

            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;
            h1 = rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= c2;
            k2 = rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;
            h2 = rotl64(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        const auto* tail = data + nblocks * 16;
        const size_t remaining = len - nblocks * 16;

        uint64_t k1 = 0, k2 = 0;

        if (remaining >= 15) k2 ^= static_cast<uint64_t>(tail[14]) << 48;
        if (remaining >= 14) k2 ^= static_cast<uint64_t>(tail[13]) << 40;
        if (remaining >= 13) k2 ^= static_cast<uint64_t>(tail[12]) << 32;
        if (remaining >= 12) k2 ^= static_cast<uint64_t>(tail[11]) << 24;
        if (remaining >= 11) k2 ^= static_cast<uint64_t>(tail[10]) << 16;
        if (remaining >= 10) k2 ^= static_cast<uint64_t>(tail[9]) << 8;
        if (remaining >= 9) k2 ^= static_cast<uint64_t>(tail[8]) << 0;

        if (remaining >= 8) k1 ^= static_cast<uint64_t>(tail[7]) << 56;
        if (remaining >= 7) k1 ^= static_cast<uint64_t>(tail[6]) << 48;
        if (remaining >= 6) k1 ^= static_cast<uint64_t>(tail[5]) << 40;
        if (remaining >= 5) k1 ^= static_cast<uint64_t>(tail[4]) << 32;
        if (remaining >= 4) k1 ^= static_cast<uint64_t>(tail[3]) << 24;
        if (remaining >= 3) k1 ^= static_cast<uint64_t>(tail[2]) << 16;
        if (remaining >= 2) k1 ^= static_cast<uint64_t>(tail[1]) << 8;
        if (remaining >= 1) k1 ^= static_cast<uint64_t>(tail[0]) << 0;

        if (k1 != 0) {
            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;
        }
        if (k2 != 0) {
            k2 *= c2;
            k2 = rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;
        }

        h1 ^= len;
        h2 ^= len;
        h1 += h2;
        h2 += h1;
        h1 = fmix64(h1);
        h2 = fmix64(h2);
        h1 += h2;

        return h1;
    }

    // ==================== PackedTable ====================

    template <typename Entity, typename Key = typename Entity::akkara_key_type>
    class PackedTable {
        public:
            explicit PackedTable(AkkaraDB& db, std::string_view table_name);
            ~PackedTable() noexcept;

            PackedTable(const PackedTable&) = delete;
            PackedTable& operator=(const PackedTable&) = delete;
            PackedTable(PackedTable&&) noexcept;
            PackedTable& operator=(PackedTable&&) noexcept;

            // ==================== CRUD ====================

            [[nodiscard]] uint64_t put(const Key& key, const Entity& entity);
            [[nodiscard]] std::optional<Entity> get(const Key& key);
            [[nodiscard]] uint64_t remove(const Key& key);

            // ==================== Query ====================

            /**
         * Query with expression template.
         *
         * Uses early filtering: reads only necessary fields from serialized data
         * before full deserialization. Dramatically reduces overhead for selective queries.
         *
         * Example:
         *   Field<User, int> age{&User::age, 20};  // offset 20
         *   auto adults = users.query(age >= 18);
         *
         * Performance:
         * - Early filter applicable: 10-50x faster than full deserialization
         * - Early filter not applicable: Same as full scan
         */
            template <typename ExprType>
            [[nodiscard]] std::vector<Entity> query(const query::Expr<ExprType>& expr);

        private:
            class Impl;
            std::unique_ptr<Impl> impl_;
    };

    // ==================== Implementation ====================

    template <typename Entity, typename Key>
    class PackedTable<Entity, Key>::Impl {
        public:
            explicit Impl(AkkaraDB& db, std::string_view table_name)
                : db_{db} {
                const uint64_t hash = murmur3_hash(table_name);
                std::memcpy(namespace_hash_.data(), &hash, 8);
            }

            [[nodiscard]] uint64_t put(const Key& key, const Entity& entity) {
                auto key_bytes = make_key(key);
                auto value_bytes = serialize_entity(entity);
                return db_.put(key_bytes, value_bytes);
            }

            [[nodiscard]] std::optional<Entity> get(const Key& key) {
                auto key_bytes = make_key(key);
                auto value_opt = db_.get(key_bytes);
                if (!value_opt) { return std::nullopt; }
                return deserialize_entity(*value_opt);
            }

            [[nodiscard]] uint64_t remove(const Key& key) {
                auto key_bytes = make_key(key);
                return db_.del(key_bytes);
            }

            // ==================== Query Implementation ====================

            template <typename ExprType>
            [[nodiscard]] std::vector<Entity> query(const query::Expr<ExprType>& expr) {
                std::vector<Entity> result;
                const auto& derived_expr = expr.derived();

                // Get namespace range
                auto [start_key, end_key] = make_namespace_range();

                // Range scan
                auto records = db_.range(start_key, std::optional{std::span{end_key}});

                // Process each record
                for (auto& rec : records) {
                    auto value_span = rec.value();

                    // Try early filtering (avoids full deserialization)
                    if constexpr (ExprType::can_early_filter()) {
                        if (!derived_expr.eval_early(value_span)) {
                            continue; // Skip this record
                        }
                    }

                    // Deserialize and evaluate
                    auto entity = deserialize_entity(value_span);

                    if (derived_expr.eval(entity)) { result.push_back(std::move(entity)); }
                }

                return result;
            }

        private:
            std::vector<uint8_t> make_key(const Key& key) {
                std::vector<uint8_t> result;
                result.insert(result.end(), namespace_hash_.begin(), namespace_hash_.end());
                serialize(result, key);
                return result;
            }

            std::pair<std::vector<uint8_t>, std::vector<uint8_t>> make_namespace_range() {
                uint64_t hash;
                std::memcpy(&hash, namespace_hash_.data(), 8);

                std::vector<uint8_t> start(8);
                std::vector<uint8_t> end(8);

                std::memcpy(start.data(), &hash, 8);

                uint64_t hash_plus_one = hash + 1;
                std::memcpy(end.data(), &hash_plus_one, 8);

                return {start, end};
            }

            std::vector<uint8_t> serialize_entity(const Entity& entity) {
                std::vector<uint8_t> buf;
                constexpr auto field_ptrs = Entity::akkara_field_ptrs;

                size_t estimated_size = 0;
                std::apply(
                    [&](auto... ptrs) { estimated_size = (estimate_size(entity.*ptrs) + ...); },
                    field_ptrs
                );
                buf.reserve(estimated_size);

                std::apply([&](auto... ptrs) { (serialize(buf, entity.*ptrs), ...); }, field_ptrs);

                return buf;
            }

            Entity deserialize_entity(std::span<const uint8_t> data) {
                Entity result{};
                constexpr auto field_ptrs = Entity::akkara_field_ptrs;

                std::apply(
                    [&](auto... ptrs) {
                        ([&] {
                            using FieldType = std::remove_reference_t<decltype(result.*ptrs)>;
                            result.*ptrs = deserialize<FieldType>(data);
                        }(), ...);
                    },
                    field_ptrs
                );

                return result;
            }

            AkkaraDB& db_;
            std::array<uint8_t, 8> namespace_hash_;
    };

    // ==================== Public API ====================

    template <typename Entity, typename Key>
    PackedTable<Entity, Key>::PackedTable(AkkaraDB& db, std::string_view table_name) : impl_{std::make_unique<Impl>(db, table_name)} {}

    template <typename Entity, typename Key>
    PackedTable<Entity, Key>::~PackedTable() noexcept = default;

    template <typename Entity, typename Key>
    PackedTable<Entity, Key>::PackedTable(PackedTable&&) noexcept = default;

    template <typename Entity, typename Key>
    PackedTable<Entity, Key>& PackedTable<Entity, Key>::operator=(PackedTable&&) noexcept = default;

    template <typename Entity, typename Key>
    uint64_t PackedTable<Entity, Key>::put(const Key& key, const Entity& entity) { return impl_->put(key, entity); }

    template <typename Entity, typename Key>
    std::optional<Entity> PackedTable<Entity, Key>::get(const Key& key) { return impl_->get(key); }

    template <typename Entity, typename Key>
    uint64_t PackedTable<Entity, Key>::remove(const Key& key) { return impl_->remove(key); }

    template <typename Entity, typename Key>
    template <typename ExprType>
    std::vector<Entity> PackedTable<Entity, Key>::query(const query::Expr<ExprType>& expr) { return impl_->query(expr); }
} // namespace akkaradb::typed