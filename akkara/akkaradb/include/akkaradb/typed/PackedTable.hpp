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
#include "Serialization.hpp"
#include <optional>
#include <string_view>
#include <memory>
#include <array>
#include <cstring>

/**
 * PackedTable - Type-safe CRUD interface for AkkaraDB.
 *
 * Provides high-level, type-safe operations on structured entities.
 * Implements namespace isolation using Murmur3 hash (JVM-compatible).
 *
 * Thread-safety: Thread-safe (delegates to AkkaraDB)
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
 *   users.put("u001", User{"u001", "Taro", 30});
 *   auto user = users.get("u001");
 */
namespace akkaradb::typed {
    [[nodiscard]] constexpr uint64_t rotl64(uint64_t x, int r) noexcept { return (x << r) | (x >> (64 - r)); }

    [[nodiscard]] constexpr uint64_t fmix64(uint64_t k) noexcept {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53ULL;
        k ^= k >> 33;
        return k;
    }

    /**
     * MurmurHash3_x64_128 (JVM-compatible).
     * Returns upper 64 bits (h1) only.
     *
     * @param str Input string (UTF-8)
     * @return 64-bit hash value
     */
    [[nodiscard]] inline uint64_t murmur3_hash(std::string_view str) noexcept {
        const auto* data = reinterpret_cast<const uint8_t*>(str.data());
        const size_t len = str.size();

        constexpr uint64_t seed = 0;
        uint64_t h1 = seed;
        uint64_t h2 = seed;

        constexpr uint64_t c1 = 0x87c37b91114253d5ULL;
        constexpr uint64_t c2 = 0x4cf5ad432745937fULL;

        // Process 16-byte blocks
        const size_t nblocks = len / 16;

        for (size_t i = 0; i < nblocks; i++) {
            uint64_t k1, k2;

            // Read 16 bytes (Little Endian)
            std::memcpy(&k1, data + i * 16, 8);
            std::memcpy(&k2, data + i * 16 + 8, 8);

            // Mix k1
            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;

            h1 = rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            // Mix k2
            k2 *= c2;
            k2 = rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;

            h2 = rotl64(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        // Process tail (remaining < 16 bytes)
        const auto* tail = data + nblocks * 16;
        const size_t remaining = len - nblocks * 16;

        uint64_t k1 = 0;
        uint64_t k2 = 0;

        // k2 (bytes 8-15)
        if (remaining >= 15) k2 ^= static_cast<uint64_t>(tail[14]) << 48;
        if (remaining >= 14) k2 ^= static_cast<uint64_t>(tail[13]) << 40;
        if (remaining >= 13) k2 ^= static_cast<uint64_t>(tail[12]) << 32;
        if (remaining >= 12) k2 ^= static_cast<uint64_t>(tail[11]) << 24;
        if (remaining >= 11) k2 ^= static_cast<uint64_t>(tail[10]) << 16;
        if (remaining >= 10) k2 ^= static_cast<uint64_t>(tail[9]) << 8;
        if (remaining >= 9)  k2 ^= static_cast<uint64_t>(tail[8]) << 0;

        // k1 (bytes 0-7)
        if (remaining >= 8)  k1 ^= static_cast<uint64_t>(tail[7]) << 56;
        if (remaining >= 7)  k1 ^= static_cast<uint64_t>(tail[6]) << 48;
        if (remaining >= 6)  k1 ^= static_cast<uint64_t>(tail[5]) << 40;
        if (remaining >= 5)  k1 ^= static_cast<uint64_t>(tail[4]) << 32;
        if (remaining >= 4)  k1 ^= static_cast<uint64_t>(tail[3]) << 24;
        if (remaining >= 3)  k1 ^= static_cast<uint64_t>(tail[2]) << 16;
        if (remaining >= 2)  k1 ^= static_cast<uint64_t>(tail[1]) << 8;
        if (remaining >= 1)  k1 ^= static_cast<uint64_t>(tail[0]) << 0;

        // Mix tail into hash
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

        // Finalization
        h1 ^= len;
        h2 ^= len;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        h2 += h1;

        // Return upper 64 bits only (matches JVM: hash[0])
        return h1;
    }

    /**
     * PackedTable - Type-safe table interface.
     *
     * Template parameters:
     *   Entity: Struct with AKKARA_TABLE macro
     *   Key: Primary key type (defaults to Entity::akkara_key_type)
     */
    template <typename Entity, typename Key = Entity::akkara_key_type>
    class PackedTable {
    public:
        /**
         * Constructs a PackedTable.
         *
         * @param db AkkaraDB instance
         * @param table_name Namespace name (typically class name)
         */
        explicit PackedTable(akkaradb::AkkaraDB& db, std::string_view table_name);

        /**
         * Destructor (no-op, uses RAII for cleanup).
         */
        ~PackedTable() noexcept;

        // Non-copyable, movable
        PackedTable(const PackedTable&) = delete;
        PackedTable& operator=(const PackedTable&) = delete;
        PackedTable(PackedTable&&) noexcept;
        PackedTable& operator=(PackedTable&&) noexcept;

        /**
         * Inserts or updates an entity.
         *
         * @param key Primary key
         * @param entity Entity to store
         * @return Sequence number
         */
        [[nodiscard]] uint64_t put(const Key& key, const Entity& entity);

        /**
         * Retrieves an entity by key.
         *
         * @param key Primary key
         * @return Entity if found, nullopt otherwise
         */
        [[nodiscard]] std::optional<Entity> get(const Key& key);

        /**
         * Deletes an entity.
         *
         * @param key Primary key
         * @return Sequence number
         */
        [[nodiscard]] uint64_t remove(const Key& key);

    private:
        class Impl;
        std::unique_ptr<Impl> impl_;
    };

    // ==================== Implementation ====================

    template <typename Entity, typename Key>
    class PackedTable<Entity, Key>::Impl {
    public:
        explicit Impl(AkkaraDB& db, std::string_view table_name) : db_{db} {
            // Compute namespace hash (JVM-compatible)
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

    private:
        std::vector<uint8_t> make_key(const Key& key) {
            std::vector<uint8_t> result;

            // Namespace hash (8 bytes)
            result.insert(result.end(),
                          namespace_hash_.begin(),
                          namespace_hash_.end()
            );

            // Key
            serialize(result, key);

            return result;
        }

        std::vector<uint8_t> serialize_entity(const Entity& entity) {
            std::vector<uint8_t> buf;

            // Get field pointers from FIELDS macro
            constexpr auto field_ptrs = Entity::akkara_field_ptrs;

            // Pre-allocate buffer (optimization)
            size_t estimated_size = 0;
            std::apply([&](auto... ptrs) { estimated_size = (estimate_size(entity.*ptrs) + ...); }, field_ptrs);
            buf.reserve(estimated_size);

            // Serialize each field
            std::apply([&](auto... ptrs) { (serialize(buf, entity.*ptrs), ...); }, field_ptrs);

            return buf;
        }

        Entity deserialize_entity(std::span<const uint8_t> data) {
            Entity result{};

            // Get field pointers from FIELDS macro
            constexpr auto field_ptrs = Entity::akkara_field_ptrs;

            // Deserialize each field in order
            std::apply([&](auto... ptrs) {
                ([&]() {
                    using FieldType = std::remove_reference_t<decltype(result.*ptrs)>;
                    result.*ptrs = deserialize<FieldType>(data);
                }(), ...);
            }, field_ptrs);

            return result;
        }

        akkaradb::AkkaraDB& db_;
        std::array<uint8_t, 8> namespace_hash_;
    };

    // ==================== Public API Implementation ====================

    template <typename Entity, typename Key>
    PackedTable<Entity, Key>::PackedTable(
        AkkaraDB& db,
        std::string_view table_name
    ) : impl_{std::make_unique<Impl>(db, table_name)} {}

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
} // namespace akkaradb::typed
