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

// akkaradb/PackedTable.hpp
#pragma once

#include "Export.hpp"
#include "binpack/BinPack.hpp"
#include "binpack/detail/MemberPtrTraits.hpp"
#include "detail/Hash.hpp"
#include "engine/AkkEngine.hpp"

#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace akkaradb {
    // =============================================================================
    // PackedTable<PrimaryKeyPtr>
    // =============================================================================
    //
    // Type-safe, namespace-isolated KV table backed by AkkEngine.
    //
    // Template parameter:
    //   PrimaryKeyPtr — a member object pointer identifying the primary key field.
    //                   The entity type and key type are inferred automatically.
    //
    // Basic usage:
    //
    //   struct User {
    //       uint64_t    id;
    //       std::string name;
    //       int32_t     age;
    //   };
    //
    //   // Open — no serializer needed
    //   auto users = db->table<&User::id>("users");
    //
    //   // Optional secondary indexes
    //   auto users = db->table<&User::id>("users")
    //                    .index<&User::name>()
    //                    .index<&User::age>();
    //
    //   // CRUD
    //   users.put({1, "Alice", 25});
    //   auto alice = users.get(1ULL);          // → std::optional<User>
    //   users.remove(1ULL);
    //
    //   // Secondary index lookup
    //   auto u = users.find_by<&User::name>("Alice"); // → std::optional<User>
    //
    // Serialization:
    //   Entity fields are serialized by BinPack in declaration order (Boost.PFR).
    //   All primitive types, std::string, std::optional<T>, std::vector<T>,
    //   std::map<K,V>, enum, and nested aggregate structs are supported with
    //   zero configuration. For custom types, specialize TypeAdapter<T>.
    //
    // Key layout:
    //   Primary:         [8B table prefix][encoded PK]  → BinPack(entity)
    //   Secondary index: [8B index prefix][encoded field value]  → BinPack(pk)
    //
    //   Both prefixes are FNV-1a hashes: table prefix = hash(table_name),
    //   index prefix = hash(table_name + ":index:" + str(field_byte_offset)).
    //   This makes them stable across runs for the same binary + struct layout.
    //
    // Thread-safety:
    //   PackedTable is NOT thread-safe. AkkEngine IS thread-safe;
    //   use one PackedTable per thread or synchronize externally.
    //
    // Secondary index notes:
    //   - Indexes are unique: one entity per field value. For non-unique fields
    //     use find_all_by() with a range scan (a future extension).
    //   - put() fetches the old entity to clean up stale index entries before
    //     writing new ones. This incurs one extra get() per put() when indexes
    //     are registered.
    //   - remove() fetches the old entity to clean up index entries.
    //     If the entity is not found, remove() is a no-op.

    template <auto PrimaryKeyPtr>
    class PackedTable {
        public:
            using Entity = binpack::detail::class_of<PrimaryKeyPtr>;
            using PK = binpack::detail::member_of<PrimaryKeyPtr>;

            // ── Construction (via AkkaraDB::table<PrimaryKeyPtr>(name)) ──────────────

            PackedTable(PackedTable&&) noexcept = default;
            PackedTable& operator=(PackedTable&&) noexcept = default;
            PackedTable(const PackedTable&) = delete;
            PackedTable& operator=(const PackedTable&) = delete;

            // ── Index builder ─────────────────────────────────────────────────────────

            /**
             * Registers a secondary index on the given field.
             *
             * Multiple indexes may be chained:
             *   auto users = db->table<&User::id>("users")
             *                    .index<&User::email>()
             *                    .index<&User::age>();
             *
             * Constraints:
             *   - FieldPtr must be a member of Entity (enforced at compile time).
             *   - Indexes are unique: find_by<FieldPtr>(value) returns at most one entity.
             *     If uniqueness is not guaranteed, the last put() for a given field value wins.
             *
             * @return *this (as rvalue ref) for fluent chaining.
             */
            template <auto FieldPtr>
            PackedTable&& index() && {
                static_assert(std::is_same_v<binpack::detail::class_of<FieldPtr>, Entity>, "index() field must be a member of the entity type");

                const size_t offset = binpack::detail::field_byte_offset<FieldPtr>();
                const auto prefix = make_index_prefix(table_name_, offset);

                indexes_.push_back(
                    {
                        prefix,
                        /*registered_offset=*/
                        offset,
                        [](const Entity& e) -> std::vector<uint8_t> {
                            using Field = binpack::detail::member_of<FieldPtr>;
                            return binpack::BinPack::encode<Field>(e.*FieldPtr);
                        }
                    }
                );
                return std::move(*this);
            }

            // ── CRUD ──────────────────────────────────────────────────────────────────

            /**
             * Inserts or replaces the entity.
             *
             * When secondary indexes are registered, the old entity is fetched first
             * to remove stale index entries before writing the new ones.
             */
            void put(const Entity& entity) {
                const PK& pk = entity.*PrimaryKeyPtr;
                const auto storage_key = make_pk_key(pk);
                const auto value = binpack::BinPack::encode(entity);

                if (!indexes_.empty()) {
                    // Clean up stale secondary index entries for the old version.
                    auto old_bytes = engine_->get(storage_key);
                    if (old_bytes) {
                        const auto old_entity = binpack::BinPack::decode<Entity>(*old_bytes);
                        remove_index_entries(old_entity);
                    }
                }

                engine_->put(storage_key, value);

                if (!indexes_.empty()) { write_index_entries(entity, pk); }
            }

            /**
             * Returns the entity for the given primary key, or nullopt if not found.
             */
            [[nodiscard]] std::optional<Entity> get(const PK& pk) const {
                auto bytes = engine_->get(make_pk_key(pk));
                if (!bytes) return std::nullopt;
                return binpack::BinPack::decode<Entity>(*bytes);
            }

            /**
             * Deletes the entity for the given primary key.
             * Also removes all associated secondary index entries.
             */
            void remove(const PK& pk) {
                const auto storage_key = make_pk_key(pk);

                if (!indexes_.empty()) {
                    auto old_bytes = engine_->get(storage_key);
                    if (old_bytes) {
                        const auto old_entity = binpack::BinPack::decode<Entity>(*old_bytes);
                        remove_index_entries(old_entity);
                    }
                }

                engine_->remove(storage_key);
            }

            /**
             * Returns true if the entity exists (no value copy, no BlobManager I/O).
             */
            [[nodiscard]] bool exists(const PK& pk) const { return engine_->exists(make_pk_key(pk)); }

            /**
             * Atomically get-or-default + update + put.
             *
             * Retrieves the current entity (or default-constructs one if absent),
             * passes a mutable reference to the update callback, then puts it back.
             *
             * Requires Entity to be default-constructible.
             */
            void upsert(const PK& pk, std::function<void(Entity&)> update) {
                Entity entity = get(pk).value_or(Entity{});
                update(entity);
                put(entity);
            }

            // ── Secondary index lookup ────────────────────────────────────────────────

            /**
             * Looks up an entity by a secondary index field.
             *
             * Requires that .index<FieldPtr>() was called during construction.
             * Throws std::runtime_error if the index was not registered.
             *
             * Returns the entity whose FieldPtr field equals value, or nullopt.
             * If multiple entities share the same field value (non-unique index),
             * the result is the last one written.
             */
            template <auto FieldPtr>
            [[nodiscard]] std::optional<Entity> find_by(const binpack::detail::member_of<FieldPtr>& value) const {
                static_assert(std::is_same_v<binpack::detail::class_of<FieldPtr>, Entity>, "find_by() field must be a member of the entity type");

                const size_t offset = binpack::detail::field_byte_offset<FieldPtr>();
                const auto prefix = make_index_prefix(table_name_, offset);

                // Verify the index was registered (guard against silent misuse).
                bool found = false;
                for (const auto& idx : indexes_) {
                    if (idx.registered_offset == offset) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw std::runtime_error(
                        "PackedTable::find_by: index not registered for this field. " "Call .index<&" + table_name_ + "::field>() during table construction."
                    );
                }

                using Field = binpack::detail::member_of<FieldPtr>;
                const auto idx_key = make_index_key(prefix, binpack::BinPack::encode<Field>(value));
                const auto pk_bytes = engine_->get(idx_key);
                if (!pk_bytes) return std::nullopt;

                const PK pk = binpack::BinPack::decode<PK>(*pk_bytes);
                return get(pk);
            }

            // ── Count ─────────────────────────────────────────────────────────────────

            /**
             * Returns the total number of live entities in this table.
             * Skips value deserialization and blob I/O.
             */
            [[nodiscard]] size_t count() const {
                auto start = std::vector<uint8_t>(pk_prefix_.begin(), pk_prefix_.end());
                auto end = std::vector<uint8_t>(pk_prefix_.begin(), pk_prefix_.end());
                detail::increment_be64(end.data());
                return engine_->count(start, end);
            }

            // ── Range scan ────────────────────────────────────────────────────────────

            /**
             * Forward iterator over PackedTable entries in primary-key ascending order.
             * Tombstones are suppressed; only live entities are yielded.
             */
            class Iterator {
                public:
                    struct Entry {
                        PK id;
                        Entity value;
                    };

                    Iterator(Iterator&&) noexcept = default;
                    Iterator& operator=(Iterator&&) noexcept = default;
                    Iterator(const Iterator&) = delete;
                    Iterator& operator=(const Iterator&) = delete;

                    [[nodiscard]] bool has_next() const noexcept { return has_next_; }

                    /**
                     * Returns the current entry and advances the iterator.
                     * Precondition: has_next() == true.
                     */
                    [[nodiscard]] Entry next() {
                        auto [raw_key, raw_val] = *pending_;

                        // Decode PK: skip ns_prefix (8 bytes), rest is encoded PK
                        auto pk_span = std::span<const uint8_t>{raw_key}.subspan(8);
                        Entry e{binpack::BinPack::decode<PK>(pk_span), binpack::BinPack::decode<Entity>(raw_val),};
                        advance();
                        return e;
                    }

                private:
                    friend class PackedTable;
                    using RawEntry = std::pair<std::vector<uint8_t>, std::vector<uint8_t>>;

                    engine::AkkEngine::ScanIterator iter_;
                    std::array<uint8_t, 8> ns_prefix_;
                    std::optional<RawEntry> pending_;
                    bool has_next_ = false;

                    Iterator(engine::AkkEngine::ScanIterator iter, std::array<uint8_t, 8> ns_prefix)
                        : iter_{std::move(iter)}, ns_prefix_{ns_prefix} { advance(); }

                    void advance() {
                        has_next_ = false;
                        pending_.reset();
                        while (iter_.has_next()) {
                            auto entry = iter_.next();
                            if (!entry) break;
                            if (entry->first.size() < 8 || std::memcmp(entry->first.data(), ns_prefix_.data(), 8) != 0) break;
                            pending_ = std::move(entry);
                            has_next_ = true;
                            return;
                        }
                    }
            };

            /**
             * Scans all entities in ascending primary key order.
             */
            [[nodiscard]] Iterator scan_all() const {
                auto start = std::vector<uint8_t>(pk_prefix_.begin(), pk_prefix_.end());
                auto end = std::vector<uint8_t>(pk_prefix_.begin(), pk_prefix_.end());
                detail::increment_be64(end.data());
                return Iterator(engine_->scan(start, end), pk_prefix_);
            }

            /**
             * Scans entities with PK >= start_pk, in ascending order.
             */
            [[nodiscard]] Iterator scan(const PK& start_pk) const {
                const auto start = make_pk_key(start_pk);
                auto end = std::vector<uint8_t>(pk_prefix_.begin(), pk_prefix_.end());
                detail::increment_be64(end.data());
                return Iterator(engine_->scan(start, end), pk_prefix_);
            }

            /**
             * Scans entities with start_pk <= PK < end_pk, in ascending order.
             */
            [[nodiscard]] Iterator scan(const PK& start_pk, const PK& end_pk) const {
                return Iterator(engine_->scan(make_pk_key(start_pk), make_pk_key(end_pk)), pk_prefix_);
            }

            // ── Query builder ─────────────────────────────────────────────────────────

            using Entry = typename Iterator::Entry;

            /**
             * Lazy query builder over this table.
             *
             * Usage:
             *   // range-for (lazy, zero intermediate allocation)
             *   for (const auto& [id, user] : users.query([](const User& u){ return u.age > 18; })) { ... }
             *
             *   // fluent builder
             *   auto vip_adults = users.query()
             *       .where([](const User& u){ return u.vip; })
             *       .where([](const User& u){ return u.age >= 18; })
             *       .limit(10);
             *
             *   // terminal shortcuts
             *   auto first = users.query([](const User& u){ return u.active; }).first();
             *   bool any   = users.query([](const User& u){ return u.age > 60; }).any();
             *   size_t n   = users.query([](const User& u){ return u.active; }).count();
             *   auto vec   = users.query([](const User& u){ return u.active; }).to_vector();
             */
            class Query {
                using ScanIter = Iterator;

                public:
                    // ── Lazy iterator ─────────────────────────────────────────────────────

                    class Iterator {
                        public:
                            struct Sentinel {};

                            using value_type = Entry;

                            [[nodiscard]] const Entry& operator*() const noexcept { return *current_; }
                            [[nodiscard]] const Entry* operator->() const noexcept { return &*current_; }

                            Iterator& operator++() {
                                advance();
                                return *this;
                            }

                            [[nodiscard]] bool operator!=(const Sentinel&) const noexcept { return current_.has_value(); }
                            [[nodiscard]] bool operator==(const Sentinel&) const noexcept { return !current_.has_value(); }

                        private:
                            friend class Query;

                            Iterator(ScanIter scan, std::function<bool(const Entity&)> pred, std::optional<size_t> limit) : scan_{std::move(scan)},
                                pred_{std::move(pred)},
                                limit_{limit} { advance(); }

                            void advance() {
                                current_.reset();
                                while (scan_.has_next()) {
                                    if (limit_ && matched_ >= *limit_) return;
                                    auto e = scan_.next();
                                    if (!pred_ || pred_(e.value)) {
                                        current_ = std::move(e);
                                        ++matched_;
                                        return;
                                    }
                                }
                            }

                            ScanIter scan_;
                            std::function<bool(const Entity&)> pred_;
                            std::optional<size_t> limit_;
                            size_t matched_ = 0;
                            std::optional<Entry> current_;
                    };

                    // ── Range interface ───────────────────────────────────────────────────

                    [[nodiscard]] Iterator begin() const { return Iterator(table_->scan_all(), predicate_, limit_); }
                    [[nodiscard]] typename Iterator::Sentinel end() const noexcept { return {}; }

                    // ── Builder ───────────────────────────────────────────────────────────

                    Query& where(std::function<bool(const Entity&)> pred) {
                        if (predicate_) {
                            auto prev = std::move(predicate_);
                            predicate_ = [prev = std::move(prev), pred](const Entity& e) { return prev(e) && pred(e); };
                        }
                        else { predicate_ = std::move(pred); }
                        return *this;
                    }

                    Query& limit(size_t n) {
                        limit_ = n;
                        return *this;
                    }

                    // ── Terminals ─────────────────────────────────────────────────────────

                    [[nodiscard]] std::optional<Entry> first() const {
                        auto it = begin();
                        if (it != end()) return *it;
                        return std::nullopt;
                    }

                    [[nodiscard]] bool any() const { return first().has_value(); }

                    [[nodiscard]] size_t count() const {
                        size_t n = 0;
                        Iterator it(table_->scan_all(), predicate_, std::nullopt);
                        while (it != typename Iterator::Sentinel{}) {
                            ++n;
                            ++it;
                        }
                        return n;
                    }

                    [[nodiscard]] std::vector<Entry> to_vector() const {
                        std::vector<Entry> result;
                        for (const auto& e : *this) result.push_back(e);
                        return result;
                    }

                private:
                    friend class PackedTable;
                    explicit Query(const PackedTable* table) : table_(table) {}

                    const PackedTable* table_;
                    std::function<bool(const Entity&)> predicate_;
                    std::optional<size_t> limit_;
            };

            [[nodiscard]] Query query() const { return Query(this); }
            [[nodiscard]] Query query(std::function<bool(const Entity&)> pred) const { return Query(this).where(std::move(pred)); }

            // ── Metadata ─────────────────────────────────────────────────────────────

            [[nodiscard]] std::string_view table_name() const noexcept { return table_name_; }
            [[nodiscard]] engine::AkkEngine& engine() noexcept { return *engine_; }
            [[nodiscard]] const engine::AkkEngine& engine() const noexcept { return *engine_; }

        private:
            friend class AkkaraDB;

            engine::AkkEngine* engine_ = nullptr;
            std::string table_name_;
            std::array<uint8_t, 8> pk_prefix_{};

            struct IndexDef {
                std::array<uint8_t, 8> prefix;
                size_t registered_offset; // for find_by validation
                std::function<std::vector<uint8_t>(const Entity&)> extract_key;
            };

            std::vector<IndexDef> indexes_;

            PackedTable() = default;

            // ── Key construction ──────────────────────────────────────────────────────

            [[nodiscard]] std::vector<uint8_t> make_pk_key(const PK& pk) const {
                std::vector<uint8_t> key(pk_prefix_.begin(), pk_prefix_.end());
                binpack::BinPack::encode_into(pk, key);
                return key;
            }

            [[nodiscard]] static std::vector<uint8_t> make_index_key(const std::array<uint8_t, 8>& prefix, const std::vector<uint8_t>& field_bytes) {
                std::vector<uint8_t> key(prefix.begin(), prefix.end());
                key.insert(key.end(), field_bytes.begin(), field_bytes.end());
                return key;
            }

            // ── Prefix helpers ────────────────────────────────────────────────────────

            [[nodiscard]] static std::array<uint8_t, 8> make_table_prefix(std::string_view name) {
                const uint64_t h = detail::fnv1a_64(name);
                std::array<uint8_t, 8> prefix{};
                detail::write_be64(h, prefix.data());
                return prefix;
            }

            [[nodiscard]] static std::array<uint8_t, 8> make_index_prefix(std::string_view table_name, size_t field_offset) {
                // Hash input: "<table_name>:index:<field_byte_offset>"
                // Field byte offset is stable for the same binary + struct layout.
                char buf[64];
                const int n = std::snprintf(buf, sizeof(buf), "%zu", field_offset);
                std::string input;
                input.reserve(table_name.size() + 8 + static_cast<size_t>(n));
                input.append(table_name);
                input.append(":index:");
                input.append(buf, static_cast<size_t>(n));
                const uint64_t h = detail::fnv1a_64(input);
                std::array<uint8_t, 8> prefix{};
                detail::write_be64(h, prefix.data());
                return prefix;
            }

            // ── Index entry helpers ───────────────────────────────────────────────────

            void write_index_entries(const Entity& entity, const PK& pk) {
                const auto pk_bytes = binpack::BinPack::encode(pk);
                for (const auto& idx : indexes_) {
                    const auto idx_key = make_index_key(idx.prefix, idx.extract_key(entity));
                    engine_->put(idx_key, pk_bytes);
                }
            }

            void remove_index_entries(const Entity& entity) {
                for (const auto& idx : indexes_) {
                    const auto idx_key = make_index_key(idx.prefix, idx.extract_key(entity));
                    engine_->remove(idx_key);
                }
            }
    };
} // namespace akkaradb
