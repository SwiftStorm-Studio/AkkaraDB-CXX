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

#include "Codec.hpp"
#include "detail/Hash.hpp"
#include "engine/AkkEngine.hpp"
#include <array>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace akkaradb {

    /**
     * PackedTable<T, ID> — Type-safe, namespace-isolated ORM layer over AkkEngine.
     *
     * Each table occupies a dedicated 8-byte namespace prefix in the key space,
     * derived deterministically from the table name via FNV-1a 64-bit hash.
     * Multiple tables can share the same AkkEngine without key collisions.
     *
     * Key layout on disk:
     *   [8 bytes: ns_prefix (FNV-1a of table name)] [encoded ID bytes]
     *
     * ID encoding:
     *   Integer types are stored big-endian (sign-bit flipped for signed), so
     *   lexicographic key order matches numeric ID order and range scans work
     *   correctly for both ascending iteration and bounded range queries.
     *
     * Thread-safety: PackedTable is NOT thread-safe. The underlying AkkEngine IS
     *   thread-safe; create one PackedTable per thread, or synchronize externally.
     *
     * --- Creating a table ---
     *
     * Option A — specialize EntityCodec<T> (zero-lambda overhead):
     *
     *   template<> struct akkaradb::EntityCodec<User> {
     *       static void serialize(const User& u, std::vector<uint8_t>& out) { ... }
     *       static User deserialize(std::span<const uint8_t> b) { return ...; }
     *   };
     *   auto tbl = PackedTable<User, uint64_t>::open(engine, "users");
     *
     * Option B — pass lambdas directly (no specialization required):
     *
     *   auto tbl = PackedTable<User, uint64_t>::open(engine, "users",
     *       [](const User& u, auto& out) { <<serialize>> }
     *       [](std::span<const uint8_t> b) { return User{ <<parse>> }; }
     *   );
     */
    template<typename T, typename ID>
    class PackedTable {
    public:
        // ── Codec function types ─────────────────────────────────────────────

        using Serialize   = std::function<void(const T&, std::vector<uint8_t>&)>;
        using Deserialize = std::function<T(std::span<const uint8_t>)>;
        using EncodeId    = std::function<void(const ID&, std::vector<uint8_t>&)>;
        using DecodeId    = std::function<ID(std::span<const uint8_t>)>;

        struct Codec {
            Serialize   serialize;
            Deserialize deserialize;
            EncodeId    encode_id;
            DecodeId    decode_id;
        };

        // ── Factory ──────────────────────────────────────────────────────────

        /**
         * Opens a table using explicit codec functions (Option B).
         *
         * encode_id / decode_id default to the built-in IdCodec<ID> specialization.
         * If IdCodec<ID> is not specialized for your ID type, pass them explicitly.
         *
         * @param engine     Owning engine (must outlive this PackedTable).
         * @param table_name Unique name for this table; determines namespace prefix.
         * @param codec      Full codec bundle.
         */
        [[nodiscard]] static std::unique_ptr<PackedTable>
        open(engine::AkkEngine& engine, std::string table_name, Codec codec) {
            auto tbl           = std::unique_ptr<PackedTable>(new PackedTable());
            tbl->engine_       = &engine;
            tbl->table_name_   = std::move(table_name);
            tbl->codec_        = std::move(codec);
            const uint64_t h   = detail::fnv1a_64(tbl->table_name_);
            detail::write_be64(h, tbl->ns_prefix_.data());
            return tbl;
        }

        /**
         * Opens a table using entity lambdas + built-in IdCodec<ID> (Option B, short form).
         *
         * Requires IdCodec<ID> to be specialized (all built-in integer/string types qualify).
         */
        [[nodiscard]] static std::unique_ptr<PackedTable>
        open(engine::AkkEngine& engine,
             std::string        table_name,
             Serialize          serialize,
             Deserialize        deserialize)
        {
            Codec c{
                std::move(serialize),
                std::move(deserialize),
                [](const ID& id, std::vector<uint8_t>& out) { IdCodec<ID>::encode(id, out); },
                [](std::span<const uint8_t> b)              { return IdCodec<ID>::decode(b); },
            };
            return open(engine, std::move(table_name), std::move(c));
        }

        /**
         * Opens a table using EntityCodec<T> + IdCodec<ID> specializations (Option A).
         *
         * Requires both EntityCodec<T> and IdCodec<ID> to be fully specialized.
         */
        [[nodiscard]] static std::unique_ptr<PackedTable>
        open(engine::AkkEngine& engine, std::string table_name)
        {
            return open(engine, std::move(table_name),
                [](const T& e, std::vector<uint8_t>& out) { EntityCodec<T>::serialize(e, out); },
                [](std::span<const uint8_t> b)            { return EntityCodec<T>::deserialize(b); });
        }

        // ── CRUD ─────────────────────────────────────────────────────────────

        /**
         * Inserts or replaces the entity for the given ID.
         */
        void put(const ID& id, const T& entity) {
            const auto key = make_key(id);
            std::vector<uint8_t> val;
            codec_.serialize(entity, val);
            engine_->put(key, val);
        }

        /**
         * Returns the entity for the given ID, or nullopt if absent/deleted.
         */
        [[nodiscard]] std::optional<T> get(const ID& id) const {
            const auto key    = make_key(id);
            auto bytes_opt    = engine_->get(key);
            if (!bytes_opt)   return std::nullopt;
            return codec_.deserialize(*bytes_opt);
        }

        /**
         * Deletes the entity for the given ID (inserts a tombstone).
         */
        void remove(const ID& id) {
            const auto key = make_key(id);
            engine_->remove(key);
        }

        /**
         * Atomically get-or-default + update + put.
         *
         * Retrieves the current entity (or default-constructs one if absent),
         * passes a mutable reference to the update callback, then puts it back.
         *
         * @param id     Key to update.
         * @param update Called with the entity (existing or default-constructed).
         *               Modify it in place; changes are persisted on return.
         */
        void upsert(const ID& id, std::function<void(T&)> update) {
            T entity = get(id).value_or(T{});
            update(entity);
            put(id, entity);
        }

        // ── Iterator ─────────────────────────────────────────────────────────

        /**
         * A record yielded by a PackedTable range scan.
         */
        struct Entry {
            ID id;
            T  value;
        };

        /**
         * Forward iterator over a namespace-scoped range.
         *
         * Wraps AkkEngine::ScanIterator; skips keys outside the namespace
         * (should not happen under normal usage, but guarded for safety).
         * Tombstones are invisible (AkkEngine suppresses them).
         */
        class Iterator {
        public:
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
                // Decode: skip ns_prefix (8 bytes), rest is encoded ID
                const std::span<const uint8_t> id_bytes{
                    raw_key.data() + 8,
                    raw_key.size() - 8
                };
                Entry e{
                    decode_id_(id_bytes),
                    deserialize_(raw_val)
                };
                advance();
                return e;
            }

        private:
            friend class PackedTable;

            using RawEntry = std::pair<std::vector<uint8_t>, std::vector<uint8_t>>;

            engine::AkkEngine::ScanIterator               iter_;
            DecodeId                                      decode_id_;
            Deserialize                                   deserialize_;
            std::array<uint8_t, 8>                        ns_prefix_;
            std::optional<RawEntry>                       pending_;
            bool                                          has_next_ = false;

            Iterator(engine::AkkEngine::ScanIterator iter,
                     DecodeId                        decode_id,
                     Deserialize                     deserialize,
                     std::array<uint8_t, 8>          ns_prefix)
                : iter_       {std::move(iter)}
                , decode_id_  {std::move(decode_id)}
                , deserialize_{std::move(deserialize)}
                , ns_prefix_  {ns_prefix}
            {
                advance();
            }

            void advance() {
                has_next_ = false;
                pending_.reset();
                while (iter_.has_next()) {
                    auto entry = iter_.next();
                    if (!entry) break;
                    // Verify namespace prefix (safety guard)
                    if (entry->first.size() < 8 ||
                        std::memcmp(entry->first.data(), ns_prefix_.data(), 8) != 0)
                    {
                        break; // left namespace — stop
                    }
                    pending_ = std::move(entry);
                    has_next_ = true;
                    return;
                }
            }
        };

        // ── Query builder ────────────────────────────────────────────────────

        /**
         * Fluent, lazy query builder. Query itself IS the range — no execute() needed.
         *
         * Iteration is lazy: entries are filtered one at a time as you advance,
         * so only one Entry lives in memory at a time (no intermediate vector).
         *
         * Usage:
         *   // Range-for (lazy, zero intermediate allocation)
         *   for (const auto& [id, user] : users.query([](const User& u) { return u.age > 19; })) {
         *       process(user);
         *   }
         *
         *   // Multiple where() compose with AND
         *   auto q = users.query()
         *                 .where([](const User& u) { return u.vip; })
         *                 .where([](const User& u) { return u.age >= 18; })
         *                 .limit(10);
         *   for (const auto& [id, user] : q) { ... }
         *
         *   // Terminal shortcuts
         *   auto first_vip  = users.query([](const User& u) { return u.vip; }).first();
         *   bool has_adults = users.query([](const User& u) { return u.age >= 18; }).any();
         *   size_t n        = users.query([](const User& u) { return u.active; }).count();
         *
         *   // Collect into vector when needed
         *   auto vec = users.query([](const User& u) { return u.active; }).to_vector();
         */
        class Query {
            // Alias to avoid shadowing PackedTable::Iterator inside this nested class.
            using ScanIter = typename PackedTable::Iterator;

        public:
            // ── Lazy iterator ─────────────────────────────────────────────────

            /**
             * Lazy forward iterator: advances through the underlying scan,
             * skipping entries that do not satisfy the predicate, stopping at limit.
             */
            class Iterator {
            public:
                // Sentinel type for range end.
                struct Sentinel {};

                using value_type = Entry;

                [[nodiscard]] const Entry& operator*()  const noexcept { return *current_; }
                [[nodiscard]] const Entry* operator->() const noexcept { return &*current_; }

                Iterator& operator++() { advance(); return *this; }

                [[nodiscard]] bool operator!=(const Sentinel&) const noexcept {
                    return current_.has_value();
                }
                [[nodiscard]] bool operator==(const Sentinel&) const noexcept {
                    return !current_.has_value();
                }

            private:
                friend class Query;

                Iterator(ScanIter                         scan,
                         std::function<bool(const T&)>    pred,
                         std::optional<size_t>            limit)
                    : scan_   {std::move(scan)}
                    , pred_   {std::move(pred)}
                    , limit_  {limit}
                {
                    advance(); // position at first matching entry
                }

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

                ScanIter                       scan_;
                std::function<bool(const T&)>  pred_;
                std::optional<size_t>          limit_;
                size_t                         matched_ = 0;
                std::optional<Entry>           current_;
            };

            // ── Range interface (range-for support) ───────────────────────────

            [[nodiscard]] Iterator begin() const {
                return Iterator(table_->scan_all(), predicate_, limit_);
            }
            [[nodiscard]] typename Iterator::Sentinel end() const noexcept {
                return {};
            }

            // ── Builder ───────────────────────────────────────────────────────

            /**
             * Adds a filter predicate. Multiple calls compose with AND.
             */
            Query& where(std::function<bool(const T&)> pred) {
                if (predicate_) {
                    auto prev = std::move(predicate_);
                    predicate_ = [prev = std::move(prev), pred](const T& t) {
                        return prev(t) && pred(t);
                    };
                } else {
                    predicate_ = std::move(pred);
                }
                return *this;
            }

            /**
             * Stops iteration after n matching entries.
             * Does not affect count() (which always counts all matches).
             */
            Query& limit(size_t n) { limit_ = n; return *this; }

            // ── Terminal shortcuts ────────────────────────────────────────────

            /** Returns the first matching entry, or nullopt. */
            [[nodiscard]] std::optional<Entry> first() const {
                auto it = begin();
                if (it != end()) return *it;
                return std::nullopt;
            }

            /** Returns true if at least one entry matches the predicate. */
            [[nodiscard]] bool any() const { return first().has_value(); }

            /**
             * Counts all matching entries. Ignores limit().
             * Iterates the full table — use PackedTable::count() for unfiltered total.
             */
            [[nodiscard]] size_t count() const {
                size_t n = 0;
                // Bypass limit_ so we count everything that matches.
                Iterator it(table_->scan_all(), predicate_, std::nullopt);
                while (it != typename Iterator::Sentinel{}) { ++n; ++it; }
                return n;
            }

            /** Collects all matching entries (up to limit) into a vector. */
            [[nodiscard]] std::vector<Entry> to_vector() const {
                std::vector<Entry> result;
                for (const auto& e : *this) result.push_back(e);
                return result;
            }

        private:
            friend class PackedTable;
            explicit Query(const PackedTable* table) : table_(table) {}

            const PackedTable*            table_;
            std::function<bool(const T&)> predicate_; ///< nullptr = no filter
            std::optional<size_t>         limit_;
        };

        /**
         * Returns a Query builder with no filter applied.
         * Chain .where() / .limit() before calling a terminal operation.
         */
        [[nodiscard]] Query query() const { return Query(this); }

        /**
         * Returns a Query builder with the given predicate pre-applied.
         * Equivalent to query().where(pred).
         */
        [[nodiscard]] Query query(std::function<bool(const T&)> pred) const {
            return Query(this).where(std::move(pred));
        }

        // ── Existence / Count ────────────────────────────────────────────────

        /**
         * Returns true if the given ID has a live (non-deleted) record.
         * Avoids value copy and blob I/O — significantly faster than get().
         */
        [[nodiscard]] bool exists(const ID& id) const {
            return engine_->exists(make_key(id));
        }

        /**
         * Returns the total number of live entries in this table.
         * Skips value deserialization and blob I/O.
         */
        [[nodiscard]] size_t count() const {
            std::vector<uint8_t> start(ns_prefix_.begin(), ns_prefix_.end());
            std::vector<uint8_t> end  (ns_prefix_.begin(), ns_prefix_.end());
            detail::increment_be64(end.data());
            return engine_->count(start, end);
        }

        // ── Range scan ───────────────────────────────────────────────────────

        /**
         * Scans all entries in this table in ascending ID order.
         */
        [[nodiscard]] Iterator scan_all() const {
            // start = ns_prefix, end = ns_prefix + 1
            std::vector<uint8_t> start(ns_prefix_.begin(), ns_prefix_.end());
            std::vector<uint8_t> end  (ns_prefix_.begin(), ns_prefix_.end());
            detail::increment_be64(end.data()); // end exclusive
            return Iterator(
                engine_->scan(start, end),
                codec_.decode_id,
                codec_.deserialize,
                ns_prefix_
            );
        }

        /**
         * Scans entries with ID >= start_id, in ascending order.
         */
        [[nodiscard]] Iterator scan(const ID& start_id) const {
            const auto start = make_key(start_id);
            std::vector<uint8_t> end(ns_prefix_.begin(), ns_prefix_.end());
            detail::increment_be64(end.data());
            return Iterator(
                engine_->scan(start, end),
                codec_.decode_id,
                codec_.deserialize,
                ns_prefix_
            );
        }

        /**
         * Scans entries with start_id <= ID < end_id, in ascending order.
         */
        [[nodiscard]] Iterator scan(const ID& start_id, const ID& end_id) const {
            const auto start = make_key(start_id);
            const auto end   = make_key(end_id);
            return Iterator(
                engine_->scan(start, end),
                codec_.decode_id,
                codec_.deserialize,
                ns_prefix_
            );
        }

        // ── Metadata ─────────────────────────────────────────────────────────

        [[nodiscard]] std::string_view table_name() const noexcept { return table_name_; }

        /// Returns the 8-byte namespace prefix used for all keys in this table.
        [[nodiscard]] std::span<const uint8_t> namespace_prefix() const noexcept {
            return ns_prefix_;
        }

        /// Direct access to the underlying engine (for advanced operations).
        [[nodiscard]] engine::AkkEngine& engine() noexcept { return *engine_; }
        [[nodiscard]] const engine::AkkEngine& engine() const noexcept { return *engine_; }

    private:
        engine::AkkEngine*     engine_     = nullptr;
        std::string            table_name_;
        std::array<uint8_t, 8> ns_prefix_{};
        Codec                  codec_;

        PackedTable() = default;

        /**
         * Builds the full storage key: [ns_prefix (8)] [encoded_id].
         */
        [[nodiscard]] std::vector<uint8_t> make_key(const ID& id) const {
            std::vector<uint8_t> key(ns_prefix_.begin(), ns_prefix_.end());
            codec_.encode_id(id, key);
            return key;
        }
    };

} // namespace akkaradb
