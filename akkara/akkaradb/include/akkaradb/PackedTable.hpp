/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable.
 */

#pragma once

#include "binpack/BinPack.hpp"
#include "binpack/detail/MemberPtrTraits.hpp"
#include "detail/Hash.hpp"
#include "engine/AkkEngine.hpp"
#include "core/buffer/BufferArena.hpp"
#include "core/record/KeyFingerprint.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace akkaradb {
    template <auto PrimaryKeyPtr>
    class PackedTable {
        class ArenaByteBuffer {
            public:
                ArenaByteBuffer() = default;
                explicit ArenaByteBuffer(core::BufferArena* arena) noexcept : arena_{arena} {}
                ArenaByteBuffer(const ArenaByteBuffer&) = default;
                ArenaByteBuffer(ArenaByteBuffer&&) noexcept = default;
                ArenaByteBuffer& operator=(ArenaByteBuffer&&) noexcept = default;

                ArenaByteBuffer& operator=(const ArenaByteBuffer& other) {
                    if (this != &other) {
                        clear();
                        insert(end(), other.begin(), other.end());
                    }
                    return *this;
                }

                void bind(core::BufferArena* arena) noexcept {
                    arena_ = arena;
                    release();
                }

                void release() noexcept {
                    data_ = nullptr;
                    size_ = 0;
                    capacity_ = 0;
                }

                void clear() noexcept { size_ = 0; }

                void reserve(size_t capacity) {
                    if (capacity <= capacity_) { return; }
                    ensure_arena();
                    std::byte* raw = arena_->allocate(capacity, alignof(uint8_t));
                    auto* next = reinterpret_cast<uint8_t*>(raw);
                    if (data_ != nullptr && size_ != 0) { std::memcpy(next, data_, size_); }
                    data_ = next;
                    capacity_ = capacity;
                }

                void resize(size_t size) {
                    const size_t old_size = size_;
                    reserve(size);
                    if (size > old_size) { std::memset(data_ + old_size, 0, size - old_size); }
                    size_ = size;
                }

                template <typename It>
                void assign(It first, It last) {
                    clear();
                    insert(end(), first, last);
                }

                template <typename It>
                void insert(uint8_t*, It first, It last) {
                    const size_t add = static_cast<size_t>(std::distance(first, last));
                    reserve(size_ + add);
                    for (; first != last; ++first) { data_[size_++] = static_cast<uint8_t>(*first); }
                }

                void push_back(uint8_t value) {
                    reserve(size_ + 1);
                    data_[size_++] = value;
                }

                [[nodiscard]] uint8_t* data() noexcept { return data_; }
                [[nodiscard]] const uint8_t* data() const noexcept { return data_; }
                [[nodiscard]] size_t size() const noexcept { return size_; }
                [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
                [[nodiscard]] uint8_t* begin() noexcept { return data_; }
                [[nodiscard]] uint8_t* end() noexcept { return data_ == nullptr ? nullptr : data_ + size_; }
                [[nodiscard]] const uint8_t* begin() const noexcept { return data_; }
                [[nodiscard]] const uint8_t* end() const noexcept { return data_ == nullptr ? nullptr : data_ + size_; }
                [[nodiscard]] uint8_t& operator[](size_t index) noexcept { return data_[index]; }
                [[nodiscard]] const uint8_t& operator[](size_t index) const noexcept { return data_[index]; }
                [[nodiscard]] operator std::span<const uint8_t>() const noexcept { return {data_, size_}; }

            private:
                void ensure_arena() const { if (arena_ == nullptr) { throw std::logic_error("PackedTable: arena buffer is not bound"); } }

                core::BufferArena* arena_ = nullptr;
                uint8_t* data_ = nullptr;
                size_t size_ = 0;
                size_t capacity_ = 0;
        };

        public:
            using Entity = binpack::detail::class_of<PrimaryKeyPtr>;
            using PK = binpack::detail::member_of<PrimaryKeyPtr>;

            struct Entry {
                PK id;
                Entity value;
            };

            PackedTable(PackedTable&&) noexcept = default;
            PackedTable& operator=(PackedTable&&) noexcept = default;
            PackedTable(const PackedTable&) = delete;
            PackedTable& operator=(const PackedTable&) = delete;

            template <auto FieldPtr>
            class Index;

            template <auto FieldPtr>
            [[nodiscard]] Index<FieldPtr> index() {
                static_assert(std::is_same_v<binpack::detail::class_of<FieldPtr>, Entity>, "index field must belong to the table entity");
                const std::string_view field_name = binpack::detail::member_name<FieldPtr>();
                const auto prefix = make_index_prefix(table_name_, field_name);

                for (const auto& idx : indexes_) { if (idx.field_name == field_name) { return Index<FieldPtr>{this, prefix}; } }

                indexes_.push_back(
                    IndexDef{
                        prefix,
                        std::string(field_name),
                        [](const Entity& entity, ArenaByteBuffer& out) {
                            out.clear();
                            binpack::BinPack::encode_into(entity.*FieldPtr, out);
                        }
                    }
                );
                return Index<FieldPtr>{this, prefix};
            }

            void put(const Entity& entity) {
                reset_temp_buffers();
                const PK& pk = entity.*PrimaryKeyPtr;
                make_pk_key(pk, pk_key_buffer_);

                if (!indexes_.empty()) {
                    std::span<const uint8_t> old_bytes;
                    if (engine_->get_into_arena(pk_key_buffer_, *temp_arena_, old_bytes)) {
                        const Entity old_entity = binpack::BinPack::decode<Entity>(old_bytes);
                        remove_index_entries(old_entity, pk_key_buffer_);
                    }
                }

                value_buffer_.clear();
                value_buffer_.reserve(binpack::BinPack::estimate_size(entity));
                binpack::BinPack::encode_into(entity, value_buffer_);

                put_hinted(pk_key_buffer_, value_buffer_);
                if (!indexes_.empty()) { write_index_entries(entity, pk_key_buffer_); }
            }

            [[nodiscard]] std::optional<Entity> get(const PK& pk) const {
                Entity out{};
                if (!get_into(pk, out)) { return std::nullopt; }
                return out;
            }

            [[nodiscard]] bool get_into(const PK& pk, Entity& out) const {
                reset_temp_buffers();
                make_pk_key(pk, pk_key_buffer_);
                std::span<const uint8_t> bytes;
                if (!engine_->get_into_arena(pk_key_buffer_, *temp_arena_, bytes)) { return false; }
                return binpack::BinPack::decode_into<Entity>(bytes, out);
            }

            void remove(const PK& pk) {
                reset_temp_buffers();
                make_pk_key(pk, pk_key_buffer_);

                if (!indexes_.empty()) {
                    std::span<const uint8_t> old_bytes;
                    if (engine_->get_into_arena(pk_key_buffer_, *temp_arena_, old_bytes)) {
                        const Entity old_entity = binpack::BinPack::decode<Entity>(old_bytes);
                        remove_index_entries(old_entity, pk_key_buffer_);
                    }
                }

                remove_hinted(pk_key_buffer_);
            }

            [[nodiscard]] bool exists(const PK& pk) const {
                reset_temp_buffers();
                make_pk_key(pk, pk_key_buffer_);
                return engine_->exists(pk_key_buffer_);
            }

            void upsert(const PK& pk, std::function<void(Entity&)> update) {
                Entity entity = get(pk).value_or(Entity{});
                entity.*PrimaryKeyPtr = pk;
                update(entity);
                put(entity);
            }

            template <auto FieldPtr>
            [[nodiscard]] std::optional<Entity> find_by(const binpack::detail::member_of<FieldPtr>& value) const {
                static_assert(std::is_same_v<binpack::detail::class_of<FieldPtr>, Entity>, "find_by field must belong to the table entity");
                const std::string_view field_name = binpack::detail::member_name<FieldPtr>();
                const auto prefix = make_index_prefix(table_name_, field_name);

                bool registered = false;
                for (const auto& idx : indexes_) {
                    if (idx.field_name == field_name) {
                        registered = true;
                        break;
                    }
                }
                if (!registered) { throw std::runtime_error("PackedTable::find_by: index is not registered for this field"); }

                reset_temp_buffers();
                field_buffer_.clear();
                binpack::BinPack::encode_into(value, field_buffer_);
                make_index_search_prefix(prefix, field_buffer_, scan_start_buffer_);
                scan_end_buffer_ = scan_start_buffer_;
                if (!detail::increment_be_bytes(scan_end_buffer_.data(), scan_end_buffer_.size())) { scan_end_buffer_.clear(); }

                auto iter = engine_->scan(scan_start_buffer_, scan_end_buffer_);
                while (iter.has_next()) {
                    auto raw = iter.next();
                    if (!raw) { return std::nullopt; }
                    const auto& key = raw->first;
                    if (key.size() <= scan_start_buffer_.size()) { continue; }

                    Entry entry;
                    const std::span<const uint8_t> pk_bytes{key.data() + scan_start_buffer_.size(), key.size() - scan_start_buffer_.size()};
                    if (get_by_pk_bytes(pk_bytes, entry)) { return entry.value; }
                }
                return std::nullopt;
            }

            [[nodiscard]] size_t count() const {
                reset_temp_buffers();
                make_prefix_start_end(pk_prefix_, scan_start_buffer_, scan_end_buffer_);
                return engine_->count(scan_start_buffer_, scan_end_buffer_);
            }

            class ScanRange {
                public:
                    ScanRange(ScanRange&&) noexcept = default;
                    ScanRange& operator=(ScanRange&&) noexcept = default;
                    ScanRange(const ScanRange&) = delete;
                    ScanRange& operator=(const ScanRange&) = delete;

                    [[nodiscard]] bool has_next() const noexcept { return pending_.has_value(); }

                    [[nodiscard]] Entry next() {
                        if (!pending_) { throw std::out_of_range("PackedTable::ScanRange: no next entry"); }
                        Entry out = std::move(*pending_);
                        advance();
                        return out;
                    }

                private:
                    friend class PackedTable;

                    ScanRange(const PackedTable* table, engine::AkkEngine::ScanIterator iter) : table_{table}, iter_{std::move(iter)} { advance(); }

                    void advance() {
                        pending_.reset();
                        while (iter_.has_next()) {
                            auto raw = iter_.next();
                            if (!raw) { return; }
                            const auto& key = raw->first;
                            if (key.size() < table_->pk_prefix_.size() || std::memcmp(key.data(), table_->pk_prefix_.data(), table_->pk_prefix_.size()) != 0) {
                                return;
                            }

                            std::span<const uint8_t> pk_bytes{key.data() + table_->pk_prefix_.size(), key.size() - table_->pk_prefix_.size()};
                            std::span<const uint8_t> value_bytes{raw->second.data(), raw->second.size()};
                            pending_ = Entry{binpack::BinPack::decode<PK>(pk_bytes), binpack::BinPack::decode<Entity>(value_bytes)};
                            return;
                        }
                    }

                    const PackedTable* table_;
                    engine::AkkEngine::ScanIterator iter_;
                    std::optional<Entry> pending_;
            };

            [[nodiscard]] ScanRange scan_all() const {
                reset_temp_buffers();
                make_prefix_start_end(pk_prefix_, scan_start_buffer_, scan_end_buffer_);
                return ScanRange{this, engine_->scan(scan_start_buffer_, scan_end_buffer_)};
            }

            [[nodiscard]] ScanRange scan(const PK& start_pk) const {
                reset_temp_buffers();
                make_pk_key(start_pk, scan_start_buffer_);
                scan_end_buffer_.assign(pk_prefix_.begin(), pk_prefix_.end());
                if (!detail::increment_be_bytes(scan_end_buffer_.data(), scan_end_buffer_.size())) { scan_end_buffer_.clear(); }
                return ScanRange{this, engine_->scan(scan_start_buffer_, scan_end_buffer_)};
            }

            [[nodiscard]] ScanRange scan(const PK& start_pk, const PK& end_pk) const {
                reset_temp_buffers();
                make_pk_key(start_pk, scan_start_buffer_);
                make_pk_key(end_pk, scan_end_buffer_);
                return ScanRange{this, engine_->scan(scan_start_buffer_, scan_end_buffer_)};
            }

            class Query {
                using TableScan = ScanRange;

                public:
                    class Iterator {
                        public:
                            struct Sentinel {};
                            using value_type = Entry;
                            using difference_type = std::ptrdiff_t;
                            using iterator_category = std::input_iterator_tag;

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

                            Iterator(TableScan scan, std::function<bool(const Entity&)> predicate, std::optional<size_t> limit)
                                : scan_{std::move(scan)}, predicate_{std::move(predicate)}, limit_{limit} { advance(); }

                            void advance() {
                                current_.reset();
                                while (scan_.has_next()) {
                                    if (limit_ && matched_ >= *limit_) { return; }
                                    auto entry = scan_.next();
                                    if (!predicate_ || predicate_(entry.value)) {
                                        current_ = std::move(entry);
                                        ++matched_;
                                        return;
                                    }
                                }
                            }

                            TableScan scan_;
                            std::function<bool(const Entity&)> predicate_;
                            std::optional<size_t> limit_;
                            size_t matched_ = 0;
                            std::optional<Entry> current_;
                    };

                    [[nodiscard]] Iterator begin() const { return Iterator{table_->scan_all(), predicate_, limit_}; }
                    [[nodiscard]] typename Iterator::Sentinel end() const noexcept { return {}; }

                    Query& where(std::function<bool(const Entity&)> predicate) {
                        if (predicate_) {
                            auto previous = std::move(predicate_);
                            predicate_ = [previous = std::move(previous), predicate = std::move(predicate)](const Entity& entity) {
                                return previous(entity) && predicate(entity);
                            };
                        }
                        else { predicate_ = std::move(predicate); }
                        return *this;
                    }

                    Query& limit(size_t n) {
                        limit_ = n;
                        return *this;
                    }

                    [[nodiscard]] std::optional<Entry> first() const {
                        auto it = begin();
                        if (it != end()) { return *it; }
                        return std::nullopt;
                    }

                    [[nodiscard]] bool any() const { return first().has_value(); }

                    [[nodiscard]] size_t count() const {
                        size_t n = 0;
                        auto it = begin();
                        while (it != end()) {
                            ++n;
                            ++it;
                        }
                        return n;
                    }

                    [[nodiscard]] std::vector<Entry> to_vector() const {
                        std::vector<Entry> out;
                        for (const auto& entry : *this) { out.push_back(entry); }
                        return out;
                    }

                private:
                    friend class PackedTable;
                    explicit Query(const PackedTable* table) : table_{table} {}

                    const PackedTable* table_;
                    std::function<bool(const Entity&)> predicate_;
                    std::optional<size_t> limit_;
            };

            template <typename Pred>
            class FilterView {
                using TableScan = ScanRange;

                public:
                    struct Sentinel {};

                    class Iterator {
                        public:
                            using value_type = Entry;
                            using difference_type = std::ptrdiff_t;
                            using iterator_category = std::input_iterator_tag;

                            [[nodiscard]] const Entry& operator*() const noexcept { return *current_; }
                            [[nodiscard]] const Entry* operator->() const noexcept { return &*current_; }

                            Iterator& operator++() {
                                advance();
                                return *this;
                            }

                            [[nodiscard]] bool operator!=(const Sentinel&) const noexcept { return current_.has_value(); }
                            [[nodiscard]] bool operator==(const Sentinel&) const noexcept { return !current_.has_value(); }

                        private:
                            friend class FilterView;

                            Iterator(TableScan scan, const Pred* predicate, std::optional<size_t> limit)
                                : scan_{std::move(scan)}, predicate_{predicate}, limit_{limit} { advance(); }

                            void advance() {
                                current_.reset();
                                while (scan_.has_next()) {
                                    if (limit_ && matched_ >= *limit_) { return; }
                                    auto entry = scan_.next();
                                    if ((*predicate_)(entry.value)) {
                                        current_ = std::move(entry);
                                        ++matched_;
                                        return;
                                    }
                                }
                            }

                            TableScan scan_;
                            const Pred* predicate_;
                            std::optional<size_t> limit_;
                            size_t matched_ = 0;
                            std::optional<Entry> current_;
                    };

                    [[nodiscard]] Iterator begin() const { return Iterator{table_->scan_all(), &predicate_, limit_}; }
                    [[nodiscard]] Sentinel end() const noexcept { return {}; }

                    FilterView& limit(size_t n) {
                        limit_ = n;
                        return *this;
                    }

                    [[nodiscard]] std::optional<Entry> first() const {
                        auto it = begin();
                        if (it != end()) { return *it; }
                        return std::nullopt;
                    }

                    [[nodiscard]] bool any() const { return first().has_value(); }

                    [[nodiscard]] size_t count() const {
                        size_t n = 0;
                        auto it = begin();
                        while (it != end()) {
                            ++n;
                            ++it;
                        }
                        return n;
                    }

                    [[nodiscard]] std::vector<Entry> to_vector() const {
                        std::vector<Entry> out;
                        for (const auto& entry : *this) { out.push_back(entry); }
                        return out;
                    }

                private:
                    friend class PackedTable;
                    FilterView(const PackedTable* table, Pred predicate) : table_{table}, predicate_{std::move(predicate)} {}

                    const PackedTable* table_;
                    Pred predicate_;
                    std::optional<size_t> limit_;
            };

            [[nodiscard]] Query query() const { return Query{this}; }

            template <typename Pred>
            [[nodiscard]] FilterView<std::decay_t<Pred>> query(Pred&& predicate) const {
                return FilterView<std::decay_t<Pred>>{this, std::forward<Pred>(predicate)};
            }

            [[nodiscard]] std::string_view table_name() const noexcept { return table_name_; }
            [[nodiscard]] engine::AkkEngine& engine() noexcept { return *engine_; }
            [[nodiscard]] const engine::AkkEngine& engine() const noexcept { return *engine_; }

            template <auto FieldPtr>
            class Index {
                public:
                    using Field = binpack::detail::member_of<FieldPtr>;

                    class FindRange {
                        public:
                            FindRange(FindRange&&) noexcept = default;
                            FindRange& operator=(FindRange&&) noexcept = default;
                            FindRange(const FindRange&) = delete;
                            FindRange& operator=(const FindRange&) = delete;

                            [[nodiscard]] bool has_next() const noexcept { return pending_.has_value(); }

                            [[nodiscard]] Entry next() {
                                if (!pending_) { throw std::out_of_range("PackedTable::Index::FindRange: no next entry"); }
                                Entry out = std::move(*pending_);
                                advance();
                                return out;
                            }

                        private:
                            friend class Index;

                            FindRange(PackedTable* table, size_t search_prefix_size, engine::AkkEngine::ScanIterator iter)
                                : table_{table}, search_prefix_size_{search_prefix_size}, iter_{std::move(iter)} { advance(); }

                            void advance() {
                                pending_.reset();
                                while (iter_.has_next()) {
                                    auto raw = iter_.next();
                                    if (!raw) { return; }
                                    const auto& key = raw->first;
                                    if (key.size() <= search_prefix_size_) { continue; }

                                    std::span pk_bytes{key.data() + search_prefix_size_, key.size() - search_prefix_size_};
                                    Entry entry;
                                    if (table_->get_by_pk_bytes(pk_bytes, entry)) {
                                        pending_ = std::move(entry);
                                        return;
                                    }
                                }
                            }

                            PackedTable* table_;
                            size_t search_prefix_size_;
                            engine::AkkEngine::ScanIterator iter_;
                            std::optional<Entry> pending_;
                    };

                    [[nodiscard]] FindRange find(const Field& value) const {
                        table_->reset_temp_buffers();
                        table_->field_buffer_.clear();
                        binpack::BinPack::encode_into(value, table_->field_buffer_);
                        table_->make_index_search_prefix(prefix_, table_->field_buffer_, table_->scan_start_buffer_);
                        table_->scan_end_buffer_ = table_->scan_start_buffer_;
                        if (!detail::increment_be_bytes(table_->scan_end_buffer_.data(), table_->scan_end_buffer_.size())) { table_->scan_end_buffer_.clear(); }
                        return FindRange{
                            table_,
                            table_->scan_start_buffer_.size(),
                            table_->engine_->scan(table_->scan_start_buffer_, table_->scan_end_buffer_)
                        };
                    }

                private:
                    friend class PackedTable;
                    Index(PackedTable* table, std::array<uint8_t, 8> prefix) : table_{table}, prefix_{prefix} {}

                    PackedTable* table_;
                    std::array<uint8_t, 8> prefix_;
            };

        private:
            friend class AkkaraDB;

            struct IndexDef {
                std::array<uint8_t, 8> prefix;
                std::string field_name;
                void (*encode_field)(const Entity&, ArenaByteBuffer&);
            };

            engine::AkkEngine* engine_ = nullptr;
            std::string table_name_;
            std::array<uint8_t, 8> pk_prefix_{};
            std::vector<IndexDef> indexes_;

            mutable std::unique_ptr<core::BufferArena> temp_arena_ = std::make_unique<core::BufferArena>();
            mutable ArenaByteBuffer pk_key_buffer_{temp_arena_.get()};
            mutable ArenaByteBuffer value_buffer_{temp_arena_.get()};
            mutable ArenaByteBuffer index_key_buffer_{temp_arena_.get()};
            mutable ArenaByteBuffer field_buffer_{temp_arena_.get()};
            mutable ArenaByteBuffer scan_start_buffer_{temp_arena_.get()};
            mutable ArenaByteBuffer scan_end_buffer_{temp_arena_.get()};

            PackedTable() = default;

            void reset_temp_buffers() const {
                temp_arena_->reset();
                pk_key_buffer_.bind(temp_arena_.get());
                value_buffer_.bind(temp_arena_.get());
                index_key_buffer_.bind(temp_arena_.get());
                field_buffer_.bind(temp_arena_.get());
                scan_start_buffer_.bind(temp_arena_.get());
                scan_end_buffer_.bind(temp_arena_.get());
            }

            void make_pk_key(const PK& pk, ArenaByteBuffer& out) const {
                out.clear();
                if constexpr (std::is_integral_v<PK> && sizeof(PK) <= 8) {
                    out.resize(8 + sizeof(PK));
                    std::memcpy(out.data(), pk_prefix_.data(), pk_prefix_.size());
                    uint64_t v = static_cast<uint64_t>(std::make_unsigned_t<PK>(pk));
                    for (size_t i = 0; i < sizeof(PK); ++i) { out[8 + i] = static_cast<uint8_t>(v >> ((sizeof(PK) - 1 - i) * 8)); }
                }
                else {
                    out.reserve(8 + binpack::BinPack::estimate_size(pk));
                    out.insert(out.end(), pk_prefix_.begin(), pk_prefix_.end());
                    binpack::BinPack::encode_into(pk, out);
                }
            }

            [[nodiscard]] bool get_by_pk_bytes(std::span<const uint8_t> pk_bytes, Entry& out) const {
                pk_key_buffer_.clear();
                pk_key_buffer_.reserve(8 + pk_bytes.size());
                pk_key_buffer_.insert(pk_key_buffer_.end(), pk_prefix_.begin(), pk_prefix_.end());
                pk_key_buffer_.insert(pk_key_buffer_.end(), pk_bytes.begin(), pk_bytes.end());

                std::span<const uint8_t> value_span;
                if (!engine_->get_into_arena(pk_key_buffer_, *temp_arena_, value_span)) { return false; }
                std::span pk_span{pk_bytes.data(), pk_bytes.size()};
                out = Entry{binpack::BinPack::decode<PK>(pk_span), binpack::BinPack::decode<Entity>(value_span)};
                return true;
            }

            void put_hinted(std::span<const uint8_t> key, std::span<const uint8_t> value) {
                engine_->put_hinted(key, value, compute_key_fp64(key), build_mini_key(key));
            }

            void remove_hinted(std::span<const uint8_t> key) { engine_->remove_hinted(key, compute_key_fp64(key), build_mini_key(key)); }

            static void make_prefix_start_end(const std::array<uint8_t, 8>& prefix, ArenaByteBuffer& start, ArenaByteBuffer& end) {
                start.assign(prefix.begin(), prefix.end());
                end.assign(prefix.begin(), prefix.end());
                if (!detail::increment_be_bytes(end.data(), end.size())) { end.clear(); }
            }

            static std::array<uint8_t, 8> make_table_prefix(std::string_view name) {
                std::array<uint8_t, 8> out{};
                detail::write_be64(detail::fnv1a_64(name), out.data());
                return out;
            }

            static std::array<uint8_t, 8> make_index_prefix(std::string_view table_name, std::string_view field_name) {
                std::string input;
                input.reserve(table_name.size() + 5 + field_name.size());
                input.append(table_name);
                input.append(":idx:");
                input.append(field_name);
                std::array<uint8_t, 8> out{};
                detail::write_be64(detail::fnv1a_64(input), out.data());
                return out;
            }

            void make_index_search_prefix(const std::array<uint8_t, 8>& prefix, std::span<const uint8_t> field_bytes, ArenaByteBuffer& out) const {
                out.clear();
                out.resize(12 + field_bytes.size());
                std::memcpy(out.data(), prefix.data(), prefix.size());
                detail::write_be32(static_cast<uint32_t>(field_bytes.size()), out.data() + 8);
                if (!field_bytes.empty()) { std::memcpy(out.data() + 12, field_bytes.data(), field_bytes.size()); }
            }

            void make_index_key(
                const std::array<uint8_t, 8>& prefix,
                std::span<const uint8_t> field_bytes,
                std::span<const uint8_t> pk_key,
                ArenaByteBuffer& out
            ) const {
                if (pk_key.size() < pk_prefix_.size()) { throw std::invalid_argument("PackedTable: malformed primary key"); }
                make_index_search_prefix(prefix, field_bytes, out);
                out.insert(out.end(), pk_key.begin() + static_cast<std::ptrdiff_t>(pk_prefix_.size()), pk_key.end());
            }

            void write_index_entries(const Entity& entity, std::span<const uint8_t> pk_key) {
                constexpr std::span<const uint8_t> empty_value{};
                for (const auto& idx : indexes_) {
                    idx.encode_field(entity, field_buffer_);
                    make_index_key(idx.prefix, field_buffer_, pk_key, index_key_buffer_);
                    put_hinted(index_key_buffer_, empty_value);
                }
            }

            void remove_index_entries(const Entity& entity, std::span<const uint8_t> pk_key) {
                for (const auto& idx : indexes_) {
                    idx.encode_field(entity, field_buffer_);
                    make_index_key(idx.prefix, field_buffer_, pk_key, index_key_buffer_);
                    remove_hinted(index_key_buffer_);
                }
            }
    };
} // namespace akkaradb
