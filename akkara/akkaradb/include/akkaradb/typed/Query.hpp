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

// akkara/akkaradb/include/akkaradb/typed/Query.hpp
#pragma once

#include <string>
#include <string_view>
#include <cstdint>
#include <type_traits>
#include <span>
#include <cstring>

/**
 * Expression Template for type-safe, zero-overhead queries.
 *
 * Usage:
 *   Field<User, int> age{&User::age};
 *   auto expr = age > 18 && age < 65;
 *   users.query(expr);
 *
 * Features:
 * - Compile-time type checking
 * - Zero runtime overhead (fully inlined)
 * - Early filtering (read fields without full deserialization)
 */
namespace akkaradb::typed::query {
    // ==================== Forward Declarations ====================

    template <typename Derived>
    struct Expr;

    template <typename T>
    struct Lit;

    template <typename Entity, typename FieldType>
    struct Field;

    template <typename L, typename R, typename Op>
    struct BinOp;

    template <typename X, typename Op>
    struct UnOp;

    // ==================== Expression Base ====================

    /**
     * CRTP base for all expressions.
     */
    template <typename Derived>
    struct Expr {
        [[nodiscard]] const Derived& derived() const noexcept { return static_cast<const Derived&>(*this); }

        [[nodiscard]] Derived& derived() noexcept { return static_cast<Derived&>(*this); }
    };

    // ==================== Literal ====================

    /**
     * Literal value (constant).
     */
    template <typename T>
    struct Lit : Expr<Lit<T>> {
        T value;

        explicit constexpr Lit(T v) noexcept : value(v) {}

        template <typename Entity>
        [[nodiscard]] constexpr T eval(const Entity&) const noexcept { return value; }

        // Early filtering support
        [[nodiscard]] constexpr T eval_early(std::span<const uint8_t>) const noexcept { return value; }

        [[nodiscard]] static constexpr bool can_early_filter() noexcept { return true; }
    };

    // ==================== Field Reference ====================

    /**
     * Field reference (entity member pointer).
     */
    template <typename Entity, typename FieldType>
    struct Field : Expr<Field<Entity, FieldType>> {
        FieldType Entity::* ptr;
        size_t offset; // Byte offset in serialized form

        explicit constexpr Field(FieldType Entity::* p, size_t off = 0) noexcept : ptr(p), offset(off) {}

        [[nodiscard]] FieldType eval(const Entity& entity) const noexcept { return entity.*ptr; }

        // Early filtering: read directly from serialized bytes
        [[nodiscard]] FieldType eval_early(std::span<const uint8_t> data) const {
            if (data.size() < offset + sizeof(FieldType)) {
                // Fallback: can't early filter
                return FieldType{};
            }

            // Read field at offset (Little Endian)
            FieldType result;
            std::memcpy(&result, data.data() + offset, sizeof(FieldType));
            return result;
        }

        [[nodiscard]] static constexpr bool can_early_filter() noexcept {
            // Only primitive types support early filtering
            return std::is_arithmetic_v<FieldType>;
        }
    };

    // ==================== Operators ====================

    struct OpEq {
        template <typename L, typename R>
        [[nodiscard]] static constexpr bool apply(const L& l, const R& r) noexcept { return l == r; }
    };

    struct OpNe {
        template <typename L, typename R>
        [[nodiscard]] static constexpr bool apply(const L& l, const R& r) noexcept { return l != r; }
    };

    struct OpGt {
        template <typename L, typename R>
        [[nodiscard]] static constexpr bool apply(const L& l, const R& r) noexcept { return l > r; }
    };

    struct OpGe {
        template <typename L, typename R>
        [[nodiscard]] static constexpr bool apply(const L& l, const R& r) noexcept { return l >= r; }
    };

    struct OpLt {
        template <typename L, typename R>
        [[nodiscard]] static constexpr bool apply(const L& l, const R& r) noexcept { return l < r; }
    };

    struct OpLe {
        template <typename L, typename R>
        [[nodiscard]] static constexpr bool apply(const L& l, const R& r) noexcept { return l <= r; }
    };

    struct OpAnd {
        [[nodiscard]] static constexpr bool apply(bool l, bool r) noexcept { return l && r; }
    };

    struct OpOr {
        [[nodiscard]] static constexpr bool apply(bool l, bool r) noexcept { return l || r; }
    };

    struct OpNot {
        [[nodiscard]] static constexpr bool apply(bool x) noexcept { return !x; }
    };

    // ==================== Binary Operation ====================

    template <typename L, typename R, typename Op>
    struct BinOp : Expr<BinOp<L, R, Op>> {
        L lhs;
        R rhs;

        constexpr BinOp(const Expr<L>& l, const Expr<R>& r) noexcept : lhs(l.derived()), rhs(r.derived()) {}

        template <typename Entity>
        [[nodiscard]] auto eval(const Entity& entity) const noexcept { return Op::apply(lhs.eval(entity), rhs.eval(entity)); }

        // Early filtering
        [[nodiscard]] auto eval_early(std::span<const uint8_t> data) const { return Op::apply(lhs.eval_early(data), rhs.eval_early(data)); }

        [[nodiscard]] static constexpr bool can_early_filter() noexcept { return L::can_early_filter() && R::can_early_filter(); }
    };

    // ==================== Unary Operation ====================

    template <typename X, typename Op>
    struct UnOp : Expr<UnOp<X, Op>> {
        X operand;

        constexpr UnOp(const Expr<X>& x) noexcept : operand(x.derived()) {}

        template <typename Entity>
        [[nodiscard]] auto eval(const Entity& entity) const noexcept { return Op::apply(operand.eval(entity)); }

        [[nodiscard]] auto eval_early(std::span<const uint8_t> data) const { return Op::apply(operand.eval_early(data)); }

        [[nodiscard]] static constexpr bool can_early_filter() noexcept { return X::can_early_filter(); }
    };

    // ==================== Operator Overloads ====================

    // Comparison operators

    template <typename Entity, typename FieldType, typename T>
    [[nodiscard]] constexpr auto operator==(const Field<Entity, FieldType>& f, T val) noexcept {
        return BinOp<Field<Entity, FieldType>, Lit<T>, OpEq>(f, Lit<T>(val));
    }

    template <typename Entity, typename FieldType, typename T>
    [[nodiscard]] constexpr auto operator!=(const Field<Entity, FieldType>& f, T val) noexcept {
        return BinOp<Field<Entity, FieldType>, Lit<T>, OpNe>(f, Lit<T>(val));
    }

    template <typename Entity, typename FieldType, typename T>
    [[nodiscard]] constexpr auto operator>(const Field<Entity, FieldType>& f, T val) noexcept {
        return BinOp<Field<Entity, FieldType>, Lit<T>, OpGt>(f, Lit<T>(val));
    }

    template <typename Entity, typename FieldType, typename T>
    [[nodiscard]] constexpr auto operator>=(const Field<Entity, FieldType>& f, T val) noexcept {
        return BinOp<Field<Entity, FieldType>, Lit<T>, OpGe>(f, Lit<T>(val));
    }

    template <typename Entity, typename FieldType, typename T>
    [[nodiscard]] constexpr auto operator<(const Field<Entity, FieldType>& f, T val) noexcept {
        return BinOp<Field<Entity, FieldType>, Lit<T>, OpLt>(f, Lit<T>(val));
    }

    template <typename Entity, typename FieldType, typename T>
    [[nodiscard]] constexpr auto operator<=(const Field<Entity, FieldType>& f, T val) noexcept {
        return BinOp<Field<Entity, FieldType>, Lit<T>, OpLe>(f, Lit<T>(val));
    }

    // Logical operators

    template <typename L, typename R>
    [[nodiscard]] constexpr auto operator&&(const Expr<L>& l, const Expr<R>& r) noexcept { return BinOp<L, R, OpAnd>(l.derived(), r.derived()); }

    template <typename L, typename R>
    [[nodiscard]] constexpr auto operator||(const Expr<L>& l, const Expr<R>& r) noexcept { return BinOp<L, R, OpOr>(l.derived(), r.derived()); }

    template <typename X>
    [[nodiscard]] constexpr auto operator!(const Expr<X>& x) noexcept { return UnOp<X, OpNot>(x.derived()); }

    // ==================== String Operations ====================

    /**
     * String starts_with operation.
     */
    struct OpStartsWith {
        [[nodiscard]] static bool apply(std::string_view str, std::string_view prefix) noexcept { return str.starts_with(prefix); }
    };

    template <typename Entity>
    [[nodiscard]] auto starts_with(const Field<Entity, std::string>& f, std::string_view prefix) {
        return BinOp<Field<Entity, std::string>, Lit<std::string_view>, OpStartsWith>(
            f,
            Lit(prefix)
        );
    }

    /**
     * String ends_with operation.
     */
    struct OpEndsWith {
        [[nodiscard]] static bool apply(std::string_view str, std::string_view suffix) noexcept { return str.ends_with(suffix); }
    };

    template <typename Entity>
    [[nodiscard]] auto ends_with(const Field<Entity, std::string>& f, std::string_view suffix) {
        return BinOp<Field<Entity, std::string>, Lit<std::string_view>, OpEndsWith>(
            f,
            Lit(suffix)
        );
    }

    /**
     * String contains operation.
     */
    struct OpContains {
        [[nodiscard]] static bool apply(std::string_view str, std::string_view substring) noexcept { return str.find(substring) != std::string_view::npos; }
    };

    template <typename Entity>
    [[nodiscard]] auto contains(const Field<Entity, std::string>& f, std::string_view substring) {
        return BinOp<Field<Entity, std::string>, Lit<std::string_view>, OpContains>(
            f,
            Lit(substring)
        );
    }
} // namespace akkaradb::typed::query