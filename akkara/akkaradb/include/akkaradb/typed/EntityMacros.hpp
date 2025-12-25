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

// akkara/akkaradb/include/akkaradb/typed/EntityMacros.hpp
#pragma once

#include <cstddef>
#include <tuple>
#include <type_traits>

namespace akkaradb::typed {
    // ========== Count Args ==========

#define AKKARA_COUNT_ARGS(...) \
    AKKARA_COUNT_ARGS_IMPL(__VA_ARGS__, \
        20, 19, 18, 17, 16, 15, 14, 13, 12, 11, \
        10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

#define AKKARA_COUNT_ARGS_IMPL( \
    _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, \
    _11, _12, _13, _14, _15, _16, _17, _18, _19, _20, \
    N, ...) N

#define AKKARA_FIRST(first, ...) first

    // ========== Recursive Map Macro (supports unlimited fields) ==========

    // Check if there are arguments
#define AKKARA_HAS_ARGS(...) AKKARA_HAS_ARGS_IMPL(__VA_ARGS__, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0)
#define AKKARA_HAS_ARGS_IMPL(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, N, ...) N

    // Conditional macro
#define AKKARA_IF_ELSE(condition) AKKARA_IF_ELSE_IMPL(condition)
#define AKKARA_IF_ELSE_IMPL(condition) AKKARA_IF_ELSE_##condition
#define AKKARA_IF_ELSE_1(true_branch, false_branch) true_branch
#define AKKARA_IF_ELSE_0(true_branch, false_branch) false_branch

    // Defer macro expansion (for recursion)
#define AKKARA_EMPTY()
#define AKKARA_DEFER(macro) macro AKKARA_EMPTY()
#define AKKARA_OBSTRUCT(...) __VA_ARGS__ AKKARA_DEFER(AKKARA_EMPTY)()

    // Evaluation (forces multiple expansion passes)
#define AKKARA_EVAL(...) AKKARA_EVAL1(AKKARA_EVAL1(AKKARA_EVAL1(__VA_ARGS__)))
#define AKKARA_EVAL1(...) AKKARA_EVAL2(AKKARA_EVAL2(AKKARA_EVAL2(__VA_ARGS__)))
#define AKKARA_EVAL2(...) AKKARA_EVAL3(AKKARA_EVAL3(AKKARA_EVAL3(__VA_ARGS__)))
#define AKKARA_EVAL3(...) AKKARA_EVAL4(AKKARA_EVAL4(AKKARA_EVAL4(__VA_ARGS__)))
#define AKKARA_EVAL4(...) __VA_ARGS__

    // Comma insertion
#define AKKARA_COMMA() ,

    // Recursive map implementation
#define AKKARA_MAP_END(...)
#define AKKARA_MAP_OUT
#define AKKARA_MAP_COMMA ,

#define AKKARA_MAP_GET_END2() 0, AKKARA_MAP_END
#define AKKARA_MAP_GET_END1(...) AKKARA_MAP_GET_END2
#define AKKARA_MAP_GET_END(...) AKKARA_MAP_GET_END1
#define AKKARA_MAP_NEXT0(test, next, ...) next AKKARA_MAP_OUT
#define AKKARA_MAP_NEXT1(test, next) AKKARA_DEFER(AKKARA_MAP_NEXT0)(test, next, 0)
#define AKKARA_MAP_NEXT(test, next) AKKARA_MAP_NEXT1(AKKARA_MAP_GET_END test, next)

#define AKKARA_MAP0(f, Type, x, peek, ...) \
    f(Type, x) AKKARA_DEFER(AKKARA_MAP_NEXT(peek, AKKARA_MAP1))(f, Type, peek, __VA_ARGS__)

#define AKKARA_MAP1(f, Type, x, peek, ...) \
    AKKARA_MAP_COMMA f(Type, x) AKKARA_DEFER(AKKARA_MAP_NEXT(peek, AKKARA_MAP0))(f, Type, peek, __VA_ARGS__)

#define AKKARA_MAP(f, Type, ...) \
    AKKARA_EVAL(AKKARA_MAP1(f, Type, __VA_ARGS__, ()()(), ()()(), ()()(), 0))

    // ========== Member Pointer Generator ==========

#define AKKARA_MAKE_PTR(Type, field) &Type::field

    // ========== PRIMARY_KEY ==========

#define PRIMARY_KEY(...) \
    using akkara_key_type = decltype(std::declval<std::remove_reference_t<decltype(*this)>>().AKKARA_FIRST(__VA_ARGS__)); \
    static constexpr size_t akkara_primary_key_count = AKKARA_COUNT_ARGS(__VA_ARGS__); \
    static constexpr bool akkara_has_primary_key = true;

    // ========== INDEXED ==========

#define INDEXED(...) \
    static constexpr size_t akkara_indexed_count = AKKARA_COUNT_ARGS(__VA_ARGS__);

    // ========== FIELDS (supports unlimited fields) ==========

#define FIELDS(...) \
    static constexpr size_t akkara_fields_count = AKKARA_COUNT_ARGS(__VA_ARGS__); \
    static constexpr auto akkara_field_ptrs = std::make_tuple( \
        AKKARA_MAP(AKKARA_MAKE_PTR, std::remove_reference_t<decltype(*this)>, __VA_ARGS__) \
    );

    // ========== AKKARA_TABLE ==========

#define AKKARA_TABLE(Type, ...) \
    __VA_ARGS__ \
    \
    static_assert(akkara_has_primary_key, \
        "\n\n" \
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n" \
        "  ❌ AKKARA_TABLE Error in '" #Type "'\n" \
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n" \
        "  PRIMARY_KEY(...) is required!\n" \
        "\n" \
        "  Expected:\n" \
        "    AKKARA_TABLE(" #Type ",\n" \
        "        PRIMARY_KEY(field_name),\n" \
        "        FIELDS(...)\n" \
        "    )\n" \
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n" \
    ); \
    \
    static_assert(akkara_fields_count >= 1, \
        "\n\n" \
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n" \
        "  ❌ AKKARA_TABLE Error in '" #Type "'\n" \
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n" \
        "  FIELDS(...) must have at least 1 field!\n" \
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n" \
    );
} // namespace akkaradb::typed
