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

// akkaradb/binpack/detail/MemberPtrTraits.hpp
#pragma once

#include <cstddef>
#include <string_view>
#include <type_traits>

namespace akkaradb::binpack::detail {

// =============================================================================
// MemberPtrTraits<MPtr>
// =============================================================================
//
// Extracts the class type and member type from a member object pointer.
//
//   MemberPtrTraits<&User::id>::ClassType  → User
//   MemberPtrTraits<&User::id>::MemberType → uint64_t
//
// Only member object pointers are supported (not member function pointers).

template<typename T>
struct MemberPtrTraits;

template<typename Class, typename Member>
struct MemberPtrTraits<Member Class::*> {
    using ClassType  = Class;
    using MemberType = std::remove_cv_t<Member>;
};

// Convenience aliases

template<auto MPtr>
using class_of = typename MemberPtrTraits<decltype(MPtr)>::ClassType;

template<auto MPtr>
using member_of = typename MemberPtrTraits<decltype(MPtr)>::MemberType;

// =============================================================================
// field_byte_offset<MPtr>()
// =============================================================================
//
// Returns the byte offset of the field within its containing struct.
// Safe for standard-layout (aggregate) types — all types supported by
// Boost.PFR qualify.
//
// Used to generate stable, unique index prefixes per field.
// The offset is stable across runs as long as the struct layout does not change
// (i.e., the same binary — which is the only ABI-stable scenario for C++).

template<auto MPtr>
[[nodiscard]] inline size_t field_byte_offset() noexcept {
    using Class = class_of<MPtr>;
    // Use aligned storage to avoid constructing Class.
    alignas(alignof(Class)) char storage[sizeof(Class)]{};
    const auto* obj   = reinterpret_cast<const Class*>(storage);
    const void* field = std::addressof(obj->*MPtr);
    return static_cast<size_t>(
        reinterpret_cast<const char*>(field) - storage);
}

// =============================================================================
// member_name<MPtr>()
// =============================================================================
//
// Extracts the field name of a member object pointer as a string_view
// by parsing the compiler's function-signature predefined macro.
//
// The returned string_view points into a static string literal (the function
// signature), so its lifetime is unbounded — safe to use as a map key.
//
// Supported compilers:
//   MSVC  — __FUNCSIG__:          "... member_name<&Class::field>(void)"
//   Clang — __PRETTY_FUNCTION__:  "... [MPtr = &Class::field]"
//   GCC   — __PRETTY_FUNCTION__:  "... [with auto MPtr = (& Class::field)]"
//
// Limitation: member pointers to fields of template classes with '<' in the
// class name may cause incorrect extraction. Non-template aggregate structs
// (the intended use-case for PackedTable) are always handled correctly.

template <auto MPtr>
[[nodiscard]] inline std::string_view member_name() noexcept {
    #if defined(_MSC_VER)
    const std::string_view sig = __FUNCSIG__;
    // Pattern: "...member_name<&ClassName::fieldName>(void)"
    // Find the last "<&", then the ">" that closes it; the field name is between
    // the last "::" before that ">" and the ">" itself.
    const auto amp_lt = sig.rfind("<&"); if (amp_lt == std::string_view::npos) return {}; const auto gt = sig.find('>', amp_lt); if (gt ==
        std::string_view::npos) return {}; const auto cc = sig.rfind("::", gt); if (cc == std::string_view::npos || cc < amp_lt) return {}; return sig.substr(
        cc + 2,
        gt - cc - 2
    );
    #elif defined(__clang__)
    const std::string_view sig = __PRETTY_FUNCTION__;
    // Pattern: "... [MPtr = &ClassName::fieldName]"
    const auto rb = sig.rfind(']'); if (rb == std::string_view::npos) return {}; const auto cc = sig.rfind("::", rb); if (cc == std::string_view::npos) return
        {}; return sig.substr(cc + 2, rb - cc - 2);
    #else // GCC
    const std::string_view sig = __PRETTY_FUNCTION__;
    // Pattern: "... [with auto MPtr = (& ClassName::fieldName)]"
    const auto rb = sig.rfind(']');
    if (rb == std::string_view::npos) return {};
    const auto rp = sig.rfind(')', rb);
    if (rp == std::string_view::npos) return {};
    const auto cc = sig.rfind("::", rp);
    if (cc == std::string_view::npos) return {};
    return sig.substr(cc + 2, rp - cc - 2);
    #endif
}
} // namespace akkaradb::binpack::detail
