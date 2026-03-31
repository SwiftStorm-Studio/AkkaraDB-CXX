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

} // namespace akkaradb::binpack::detail
