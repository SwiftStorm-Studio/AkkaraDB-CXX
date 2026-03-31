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

// akkaradb/binpack/TypeAdapter.hpp
#pragma once

#include "detail/WireHelpers.hpp"

#include <array>
#include <cstring>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/pfr.hpp>

namespace akkaradb::binpack {

namespace detail {
    // Dependent false for static_assert in template bodies
    template<typename T>
    inline constexpr bool always_false = false;
} // namespace detail

// =============================================================================
// TypeAdapter<T>
// =============================================================================
//
// Serializes/deserializes T to/from a byte stream.
//
// Interface:
//   static void   write(const T& v, std::vector<uint8_t>& out);
//   static T      read(std::span<const uint8_t>& in);   // advances span
//   static size_t estimate_size(const T& v);            // upper bound ok
//
// Customization:
//   - For custom types, fully specialize TypeAdapter<MyType>.
//   - Built-in support covers: all integer/float primitives, bool,
//     std::string, std::optional<T>, std::vector<T>, std::array<T,N>,
//     std::map<K,V>, std::unordered_map<K,V>, std::pair<A,B>,
//     std::tuple<Ts...>, enum (via underlying_type), and any aggregate
//     struct (via Boost.PFR — requires T to be default-constructible).
//
// Limitations:
//   - std::string_view: write-only (no read — views can't own data).
//   - Aggregate read: T must be default-constructible.
//   - std::span, raw pointers: not supported; use std::vector<uint8_t>.
// =============================================================================

// ─── Primary template ─────────────────────────────────────────────────────────
// Handles enums and aggregate structs. Everything else uses specializations below.

template<typename T>
struct TypeAdapter {
    static void write(const T& v, std::vector<uint8_t>& out) {
        if constexpr (std::is_enum_v<T>) {
            TypeAdapter<std::underlying_type_t<T>>::write(
                static_cast<std::underlying_type_t<T>>(v), out);
        } else if constexpr (std::is_aggregate_v<T> && !std::is_array_v<T>) {
            boost::pfr::for_each_field(v, [&out](const auto& field) {
                TypeAdapter<std::remove_cvref_t<decltype(field)>>::write(field, out);
            });
        } else {
            static_assert(detail::always_false<T>,
                "akkaradb::binpack::TypeAdapter<T>: no adapter for this type. "
                "Specialize TypeAdapter<T>, or ensure T is an aggregate struct or enum.");
        }
    }

    static T read(std::span<const uint8_t>& in) {
        if constexpr (std::is_enum_v<T>) {
            return static_cast<T>(TypeAdapter<std::underlying_type_t<T>>::read(in));
        } else if constexpr (std::is_aggregate_v<T> && !std::is_array_v<T>) {
            static_assert(std::is_default_constructible_v<T>,
                "BinPack aggregate read: T must be default-constructible. "
                "Specialize TypeAdapter<T> for non-default-constructible types.");
            T result{};
            boost::pfr::for_each_field(result, [&in](auto& field) {
                field = TypeAdapter<std::remove_cvref_t<decltype(field)>>::read(in);
            });
            return result;
        } else {
            static_assert(detail::always_false<T>,
                "akkaradb::binpack::TypeAdapter<T>: no adapter for this type.");
        }
    }

    static size_t estimate_size(const T& v) {
        if constexpr (std::is_enum_v<T>) {
            return sizeof(std::underlying_type_t<T>);
        } else if constexpr (std::is_aggregate_v<T> && !std::is_array_v<T>) {
            size_t total = 0;
            boost::pfr::for_each_field(v, [&total](const auto& field) {
                total += TypeAdapter<std::remove_cvref_t<decltype(field)>>::estimate_size(field);
            });
            return total;
        } else {
            static_assert(detail::always_false<T>,
                "akkaradb::binpack::TypeAdapter<T>: no adapter for this type.");
        }
    }
};

// ─── Full specializations: primitives ────────────────────────────────────────

template<> struct TypeAdapter<bool> {
    static void   write(bool v, std::vector<uint8_t>& out) { out.push_back(v ? 1u : 0u); }
    static bool   read(std::span<const uint8_t>& in)       { return detail::read_u8(in) != 0; }
    static size_t estimate_size(bool)                       { return 1; }
};

template<> struct TypeAdapter<uint8_t> {
    static void    write(uint8_t v, std::vector<uint8_t>& out) { detail::write_u8(v, out); }
    static uint8_t read(std::span<const uint8_t>& in)          { return detail::read_u8(in); }
    static size_t  estimate_size(uint8_t)                      { return 1; }
};

template<> struct TypeAdapter<uint16_t> {
    static void     write(uint16_t v, std::vector<uint8_t>& out) { detail::write_u16(v, out); }
    static uint16_t read(std::span<const uint8_t>& in)           { return detail::read_u16(in); }
    static size_t   estimate_size(uint16_t)                      { return 2; }
};

template<> struct TypeAdapter<uint32_t> {
    static void     write(uint32_t v, std::vector<uint8_t>& out) { detail::write_u32(v, out); }
    static uint32_t read(std::span<const uint8_t>& in)           { return detail::read_u32(in); }
    static size_t   estimate_size(uint32_t)                      { return 4; }
};

template<> struct TypeAdapter<uint64_t> {
    static void     write(uint64_t v, std::vector<uint8_t>& out) { detail::write_u64(v, out); }
    static uint64_t read(std::span<const uint8_t>& in)           { return detail::read_u64(in); }
    static size_t   estimate_size(uint64_t)                      { return 8; }
};

template<> struct TypeAdapter<int8_t> {
    static void   write(int8_t v, std::vector<uint8_t>& out) { detail::write_u8(static_cast<uint8_t>(v), out); }
    static int8_t read(std::span<const uint8_t>& in)         { return static_cast<int8_t>(detail::read_u8(in)); }
    static size_t estimate_size(int8_t)                      { return 1; }
};

template<> struct TypeAdapter<int16_t> {
    static void    write(int16_t v, std::vector<uint8_t>& out) { detail::write_u16(static_cast<uint16_t>(v), out); }
    static int16_t read(std::span<const uint8_t>& in)          { return static_cast<int16_t>(detail::read_u16(in)); }
    static size_t  estimate_size(int16_t)                      { return 2; }
};

template<> struct TypeAdapter<int32_t> {
    static void    write(int32_t v, std::vector<uint8_t>& out) { detail::write_u32(static_cast<uint32_t>(v), out); }
    static int32_t read(std::span<const uint8_t>& in)          { return static_cast<int32_t>(detail::read_u32(in)); }
    static size_t  estimate_size(int32_t)                      { return 4; }
};

template<> struct TypeAdapter<int64_t> {
    static void    write(int64_t v, std::vector<uint8_t>& out) { detail::write_u64(static_cast<uint64_t>(v), out); }
    static int64_t read(std::span<const uint8_t>& in)          { return static_cast<int64_t>(detail::read_u64(in)); }
    static size_t  estimate_size(int64_t)                      { return 8; }
};

template<> struct TypeAdapter<float> {
    static void write(float v, std::vector<uint8_t>& out) {
        uint32_t bits; std::memcpy(&bits, &v, 4);
        detail::write_u32(bits, out);
    }
    static float read(std::span<const uint8_t>& in) {
        const uint32_t bits = detail::read_u32(in);
        float v; std::memcpy(&v, &bits, 4); return v;
    }
    static size_t estimate_size(float) { return 4; }
};

template<> struct TypeAdapter<double> {
    static void write(double v, std::vector<uint8_t>& out) {
        uint64_t bits; std::memcpy(&bits, &v, 8);
        detail::write_u64(bits, out);
    }
    static double read(std::span<const uint8_t>& in) {
        const uint64_t bits = detail::read_u64(in);
        double v; std::memcpy(&v, &bits, 8); return v;
    }
    static size_t estimate_size(double) { return 8; }
};

// ─── std::string ──────────────────────────────────────────────────────────────
// Wire: [uint32 len][utf-8 bytes]

template<> struct TypeAdapter<std::string> {
    static void write(const std::string& v, std::vector<uint8_t>& out) {
        detail::write_u32(static_cast<uint32_t>(v.size()), out);
        out.insert(out.end(),
            reinterpret_cast<const uint8_t*>(v.data()),
            reinterpret_cast<const uint8_t*>(v.data()) + v.size());
    }
    static std::string read(std::span<const uint8_t>& in) {
        const uint32_t len = detail::read_u32(in);
        if (in.size() < len) throw std::runtime_error("BinPack: buffer underflow (string)");
        std::string result(reinterpret_cast<const char*>(in.data()), len);
        in = in.subspan(len);
        return result;
    }
    static size_t estimate_size(const std::string& v) { return 4 + v.size(); }
};

// std::string_view: write-only — views cannot own deserialized data
template<> struct TypeAdapter<std::string_view> {
    static void write(std::string_view v, std::vector<uint8_t>& out) {
        detail::write_u32(static_cast<uint32_t>(v.size()), out);
        out.insert(out.end(),
            reinterpret_cast<const uint8_t*>(v.data()),
            reinterpret_cast<const uint8_t*>(v.data()) + v.size());
    }
    static std::string_view read(std::span<const uint8_t>&) = delete; // use std::string as field type
    static size_t estimate_size(std::string_view v) { return 4 + v.size(); }
};

// ─── std::vector<uint8_t>  (raw bytes — optimized bulk path) ─────────────────
// Wire: [uint32 len][raw bytes]
// Note: full specialization takes priority over partial TypeAdapter<std::vector<T>>.

template<> struct TypeAdapter<std::vector<uint8_t>> {
    static void write(const std::vector<uint8_t>& v, std::vector<uint8_t>& out) {
        detail::write_u32(static_cast<uint32_t>(v.size()), out);
        out.insert(out.end(), v.begin(), v.end());
    }
    static std::vector<uint8_t> read(std::span<const uint8_t>& in) {
        const uint32_t len = detail::read_u32(in);
        if (in.size() < len) throw std::runtime_error("BinPack: buffer underflow (bytes)");
        std::vector<uint8_t> result(in.data(), in.data() + len);
        in = in.subspan(len);
        return result;
    }
    static size_t estimate_size(const std::vector<uint8_t>& v) { return 4 + v.size(); }
};

// ─── Partial specializations: collections ────────────────────────────────────

// std::optional<T>
// Wire: [uint8 tag: 0=null | 1=present][T bytes if present]
template<typename T>
struct TypeAdapter<std::optional<T>> {
    static void write(const std::optional<T>& v, std::vector<uint8_t>& out) {
        out.push_back(v.has_value() ? 1u : 0u);
        if (v.has_value()) TypeAdapter<T>::write(*v, out);
    }
    static std::optional<T> read(std::span<const uint8_t>& in) {
        if (detail::read_u8(in) == 0) return std::nullopt;
        return TypeAdapter<T>::read(in);
    }
    static size_t estimate_size(const std::optional<T>& v) {
        return 1 + (v.has_value() ? TypeAdapter<T>::estimate_size(*v) : 0);
    }
};

// std::vector<T>  (generic — std::vector<uint8_t> has a full specialization above)
// Wire: [uint32 count][T bytes * count]
template<typename T>
struct TypeAdapter<std::vector<T>> {
    static void write(const std::vector<T>& v, std::vector<uint8_t>& out) {
        detail::write_u32(static_cast<uint32_t>(v.size()), out);
        for (const auto& e : v) TypeAdapter<T>::write(e, out);
    }
    static std::vector<T> read(std::span<const uint8_t>& in) {
        const uint32_t count = detail::read_u32(in);
        std::vector<T> result;
        result.reserve(count);
        for (uint32_t i = 0; i < count; ++i)
            result.push_back(TypeAdapter<T>::read(in));
        return result;
    }
    static size_t estimate_size(const std::vector<T>& v) {
        size_t total = 4;
        for (const auto& e : v) total += TypeAdapter<T>::estimate_size(e);
        return total;
    }
};

// std::array<T, N>
// Wire: [T bytes * N]  (no length prefix — N is known at compile time)
template<typename T, size_t N>
struct TypeAdapter<std::array<T, N>> {
    static void write(const std::array<T, N>& v, std::vector<uint8_t>& out) {
        for (const auto& e : v) TypeAdapter<T>::write(e, out);
    }
    static std::array<T, N> read(std::span<const uint8_t>& in) {
        std::array<T, N> result;
        for (auto& e : result) e = TypeAdapter<T>::read(in);
        return result;
    }
    static size_t estimate_size(const std::array<T, N>& v) {
        size_t total = 0;
        for (const auto& e : v) total += TypeAdapter<T>::estimate_size(e);
        return total;
    }
};

// std::map<K, V>
// Wire: [uint32 count][K bytes, V bytes * count]
template<typename K, typename V, typename C, typename A>
struct TypeAdapter<std::map<K, V, C, A>> {
    static void write(const std::map<K, V, C, A>& m, std::vector<uint8_t>& out) {
        detail::write_u32(static_cast<uint32_t>(m.size()), out);
        for (const auto& [k, v] : m) {
            TypeAdapter<K>::write(k, out);
            TypeAdapter<V>::write(v, out);
        }
    }
    static std::map<K, V, C, A> read(std::span<const uint8_t>& in) {
        const uint32_t count = detail::read_u32(in);
        std::map<K, V, C, A> result;
        for (uint32_t i = 0; i < count; ++i) {
            K k = TypeAdapter<K>::read(in);
            V v = TypeAdapter<V>::read(in);
            result.emplace(std::move(k), std::move(v));
        }
        return result;
    }
    static size_t estimate_size(const std::map<K, V, C, A>& m) {
        size_t total = 4;
        for (const auto& [k, v] : m) {
            total += TypeAdapter<K>::estimate_size(k);
            total += TypeAdapter<V>::estimate_size(v);
        }
        return total;
    }
};

// std::unordered_map<K, V>
// Wire: same as std::map (order not preserved on round-trip, but values are)
template<typename K, typename V, typename H, typename E, typename A>
struct TypeAdapter<std::unordered_map<K, V, H, E, A>> {
    static void write(const std::unordered_map<K, V, H, E, A>& m, std::vector<uint8_t>& out) {
        detail::write_u32(static_cast<uint32_t>(m.size()), out);
        for (const auto& [k, v] : m) {
            TypeAdapter<K>::write(k, out);
            TypeAdapter<V>::write(v, out);
        }
    }
    static std::unordered_map<K, V, H, E, A> read(std::span<const uint8_t>& in) {
        const uint32_t count = detail::read_u32(in);
        std::unordered_map<K, V, H, E, A> result;
        result.reserve(count);
        for (uint32_t i = 0; i < count; ++i) {
            K k = TypeAdapter<K>::read(in);
            V v = TypeAdapter<V>::read(in);
            result.emplace(std::move(k), std::move(v));
        }
        return result;
    }
    static size_t estimate_size(const std::unordered_map<K, V, H, E, A>& m) {
        size_t total = 4;
        for (const auto& [k, v] : m) {
            total += TypeAdapter<K>::estimate_size(k);
            total += TypeAdapter<V>::estimate_size(v);
        }
        return total;
    }
};

// std::pair<A, B>
// Wire: [A bytes][B bytes]
template<typename A, typename B>
struct TypeAdapter<std::pair<A, B>> {
    static void write(const std::pair<A, B>& v, std::vector<uint8_t>& out) {
        TypeAdapter<A>::write(v.first, out);
        TypeAdapter<B>::write(v.second, out);
    }
    static std::pair<A, B> read(std::span<const uint8_t>& in) {
        // Read in declaration order — two separate statements guarantee sequence.
        A a = TypeAdapter<A>::read(in);
        B b = TypeAdapter<B>::read(in);
        return {std::move(a), std::move(b)};
    }
    static size_t estimate_size(const std::pair<A, B>& v) {
        return TypeAdapter<A>::estimate_size(v.first)
             + TypeAdapter<B>::estimate_size(v.second);
    }
};

// std::tuple<Ts...>
// Wire: [T0 bytes][T1 bytes]...[Tn bytes]
//
// read() uses a fold expression over operator, which is sequenced left-to-right
// in C++17 (guaranteed order). Requires all element types to be
// default-constructible; specialize TypeAdapter<std::tuple<...>> otherwise.
template<typename... Ts>
struct TypeAdapter<std::tuple<Ts...>> {
    using Tuple = std::tuple<Ts...>;

    static void write(const Tuple& v, std::vector<uint8_t>& out) {
        // std::apply with fold — comma operator is sequenced left-to-right.
        std::apply([&out](const auto&... args) {
            (TypeAdapter<std::remove_cvref_t<decltype(args)>>::write(args, out), ...);
        }, v);
    }

    static Tuple read(std::span<const uint8_t>& in) {
        // Assign each element via index sequence; fold over comma is left-to-right.
        return read_impl(in, std::index_sequence_for<Ts...>{});
    }

    static size_t estimate_size(const Tuple& v) {
        size_t total = 0;
        std::apply([&total](const auto&... args) {
            (void)std::initializer_list<int>{
                (total += TypeAdapter<std::remove_cvref_t<decltype(args)>>::estimate_size(args), 0)...
            };
        }, v);
        return total;
    }

private:
    template<size_t... Is>
    static Tuple read_impl(std::span<const uint8_t>& in, std::index_sequence<Is...>) {
        Tuple result;
        // Fold over comma: left-to-right sequence guaranteed (C++17 §8.1.6).
        ((std::get<Is>(result) = TypeAdapter<std::tuple_element_t<Is, Tuple>>::read(in)), ...);
        return result;
    }
};

} // namespace akkaradb::binpack
