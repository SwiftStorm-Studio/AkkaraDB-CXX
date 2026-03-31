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

// akkaradb/binpack/BinPack.hpp
#pragma once

#include "TypeAdapter.hpp"
#include <span>
#include <stdexcept>
#include <vector>

namespace akkaradb::binpack {

// =============================================================================
// BinPack — public serialization API
// =============================================================================
//
// Zero-config binary serialization for C++ types.
//
// Supported out of the box (no specialization required):
//   - Primitives: bool, int8_t..int64_t, uint8_t..uint64_t, float, double
//   - std::string
//   - std::optional<T>, std::vector<T>, std::array<T,N>
//   - std::map<K,V>, std::unordered_map<K,V>
//   - std::pair<A,B>, std::tuple<Ts...>
//   - enum types (serialized as their underlying integer type)
//   - Any aggregate struct (plain struct with no user-provided constructor)
//     — fields are serialized in declaration order via Boost.PFR.
//     — nested aggregates and all of the above are supported recursively.
//
// Custom types:
//   Specialize TypeAdapter<MyType> to add support for a type:
//
//     template<>
//     struct akkaradb::binpack::TypeAdapter<MyType> {
//         static void   write(const MyType&, std::vector<uint8_t>&);
//         static MyType read(std::span<const uint8_t>&); // must advance span
//         static size_t estimate_size(const MyType&);
//     };
//
// Usage:
//   struct Point { float x; float y; };   // zero-config aggregate
//
//   auto bytes = BinPack::encode(Point{1.0f, 2.0f});   // → vector<uint8_t>
//   auto p     = BinPack::decode<Point>(bytes);         // → Point
// =============================================================================

struct BinPack {
    // ── encode ────────────────────────────────────────────────────────────────

    /**
     * Serializes value into a new byte vector.
     *
     * The output is pre-reserved using estimate_size() to minimize reallocations.
     */
    template<typename T>
    [[nodiscard]] static std::vector<uint8_t> encode(const T& value) {
        std::vector<uint8_t> out;
        out.reserve(TypeAdapter<T>::estimate_size(value));
        TypeAdapter<T>::write(value, out);
        return out;
    }

    /**
     * Serializes value into an existing vector (appends).
     *
     * Useful in hot loops where the same buffer is reused across calls.
     * Pre-reserve out to avoid repeated allocations:
     *
     *   std::vector<uint8_t> buf;
     *   buf.reserve(expected_size);
     *   for (...) { buf.clear(); BinPack::encode_into(value, buf); }
     */
    template<typename T>
    static void encode_into(const T& value, std::vector<uint8_t>& out) {
        TypeAdapter<T>::write(value, out);
    }

    // ── decode ────────────────────────────────────────────────────────────────

    /**
     * Deserializes a value from a byte span.
     *
     * Throws std::runtime_error on malformed input (e.g. truncated buffer).
     */
    template<typename T>
    [[nodiscard]] static T decode(std::span<const uint8_t> bytes) {
        return TypeAdapter<T>::read(bytes);
    }

    /**
     * Deserializes a value from a byte vector.
     */
    template<typename T>
    [[nodiscard]] static T decode(const std::vector<uint8_t>& bytes) {
        return decode<T>(std::span<const uint8_t>{bytes});
    }

    // ── estimate_size ─────────────────────────────────────────────────────────

    /**
     * Returns an upper-bound estimate of the encoded size in bytes.
     *
     * The actual encoded size may be smaller (e.g. for empty strings/vectors).
     * Useful for pre-reserving buffers.
     */
    template<typename T>
    [[nodiscard]] static size_t estimate_size(const T& value) {
        return TypeAdapter<T>::estimate_size(value);
    }

    BinPack() = delete;
};

} // namespace akkaradb::binpack
