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

// akkaradb/Codec.hpp
#pragma once

#include "detail/Hash.hpp"
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace akkaradb {

    // =========================================================================
    // IdCodec<ID>
    // =========================================================================
    //
    // Translates a key ID type to/from a byte sequence that preserves
    // lexicographic ordering so that AkkEngine range scans work correctly.
    //
    // Built-in specializations are provided for:
    //   uint8_t, uint16_t, uint32_t, uint64_t
    //   int8_t,  int16_t,  int32_t,  int64_t
    //   std::string, std::string_view
    //
    // For custom ID types, specialize this template:
    //
    //   template<>
    //   struct akkaradb::IdCodec<MyId> {
    //       static void encode(const MyId& id, std::vector<uint8_t>& out);
    //       static MyId decode(std::span<const uint8_t> bytes);
    //   };

    template<typename ID>
    struct IdCodec {
        static_assert(sizeof(ID) == 0,
            "IdCodec<ID> must be specialized for this ID type. "
            "Built-in specializations exist for integer types and std::string. "
            "See akkaradb/Codec.hpp for the specialization interface.");

        static void encode(const ID& id, std::vector<uint8_t>& out);
        static ID   decode(std::span<const uint8_t> bytes);
    };

    // ── Unsigned integers (big-endian, naturally ordered) ────────────────────

    template<>
    struct IdCodec<uint8_t> {
        static void encode(uint8_t id, std::vector<uint8_t>& out) {
            out.push_back(id);
        }
        static uint8_t decode(std::span<const uint8_t> b) {
            if (b.size() < 1) throw std::invalid_argument("IdCodec<uint8_t>::decode: too short");
            return b[0];
        }
    };

    template<>
    struct IdCodec<uint16_t> {
        static void encode(uint16_t id, std::vector<uint8_t>& out) {
            out.push_back(static_cast<uint8_t>(id >> 8));
            out.push_back(static_cast<uint8_t>(id));
        }
        static uint16_t decode(std::span<const uint8_t> b) {
            if (b.size() < 2) throw std::invalid_argument("IdCodec<uint16_t>::decode: too short");
            return static_cast<uint16_t>((static_cast<uint16_t>(b[0]) << 8) | b[1]);
        }
    };

    template<>
    struct IdCodec<uint32_t> {
        static void encode(uint32_t id, std::vector<uint8_t>& out) {
            out.push_back(static_cast<uint8_t>(id >> 24));
            out.push_back(static_cast<uint8_t>(id >> 16));
            out.push_back(static_cast<uint8_t>(id >>  8));
            out.push_back(static_cast<uint8_t>(id));
        }
        static uint32_t decode(std::span<const uint8_t> b) {
            if (b.size() < 4) throw std::invalid_argument("IdCodec<uint32_t>::decode: too short");
            return (static_cast<uint32_t>(b[0]) << 24)
                 | (static_cast<uint32_t>(b[1]) << 16)
                 | (static_cast<uint32_t>(b[2]) <<  8)
                 |  static_cast<uint32_t>(b[3]);
        }
    };

    template<>
    struct IdCodec<uint64_t> {
        static void encode(uint64_t id, std::vector<uint8_t>& out) {
            const size_t off = out.size();
            out.resize(off + 8);
            detail::write_be64(id, out.data() + off);
        }
        static uint64_t decode(std::span<const uint8_t> b) {
            if (b.size() < 8) throw std::invalid_argument("IdCodec<uint64_t>::decode: too short");
            return detail::read_be64(b.data());
        }
    };

    // ── Signed integers (sign-bit flip → big-endian, order preserved) ────────
    //
    // Flip the MSB so that negative values sort before positive ones:
    //   INT64_MIN (0x8000...) → 0x0000... (smallest encoded)
    //   -1        (0xFFFF...) → 0x7FFF... (just below 0)
    //    0        (0x0000...) → 0x8000... (zero)
    //   INT64_MAX (0x7FFF...) → 0xFFFF... (largest encoded)

    template<>
    struct IdCodec<int8_t> {
        static void encode(int8_t id, std::vector<uint8_t>& out) {
            out.push_back(static_cast<uint8_t>(id) ^ 0x80u);
        }
        static int8_t decode(std::span<const uint8_t> b) {
            if (b.size() < 1) throw std::invalid_argument("IdCodec<int8_t>::decode: too short");
            return static_cast<int8_t>(b[0] ^ 0x80u);
        }
    };

    template<>
    struct IdCodec<int16_t> {
        static void encode(int16_t id, std::vector<uint8_t>& out) {
            auto u = static_cast<uint16_t>(id);
            u ^= 0x8000u;
            out.push_back(static_cast<uint8_t>(u >> 8));
            out.push_back(static_cast<uint8_t>(u));
        }
        static int16_t decode(std::span<const uint8_t> b) {
            if (b.size() < 2) throw std::invalid_argument("IdCodec<int16_t>::decode: too short");
            const uint16_t u = (static_cast<uint16_t>(b[0]) << 8) | b[1];
            return static_cast<int16_t>(u ^ 0x8000u);
        }
    };

    template<>
    struct IdCodec<int32_t> {
        static void encode(int32_t id, std::vector<uint8_t>& out) {
            auto u = static_cast<uint32_t>(id);
            u ^= 0x80000000u;
            out.push_back(static_cast<uint8_t>(u >> 24));
            out.push_back(static_cast<uint8_t>(u >> 16));
            out.push_back(static_cast<uint8_t>(u >>  8));
            out.push_back(static_cast<uint8_t>(u));
        }
        static int32_t decode(std::span<const uint8_t> b) {
            if (b.size() < 4) throw std::invalid_argument("IdCodec<int32_t>::decode: too short");
            const uint32_t u = (static_cast<uint32_t>(b[0]) << 24)
                             | (static_cast<uint32_t>(b[1]) << 16)
                             | (static_cast<uint32_t>(b[2]) <<  8)
                             |  static_cast<uint32_t>(b[3]);
            return static_cast<int32_t>(u ^ 0x80000000u);
        }
    };

    template<>
    struct IdCodec<int64_t> {
        static void encode(int64_t id, std::vector<uint8_t>& out) {
            const uint64_t u = static_cast<uint64_t>(id) ^ 0x8000000000000000ULL;
            const size_t off = out.size();
            out.resize(off + 8);
            detail::write_be64(u, out.data() + off);
        }
        static int64_t decode(std::span<const uint8_t> b) {
            if (b.size() < 8) throw std::invalid_argument("IdCodec<int64_t>::decode: too short");
            const uint64_t u = detail::read_be64(b.data());
            return static_cast<int64_t>(u ^ 0x8000000000000000ULL);
        }
    };

    // ── std::string (raw bytes, already lexicographically ordered) ───────────

    template<>
    struct IdCodec<std::string> {
        static void encode(const std::string& id, std::vector<uint8_t>& out) {
            out.insert(out.end(),
                reinterpret_cast<const uint8_t*>(id.data()),
                reinterpret_cast<const uint8_t*>(id.data()) + id.size());
        }
        static std::string decode(std::span<const uint8_t> b) {
            return std::string(reinterpret_cast<const char*>(b.data()), b.size());
        }
    };

    // =========================================================================
    // EntityCodec<T>
    // =========================================================================
    //
    // Serializes/deserializes entity objects to/from raw bytes.
    // There is NO built-in default — users must either:
    //   a) Specialize this template for their type, or
    //   b) Pass explicit serialize/deserialize lambdas to PackedTable::open().
    //
    // Specialization example (using a simple flatbuffer / hand-rolled codec):
    //
    //   template<>
    //   struct akkaradb::EntityCodec<User> {
    //       static void serialize(const User& u, std::vector<uint8_t>& out) {
    //           // append bytes...
    //       }
    //       static User deserialize(std::span<const uint8_t> bytes) {
    //           return User{ /* parse bytes */ };
    //       }
    //   };

    template<typename T>
    struct EntityCodec {
        static_assert(sizeof(T) == 0,
            "EntityCodec<T> must be specialized for this entity type, "
            "or pass serialize/deserialize lambdas to PackedTable::open(). "
            "See akkaradb/Codec.hpp for the specialization interface.");

        static void serialize(const T& entity, std::vector<uint8_t>& out);
        static T    deserialize(std::span<const uint8_t> bytes);
    };

} // namespace akkaradb
