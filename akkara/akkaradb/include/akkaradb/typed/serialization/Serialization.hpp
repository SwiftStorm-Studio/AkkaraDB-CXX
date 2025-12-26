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

// akkara/akkaradb/include/akkaradb/typed/Serialization.hpp
#pragma once

// Import all serialization implementations
#include "serialization/Core.hpp"
#include "serialization/serializers/Primitives.hpp"
#include "serialization/serializers/String.hpp"
#include "serialization/serializers/Bool.hpp"
#include "serialization/serializers/Vector.hpp"
#include "serialization/serializers/Optional.hpp"
#include "serialization/serializers/Map.hpp"
#include "serialization/serializers/Set.hpp"
#include "serialization/serializers/Pair.hpp"
#include "serialization/serializers/Array.hpp"

#include <optional>
#include <vector>
#include <map>
#include <unordered_map>
#include <set>
#include <unordered_set>
#include <utility>
#include <array>
#include <type_traits>

/**
 * Main serialization header for AkkaraDB Typed API.
 *
 * Provides JVM-compatible binary serialization for:
 * - Primitives: int, int64_t, float, double, bool
 * - String: std::string
 * - Containers: std::vector, std::map, std::set, std::array
 * - Optional: std::optional
 * - Pair: std::pair
 *
 * All values use Little Endian format.
 * Uses std::bit_cast for type-safe, UB-free conversions.
 */
namespace akkaradb::typed {
    // Re-export everything from serialization namespace
    using namespace serialization;

    template <typename T>
    struct is_std_array : std::false_type {};

    template <typename T, size_t N>
    struct is_std_array<std::array<T, N>> : std::true_type {
        using element_type = T;
        static constexpr size_t size = N;
    };

    template <typename T>
    inline constexpr bool is_std_array_v = is_std_array<T>::value;

    // ========== Generic Deserialize ==========

    /**
     * Generic deserialize function that dispatches to the correct type handler.
     *
     * This function fulfills the forward declarations in all serialization headers.
     * Supports recursive deserialization for nested types.
     *
     * @param data Input data (will be advanced)
     * @return Deserialized value
     * @throws SerializationException if deserialization fails
     */
    template <typename T>
    [[nodiscard]] T deserialize(std::span<const uint8_t>& data) {
        // ========== Primitives ==========
        if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) { return deserialize_integral<T>(data); }
        else if constexpr (std::is_floating_point_v<T>) { return deserialize_float<T>(data); }

        // ========== Basic Types ==========
        else if constexpr (std::is_same_v<T, std::string>) { return deserialize_string(data); }
        else if constexpr (std::is_same_v<T, bool>) { return deserialize_bool(data); }

        // ========== std::vector ==========
        else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            // Special case: byte array
            return deserialize_byte_array(data);
        }
        else if constexpr (requires { typename T::value_type; } &&
            std::is_same_v<T, std::vector<typename T::value_type>>) {
            // Generic vector
            return deserialize_vector<typename T::value_type>(data);
        }

        // ========== std::optional ==========
        else if constexpr (requires { typename T::value_type; } &&
            std::is_same_v<T, std::optional<typename T::value_type>>) { return deserialize_optional<typename T::value_type>(data); }

        // ========== std::map ==========
        else if constexpr (requires { typename T::key_type; typename T::mapped_type; } &&
            std::is_same_v<T, std::map<typename T::key_type, typename T::mapped_type>>) {
            return deserialize_map<typename T::key_type, typename T::mapped_type>(data);
        }
        else if constexpr (requires { typename T::key_type; typename T::mapped_type; } &&
            std::is_same_v<T, std::unordered_map<typename T::key_type, typename T::mapped_type>>) {
            return deserialize_unordered_map<typename T::key_type, typename T::mapped_type>(data);
        }

        // ========== std::set ==========
        else if constexpr (requires { typename T::value_type; } &&
            std::is_same_v<T, std::set<typename T::value_type>>) { return deserialize_set<typename T::value_type>(data); }
        else if constexpr (requires { typename T::value_type; } &&
            std::is_same_v<T, std::unordered_set<typename T::value_type>>) { return deserialize_unordered_set<typename T::value_type>(data); }

        // ========== std::pair ==========
        else if constexpr (requires { typename T::first_type; typename T::second_type; } &&
            std::is_same_v<T, std::pair<typename T::first_type, typename T::second_type>>) {
            return deserialize_pair<typename T::first_type, typename T::second_type>(data);
        }

        // ========== std::array ==========
        else if constexpr (is_std_array_v<T>) {
            using Traits = is_std_array<T>;
            return deserialize_array<typename Traits::element_type, Traits::size>(data);
        }

        // ========== Unsupported ==========
        else {
            static_assert(std::is_void_v<T>,
                          "Unsupported type for deserialization. "
                          "Supported: primitives, bool, std::string, std::vector, std::optional, "
                          "std::map, std::unordered_map, std::set, std::unordered_set, std::pair. "
                          "For std::array<T,N>, use deserialize_array<T,N>() explicitly.");
            throw SerializationException("Unsupported type for deserialization");
        }
    }

} // namespace akkaradb::typed