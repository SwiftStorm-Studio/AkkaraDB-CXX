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

#include <cstdint>
#include <cstring>
#include <span>
#include <vector>
#include <string>
#include <concepts>
#include <stdexcept>
#include <type_traits>

/**
 * Serialization utilities for AkkaraDB Typed API.
 *
 * Provides JVM-compatible binary serialization for:
 * - Integral types (int8_t, int16_t, int32_t, int64_t, uint*, etc.)
 * - Floating point types (float, double)
 * - std::string (UTF-8)
 *
 * All values are serialized in Little Endian format for cross-platform compatibility.
 *
 * Format:
 *   String:  [u32 length (LE)] + [UTF-8 bytes]
 *   Int32:   [i32 (LE)]
 *   Int64:   [i64 (LE)]
 *   Float:   [f32 (LE)]
 *   Double:  [f64 (LE)]
 */
namespace akkaradb::typed {
    // ========== Error Handling ==========

    /**
     * Exception thrown when serialization/deserialization fails.
     */
    class SerializationException : public std::runtime_error {
    public:
        explicit SerializationException(const std::string& msg) : std::runtime_error(msg) {}
    };

    // ========== Concepts ==========

    /**
     * Check if a type is serializable.
     */
    template <typename T>
    concept Serializable =
        std::is_integral_v<T> ||
        std::is_floating_point_v<T> ||
        std::is_same_v<T, std::string>;

    // ========== Size Estimation ==========

    /**
     * Estimates the serialized size of a value.
     * Used for buffer pre-allocation.
     */
    template <typename T>
    [[nodiscard]] inline size_t estimate_size(const T& value) {
        if constexpr (std::is_same_v<T, std::string>) {
            return 4 + value.size(); // length prefix + data
        }
        else { return sizeof(T); }
    }

    // ========== std::string Serialization ==========

    /**
     * Serializes a string to binary format.
     * Format: [u32 length (LE)] + [UTF-8 bytes]
     *
     * @param buf Output buffer
     * @param str String to serialize
     */
    inline void serialize(std::vector<uint8_t>& buf, const std::string& str) {
        // Length prefix (u32, Little Endian)
        const auto len = static_cast<uint32_t>(str.size());
        const auto* len_bytes = reinterpret_cast<const uint8_t*>(&len);
        buf.insert(buf.end(), len_bytes, len_bytes + 4);

        // String data (UTF-8)
        buf.insert(buf.end(), str.begin(), str.end());
    }

    /**
     * Deserializes a string from binary format.
     * Advances the data span past consumed bytes.
     *
     * @param data Input data (will be advanced)
     * @return Deserialized string
     * @throws SerializationException if data is malformed
     */
    [[nodiscard]] inline std::string deserialize_string(std::span<const uint8_t>& data) {
        // Read length prefix
        if (data.size() < 4) {
            throw SerializationException(
                "deserialize_string: insufficient data for length prefix (need 4, got " +
                std::to_string(data.size()) + ")"
            );
        }

        uint32_t len;
        std::memcpy(&len, data.data(), 4);
        data = data.subspan(4);

        // Validate length
        if (data.size() < len) {
            throw SerializationException(
                "deserialize_string: insufficient data for string content (need " +
                std::to_string(len) + ", got " + std::to_string(data.size()) + ")"
            );
        }

        // Extract string
        std::string result(reinterpret_cast<const char*>(data.data()), len);
        data = data.subspan(len);

        return result;
    }

    // ========== Integral Types Serialization ==========

    /**
     * Serializes an integral type to binary format.
     * Uses Little Endian byte order.
     *
     * @param buf Output buffer
     * @param value Value to serialize
     */
    template <std::integral T>
    void serialize(std::vector<uint8_t>& buf, T value) {
        const auto* bytes = reinterpret_cast<const uint8_t*>(&value);
        buf.insert(buf.end(), bytes, bytes + sizeof(T));
    }

    /**
     * Deserializes an integral type from binary format.
     * Advances the data span past consumed bytes.
     *
     * @param data Input data (will be advanced)
     * @return Deserialized value
     * @throws SerializationException if data is insufficient
     */
    template <std::integral T>
    [[nodiscard]] T deserialize_integral(std::span<const uint8_t>& data) {
        if (data.size() < sizeof(T)) {
            throw SerializationException(
                "deserialize_integral: insufficient data (need " +
                std::to_string(sizeof(T)) + ", got " + std::to_string(data.size()) + ")"
            );
        }

        T value;
        std::memcpy(&value, data.data(), sizeof(T));
        data = data.subspan(sizeof(T));

        return value;
    }

    // ========== Floating Point Types Serialization ==========

    /**
     * Serializes a floating point type to binary format.
     * Uses Little Endian byte order.
     *
     * @param buf Output buffer
     * @param value Value to serialize
     */
    template <std::floating_point T>
    void serialize(std::vector<uint8_t>& buf, T value) {
        const auto* bytes = reinterpret_cast<const uint8_t*>(&value);
        buf.insert(buf.end(), bytes, bytes + sizeof(T));
    }

    /**
     * Deserializes a floating point type from binary format.
     * Advances the data span past consumed bytes.
     *
     * @param data Input data (will be advanced)
     * @return Deserialized value
     * @throws SerializationException if data is insufficient
     */
    template <std::floating_point T>
    [[nodiscard]] T deserialize_float(std::span<const uint8_t>& data) {
        if (data.size() < sizeof(T)) {
            throw SerializationException(
                "deserialize_float: insufficient data (need " +
                std::to_string(sizeof(T)) + ", got " + std::to_string(data.size()) + ")"
            );
        }

        T value;
        std::memcpy(&value, data.data(), sizeof(T));
        data = data.subspan(sizeof(T));

        return value;
    }

    // ========== Generic Deserialize ==========

    /**
     * Generic deserialize function that dispatches to the correct type handler.
     *
     * @param data Input data (will be advanced)
     * @return Deserialized value
     */
    template <typename T>
    [[nodiscard]] T deserialize(std::span<const uint8_t>& data) {
        if constexpr (std::is_same_v<T, std::string>) { return deserialize_string(data); }
        else if constexpr (std::is_integral_v<T>) { return deserialize_integral<T>(data); }
        else if constexpr (std::is_floating_point_v<T>) { return deserialize_float<T>(data); }
        else { static_assert(std::is_void_v<T>, "Unsupported type for deserialization"); }
    }
} // namespace akkaradb::typed
