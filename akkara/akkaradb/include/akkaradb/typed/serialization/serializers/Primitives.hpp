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

// akkara/akkaradb/include/akkaradb/typed/serialization/Primitives.hpp
#pragma once

#include "akkaradb/typed/serialization/Core.hpp"
#include <bit>
#include <array>
#include <cstdint>
#include <span>
#include <vector>
#include <concepts>
#include <algorithm>

/**
 * Primitive types serialization (int, float, etc).
 * All values use Little Endian format for JVM compatibility.
 * Uses std::bit_cast for type-safe, UB-free conversions.
 */
namespace akkaradb::typed::serialization {
    // ========== Integral Types ==========

    template <std::integral T>
    void serialize(std::vector<uint8_t>& buf, T value) {
        auto bytes = std::bit_cast<std::array<uint8_t, sizeof(T)>>(value);
        buf.insert(buf.end(), bytes.begin(), bytes.end());
    }

    template <std::integral T>
    [[nodiscard]] T deserialize_integral(std::span<const uint8_t>& data) {
        if (data.size() < sizeof(T)) {
            throw SerializationException(
                "deserialize_integral: insufficient data (need " +
                std::to_string(sizeof(T)) + ", got " + std::to_string(data.size()) + ")"
            );
        }

        std::array<uint8_t, sizeof(T)> bytes;
        std::copy_n(data.begin(), sizeof(T), bytes.begin());
        data = data.subspan(sizeof(T));

        return std::bit_cast < T > (bytes);
    }

    template <std::integral T>
    [[nodiscard]] constexpr size_t estimate_size(const T&) noexcept { return sizeof(T); }

    // ========== Floating Point Types ==========

    template <std::floating_point T>
    void serialize(std::vector<uint8_t>& buf, T value) {
        auto bytes = std::bit_cast<std::array<uint8_t, sizeof(T)>>(value);
        buf.insert(buf.end(), bytes.begin(), bytes.end());
    }

    template <std::floating_point T>
    [[nodiscard]] T deserialize_float(std::span<const uint8_t>& data) {
        if (data.size() < sizeof(T)) {
            throw SerializationException(
                "deserialize_float: insufficient data (need " +
                std::to_string(sizeof(T)) + ", got " + std::to_string(data.size()) + ")"
            );
        }

        std::array<uint8_t, sizeof(T)> bytes;
        std::copy_n(data.begin(), sizeof(T), bytes.begin());
        data = data.subspan(sizeof(T));

        return std::bit_cast < T > (bytes);
    }

    template <std::floating_point T>
    [[nodiscard]] constexpr size_t estimate_size(const T&) noexcept { return sizeof(T); }
} // namespace akkaradb::typed::serialization