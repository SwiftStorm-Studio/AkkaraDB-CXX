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

// akkara/akkaradb/include/akkaradb/typed/serialization/Array.hpp
#pragma once

#include <array>
#include <span>
#include <vector>

/**
 * std::array<T, N> serialization.
 * Format: [element1] + [element2] + ... (no size prefix, size is known at compile time)
 */
namespace akkaradb::typed::serialization {
    // Forward declaration
    template <typename T>
    [[nodiscard]] T deserialize(std::span<const uint8_t>& data);

    template <typename T, size_t N>
    void serialize(std::vector<uint8_t>& buf, const std::array<T, N>& arr) { for (const auto& item : arr) { serialize(buf, item); } }

    template <typename T, size_t N>
    [[nodiscard]] std::array<T, N> deserialize_array(std::span<const uint8_t>& data) {
        std::array<T, N> result;

        for (size_t i = 0; i < N; i++) { result[i] = deserialize<T>(data); }

        return result;
    }

    template <typename T, size_t N>
    [[nodiscard]] size_t estimate_size(const std::array<T, N>& arr) {
        size_t total = 0;
        for (const auto& item : arr) { total += estimate_size(item); }
        return total;
    }
} // namespace akkaradb::typed::serialization
