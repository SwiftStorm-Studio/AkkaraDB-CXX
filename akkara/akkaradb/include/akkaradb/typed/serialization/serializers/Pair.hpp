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

// akkara/akkaradb/include/akkaradb/typed/serialization/Pair.hpp
#pragma once

#include <utility>
#include <span>
#include <vector>

/**
 * std::pair<T1, T2> serialization.
 * Format: [first] + [second]
 */
namespace akkaradb::typed::serialization {
    // Forward declaration
    template <typename T>
    [[nodiscard]] T deserialize(std::span<const uint8_t> & data);

    template <typename T1, typename T2>
    void serialize(std::vector<uint8_t>& buf, const std::pair<T1, T2>& pair) {
        serialize(buf, pair.first);
        serialize(buf, pair.second);
    }

    template <typename T1, typename T2>
    [[nodiscard]] std::pair<T1, T2> deserialize_pair(std::span<const uint8_t>& data) {
        T1 first = deserialize<T1>(data);
        T2 second = deserialize<T2>(data);
        return {std::move(first), std::move(second)};
    }

    template <typename T1, typename T2>
    [[nodiscard]] size_t estimate_size(const std::pair<T1, T2>& pair) { return estimate_size(pair.first) + estimate_size(pair.second); }
} // namespace akkaradb::typed::serialization