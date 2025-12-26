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

// akkara/akkaradb/include/akkaradb/typed/serialization/Set.hpp
#pragma once

#include "Primitives.hpp"
#include <set>
#include <unordered_set>
#include <span>
#include <vector>

/**
 * std::set<T> and std::unordered_set<T> serialization.
 * Format: [u32 size (LE)] + [element1] + [element2] + ...
 */
namespace akkaradb::typed::serialization {
    // Forward declaration
    template <typename T>
    [[nodiscard]] T deserialize(std::span<const uint8_t> & data);

    // ========== std::set ==========

    template <typename T>
    void serialize(std::vector<uint8_t>& buf, const std::set<T>& set) {
        const auto size = static_cast<uint32_t>(set.size());
        serialize(buf, size);

        for (const auto& item : set) { serialize(buf, item); }
    }

    template <typename T>
    [[nodiscard]] std::set<T> deserialize_set(std::span<const uint8_t>& data) {
        const uint32_t size = deserialize_integral<uint32_t>(data);

        std::set<T> result;

        for (uint32_t i = 0; i < size; i++) { result.insert(deserialize<T>(data)); }

        return result;
    }

    template <typename T>
    [[nodiscard]] size_t estimate_size(const std::set<T>& set) {
        size_t total = 4; // size prefix
        for (const auto& item : set) { total += estimate_size(item); }
        return total;
    }

    // ========== std::unordered_set ==========

    template <typename T>
    void serialize(std::vector<uint8_t>& buf, const std::unordered_set<T>& set) {
        const auto size = static_cast<uint32_t>(set.size());
        serialize(buf, size);

        for (const auto& item : set) { serialize(buf, item); }
    }

    template <typename T>
    [[nodiscard]] std::unordered_set<T> deserialize_unordered_set(std::span<const uint8_t>& data) {
        const uint32_t size = deserialize_integral<uint32_t>(data);

        std::unordered_set<T> result;
        result.reserve(size);

        for (uint32_t i = 0; i < size; i++) { result.insert(deserialize<T>(data)); }

        return result;
    }

    template <typename T>
    [[nodiscard]] size_t estimate_size(const std::unordered_set<T>& set) {
        size_t total = 4; // size prefix
        for (const auto& item : set) { total += estimate_size(item); }
        return total;
    }
} // namespace akkaradb::typed::serialization