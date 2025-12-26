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

// akkara/akkaradb/include/akkaradb/typed/serialization/Map.hpp
#pragma once

#include "Primitives.hpp"
#include <map>
#include <unordered_map>
#include <span>
#include <vector>

/**
 * std::map<K, V> and std::unordered_map<K, V> serialization.
 * Format: [u32 size (LE)] + [key1][value1] + [key2][value2] + ...
 */
namespace akkaradb::typed::serialization {
    // Forward declaration
    template <typename T>
    [[nodiscard]] T deserialize(std::span<const uint8_t> & data);

    // ========== std::map ==========

    template <typename K, typename V>
    void serialize(std::vector<uint8_t>& buf, const std::map<K, V>& map) {
        const auto size = static_cast<uint32_t>(map.size());
        serialize(buf, size);

        for (const auto& [key, value] : map) {
            serialize(buf, key);
            serialize(buf, value);
        }
    }

    template <typename K, typename V>
    [[nodiscard]] std::map<K, V> deserialize_map(std::span<const uint8_t>& data) {
        const uint32_t size = deserialize_integral<uint32_t>(data);

        std::map<K, V> result;

        for (uint32_t i = 0; i < size; i++) {
            K key = deserialize<K>(data);
            V value = deserialize<V>(data);
            result.emplace(std::move(key), std::move(value));
        }

        return result;
    }

    template <typename K, typename V>
    [[nodiscard]] size_t estimate_size(const std::map<K, V>& map) {
        size_t total = 4; // size prefix
        for (const auto& [key, value] : map) {
            total += estimate_size(key);
            total += estimate_size(value);
        }
        return total;
    }

    // ========== std::unordered_map ==========

    template <typename K, typename V>
    void serialize(std::vector<uint8_t>& buf, const std::unordered_map<K, V>& map) {
        const auto size = static_cast<uint32_t>(map.size());
        serialize(buf, size);

        for (const auto& [key, value] : map) {
            serialize(buf, key);
            serialize(buf, value);
        }
    }

    template <typename K, typename V>
    [[nodiscard]] std::unordered_map<K, V> deserialize_unordered_map(std::span<const uint8_t>& data) {
        const uint32_t size = deserialize_integral<uint32_t>(data);

        std::unordered_map<K, V> result;
        result.reserve(size);

        for (uint32_t i = 0; i < size; i++) {
            K key = deserialize<K>(data);
            V value = deserialize<V>(data);
            result.emplace(std::move(key), std::move(value));
        }

        return result;
    }

    template <typename K, typename V>
    [[nodiscard]] size_t estimate_size(const std::unordered_map<K, V>& map) {
        size_t total = 4; // size prefix
        for (const auto& [key, value] : map) {
            total += estimate_size(key);
            total += estimate_size(value);
        }
        return total;
    }
} // namespace akkaradb::typed::serialization