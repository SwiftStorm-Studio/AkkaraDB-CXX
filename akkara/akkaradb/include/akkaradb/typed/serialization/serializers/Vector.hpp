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

// akkara/akkaradb/include/akkaradb/typed/serialization/Vector.hpp
#pragma once

#include "akkaradb/typed/serialization/Core.hpp"
#include "Primitives.hpp"
#include <bit>
#include <array>
#include <vector>
#include <span>
#include <algorithm>

/**
 * std::vector<T> serialization.
 * Format: [u32 size (LE)] + [element1] + [element2] + ...
 */
namespace akkaradb::typed::serialization {
    // Forward declaration for generic deserialize
    template <typename T>
    [[nodiscard]] T deserialize(std::span<const uint8_t>& data);

    // ========== std::vector<uint8_t> (special case: ByteArray) ==========

    inline void serialize(std::vector<uint8_t>& buf, const std::vector<uint8_t>& bytes) {
        const auto len = static_cast<uint32_t>(bytes.size());
        auto len_bytes = std::bit_cast<std::array<uint8_t, 4>>(len);
        buf.insert(buf.end(), len_bytes.begin(), len_bytes.end());
        buf.insert(buf.end(), bytes.begin(), bytes.end());
    }

    [[nodiscard]] inline std::vector<uint8_t> deserialize_byte_array(std::span<const uint8_t>& data) {
        if (data.size() < 4) { throw SerializationException("deserialize_byte_array: insufficient data for length"); }

        std::array<uint8_t, 4> len_bytes;
        std::copy_n(data.begin(), 4, len_bytes.begin());
        const uint32_t len = std::bit_cast<uint32_t>(len_bytes);
        data = data.subspan(4);

        if (data.size() < len) {
            throw SerializationException(
                "deserialize_byte_array: insufficient data (need " +
                std::to_string(len) + ", got " + std::to_string(data.size()) + ")"
            );
        }

        std::vector<uint8_t> result(data.begin(), data.begin() + len);
        data = data.subspan(len);

        return result;
    }

    [[nodiscard]] inline size_t estimate_size(const std::vector<uint8_t>& bytes) { return 4 + bytes.size(); }

    // ========== std::vector<T> (generic) ==========

    template <typename T>
        requires (!std::is_same_v<T, uint8_t>)
    void serialize(std::vector<uint8_t>& buf, const std::vector<T>& vec) {
        const auto size = static_cast<uint32_t>(vec.size());
        serialize(buf, size);

        for (const auto& item : vec) { serialize(buf, item); }
    }

    template <typename T>
        requires (!std::is_same_v<T, uint8_t>)
    [[nodiscard]] std::vector<T> deserialize_vector(std::span<const uint8_t>& data) {
        const uint32_t size = deserialize_integral<uint32_t>(data);

        std::vector<T> result;
        result.reserve(size);

        for (uint32_t i = 0; i < size; i++) { result.push_back(deserialize<T>(data)); }

        return result;
    }

    template <typename T>
        requires (!std::is_same_v<T, uint8_t>)
    [[nodiscard]] size_t estimate_size(const std::vector<T>& vec) {
        size_t total = 4; // size prefix
        for (const auto& item : vec) {
        total += estimate_size(item);
    }
    return total;
}

} // namespace akkaradb::typed::serialization