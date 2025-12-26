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

// akkara/akkaradb/include/akkaradb/typed/serialization/String.hpp
#pragma once

#include "akkaradb/typed/serialization/Core.hpp"
#include <bit>
#include <array>
#include <string>
#include <vector>
#include <span>
#include <algorithm>

/**
 * std::string serialization.
 * Format: [u32 length (LE)] + [UTF-8 bytes]
 */
namespace akkaradb::typed::serialization {
    inline void serialize(std::vector<uint8_t>& buf, const std::string& str) {
        // Length prefix using bit_cast
        const auto len = static_cast<uint32_t>(str.size());
        auto len_bytes = std::bit_cast<std::array<uint8_t, 4>>(len);
        buf.insert(buf.end(), len_bytes.begin(), len_bytes.end());

        // String data (UTF-8)
        buf.insert(buf.end(), str.begin(), str.end());
    }

    [[nodiscard]] inline std::string deserialize_string(std::span<const uint8_t>& data) {
        // Read length prefix using bit_cast
        if (data.size() < 4) {
            throw SerializationException(
                "deserialize_string: insufficient data for length prefix (need 4, got " +
                std::to_string(data.size()) + ")"
            );
        }

        std::array<uint8_t, 4> len_bytes;
        std::copy_n(data.begin(), 4, len_bytes.begin());
        const uint32_t len = std::bit_cast<uint32_t>(len_bytes);
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

    [[nodiscard]] inline size_t estimate_size(const std::string& str) { return 4 + str.size(); }
} // namespace akkaradb::typed::serialization