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

// akkara/akkaradb/include/akkaradb/typed/serialization/Bool.hpp
#pragma once

#include "akkaradb/typed/serialization/Core.hpp"
#include <vector>
#include <span>

/**
 * bool serialization.
 * Format: [u8] (0 = false, 1 = true)
 */
namespace akkaradb::typed::serialization {
    inline void serialize(std::vector<uint8_t>& buf, bool value) { buf.push_back(value ? 1 : 0); }

    [[nodiscard]] inline bool deserialize_bool(std::span<const uint8_t>& data) {
        if (data.empty()) { throw SerializationException("deserialize_bool: insufficient data"); }

        const uint8_t byte = data[0];
        data = data.subspan(1);

        return byte != 0;
    }

    [[nodiscard]] inline constexpr size_t estimate_size(bool) noexcept { return 1; }
} // namespace akkaradb::typed::serialization