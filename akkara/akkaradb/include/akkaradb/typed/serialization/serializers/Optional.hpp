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

// akkara/akkaradb/include/akkaradb/typed/serialization/Optional.hpp
#pragma once

#include "Bool.hpp"
#include <optional>
#include <span>
#include <vector>

/**
 * std::optional<T> serialization.
 * Format: [u8 has_value] + [value if has_value]
 */
namespace akkaradb::typed::serialization {
    // Forward declaration
    template <typename T>
    [[nodiscard]] T deserialize(std::span<const uint8_t> & data);

    template <typename T>
    void serialize(std::vector<uint8_t>& buf, const std::optional<T>& opt) {
        serialize(buf, opt.has_value());

        if (opt.has_value()) { serialize(buf, *opt); }
    }

    template <typename T>
    [[nodiscard]] std::optional<T> deserialize_optional(std::span<const uint8_t>& data) {
        if (deserialize_bool(data)) { return deserialize<T>(data); }

        return std::nullopt;
    }

    template <typename T>
    [[nodiscard]] size_t estimate_size(const std::optional<T>& opt) {
        if (opt.has_value()) { return 1 + estimate_size(*opt); }
        return 1;
    }
} // namespace akkaradb::typed::serialization