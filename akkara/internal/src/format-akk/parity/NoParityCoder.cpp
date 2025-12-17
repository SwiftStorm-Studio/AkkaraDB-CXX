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

// internal/src/format-akk/parity/NoParityCoder.cpp
#include "format-akk/parity/NoParityCoder.hpp"
#include <stdexcept>

namespace akkaradb::format {
    std::unique_ptr<NoParityCoder> NoParityCoder::create() { return std::make_unique<akk::NoParityCoderImpl>(); }

    std::vector<core::OwnedBuffer> NoParityCoder::encode(
        std::span<const core::BufferView> data_blocks
    ) {
        // No parity blocks to generate
        (void)data_blocks;
        return {};
    }

    bool NoParityCoder::verify(
        std::span<const core::BufferView> data_blocks,
        std::span<const core::BufferView> parity_blocks
    ) const noexcept {
        // No parity to verify, always valid
        (void)data_blocks;
        (void)parity_blocks;
        return true;
    }

    std::vector<core::OwnedBuffer> NoParityCoder::reconstruct(
        std::span<const core::BufferView> data_blocks,
        std::span<const core::BufferView> parity_blocks,
        std::span<const size_t> missing_indices
    ) {
        (void)data_blocks;
        (void)parity_blocks;

        if (!missing_indices.empty()) { throw std::runtime_error("NoParityCoder: cannot reconstruct without parity"); }

        return {};
    }
} // namespace akkaradb::format