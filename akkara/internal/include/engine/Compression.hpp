/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/include/engine/Compression.hpp
#pragma once

#include <cstdint>

namespace akkaradb::engine {

    /**
     * Codec — compression algorithm used for SST data sections and .blob files.
     *
     *   None — No compression. Maximum read speed, full backward compatibility.
     *           Index, bloom filter, and file headers are always uncompressed
     *           regardless of this setting.
     *
     *   Zstd — Zstandard (https://facebook.github.io/zstd/).
     *           ~50-70% size reduction for typical KV workloads.
     *           Default compression level 3: very fast decompression,
     *           single-digit microseconds for 64 KiB blocks.
     *
     * The codec is stored in the file header (SST: flags byte; Blob: flags field)
     * so each file is fully self-describing.  Mixed-codec databases are supported:
     * old uncompressed files can coexist with new compressed files.
     */
    enum class Codec : uint8_t {
        None = 0,   ///< Uncompressed (default, backward compatible)
        Zstd = 1,   ///< Zstandard compression
    };

} // namespace akkaradb::engine
