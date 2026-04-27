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

// internal/src/core/record/OwnedRecord.cpp
#include "core/record/OwnedRecord.hpp"

namespace akkaradb::core {

    // ==================== Factory ====================

    OwnedRecord OwnedRecord::create(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint8_t flags,
        BufferArena& arena,
        uint64_t fp64,
        uint64_t mk
    ) {
        OwnedRecord r;

        r.hdr.k_len = static_cast<uint16_t>(key.size());
        r.hdr.v_len = static_cast<uint16_t>(value.size());
        r.hdr.seq   = seq;
        r.hdr.flags = flags;

        r.key_fp64 = fp64;

        if (mk != 0) {
            r.mini_key = mk;
        } else {
            r.mini_key = 0;
            const size_t n = std::min<size_t>(key.size(), 8);
            for (size_t i = 0; i < n; ++i) {
                r.mini_key |= static_cast<uint64_t>(key[i]) << (i * 8);
            }
        }

        r.data = SmallBuffer(
            key.data(), key.size(),
            value.data(), value.size(),
            arena
        );

        return r;
    }

    OwnedRecord OwnedRecord::create(
        std::string_view key,
        std::string_view value,
        uint64_t seq,
        uint8_t flags,
        BufferArena& arena
    ) {
        return create(
            std::span{reinterpret_cast<const uint8_t*>(key.data()), key.size()},
            std::span{reinterpret_cast<const uint8_t*>(value.data()), value.size()},
            seq,
            flags,
            arena
        );
    }

    OwnedRecord OwnedRecord::tombstone(
        std::span<const uint8_t> key,
        uint64_t seq,
        BufferArena& arena,
        uint64_t fp64
    ) {
        return create(
            key,
            {},
            seq,
            MemHdr16::FLAG_TOMBSTONE,
            arena,
            fp64
        );
    }

} // namespace akkaradb::core