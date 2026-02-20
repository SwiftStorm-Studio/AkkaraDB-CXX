/*
 * AkkaraDB - Low-latency, crash-safe JVM KV store with WAL & stripe parity
 * Copyright (C) 2026 RiriFa
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

// internal/src/engine/wal/WalOp.cpp
#include "engine/wal/WalOp.hpp"

#include <cstring>
#include <stdexcept>
#include <chrono>

namespace akkaradb::wal {
    // ============================================================================
    // Internal helpers
    // ============================================================================

    namespace internal {
        size_t write_header(core::BufferView buffer, uint32_t total_len, WalEntryType entry_type) noexcept {
            WalEntryHeader header{.total_len = total_len, .entry_type = entry_type, .reserved = {0, 0, 0},};
            std::memcpy(buffer.data(), &header, sizeof(header));
            return sizeof(header);
        }
    } // namespace internal

    // ============================================================================
    // Zero-allocation Write Path
    // ============================================================================

    size_t serialize_add_direct(
        core::BufferView buffer,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint64_t key_fp64,
        uint64_t mini_key
    ) {
        const size_t total = WalEntryHeader::SIZE + sizeof(core::AKHdr32) + key.size() + value.size();

        if (buffer.size() < total) { throw std::out_of_range("serialize_add_direct: buffer too small"); }

        size_t offset = internal::write_header(buffer, static_cast<uint32_t>(total), WalEntryType::Record);

        const core::AKHdr32 hdr{
            .k_len = static_cast<uint16_t>(key.size()),
            .v_len = static_cast<uint32_t>(value.size()),
            .seq = seq,
            .flags = core::AKHdr32::FLAG_NORMAL,
            .pad0 = 0,
            .key_fp64 = key_fp64,
            .mini_key = mini_key,
        };
        std::memcpy(buffer.data() + offset, &hdr, sizeof(hdr));
        offset += sizeof(hdr);

        if (!key.empty()) {
            std::memcpy(buffer.data() + offset, key.data(), key.size());
            offset += key.size();
        }
        if (!value.empty()) {
            std::memcpy(buffer.data() + offset, value.data(), value.size());
            offset += value.size();
        }

        return offset;
    }

    size_t serialize_delete_direct(core::BufferView buffer, std::span<const uint8_t> key, uint64_t seq, uint64_t key_fp64, uint64_t mini_key) {
        const size_t total = WalEntryHeader::SIZE + sizeof(core::AKHdr32) + key.size();

        if (buffer.size() < total) { throw std::out_of_range("serialize_delete_direct: buffer too small"); }

        size_t offset = internal::write_header(buffer, static_cast<uint32_t>(total), WalEntryType::Record);

        const core::AKHdr32 hdr{
            .k_len = static_cast<uint16_t>(key.size()),
            .v_len = 0,
            .seq = seq,
            .flags = core::AKHdr32::FLAG_TOMBSTONE,
            .pad0 = 0,
            .key_fp64 = key_fp64,
            .mini_key = mini_key,
        };
        std::memcpy(buffer.data() + offset, &hdr, sizeof(hdr));
        offset += sizeof(hdr);

        if (!key.empty()) {
            std::memcpy(buffer.data() + offset, key.data(), key.size());
            offset += key.size();
        }

        return offset;
    }

    size_t serialize_commit_direct(core::BufferView buffer, uint64_t seq, uint64_t timestamp) {
        constexpr size_t total = WalEntryHeader::SIZE + 16; // seq:u64 + timestamp:u64

        if (buffer.size() < total) { throw std::out_of_range("serialize_commit_direct: buffer too small"); }

        size_t offset = internal::write_header(buffer, static_cast<uint32_t>(total), WalEntryType::Commit);

        if (timestamp == 0) {
            auto now = std::chrono::system_clock::now();
            auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());
            timestamp = static_cast<uint64_t>(micros.count());
        }

        buffer.write_u64_le(offset, seq);
        offset += 8;
        buffer.write_u64_le(offset, timestamp);
        offset += 8;

        return offset;
    }

    // ============================================================================
    // Zero-copy Read Path
    // ============================================================================

    WalRecordOpRef::WalRecordOpRef(core::BufferView buffer)
        : buffer_{buffer}, header_{}, key_offset_{0}, value_offset_{0} {
        if (buffer.size() < WalEntryHeader::SIZE + sizeof(core::AKHdr32)) { throw std::runtime_error("WalRecordOpRef: buffer too small"); }

        WalEntryHeader wal_header{};
        std::memcpy(&wal_header, buffer.data(), sizeof(wal_header));

        if (buffer.size() < wal_header.total_len) { throw std::runtime_error("WalRecordOpRef: buffer too small for entry"); }
        if (wal_header.entry_type != WalEntryType::Record) { throw std::runtime_error("WalRecordOpRef: not a Record entry"); }

        std::memcpy(&header_, buffer.data() + WalEntryHeader::SIZE, sizeof(header_));

        key_offset_ = WalEntryHeader::SIZE + sizeof(core::AKHdr32);
        value_offset_ = key_offset_ + header_.k_len;

        if (value_offset_ + header_.v_len > buffer.size()) { throw std::runtime_error("WalRecordOpRef: buffer too small for key/value"); }
    }
} // namespace akkaradb::wal