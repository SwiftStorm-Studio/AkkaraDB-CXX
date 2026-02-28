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

// internal/src/engine/wal/WalFraming.cpp
#include "engine/wal/WalFraming.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <thread>

namespace akkaradb::wal {
    // ============================================================================
    // Shard count
    // ============================================================================

    uint32_t compute_shard_count() noexcept {
        const uint32_t cores = static_cast<uint32_t>(std::thread::hardware_concurrency());

        // Clamp to [2, 16]
        const uint32_t clamped = std::max(2u, std::min(cores, 16u));

        // Round up to nearest power of 2
        uint32_t n = 2;
        while (n < clamped) n <<= 1;

        return std::min(n, 16u);
    }

    // ============================================================================
    // WalSegmentHeader
    // ============================================================================

    bool WalSegmentHeader::verify_checksum(core::BufferView buffer) const noexcept {
        if (buffer.size() < SIZE) return false;

        // Copy header into local buffer, zero the crc32c field, then compute  E        // avoids const_cast on this and avoids mutating the caller's buffer.
        std::byte tmp[SIZE];
        std::memcpy(tmp, buffer.data(), SIZE);

        const uint32_t stored = crc32c;
        const uint32_t zero = 0;
        std::memcpy(tmp + offsetof(WalSegmentHeader, crc32c), &zero, sizeof(zero));

        const uint32_t computed = core::BufferView{tmp, SIZE}.crc32c(0, SIZE);
        return stored == computed;
    }

    size_t WalSegmentHeader::write(core::BufferView buffer, uint16_t shard_id, uint64_t segment_id) noexcept {
        auto now = std::chrono::system_clock::now();
        auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());

        WalSegmentHeader hdr{
            .magic = MAGIC,
            .version = VERSION,
            .shard_id = shard_id,
            .segment_id = segment_id,
            .created_at = static_cast<uint64_t>(micros.count()),
            .crc32c = 0,
            // Filled after
            .reserved = 0,
        };

        std::memcpy(buffer.data(), &hdr, SIZE);

        // Compute and write CRC over header (crc32c field = 0)
        const uint32_t crc = buffer.crc32c(0, SIZE);
        std::memcpy(buffer.data() + offsetof(WalSegmentHeader, crc32c), &crc, sizeof(crc));

        return SIZE;
    }

    // ============================================================================
    // WalBatchHeader
    // ============================================================================

    uint32_t WalBatchHeader::compute_checksum(core::BufferView buffer, size_t total_size) noexcept {
        // buffer.data() is non-const std::byte*, so direct memcpy is safe.
        // Save the stored crc32c, zero it in-place, compute, then restore  E        // no const_cast needed.
        uint32_t original;
        std::memcpy(&original, buffer.data() + offsetof(WalBatchHeader, crc32c), sizeof(original));

        const uint32_t zero = 0;
        std::memcpy(buffer.data() + offsetof(WalBatchHeader, crc32c), &zero, sizeof(zero));

        const uint32_t crc = buffer.crc32c(0, total_size);

        // Restore
        std::memcpy(buffer.data() + offsetof(WalBatchHeader, crc32c), &original, sizeof(original));

        return crc;
    }

    bool WalBatchHeader::verify_checksum(core::BufferView buffer) const noexcept {
        if (buffer.size() < batch_size) return false;
        const uint32_t expected = compute_checksum(buffer, batch_size);
        return crc32c == expected;
    }

    size_t WalBatchHeader::write(core::BufferView buffer, uint64_t batch_seq, uint32_t entry_count, uint32_t batch_size) noexcept {
        WalBatchHeader hdr{
            .magic = MAGIC,
            .batch_seq = batch_seq,
            .entry_count = entry_count,
            .batch_size = batch_size,
            .crc32c = 0,
            // Filled by finalize_checksum after entries are written
        };
        std::memcpy(buffer.data(), &hdr, SIZE);
        return SIZE;
    }

    void WalBatchHeader::finalize_checksum(core::BufferView buffer, size_t total_size) noexcept {
        const uint32_t crc = compute_checksum(buffer, total_size);
        std::memcpy(buffer.data() + offsetof(WalBatchHeader, crc32c), &crc, sizeof(crc));
    }

    // ============================================================================
    // WalIterator
    // ============================================================================

    bool WalIterator::next() noexcept {
        // Check if there is at least one more header
        if (offset_ + WalEntryHeader::SIZE > buffer_.size()) { return false; }

        // Read the header
        std::memcpy(&current_header_, buffer_.data() + offset_, sizeof(current_header_));

        // Validate: total_len must cover at least the header itself
        if (current_header_.total_len < WalEntryHeader::SIZE) { return false; }

        // Validate: entry must fit within buffer
        if (offset_ + current_header_.total_len > buffer_.size()) { return false; }

        offset_ += current_header_.total_len;
        return true;
    }

    std::pair<uint64_t, uint64_t> WalIterator::as_commit() const noexcept {
        const core::BufferView entry = entry_buffer();

        uint64_t seq = entry.read_u64_le(WalEntryHeader::SIZE);
        uint64_t timestamp = entry.read_u64_le(WalEntryHeader::SIZE + 8);

        return {seq, timestamp};
    }
} // namespace akkaradb::wal