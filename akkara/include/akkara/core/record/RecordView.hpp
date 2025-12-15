// RecordView.hpp
#pragma once

#include "AKHdr32.hpp"
#include <string_view>

namespace akkaradb::core {
    /**
 * RecordView - Zero-copy view over an on-disk record.
 *
 * This class provides a read-only view of a record stored in a block
 * payload, without copying any data. It corresponds to JVM's RecordView.
 *
 * Design principles:
 * - No ownership: views into a block buffer
 * - Zero-copy: key/value are string_views into the block
 * - Lightweight: 56 bytes (fits in 1 cache line)
 *
 * Thread-safety: Immutable after construction. The underlying buffer
 * must remain valid for the lifetime of this view.
 *
 * Typical usage:
 * ```cpp
 * auto view = RecordView::parse(block_buffer, offset);
 * std::string_view key = view.key();
 * std::string_view value = view.value();
 * uint64_t seq = view.header().seq;
 * ```
 */
    class RecordView {
    public:
        /**
     * Constructs a RecordView from header and buffer regions.
     *
     * @param header Parsed AKHdr32 header
     * @param key_view View of key data
     * @param value_view View of value data
     */
        RecordView(AKHdr32& header, std::string_view key_view, std::string_view value_view) noexcept
            : header_{header}, key_{key_view}, value_{value_view} {
        }

        /**
     * Parses a RecordView from a block buffer.
     *
     * Format: [AKHdr32 (32B)][key (k_len)][value (v_len)]
     *
     * @param buf Block buffer
     * @param offset Starting offset (updated to point after this record)
     * @return Parsed RecordView
     * @throws std::out_of_range if buffer is too small or record is invalid
     */
        [[nodiscard]] static RecordView parse(BufferView buf, size_t& offset);

        /**
     * Returns the record header.
     */
        [[nodiscard]] const AKHdr32& header() const noexcept { return header_; }

        /**
     * Returns the key as a string_view.
     */
        [[nodiscard]] std::string_view key() const noexcept { return key_; }

        /**
     * Returns the value as a string_view.
     */
        [[nodiscard]] std::string_view value() const noexcept { return value_; }

        /**
     * Returns true if this is a tombstone (deletion marker).
     */
        [[nodiscard]] bool is_tombstone() const noexcept {
            return (header_.flags & 0x01) != 0;
        }

        /**
     * Returns the total size of this record in bytes.
     *
     * Size = 32 (header) + k_len + v_len
     */
        [[nodiscard]] size_t total_size() const noexcept {
            return AKHdr32::SIZE + header_.k_len + header_.v_len;
        }

    private:
        AKHdr32 header_;
        std::string_view key_;
        std::string_view value_;
    };
} // namespace akkaradb::core