// internal/include/format-api/RecordCursor.hpp
#pragma once

#include "core/record/RecordView.hpp"
#include <optional>
#include <stdexcept>

namespace akkaradb::format {

    /**
     * RecordCursor - Forward-only iterator over records in a block.
     *
     * This abstract interface provides zero-copy iteration through records
     * without materializing them into a vector.
     *
     * Design principles:
     * - Forward-only: Single-pass iteration
     * - Zero-copy: Returns RecordView (non-owning)
     * - Fail-fast: Returns std::nullopt on malformed data
     * - Abstract interface: Allows different cursor implementations
     *
     * Typical usage:
     * ```cpp
     * auto cursor = unpacker->cursor(block);
     *
     * while (cursor->has_next()) {
     *     auto record_opt = cursor->try_next();
     *     if (!record_opt) {
     *         // Malformed record, stop iteration
     *         break;
     *     }
     *
     *     auto& record = *record_opt;
     *     // Process record...
     * }
     * ```
     *
     * Thread-safety: NOT thread-safe.
     */
    class RecordCursor {
    public:
        virtual ~RecordCursor() = default;

        /**
         * Checks if more records are available.
         *
         * @return true if try_next() will return a record
         */
        [[nodiscard]] virtual bool has_next() const noexcept = 0;

        /**
         * Reads the next record.
         *
         * @return RecordView if successful, std::nullopt if malformed or end
         */
        [[nodiscard]] virtual std::optional<core::RecordView> try_next() = 0;

        /**
         * Convenience method that throws on malformed data.
         *
         * @return RecordView
         * @throws std::runtime_error if no more records or malformed
         */
        [[nodiscard]] core::RecordView next() {
            auto opt = try_next();
            if (!opt) {
                throw std::runtime_error("RecordCursor::next: no more records or malformed data");
            }
            return *opt;
        }
    };

} // namespace akkaradb::format