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

#pragma once

#include "core/record/RecordView.hpp"
#include "core/types/ByteView.hpp"

using namespace akkaradb::core;

namespace akkaradb::engine {

    /**
     * @brief Abstract ordered iterator over MemTable snapshot view.
     *
     * IMemTableIterator provides ordered traversal of records visible
     * under a specific snapshot sequence boundary.
     *
     * Implementations are backend-specific:
     * - Skip List
     * - B+ Tree
     * - Red-Black Tree
     *
     * Iterator validity rules:
     * - valid() must be checked before value()
     * - next() advances to next visible record
     * - seek() repositions iterator to lower bound
     */
    class IMemTableIterator {
        public:
            virtual ~IMemTableIterator() = default;

            /**
             * @brief Check whether iterator currently points to a valid record.
             *
             * @return true if valid.
             * @return false if end reached.
             */
            [[nodiscard]] virtual bool valid() const = 0;

            /**
             * @brief Advance iterator to next visible record.
             */
            virtual void next() = 0;

            /**
             * @brief Seek to first record whose key is >= target key.
             *
             * Position after seek() may be invalid if no matching
             * or larger key exists.
             *
             * @param key Target key bytes.
             */
            virtual void seek(ByteView key) = 0;

            /**
             * @brief Retrieve current visible record.
             *
             * valid() must be true before calling value().
             *
             * @return Current record view.
             */
            [[nodiscard]] virtual RecordView value() const = 0;
    };

} // namespace akkaradb::engine