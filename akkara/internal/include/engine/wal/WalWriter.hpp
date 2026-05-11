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

// internal/include/engine/wal/WalWriter.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>

namespace akkaradb::engine::wal {
    enum class WalSyncMode : uint8_t {
        Sync = 0,
        Async = 1,
        Off = 2,
    };

    struct WalOptions {
        std::filesystem::path wal_dir;
        WalSyncMode sync_mode = WalSyncMode::Sync;
        uint16_t shard_count = 4;
        uint32_t group_n = 128;
        uint32_t group_micros = 100;
    };

    class WalWriter {
        public:
            [[nodiscard]] static std::unique_ptr<WalWriter> create(WalOptions options);
            ~WalWriter();

            WalWriter(const WalWriter&) = delete;
            WalWriter& operator=(const WalWriter&) = delete;
            WalWriter(WalWriter&&) = delete;
            WalWriter& operator=(WalWriter&&) = delete;

            void append(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputed_fp64 = 0
            );

            void force_sync();
            void prune_until(uint64_t checkpoint_seq);
            void close();

        private:
            WalWriter() = default;

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::wal
