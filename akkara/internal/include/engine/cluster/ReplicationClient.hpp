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

// internal/include/engine/cluster/ReplicationClient.hpp
#pragma once

#include "engine/wal/WalOp.hpp"
#include "core/net/TlsStream.hpp"
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>

namespace akkaradb::engine::cluster {

    /**
     * ReplicationClient — TCP client running on the Replica node.
     *
     * Responsibilities:
     *  - Connects to the Primary's repl_host:repl_port.
     *  - Sends ReplClientHello with last_applied_seq so the Primary can replay
     *    any missed WAL entries.
     *  - Reads the incoming stream (ReplEntry, ReplBlobPut messages) and calls
     *    the registered callbacks to apply them locally.
     *  - After applying each WAL entry, sends a ReplAck back to the Primary.
     *  - Automatically reconnects 500 ms after a disconnect.
     *
     * Callback contract:
     *  - blob_callback  is called BEFORE apply_callback for the same blob_id.
     *    (TCP ordering ensures the blob message arrives first.)
     *  - apply_callback must write to WAL + MemTable using the seq supplied by
     *    the Primary (INV-1: only Primary calls next_seq()).
     */
    class ReplicationClient {
    public:
        /** Called when a blob payload arrives (before the WAL entry referencing it). */
        using BlobCallback  = std::function<void(
            uint64_t                 blob_id,
            std::span<const uint8_t> content
        )>;

        /** Called when a WAL entry arrives and passes CRC verification. */
        using ApplyCallback = std::function<void(
            uint64_t                 seq,
            wal::WalEntryType        wal_type,
            std::span<const uint8_t> key,
            std::span<const uint8_t> val
        )>;

        /**
         * Callback invoked by the client to obtain the last applied seq number.
         * Used to build the ReplClientHello at (re)connect time.
         */
        using GetLastSeqFn  = std::function<uint64_t()>;

        /**
         * Creates (but does not start) the client.
         *
         * @param primary_host     Hostname / IP of the Primary.
         * @param primary_repl_port TCP port of the Primary's ReplicationServer.
         * @param self_node_id     This Replica's node ID (sent in ClientHello).
         * @param get_last_seq     Callback that returns the last seq this Replica applied.
         */
        [[nodiscard]] static std::unique_ptr<ReplicationClient> create(
            std::string     primary_host,
            uint16_t        primary_repl_port,
            uint64_t        self_node_id,
            GetLastSeqFn get_last_seq,
            core::TlsConfig tls = {}
        );

        ~ReplicationClient();

        /** Registers the blob reception callback (must be set before start()). */
        void set_blob_callback(BlobCallback cb);

        /** Registers the WAL-apply callback (must be set before start()). */
        void set_apply_callback(ApplyCallback cb);

        /** Starts the background receive loop (with auto-reconnect). */
        void start();

        /** Stops the receive loop and closes the connection. */
        void close();

        /**
         * Returns true if currently connected to the Primary.
         * May briefly return false during reconnect attempts.
         */
        [[nodiscard]] bool connected() const noexcept;

    private:
        struct Impl;
        std::unique_ptr<Impl> impl_;

        explicit ReplicationClient(std::unique_ptr<Impl> impl);
    };

} // namespace akkaradb::engine::cluster
