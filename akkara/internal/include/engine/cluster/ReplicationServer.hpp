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

// internal/include/engine/cluster/ReplicationServer.hpp
#pragma once

#include "engine/wal/WalOp.hpp"
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>

namespace akkaradb::engine::cluster {

    /**
     * ReplicationServer — TCP server running on the Primary node.
     *
     * Responsibilities:
     *  - Listens on repl_port for incoming Replica connections.
     *  - On connection: performs handshake (ReplClientHello / ReplServerHello),
     *    replays buffered entries the Replica missed, then streams live entries.
     *  - Buffers the last ENTRY_BUFFER_SIZE WAL entries for catch-up.
     *  - ship()      — Called by AkkEngine after every WAL write; fans out to all Replicas.
     *  - ship_blob() — Called before ship() when the WAL entry has FLAG_BLOB set.
     *
     * sync_mode = true  → ship() blocks until every connected Replica has ACK'd.
     * sync_mode = false → ship() returns immediately after enqueueing (fire-and-forget).
     */
    class ReplicationServer {
    public:
        static constexpr size_t ENTRY_BUFFER_SIZE = 4096;

        /**
         * Creates (but does not start) the server.
         *
         * @param repl_port       TCP port to listen on.
         * @param self_node_id    This node's ID (sent in ServerHello).
         * @param get_current_seq Callback that returns the Primary's latest committed seq
         *                        (used in ServerHello so the Replica knows how far behind it is).
         * @param sync_mode       If true, ship() waits for all Replica ACKs.
         */
        [[nodiscard]] static std::unique_ptr<ReplicationServer> create(
            uint16_t                   repl_port,
            uint64_t                   self_node_id,
            std::function<uint64_t()>  get_current_seq,
            bool                       sync_mode = false
        );

        ~ReplicationServer();

        /** Binds the listen socket and starts the accept-loop thread. */
        void start();

        /**
         * Ships a WAL entry (TYPE_ENTRY = 0x01) to every connected Replica.
         * In sync_mode, blocks until all connected Replicas have sent a ReplAck
         * for this seq (or up to a 5-second timeout).
         */
        void ship(
            uint64_t                  seq,
            wal::WalEntryType         wal_type,
            std::span<const uint8_t>  key,
            std::span<const uint8_t>  val
        );

        /**
         * Ships a BLOB payload (TYPE_BLOB = 0x02) to every connected Replica.
         * MUST be called before the ship() for the WAL entry that references
         * this blob_id, so the Replica can write the blob file first.
         * TCP ordering guarantees the blob arrives before the WAL entry.
         */
        void ship_blob(
            uint64_t                  blob_id,
            std::span<const uint8_t>  content
        );

        /** Returns the number of currently connected Replicas. */
        [[nodiscard]] size_t replica_count() const noexcept;

        /** Gracefully closes all connections and stops background threads. */
        void close();

    private:
        struct Impl;
        std::unique_ptr<Impl> impl_;

        explicit ReplicationServer(std::unique_ptr<Impl> impl);
    };

} // namespace akkaradb::engine::cluster
