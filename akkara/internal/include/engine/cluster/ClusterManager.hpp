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

// internal/include/engine/cluster/ClusterManager.hpp
#pragma once

#include "engine/cluster/ClusterConfig.hpp"
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <cstdint>

namespace akkaradb::engine::cluster {

    // ============================================================================
    // NodeRole
    // ============================================================================

    /**
     * NodeRole - The role this node currently holds in the cluster.
     *
     * Standalone: No replication. Single-node mode.
     * Primary   : Coordinator-only. Owns Manifest, routing, election, and
     *             the ReplicationServer. Does NOT hold data shards.
     * Replica   : Data-bearing node. Receives writes from Primary via
     *             ReplicationClient. Serves reads directly.
     */
    enum class NodeRole { Standalone, Primary, Replica };

    // ============================================================================
    // ClusterManager
    // ============================================================================

    /**
     * ClusterManager - Node identity, role election, and health monitoring.
     *
     * Role election uses an OS-level exclusive file lock on PRIMARY.lock:
     *   - First node to acquire the lock becomes Primary.
     *   - Other nodes read the lock file to discover Primary's address.
     *
     * Lock file layout:
     *   [node_id:u64][repl_port:u16][host_len:u16][host bytes]
     *
     * Health monitoring (Replica side):
     *   A background thread attempts a TCP connect to Primary every 2 s.
     *   After 3 consecutive failures, the lock is considered stale and
     *   this node tries to re-acquire Primary role (re-flock).
     *
     * Thread-safety: All public methods are thread-safe after start().
     */
    class ClusterManager {
        public:
            // ----------------------------------------------------------------
            // Factory
            // ----------------------------------------------------------------

            /**
             * Creates and initialises the ClusterManager.
             *
             * @param db_dir      Directory containing PRIMARY.lock
             * @param config      Cluster topology and settings
             * @param self_node_id This node's identifier (must be in config.nodes())
             * @throws std::runtime_error if self_node_id is not found in config.
             */
            [[nodiscard]] static std::unique_ptr<ClusterManager> create(
                const std::filesystem::path& db_dir,
                const ClusterConfig&         config,
                uint64_t                     self_node_id
            );

            ~ClusterManager();

            ClusterManager(const ClusterManager&)            = delete;
            ClusterManager& operator=(const ClusterManager&) = delete;

            // ----------------------------------------------------------------
            // Lifecycle
            // ----------------------------------------------------------------

            /**
             * Performs role election and starts the health monitor thread.
             * Must be called before any other method.
             */
            void start();

            /**
             * Releases the Primary lock (if held) and stops all threads.
             * Idempotent.
             */
            void close();

            // ----------------------------------------------------------------
            // Role callbacks
            // ----------------------------------------------------------------

            /**
             * Registers a callback invoked whenever the role changes.
             * Called with the NEW role. May be called from a background thread.
             * Must be registered before start().
             */
            void set_role_change_callback(std::function<void(NodeRole)> cb);

            // ----------------------------------------------------------------
            // State queries
            // ----------------------------------------------------------------

            [[nodiscard]] NodeRole   role()             const noexcept;
            [[nodiscard]] uint64_t   self_node_id()     const noexcept;

            /**
             * Returns the Primary node's replication host.
             * Valid only when role() == NodeRole::Replica.
             */
            [[nodiscard]] std::string primary_host()      const;
            [[nodiscard]] uint16_t    primary_repl_port() const;

            /**
             * Returns true if this node is in standalone mode
             * (config.is_standalone() or ClusterConfig absent).
             */
            [[nodiscard]] bool is_standalone() const noexcept;

        private:
            explicit ClusterManager(
                const std::filesystem::path& db_dir,
                const ClusterConfig&         config,
                uint64_t                     self_node_id
            );

            class Impl;
            std::unique_ptr<Impl> impl_;
    };

} // namespace akkaradb::engine::cluster
