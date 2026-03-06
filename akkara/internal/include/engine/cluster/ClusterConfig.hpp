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

// internal/include/engine/cluster/ClusterConfig.hpp
#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace akkaradb::engine::cluster {

    // ============================================================================
    // ReplicationMode
    // ============================================================================

    /**
     * ReplicationMode - How data is distributed across Replica nodes.
     *
     * STANDALONE: Single node, no replication.
     * MIRROR    : Full copies on all Replica nodes (RAID1-like).
     *             Every write is replicated to all Replicas.
     *             Tolerates R-1 Replica failures.
     * STRIPE    : Data shards round-robined across Replicas (RAID0-like).
     *             No redundancy. Primary is coordinator-only (no data shard).
     *             Any single Replica failure = data loss for that shard.
     */
    enum class ReplicationMode : uint8_t {
        Standalone = 0x00,
        Mirror     = 0x01,
        Stripe     = 0x02,
    };

    // ============================================================================
    // NodeInfo
    // ============================================================================

    /**
     * NodeInfo - Identity and address of a single cluster node.
     */
    struct NodeInfo {
        uint64_t    node_id;    ///< Unique 64-bit node identifier
        std::string host;       ///< Hostname or IP address
        uint16_t    data_port;  ///< Port for client-facing traffic
        uint16_t    repl_port;  ///< Port for inter-node replication stream
    };

    // ============================================================================
    // ClusterConfig - Cluster topology and replication settings
    // ============================================================================

    /**
     * ClusterConfig - Persistent cluster configuration (.akcc file).
     *
     * Binary on-disk format:
     *
     * FileHeader (32 bytes):
     *   [magic:u32="AKCC"][version:u16][flags:u16]
     *   [node_count:u16][mode:u8][repl_factor:u8]
     *   [created_at_us:u64][crc32c:u32][reserved:u8×8]
     *
     * Per-node entry (variable):
     *   [node_id:u64][data_port:u16][repl_port:u16][host_len:u16][host bytes]
     *
     * Design:
     * - Primary is coordinator-only: holds Manifest, routing, and election state.
     *   Data shards live exclusively on Replica nodes.
     * - repl_factor: how many Replica nodes hold a copy (MIRROR mode only).
     *   In STRIPE mode this equals the total number of Replicas.
     */
    class ClusterConfig {
        public:
            static constexpr uint32_t MAGIC   = 0x414B4343; ///< "AKCC"
            static constexpr uint16_t VERSION = 0x0001;

            // ----------------------------------------------------------------
            // flags field bits
            // ----------------------------------------------------------------

            /// Enable the built-in WebConfig HTTP server (for browser-based management).
            /// When set, the engine starts an embedded HTTP server on web_config_port.
            static constexpr uint16_t FLAG_WEB_CONFIG_ENABLED = 0x0001;

            // ----------------------------------------------------------------
            // Factory
            // ----------------------------------------------------------------

            /**
             * Loads a ClusterConfig from a .akcc binary file.
             * @throws std::runtime_error on I/O or format error.
             */
            [[nodiscard]] static ClusterConfig load(const std::filesystem::path& path);

            /**
             * Saves this config to a .akcc binary file (atomic write).
             * @throws std::runtime_error on I/O error.
             */
            static void save(const std::filesystem::path& path, const ClusterConfig& cfg);

            // ----------------------------------------------------------------
            // Constructors
            // ----------------------------------------------------------------

            ClusterConfig() = default;

            ClusterConfig(
                std::vector<NodeInfo> nodes,
                ReplicationMode       mode,
                uint8_t               repl_factor
            ) : nodes_{std::move(nodes)}, mode_{mode}, repl_factor_{repl_factor} {}

            // ----------------------------------------------------------------
            // Accessors
            // ----------------------------------------------------------------

            [[nodiscard]] const std::vector<NodeInfo>& nodes()       const noexcept { return nodes_; }
            [[nodiscard]] ReplicationMode               mode()        const noexcept { return mode_; }
            [[nodiscard]] uint8_t                       repl_factor() const noexcept { return repl_factor_; }
            [[nodiscard]] uint16_t flags() const noexcept { return flags_; }

            /// Returns true if the integrated WebConfig HTTP server should be started.
            [[nodiscard]] bool web_config_enabled() const noexcept { return (flags_ & FLAG_WEB_CONFIG_ENABLED) != 0; }

            void set_web_config_enabled(bool enabled) noexcept {
                if (enabled) flags_ |= FLAG_WEB_CONFIG_ENABLED;
                else flags_ &= ~FLAG_WEB_CONFIG_ENABLED;
            }

            /**
             * Finds a node by its node_id.
             * @return Pointer to NodeInfo, or nullptr if not found.
             */
            [[nodiscard]] const NodeInfo* find_by_id(uint64_t node_id) const noexcept;

            /**
             * Returns true if this is a single-node (standalone) configuration.
             */
            [[nodiscard]] bool is_standalone() const noexcept {
                return mode_ == ReplicationMode::Standalone || nodes_.size() <= 1;
            }

        private:
            std::vector<NodeInfo> nodes_;
            ReplicationMode       mode_        = ReplicationMode::Standalone;
            uint8_t               repl_factor_ = 1;
            uint16_t flags_ = 0;
    };

} // namespace akkaradb::engine::cluster
