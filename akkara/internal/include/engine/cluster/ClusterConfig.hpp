/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace akkaradb::engine::cluster {
    enum class ReplicationMode : uint8_t {
        Standalone = 0,
        Mirror = 1,
        Stripe = 2,
    };

    enum class AckPolicyMode : uint8_t {
        Async = 0,
        All = 1,
        Quorum = 2,
    };

    enum class TransportMode : uint8_t {
        TLS = 0,
        Plain = 1,
    };

    enum class NodeRole : uint8_t {
        Standalone = 0,
        Primary = 1,
        Replica = 2,
    };

    enum NodeCapability : uint32_t {
        CoordinatorEligible = 1u << 0,
        DataBearing = 1u << 1,
    };

    struct AckPolicy {
        AckPolicyMode mode = AckPolicyMode::Async;
        uint16_t quorum = 0;
    };

    struct NodeInfo {
        uint64_t node_id = 0;
        std::string host;
        uint16_t data_port = 0;
        uint16_t repl_port = 0;
        uint32_t capabilities = DataBearing;

        [[nodiscard]] bool coordinator_eligible() const noexcept {
            return (capabilities & CoordinatorEligible) != 0;
        }

        [[nodiscard]] bool data_bearing() const noexcept {
            return (capabilities & DataBearing) != 0;
        }
    };

    struct ClusterTlsOptions {
        std::filesystem::path cert_path;
        std::filesystem::path key_path;
        std::filesystem::path ca_path;
        bool verify_peer = true;
    };

    struct ClusterRuntimeOptions {
        TransportMode transport_mode = TransportMode::TLS;
        ClusterTlsOptions tls;
    };

    class ClusterConfig {
        public:
            static constexpr uint32_t MAGIC = 0x35434B41; // "AKC5"
            static constexpr uint16_t VERSION = 1;

            ClusterConfig() = default;
            ClusterConfig(std::vector<NodeInfo> nodes, ReplicationMode mode, AckPolicy ack_policy);

            [[nodiscard]] static ClusterConfig load(const std::filesystem::path& path);
            static void save(const std::filesystem::path& path, const ClusterConfig& config);

            [[nodiscard]] const std::vector<NodeInfo>& nodes() const noexcept { return nodes_; }
            [[nodiscard]] ReplicationMode mode() const noexcept { return mode_; }
            [[nodiscard]] AckPolicy ack_policy() const noexcept { return ack_policy_; }
            [[nodiscard]] uint16_t flags() const noexcept { return flags_; }

            [[nodiscard]] const NodeInfo* find_by_id(uint64_t node_id) const noexcept;
            [[nodiscard]] std::vector<NodeInfo> data_nodes() const;
            [[nodiscard]] std::vector<NodeInfo> coordinator_nodes() const;
            [[nodiscard]] bool is_standalone() const noexcept;

            void validate() const;

        private:
            std::vector<NodeInfo> nodes_;
            ReplicationMode mode_ = ReplicationMode::Standalone;
            AckPolicy ack_policy_{};
            uint16_t flags_ = 0;
    };
} // namespace akkaradb::engine::cluster
