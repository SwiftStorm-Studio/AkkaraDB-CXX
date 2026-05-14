/*
 * AkkaraDB - The all-purpose KV store
 * Copyright (C) 2026 Swift Storm Studio
 */

#include "engine/cluster/ClusterRouter.hpp"

#include <limits>
#include <stdexcept>

namespace akkaradb::engine::cluster {
    namespace {
        uint64_t fnv1a64(std::span<const uint8_t> bytes, uint64_t seed = 14695981039346656037ull) noexcept {
            uint64_t hash = seed;
            for (uint8_t b : bytes) {
                hash ^= b;
                hash *= 1099511628211ull;
            }
            return hash;
        }

        uint64_t rendezvous_score(std::span<const uint8_t> key, uint64_t node_id) noexcept {
            uint8_t id_bytes[8];
            for (size_t i = 0; i < 8; ++i) {
                id_bytes[i] = static_cast<uint8_t>(node_id >> (8 * i));
            }
            return fnv1a64(std::span<const uint8_t>(id_bytes, 8), fnv1a64(key));
        }
    } // namespace

    ClusterRouter::ClusterRouter(ClusterConfig config)
        : config_{std::move(config)}, data_nodes_{config_.data_nodes()} {
        config_.validate();
    }

    std::vector<NodeInfo> ClusterRouter::write_targets(std::span<const uint8_t> key) const {
        switch (config_.mode()) {
            case ReplicationMode::Standalone:
                return data_nodes_.empty() ? std::vector<NodeInfo>{} : std::vector<NodeInfo>{data_nodes_.front()};
            case ReplicationMode::Mirror:
                return data_nodes_;
            case ReplicationMode::Stripe:
                return {stripe_target(key)};
        }
        throw std::logic_error("ClusterRouter: invalid replication mode");
    }

    std::vector<NodeInfo> ClusterRouter::read_candidates(std::span<const uint8_t> key) const {
        switch (config_.mode()) {
            case ReplicationMode::Standalone:
                return data_nodes_.empty() ? std::vector<NodeInfo>{} : std::vector<NodeInfo>{data_nodes_.front()};
            case ReplicationMode::Mirror:
                return data_nodes_;
            case ReplicationMode::Stripe:
                return {stripe_target(key)};
        }
        throw std::logic_error("ClusterRouter: invalid replication mode");
    }

    NodeInfo ClusterRouter::stripe_target(std::span<const uint8_t> key) const {
        if (data_nodes_.empty()) {
            throw std::runtime_error("ClusterRouter: no data-bearing nodes");
        }

        const NodeInfo* best = nullptr;
        uint64_t best_score = 0;
        for (const auto& node : data_nodes_) {
            const uint64_t score = rendezvous_score(key, node.node_id);
            if (best == nullptr || score > best_score || (score == best_score && node.node_id < best->node_id)) {
                best = &node;
                best_score = score;
            }
        }
        return *best;
    }
} // namespace akkaradb::engine::cluster
