/*
 * AkkaraDB - The all-purpose KV store
 * Copyright (C) 2026 Swift Storm Studio
 */

#pragma once

#include "engine/cluster/ClusterConfig.hpp"

#include <cstdint>
#include <span>
#include <vector>

namespace akkaradb::engine::cluster {
    class ClusterRouter {
        public:
            explicit ClusterRouter(ClusterConfig config);

            [[nodiscard]] std::vector<NodeInfo> write_targets(std::span<const uint8_t> key) const;
            [[nodiscard]] std::vector<NodeInfo> read_candidates(std::span<const uint8_t> key) const;

        private:
            [[nodiscard]] NodeInfo stripe_target(std::span<const uint8_t> key) const;

            ClusterConfig config_;
            std::vector<NodeInfo> data_nodes_;
    };
} // namespace akkaradb::engine::cluster
