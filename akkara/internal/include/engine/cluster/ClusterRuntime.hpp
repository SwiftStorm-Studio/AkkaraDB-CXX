/*
 * AkkaraDB - The all-purpose KV store
 * Copyright (C) 2026 Swift Storm Studio
 */

#pragma once

#include "engine/cluster/ClusterConfig.hpp"
#include "engine/cluster/ClusterManager.hpp"
#include "engine/cluster/ClusterRouter.hpp"
#include "engine/cluster/ReplicationClient.hpp"
#include "engine/cluster/ReplicationServer.hpp"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <span>

namespace akkaradb::engine::cluster {
    struct ClusterEngineCallbacks {
        std::function<uint64_t()> get_current_seq;
        std::function<uint64_t()> get_last_seq;
        ReplicationClient::ApplyCallback apply;
        ReplicationClient::BlobCallback apply_blob;
        ClusterManager::RoleChangeCallback role_change;
    };

    class ClusterRuntime {
        public:
            [[nodiscard]] static std::unique_ptr<ClusterRuntime> create(
                std::filesystem::path db_dir,
                ClusterConfig config,
                uint64_t self_node_id,
                ClusterEngineCallbacks callbacks,
                ClusterRuntimeOptions runtime_options = {}
            );

            ~ClusterRuntime();

            ClusterRuntime(const ClusterRuntime&) = delete;
            ClusterRuntime& operator=(const ClusterRuntime&) = delete;

            void start();
            void close();

            [[nodiscard]] NodeRole role() const noexcept;
            [[nodiscard]] const ClusterRouter& router() const noexcept;

            void ship_entry(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t record_flags,
                uint64_t source_node_id
            );

            void ship_blob(uint64_t seq, uint64_t blob_id, std::span<const uint8_t> content);

        private:
            class Impl;
            explicit ClusterRuntime(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::cluster
