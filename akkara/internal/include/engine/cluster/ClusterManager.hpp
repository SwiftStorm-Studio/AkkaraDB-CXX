/*
 * AkkaraDB - The all-purpose KV store
 * Copyright (C) 2026 Swift Storm Studio
 */

#pragma once

#include "engine/cluster/ClusterConfig.hpp"

#include <filesystem>
#include <functional>
#include <memory>
#include <string>

namespace akkaradb::engine::cluster {
    class ClusterManager {
        public:
            using RoleChangeCallback = std::function<void(NodeRole)>;

            [[nodiscard]] static std::unique_ptr<ClusterManager> create(
                std::filesystem::path db_dir,
                ClusterConfig config,
                uint64_t self_node_id
            );

            ~ClusterManager();

            ClusterManager(const ClusterManager&) = delete;
            ClusterManager& operator=(const ClusterManager&) = delete;

            void set_role_change_callback(RoleChangeCallback callback);
            void start();
            void close();

            [[nodiscard]] NodeRole role() const noexcept;
            [[nodiscard]] uint64_t self_node_id() const noexcept;
            [[nodiscard]] std::string primary_host() const;
            [[nodiscard]] uint16_t primary_repl_port() const;
            [[nodiscard]] bool is_standalone() const noexcept;

        private:
            class Impl;
            explicit ClusterManager(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::cluster
