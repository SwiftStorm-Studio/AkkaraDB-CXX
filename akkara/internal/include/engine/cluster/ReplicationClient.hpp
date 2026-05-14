/*
 * AkkaraDB - The all-purpose KV store
 * Copyright (C) 2026 Swift Storm Studio
 */

#pragma once

#include "engine/cluster/ClusterConfig.hpp"
#include "engine/cluster/ReplFraming.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>

namespace akkaradb::engine::cluster {
    class ReplicationClient {
        public:
            using ApplyCallback = std::function<void(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t record_flags,
                uint64_t source_node_id
            )>;

            using BlobCallback = std::function<void(
                uint64_t seq,
                uint64_t blob_id,
                std::span<const uint8_t> content
            )>;

            [[nodiscard]] static std::unique_ptr<ReplicationClient> create(
                std::string primary_host,
                uint16_t primary_repl_port,
                uint64_t self_node_id,
                std::function<uint64_t()> get_last_seq,
                ClusterRuntimeOptions runtime_options = {}
            );

            ~ReplicationClient();

            ReplicationClient(const ReplicationClient&) = delete;
            ReplicationClient& operator=(const ReplicationClient&) = delete;

            void set_apply_callback(ApplyCallback callback);
            void set_blob_callback(BlobCallback callback);

            void start();
            void close();

            [[nodiscard]] bool connected() const noexcept;

        private:
            class Impl;
            explicit ReplicationClient(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::cluster
