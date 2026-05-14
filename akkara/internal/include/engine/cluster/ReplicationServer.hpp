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

namespace akkaradb::engine::cluster {
    class ReplicationServer {
        public:
            static constexpr size_t ENTRY_BUFFER_SIZE = 4096;

            [[nodiscard]] static std::unique_ptr<ReplicationServer> create(
                uint16_t repl_port,
                uint64_t self_node_id,
                std::function<uint64_t()> get_current_seq,
                AckPolicy ack_policy,
                ClusterRuntimeOptions runtime_options = {}
            );

            ~ReplicationServer();

            ReplicationServer(const ReplicationServer&) = delete;
            ReplicationServer& operator=(const ReplicationServer&) = delete;

            void start();
            void close();

            void ship_entry(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t record_flags,
                uint64_t source_node_id
            );

            void ship_blob(uint64_t seq, uint64_t blob_id, std::span<const uint8_t> content);

            [[nodiscard]] size_t replica_count() const noexcept;

        private:
            class Impl;
            explicit ReplicationServer(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::cluster
