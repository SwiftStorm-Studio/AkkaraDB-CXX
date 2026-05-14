/*
 * AkkaraDB - The all-purpose KV store
 * Copyright (C) 2026 Swift Storm Studio
 */

#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include "engine/cluster/ClusterConfig.hpp"

namespace akkaradb::engine::cluster {
    enum class ReplMsgType : uint8_t {
        ClientHello = 0x01,
        ServerHello = 0x02,
        Entry = 0x10,
        BlobPut = 0x11,
        Ack = 0x12,
        ReadRequest = 0x20,
        ReadResponse = 0x21,
    };

    enum class ReplOpType : uint8_t {
        Put = 1,
        Remove = 2,
    };

    enum class ReadStatus : uint8_t {
        Found = 0,
        NotFound = 1,
        Error = 2,
    };

    struct ReplFrameHeader {
        static constexpr uint32_t MAGIC = 0x35524B41; // "AKR5"
        static constexpr size_t SIZE = 14;

        ReplMsgType type{};
        uint8_t flags = 0;
        uint32_t payload_len = 0;
        uint32_t crc32c = 0;
    };

    struct DecodedFrame {
        ReplMsgType type{};
        uint8_t flags = 0;
        std::vector<uint8_t> payload;
    };

    struct ClientHello {
        uint64_t node_id = 0;
        uint64_t last_seq = 0;
        NodeRole role = NodeRole::Replica;
    };

    struct ServerHello {
        uint64_t node_id = 0;
        uint64_t current_seq = 0;
        NodeRole role = NodeRole::Primary;
    };

    struct ReplEntry {
        uint64_t seq = 0;
        uint64_t source_node_id = 0;
        ReplOpType op = ReplOpType::Put;
        uint8_t record_flags = 0;
        std::vector<uint8_t> key;
        std::vector<uint8_t> value;
    };

    struct ReplBlob {
        uint64_t seq = 0;
        uint64_t blob_id = 0;
        std::vector<uint8_t> content;
    };

    struct ReplAck {
        uint64_t seq = 0;
    };

    struct ReadRequest {
        uint64_t request_id = 0;
        uint64_t snapshot_seq = 0;
        std::vector<uint8_t> key;
    };

    struct ReadResponse {
        uint64_t request_id = 0;
        ReadStatus status = ReadStatus::Error;
        uint8_t record_flags = 0;
        uint64_t seq = 0;
        std::vector<uint8_t> value;
    };

    [[nodiscard]] std::vector<uint8_t> encode_frame(ReplMsgType type, std::span<const uint8_t> payload, uint8_t flags = 0);
    [[nodiscard]] bool decode_frame(std::span<const uint8_t> wire, DecodedFrame& out);

    [[nodiscard]] std::vector<uint8_t> encode_client_hello(const ClientHello& hello);
    [[nodiscard]] std::vector<uint8_t> encode_server_hello(const ServerHello& hello);
    [[nodiscard]] std::vector<uint8_t> encode_entry(const ReplEntry& entry);
    [[nodiscard]] std::vector<uint8_t> encode_blob(const ReplBlob& blob);
    [[nodiscard]] std::vector<uint8_t> encode_ack(const ReplAck& ack);
    [[nodiscard]] std::vector<uint8_t> encode_read_request(const ReadRequest& request);
    [[nodiscard]] std::vector<uint8_t> encode_read_response(const ReadResponse& response);

    [[nodiscard]] bool decode_client_hello(std::span<const uint8_t> payload, ClientHello& out);
    [[nodiscard]] bool decode_server_hello(std::span<const uint8_t> payload, ServerHello& out);
    [[nodiscard]] bool decode_entry(std::span<const uint8_t> payload, ReplEntry& out);
    [[nodiscard]] bool decode_blob(std::span<const uint8_t> payload, ReplBlob& out);
    [[nodiscard]] bool decode_ack(std::span<const uint8_t> payload, ReplAck& out);
    [[nodiscard]] bool decode_read_request(std::span<const uint8_t> payload, ReadRequest& out);
    [[nodiscard]] bool decode_read_response(std::span<const uint8_t> payload, ReadResponse& out);
} // namespace akkaradb::engine::cluster
