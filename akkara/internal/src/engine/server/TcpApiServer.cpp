/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 */

#include "engine/server/TcpApiServer.hpp"
#include "engine/server/ApiFraming.hpp"
#include "core/CRC32C.hpp"

#ifdef _WIN32
#  include <winsock2.h>
#  include <ws2tcpip.h>
   using sock_t = SOCKET;
   static constexpr sock_t BAD_SOCK = INVALID_SOCKET;
   static inline void close_sock(sock_t s) noexcept { ::closesocket(s); }
   static inline bool  sock_ok(sock_t s)   noexcept { return s != INVALID_SOCKET; }
#else
#  include <sys/socket.h>
#  include <netinet/in.h>
#  include <netinet/tcp.h>
#  include <arpa/inet.h>
#  include <unistd.h>
   using sock_t = int;
   static constexpr sock_t BAD_SOCK = -1;
   static inline void close_sock(sock_t s) noexcept { ::close(s); }
   static inline bool  sock_ok(sock_t s)   noexcept { return s >= 0; }
#endif

#include <cstring>
#include <stdexcept>

#ifdef _WIN32
#  include <mutex>
   static void init_winsock() {
       static std::once_flag f;
       std::call_once(f, [] {
           WSADATA wd{};
           if (::WSAStartup(MAKEWORD(2, 2), &wd) != 0)
               throw std::runtime_error("TcpApiServer: WSAStartup failed");
       });
   }
#else
   static void init_winsock() {}
#endif

// ── low-level send/recv helpers ───────────────────────────────────────────────

static bool send_all(sock_t s, const uint8_t* data, size_t len) noexcept {
    while (len > 0) {
        const auto n = ::send(s, reinterpret_cast<const char*>(data),
                              static_cast<int>(len), 0);
        if (n <= 0) return false;
        data += n; len -= static_cast<size_t>(n);
    }
    return true;
}

static bool recv_all(sock_t s, uint8_t* data, size_t len) noexcept {
    while (len > 0) {
        const auto n = ::recv(s, reinterpret_cast<char*>(data),
                              static_cast<int>(len), 0);
        if (n <= 0) return false;
        data += n; len -= static_cast<size_t>(n);
    }
    return true;
}

namespace akkaradb::server {

    // ── factory / ctor ────────────────────────────────────────────────────────

    TcpApiServer::TcpApiServer(engine::AkkEngine& engine, uint16_t port)
        : engine_{engine}, port_{port} {}

    std::unique_ptr<TcpApiServer>
    TcpApiServer::create(engine::AkkEngine& engine, uint16_t port) {
        return std::unique_ptr<TcpApiServer>(new TcpApiServer{engine, port});
    }

    TcpApiServer::~TcpApiServer() { close(); }

    // ── start / close ─────────────────────────────────────────────────────────

    void TcpApiServer::start() {
        init_winsock();

        const sock_t fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (!sock_ok(fd)) throw std::runtime_error("TcpApiServer: socket() failed");

        int opt = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&opt), sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(port_);

        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
            ::listen(fd, 16) != 0) {
            close_sock(fd);
            throw std::runtime_error("TcpApiServer: bind/listen failed on port "
                                     + std::to_string(port_));
        }

        listen_fd_ = static_cast<int>(fd);
        running_.store(true, std::memory_order_release);
        accept_thr_ = std::thread([this] { accept_loop(); });
    }

    void TcpApiServer::close() {
        if (!running_.exchange(false)) return;
        // Close the listener to unblock accept()
        if (listen_fd_ != -1) {
            close_sock(static_cast<sock_t>(listen_fd_));
            listen_fd_ = -1;
        }
        if (accept_thr_.joinable()) accept_thr_.join();
    }

    // ── accept loop ───────────────────────────────────────────────────────────

    void TcpApiServer::accept_loop() {
        while (running_.load(std::memory_order_acquire)) {
            sockaddr_in peer{};
            socklen_t peer_len = sizeof(peer);
            const sock_t client = ::accept(static_cast<sock_t>(listen_fd_),
                                           reinterpret_cast<sockaddr*>(&peer),
                                           &peer_len);
            if (!sock_ok(client)) break;  // listener closed or error

            // Spawn a detached connection-handler thread
            std::thread([this, client] {
                handle_connection(static_cast<int>(client));
                close_sock(client);
            }).detach();
        }
    }

    // ── connection handler ────────────────────────────────────────────────────

    void TcpApiServer::handle_connection(int raw_fd) {
        const sock_t fd = static_cast<sock_t>(raw_fd);

        // Enable TCP_NODELAY: requests are small; no benefit from Nagle
        int flag = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,
                     reinterpret_cast<const char*>(&flag), sizeof(flag));

        std::vector<uint8_t> key_buf, val_buf, resp_buf;

        while (running_.load(std::memory_order_relaxed)) {
            // ── Read request header ───────────────────────────────────────────
            ApiRequestHeader hdr{};
            if (!recv_all(fd, reinterpret_cast<uint8_t*>(&hdr), sizeof(hdr))) break;

            if (std::memcmp(hdr.magic, REQUEST_MAGIC, 4) != 0 ||
                hdr.version != PROTOCOL_VERSION) {
                encode_error(hdr.request_id, resp_buf);
                send_all(fd, resp_buf.data(), resp_buf.size());
                break;  // protocol error → drop connection
            }

            // ── Read key + val ────────────────────────────────────────────────
            key_buf.resize(hdr.key_len);
            val_buf.resize(hdr.val_len);
            if (!recv_all(fd, key_buf.data(), key_buf.size())) break;
            if (hdr.val_len && !recv_all(fd, val_buf.data(), val_buf.size())) break;

            // ── Verify CRC32C (key + val) ─────────────────────────────────────
            uint32_t received_crc = 0;
            if (!recv_all(fd, reinterpret_cast<uint8_t*>(&received_crc), 4)) break;

            const uint32_t expected_crc = [&] {
                auto crc = core::CRC32C::compute(key_buf.data(), key_buf.size());
                if (!val_buf.empty())
                    crc = core::CRC32C::append(val_buf.data(), val_buf.size(), crc);
                return crc;
            }();

            if (received_crc != expected_crc) {
                encode_error(hdr.request_id, resp_buf);
                send_all(fd, resp_buf.data(), resp_buf.size());
                break;
            }

            // ── Dispatch ──────────────────────────────────────────────────────
            const std::span<const uint8_t> key{key_buf};

            switch (hdr.opcode) {
                case ApiOp::Put: {
                    engine_.put(key, std::span<const uint8_t>{val_buf});
                    encode_response(ApiStatus::Ok, hdr.request_id, {}, resp_buf);
                    break;
                }
                case ApiOp::Get: {
                    std::vector<uint8_t> out;
                    if (engine_.get_into(key, out)) {
                        encode_response(ApiStatus::Ok, hdr.request_id, out, resp_buf);
                    } else {
                        encode_response(ApiStatus::NotFound, hdr.request_id, {}, resp_buf);
                    }
                    break;
                }
                case ApiOp::Remove: {
                    engine_.remove(key);
                    encode_response(ApiStatus::Ok, hdr.request_id, {}, resp_buf);
                    break;
                }
                case ApiOp::GetAt: {
                    // seq encoded as first 8 bytes of val_buf
                    if (val_buf.size() < 8) {
                        encode_error(hdr.request_id, resp_buf);
                        break;
                    }
                    uint64_t seq = 0;
                    std::memcpy(&seq, val_buf.data(), 8);
                    const auto v = engine_.get_at(key, seq);
                    if (v) {
                        encode_response(ApiStatus::Ok, hdr.request_id, *v, resp_buf);
                    } else {
                        encode_response(ApiStatus::NotFound, hdr.request_id, {}, resp_buf);
                    }
                    break;
                }
                default:
                    encode_error(hdr.request_id, resp_buf);
                    break;
            }

            if (!send_all(fd, resp_buf.data(), resp_buf.size())) break;
        }
    }

} // namespace akkaradb::server
