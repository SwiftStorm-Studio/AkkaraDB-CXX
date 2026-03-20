/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 */

#include "engine/server/HttpApiServer.hpp"

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

#include <algorithm>
#include <charconv>
#include <cstring>
#include <format>
#include <stdexcept>
#include <string_view>

#ifdef _WIN32
#  include <mutex>
   static void init_winsock_http() {
       static std::once_flag f;
       std::call_once(f, [] {
           WSADATA wd{};
           if (::WSAStartup(MAKEWORD(2, 2), &wd) != 0)
               throw std::runtime_error("HttpApiServer: WSAStartup failed");
       });
   }
#else
   static void init_winsock_http() {}
#endif

namespace akkaradb::server {

    // ── factory / ctor ────────────────────────────────────────────────────────

    HttpApiServer::HttpApiServer(engine::AkkEngine& engine, uint16_t port, core::TlsConfig tls) : engine_{engine}, port_{port}, tls_cfg_{std::move(tls)} {}

    std::unique_ptr<HttpApiServer> HttpApiServer::create(engine::AkkEngine& engine, uint16_t port, core::TlsConfig tls) {
        return std::unique_ptr < HttpApiServer > (new HttpApiServer{engine, port, std::move(tls)});
    }

    HttpApiServer::~HttpApiServer() { close(); }

    // ── start / close ─────────────────────────────────────────────────────────

    void HttpApiServer::start() {
        init_winsock_http();

        // Initialise TLS context if enabled
        tls_ctx_ = core::TlsContext::make_server(tls_cfg_);

        const sock_t fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (!sock_ok(fd)) throw std::runtime_error("HttpApiServer: socket() failed");

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
            throw std::runtime_error("HttpApiServer: bind/listen failed on port "
                                     + std::to_string(port_));
        }

        listen_fd_ = static_cast<int>(fd);
        running_.store(true, std::memory_order_release);
        accept_thr_ = std::thread([this] { accept_loop(); });
    }

    void HttpApiServer::close() {
        if (!running_.exchange(false)) return;
        if (listen_fd_ != -1) {
            close_sock(static_cast<sock_t>(listen_fd_));
            listen_fd_ = -1;
        }
        if (accept_thr_.joinable()) accept_thr_.join();
    }

    // ── accept loop ───────────────────────────────────────────────────────────

    void HttpApiServer::accept_loop() {
        while (running_.load(std::memory_order_acquire)) {
            sockaddr_in peer{};
            socklen_t peer_len = sizeof(peer);
            const sock_t client = ::accept(static_cast<sock_t>(listen_fd_),
                                           reinterpret_cast<sockaddr*>(&peer),
                                           &peer_len);
            if (!sock_ok(client)) break;

            std::thread([this, client] {
                try {
                    auto stream = core::TlsStream::server_wrap(client, tls_ctx_.get());
                    handle_connection(*stream);
                    stream->shutdown();
                }
                catch (...) {
                    // TLS handshake failure or other error — just close the socket
                }
                close_sock(client);
            }).detach();
        }
    }

    // ── HTTP request parsing ──────────────────────────────────────────────────

    bool HttpApiServer::read_request(core::TlsStream& stream, ParsedRequest& req) {
        // Read one line (up to \r\n) using TlsStream::recv_all byte-by-byte
        auto recv_line = [&](std::string& line, size_t max = 8192) -> bool {
            line.clear();
            char c;
            while (line.size() < max) {
                if (!stream.recv_all(reinterpret_cast<uint8_t*>(&c), 1)) return false;
                if (c == '\n') {
                    if (!line.empty() && line.back() == '\r') line.pop_back();
                    return true;
                }
                line += c;
            }
            return false; // line too long
        };

        std::string line;

        // Request line: "METHOD /path?query HTTP/1.1"
        if (!recv_line(line) || line.empty()) return false;

        const auto sp1 = line.find(' ');
        const auto sp2 = line.find(' ', sp1 + 1);
        if (sp1 == std::string::npos || sp2 == std::string::npos) return false;

        req.method = line.substr(0, sp1);
        const std::string target = line.substr(sp1 + 1, sp2 - sp1 - 1);
        const auto qmark = target.find('?');
        req.path  = (qmark == std::string::npos) ? target : target.substr(0, qmark);
        req.query = (qmark == std::string::npos) ? "" : target.substr(qmark + 1);

        // Headers
        size_t content_length = 0;
        req.keep_alive = true;
        while (recv_line(line)) {
            if (line.empty()) break;  // end of headers
            // Case-insensitive header matching
            std::string lower = line;
            std::transform(lower.begin(), lower.end(), lower.begin(),
                       [](unsigned char c) -> char { return static_cast<char>(::tolower(c)); });

            if (lower.starts_with("content-length:")) {
                const auto val = line.substr(line.find(':') + 1);
                std::from_chars(val.data() + val.find_first_not_of(' '),
                                val.data() + val.size(), content_length);
            } else if (lower.starts_with("connection:")) {
                if (lower.find("close") != std::string::npos) req.keep_alive = false;
            }
        }

        // Body
        if (content_length > 0) {
            req.body.resize(content_length);
            if (!stream.recv_all(req.body.data(), content_length)) return false;
        }
        return true;
    }

    // ── URL decode ────────────────────────────────────────────────────────────

    std::vector<uint8_t> HttpApiServer::url_decode(std::string_view encoded) {
        std::vector<uint8_t> out;
        out.reserve(encoded.size());
        for (size_t i = 0; i < encoded.size(); ++i) {
            if (encoded[i] == '%' && i + 2 < encoded.size()) {
                const char hex[3] = { encoded[i+1], encoded[i+2], '\0' };
                uint8_t byte = 0;
                std::from_chars(hex, hex + 2, byte, 16);
                out.push_back(byte);
                i += 2;
            } else if (encoded[i] == '+') {
                out.push_back(' ');
            } else {
                out.push_back(static_cast<uint8_t>(encoded[i]));
            }
        }
        return out;
    }

    // ── Query param extraction ────────────────────────────────────────────────

    std::string HttpApiServer::query_param(const std::string& query,
                                           std::string_view   name) {
        std::string_view qv{query};
        while (!qv.empty()) {
            const auto amp = qv.find('&');
            const auto part = qv.substr(0, amp);
            const auto eq   = part.find('=');
            if (eq != std::string_view::npos && part.substr(0, eq) == name)
                return std::string{part.substr(eq + 1)};
            qv = (amp == std::string_view::npos) ? "" : qv.substr(amp + 1);
        }
        return {};
    }

    // ── Response helpers ──────────────────────────────────────────────────────

    void HttpApiServer::send_response(core::TlsStream& stream, int status_code, std::span<const uint8_t> body) {
        const std::string_view reason =
            status_code == 200 ? "OK" :
            status_code == 204 ? "No Content" :
            status_code == 400 ? "Bad Request" :
            status_code == 404 ? "Not Found" : "Internal Server Error";

        const std::string header = std::format(
            "HTTP/1.1 {} {}\r\n"
            "Content-Type: application/octet-stream\r\n"
            "Content-Length: {}\r\n"
            "\r\n",
            status_code, reason, body.size());

        stream.send_all(reinterpret_cast<const uint8_t*>(header.data()), header.size());
        if (!body.empty()) stream.send_all(body.data(), body.size());
    }

    void HttpApiServer::send_empty(core::TlsStream& stream, int status_code) { send_response(stream, status_code, {}); }

    // ── Router ────────────────────────────────────────────────────────────────

    bool HttpApiServer::route(core::TlsStream& stream, const ParsedRequest& req) {
        const std::string raw_key = query_param(req.query, "key");
        if (raw_key.empty() && req.path != "/v1/ping") {
            send_empty(stream, 400);
            return req.keep_alive;
        }
        const std::vector<uint8_t> key = url_decode(raw_key);
        const std::span<const uint8_t> key_span{key};

        if (req.path == "/v1/put" && req.method == "POST") {
            engine_.put(key_span, std::span<const uint8_t>{req.body});
            send_empty(stream, 204);
        } else if (req.path == "/v1/get" && req.method == "GET") {
            std::vector<uint8_t> val;
            if (engine_.get_into(key_span, val)) { send_response(stream, 200, val); } else { send_empty(stream, 404); }

        } else if (req.path == "/v1/remove" && req.method == "DELETE") {
            engine_.remove(key_span);
            send_empty(stream, 204);
        } else if (req.path == "/v1/get_at" && req.method == "GET") {
            const std::string seq_str = query_param(req.query, "seq");
            uint64_t seq = 0;
            std::from_chars(seq_str.data(), seq_str.data() + seq_str.size(), seq);
            const auto v = engine_.get_at(key_span, seq);
            if (v) { send_response(stream, 200, *v); } else { send_empty(stream, 404); }

        } else if (req.path == "/v1/ping" && req.method == "GET") {
            const std::string_view pong = "pong";
            send_response(stream, 200, {reinterpret_cast<const uint8_t*>(pong.data()), pong.size()});

        } else { send_empty(stream, 404); }

        return req.keep_alive;
    }

    // ── connection handler ────────────────────────────────────────────────────

    void HttpApiServer::handle_connection(core::TlsStream& stream) {
        while (running_.load(std::memory_order_relaxed)) {
            ParsedRequest req;
            if (!read_request(stream, req)) break;
            if (!route(stream, req)) break; // Connection: close
        }
    }

} // namespace akkaradb::server
