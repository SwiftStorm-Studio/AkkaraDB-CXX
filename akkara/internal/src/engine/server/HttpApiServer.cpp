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
#include <array>
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
    namespace {
        // ── HTTP protocol constants ───────────────────────────────────────────

        /// Maximum byte length of a single HTTP header or request line.
        constexpr size_t kMaxHttpLineBytes = 8192;

        /// Maximum allowed Content-Length — prevents memory exhaustion from
        /// malicious clients sending a huge value (DoS guard).
        constexpr size_t kMaxContentLength = 64u * 1024 * 1024; // 64 MiB

        /// Receive buffer size for the buffered line reader.
        /// Reduces syscall count from O(request_bytes) to O(request_bytes / kRecvBufSize).
        constexpr size_t kRecvBufSize = 4096;

        // ── Case-insensitive helpers ──────────────────────────────────────────
        // Both helpers expect `prefix`/`needle` to already be lowercase, and
        // do the tolower conversion only on the haystack side.  This avoids
        // allocating a lowercased copy of every incoming header line.

        /// Returns true if `line` starts with `prefix` (case-insensitively).
        bool iequal_prefix(std::string_view line, std::string_view prefix) noexcept {
            if (line.size() < prefix.size()) return false;
            for (size_t i = 0; i < prefix.size(); ++i) { if (static_cast<char>(::tolower(static_cast<unsigned char>(line[i]))) != prefix[i]) return false; }
            return true;
        }

        /// Returns true if `haystack` contains `needle` (case-insensitively).
        bool icontains(std::string_view haystack, std::string_view needle) noexcept {
            if (needle.empty()) return true;
            if (haystack.size() < needle.size()) return false;
            for (size_t i = 0; i <= haystack.size() - needle.size(); ++i) {
                bool ok = true;
                for (size_t j = 0; j < needle.size() && ok; ++j) ok = (static_cast<char>(::tolower(static_cast<unsigned char>(haystack[i + j]))) == needle[j]);
                if (ok) return true;
            }
            return false;
        }
    } // namespace

    // ── factory / ctor ────────────────────────────────────────────────────────

    HttpApiServer::HttpApiServer(engine::AkkEngine& engine, uint16_t port, core::TlsConfig tls) : engine_{engine}, port_{port}, tls_cfg_{std::move(tls)} {}

    std::unique_ptr<HttpApiServer> HttpApiServer::create(engine::AkkEngine& engine, uint16_t port, core::TlsConfig tls) {
        return std::unique_ptr < HttpApiServer > (new HttpApiServer{engine, port, std::move(tls)});
    }

    HttpApiServer::~HttpApiServer() { close(); }

    // ── start / close ─────────────────────────────────────────────────────────

    void HttpApiServer::start() {
        init_winsock_http();

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
            throw std::runtime_error(std::format("HttpApiServer: bind/listen failed on port {}", port_));
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
                    // TLS handshake failure or connection error — socket is closed below
                }
                close_sock(client);
            }).detach();
        }
    }

    // ── HTTP request parsing ──────────────────────────────────────────────────

    bool HttpApiServer::read_request(core::TlsStream& stream, ParsedRequest& req) {
        // ── Buffered line reader ──────────────────────────────────────────────
        // recv_some() fills a 4 KB stack buffer in one syscall.  The old
        // byte-by-byte approach issued O(request_bytes) syscalls (~500 for a
        // typical request); this reduces that to O(request_bytes / kRecvBufSize).
        // The buffer is stack-allocated — no heap allocation per request.
        std::array<uint8_t, kRecvBufSize> rbuf;
        size_t rbuf_pos = 0, rbuf_len = 0;

        const auto refill = [&]() -> bool {
            rbuf_len = stream.recv_some(rbuf.data(), rbuf.size());
            rbuf_pos = 0;
            return rbuf_len > 0;
        };

        const auto read_byte = [&](char& c) -> bool {
            if (rbuf_pos >= rbuf_len && !refill()) return false;
            c = static_cast<char>(rbuf[rbuf_pos++]);
            return true;
        };

        // Read one CRLF-terminated line. Returns false on error or line overflow.
        const auto recv_line = [&](std::string& line) -> bool {
            line.clear();
            char c;
            while (line.size() < kMaxHttpLineBytes) {
                if (!read_byte(c)) return false;
                if (c == '\n') {
                    if (!line.empty() && line.back() == '\r') line.pop_back();
                    return true;
                }
                line += c;
            }
            return false; // line too long → reject request
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

        // ── Headers ───────────────────────────────────────────────────────────
        // iequal_prefix / icontains compare case-insensitively without allocating
        // a lowercased copy of the header line (eliminates 8-12 allocs/request).
        size_t content_length = 0;
        req.keep_alive = true;
        while (recv_line(line)) {
            if (line.empty()) break; // blank line = end of headers
            if (iequal_prefix(line, "content-length:")) {
                const auto colon = line.find(':');
                std::string_view val{line.data() + colon + 1, line.size() - colon - 1};
                while (!val.empty() && val.front() == ' ') val.remove_prefix(1);
                const auto res = std::from_chars(val.data(), val.data() + val.size(), content_length);
                if (res.ec != std::errc{}) return false; // malformed → reject
                if (content_length > kMaxContentLength) return false; // DoS guard
            }
            else if (iequal_prefix(line, "connection:")) {
                const auto colon = line.find(':');
                const std::string_view val{line.data() + colon + 1, line.size() - colon - 1};
                if (icontains(val, "close")) req.keep_alive = false;
            }
        }

        // ── Body ─────────────────────────────────────────────────────────────
        // Bytes already in rbuf (pipelined with headers) are drained first;
        // the remainder is read via recv_all.
        if (content_length > 0) {
            req.body.resize(content_length);
            size_t body_pos = 0;
            const size_t buffered = rbuf_len - rbuf_pos;
            if (buffered > 0) {
                const size_t take = std::min(buffered, content_length);
                std::memcpy(req.body.data(), rbuf.data() + rbuf_pos, take);
                rbuf_pos += take;
                body_pos = take;
            }
            if (body_pos < content_length) { if (!stream.recv_all(req.body.data() + body_pos, content_length - body_pos)) return false; }
        }
        return true;
    }

    // ── URL decode ────────────────────────────────────────────────────────────

    std::vector<uint8_t> HttpApiServer::url_decode(std::string_view encoded) {
        std::vector<uint8_t> out;
        out.reserve(encoded.size());
        for (size_t i = 0; i < encoded.size(); ++i) {
            if (encoded[i] == '%' && i + 2 < encoded.size()) {
                const char hex[2] = {encoded[i + 1], encoded[i + 2]};
                uint8_t byte = 0;
                const auto res = std::from_chars(hex, hex + 2, byte, 16);
                if (res.ec != std::errc{}) {
                    // Invalid percent-sequence: emit '%' literally, re-process next char
                    out.push_back(static_cast<uint8_t>('%'));
                }
                else {
                    out.push_back(byte);
                    i += 2;
                }
            } else if (encoded[i] == '+') {
                out.push_back(' ');
            } else {
                out.push_back(static_cast<uint8_t>(encoded[i]));
            }
        }
        return out;
    }

    // ── Query param extraction ────────────────────────────────────────────────

    std::string HttpApiServer::query_param(std::string_view query, std::string_view name) {
        while (!query.empty()) {
            const auto amp = query.find('&');
            const auto part = query.substr(0, amp);
            const auto eq   = part.find('=');
            if (eq != std::string_view::npos && part.substr(0, eq) == name)
                return std::string{part.substr(eq + 1)};
            query = (amp == std::string_view::npos) ? "" : query.substr(amp + 1);
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

    bool HttpApiServer::route(core::TlsStream& stream, const ParsedRequest& req, std::vector<uint8_t>& val_buf) {
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
            // val_buf is reused across keep-alive requests — no per-request alloc
            val_buf.clear();
            if (engine_.get_into(key_span, val_buf)) { send_response(stream, 200, val_buf); }
            else { send_empty(stream, 404); }
        } else if (req.path == "/v1/remove" && req.method == "DELETE") {
            engine_.remove(key_span);
            send_empty(stream, 204);
        } else if (req.path == "/v1/get_at" && req.method == "GET") {
            const std::string seq_str = query_param(req.query, "seq");
            if (seq_str.empty()) {
                send_empty(stream, 400);
                return req.keep_alive;
            }
            uint64_t seq = 0;
            const auto res = std::from_chars(seq_str.data(), seq_str.data() + seq_str.size(), seq);
            if (res.ec != std::errc{}) {
                send_empty(stream, 400);
                return req.keep_alive;
            }
            const auto v = engine_.get_at(key_span, seq);
            if (v) { send_response(stream, 200, *v); } else { send_empty(stream, 404); }

        } else if (req.path == "/v1/ping" && req.method == "GET") {
            static constexpr std::string_view pong = "pong";
            send_response(stream, 200, {reinterpret_cast<const uint8_t*>(pong.data()), pong.size()});
        }
        else { send_empty(stream, 404); }

        return req.keep_alive;
    }

    // ── connection handler ────────────────────────────────────────────────────

    void HttpApiServer::handle_connection(core::TlsStream& stream) {
        // val_buf is allocated once per connection and reused across all
        // keep-alive requests, eliminating a heap alloc on every GET response.
        std::vector<uint8_t> val_buf;
        while (running_.load(std::memory_order_relaxed)) {
            ParsedRequest req;
            if (!read_request(stream, req)) break;
            if (!route(stream, req, val_buf)) break; // Connection: close
        }
    }

} // namespace akkaradb::server
