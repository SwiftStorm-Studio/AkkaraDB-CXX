/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/include/engine/server/HttpApiServer.hpp
#pragma once

#include "engine/AkkEngine.hpp"
#include "core/net/TlsStream.hpp"
#include <atomic>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace akkaradb::server {

    /**
     * HttpApiServer — minimal HTTP/1.1 REST API server.
     *
     * Endpoints (key is URL-encoded in the query string):
     *   POST   /v1/put?key=<key>      body = raw value bytes  → 204
     *   GET    /v1/get?key=<key>                              → 200 raw bytes / 404
     *   DELETE /v1/remove?key=<key>                          → 204
     *   GET    /v1/get_at?key=<key>&seq=<n>                  → 200 raw bytes / 404
     *
     * Keys are URL-decoded (percent-encoding supported, binary-safe).
     * Values are raw bytes in request/response bodies.
     * Content-Type: application/octet-stream.
     *
     * One accept thread + one thread per connection.
     * Keep-alive supported (Connection: keep-alive).
     * Optionally wraps connections with TLS when a non-empty TlsConfig is provided.
     */
    class HttpApiServer {
        public:
            [[nodiscard]] static std::unique_ptr<HttpApiServer> create(engine::AkkEngine& engine, uint16_t port, core::TlsConfig tls = {});

            ~HttpApiServer();

            HttpApiServer(const HttpApiServer&)            = delete;
            HttpApiServer& operator=(const HttpApiServer&) = delete;

            void start();
            void close();

        private:
            explicit HttpApiServer(engine::AkkEngine& engine, uint16_t port, core::TlsConfig tls);

            void accept_loop();
            void handle_connection(core::TlsStream& stream);

            // HTTP parsing helpers
            struct ParsedRequest {
                std::string              method;
                std::string              path;       // without query string
                std::string              query;      // raw query string
                std::vector<uint8_t>     body;
                bool                     keep_alive{true};
            };

            bool read_request(core::TlsStream& stream, ParsedRequest& req);
            /// val_buf is passed in from handle_connection to be reused across
            /// keep-alive requests, eliminating a per-request heap allocation.
            bool route(core::TlsStream& stream, const ParsedRequest& req, std::vector<uint8_t>& val_buf);
            void send_response(core::TlsStream& stream, int status_code, std::span<const uint8_t> body);
            void send_empty(core::TlsStream& stream, int status_code);

            /// Extract a named query-string parameter. query and name are string_view
            /// to avoid copies; returns std::string (decoded value) or empty string.
            static std::string query_param(std::string_view query, std::string_view name);
            static std::vector<uint8_t> url_decode(std::string_view encoded);

            engine::AkkEngine& engine_;
            uint16_t port_;
            std::atomic<bool> running_{false};
            int listen_fd_{-1};
            std::thread accept_thr_;

            core::TlsConfig tls_cfg_;
            std::unique_ptr<core::TlsContext> tls_ctx_;
    };

} // namespace akkaradb::server
