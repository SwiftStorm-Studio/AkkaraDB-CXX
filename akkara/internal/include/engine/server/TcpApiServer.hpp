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

// internal/include/engine/server/TcpApiServer.hpp
#pragma once

#include "engine/AkkEngine.hpp"
#include <atomic>
#include <memory>
#include <thread>

namespace akkaradb::server {

    /**
     * TcpApiServer — AkkaraDB binary TCP protocol server.
     *
     * Wire format: see ApiFraming.hpp (ApiRequestHeader / ApiResponseHeader).
     * One accept thread + one thread per accepted connection.
     * Supports request pipelining via request_id echo.
     *
     * Thread-safety: start/close from one thread; connections handled concurrently.
     */
    class TcpApiServer {
        public:
            [[nodiscard]] static std::unique_ptr<TcpApiServer>
            create(engine::AkkEngine& engine, uint16_t port);

            ~TcpApiServer();

            TcpApiServer(const TcpApiServer&)            = delete;
            TcpApiServer& operator=(const TcpApiServer&) = delete;

            void start();
            void close();

        private:
            explicit TcpApiServer(engine::AkkEngine& engine, uint16_t port);

            void accept_loop();
            void handle_connection(int fd);  // sock_t aliased to int/SOCKET

            engine::AkkEngine& engine_;
            uint16_t           port_;
            std::atomic<bool>  running_{false};
            int                listen_fd_{-1};  // platform sock_t; -1 = not open
            std::thread        accept_thr_;
    };

} // namespace akkaradb::server
