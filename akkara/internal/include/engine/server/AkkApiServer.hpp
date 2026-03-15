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

// internal/include/engine/server/AkkApiServer.hpp
#pragma once

#include "engine/AkkEngine.hpp"
#include <memory>

namespace akkaradb::server {

    class HttpApiServer;
    class TcpApiServer;

    /**
     * AkkApiServer — public data API orchestrator.
     *
     * Owned by AkkEngine::Impl. Started in open(), stopped in close().
     * Manages one or both of: HttpApiServer, TcpApiServer.
     *
     * Thread-safety: start/close must be called from a single thread.
     * The individual backend servers are themselves thread-safe after start().
     */
    class AkkApiServer {
        public:
            [[nodiscard]] static std::unique_ptr<AkkApiServer>
            create(engine::AkkEngine& engine, const engine::AkkEngineOptions::ApiOptions& opts);

            ~AkkApiServer();

            AkkApiServer(const AkkApiServer&)            = delete;
            AkkApiServer& operator=(const AkkApiServer&) = delete;

            /// Starts all enabled backend servers (non-blocking after bind).
            void start();

            /// Stops all backends gracefully and joins worker threads.
            void close();

        private:
            AkkApiServer() = default;

            std::unique_ptr<HttpApiServer> http_;
            std::unique_ptr<TcpApiServer>  tcp_;
    };

} // namespace akkaradb::server
