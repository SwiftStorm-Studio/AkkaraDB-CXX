/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 */

#include "engine/server/AkkApiServer.hpp"
#include "engine/server/HttpApiServer.hpp"
#include "engine/server/TcpApiServer.hpp"

namespace akkaradb::server {

    std::unique_ptr<AkkApiServer>
    AkkApiServer::create(engine::AkkEngine& engine,
                         const engine::AkkEngineOptions::ApiOptions& opts) {
        auto srv = std::unique_ptr<AkkApiServer>(new AkkApiServer{});

        using Backend = engine::AkkEngineOptions::ApiBackend;
        for (const auto backend : opts.backends) {
            switch (backend) {
                case Backend::Http:
                    srv->http_ = HttpApiServer::create(engine, opts.http_port);
                    break;
                case Backend::Tcp:
                    srv->tcp_ = TcpApiServer::create(engine, opts.tcp_port);
                    break;
            }
        }
        return srv;
    }

    AkkApiServer::~AkkApiServer() { close(); }

    void AkkApiServer::start() {
        if (http_) http_->start();
        if (tcp_)  tcp_->start();
    }

    void AkkApiServer::close() {
        if (http_) { http_->close(); http_.reset(); }
        if (tcp_)  { tcp_->close();  tcp_.reset();  }
    }

} // namespace akkaradb::server
