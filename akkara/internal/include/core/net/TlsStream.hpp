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

// akkara/internal/include/core/net/TlsStream.hpp
#pragma once
#include <memory>
#include <string>
#include <cstdint>

#ifdef _WIN32
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  include <winsock2.h>
   using akk_sock_t = SOCKET;
#else
   using akk_sock_t = int;
#endif

namespace akkaradb::core {

/**
 * TlsConfig — TLS options for server and client.
 * enabled=false → plain TCP (zero overhead, no mbedTLS dependency at runtime).
 */
struct TlsConfig {
    bool        enabled     = false;
    std::string cert_path;          ///< PEM certificate (server cert / client cert for mTLS)
    std::string key_path;           ///< PEM private key
    std::string ca_path;            ///< PEM CA cert for peer verification (mTLS)
    bool        verify_peer = false; ///< Require peer certificate (mTLS)
};

/**
 * TlsContext — Shared TLS configuration loaded once per server/client.
 * Holds mbedTLS entropy, drbg, ssl_config, x509 cert, pk key.
 * Thread-safe for concurrent TlsStream::server_wrap / client_wrap calls.
 *
 * When AKKARADB_TLS_ENABLED is not defined, make_server/make_client return
 * nullptr if cfg.enabled==false, or throw if cfg.enabled==true.
 */
class TlsContext {
public:
    /// Create server-side context: loads cert + key, optionally CA for mTLS.
    [[nodiscard]] static std::unique_ptr<TlsContext> make_server(const TlsConfig& cfg);
    /// Create client-side context: loads CA cert for peer verification.
    [[nodiscard]] static std::unique_ptr<TlsContext> make_client(const TlsConfig& cfg);

    ~TlsContext();
    TlsContext(const TlsContext&) = delete;
    TlsContext& operator=(const TlsContext&) = delete;

    struct Impl;
    Impl* impl() const noexcept { return impl_.get(); }

private:
    explicit TlsContext(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

/**
 * TlsStream — Per-connection I/O wrapper.
 *
 * Plain TCP (ctx == nullptr):
 *   send_all / recv_all delegate directly to ::send / ::recv. Zero overhead.
 *
 * TLS (ctx != nullptr, AKKARADB_TLS_ENABLED defined):
 *   Performs TLS handshake in server_wrap / client_wrap.
 *   send_all / recv_all route through mbedtls_ssl_write / mbedtls_ssl_read.
 */
class TlsStream {
public:
    /// Server side: wrap an accepted socket. Performs handshake if ctx != nullptr.
    [[nodiscard]] static std::unique_ptr<TlsStream>
    server_wrap(akk_sock_t fd, TlsContext* ctx);

    /// Client side: wrap a connected socket. Performs handshake if ctx != nullptr.
    [[nodiscard]] static std::unique_ptr<TlsStream>
    client_wrap(akk_sock_t fd, const char* hostname, TlsContext* ctx);

    ~TlsStream();
    TlsStream(const TlsStream&) = delete;
    TlsStream& operator=(const TlsStream&) = delete;

    bool send_all(const uint8_t* data, size_t len) noexcept;
    bool recv_all(uint8_t* data, size_t len) noexcept;
    /// Read up to max_len bytes in a single syscall.
    /// Returns bytes read (> 0), or 0 on connection close / error.
    /// Unlike recv_all, this may return fewer bytes than requested.
    size_t recv_some(uint8_t* data, size_t max_len) noexcept;
    /// TLS close_notify + does NOT close the underlying fd.
    void shutdown() noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    explicit TlsStream(std::unique_ptr<Impl> impl);
};

} // namespace akkaradb::core
