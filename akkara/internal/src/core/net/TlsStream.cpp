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
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/src/net/TlsStream.cpp
#include "core/net/TlsStream.hpp"

#include <mbedtls/ctr_drbg.h>
#include <mbedtls/entropy.h>
#include <mbedtls/error.h>
#include <mbedtls/net_sockets.h>
#include <mbedtls/pk.h>
#include <mbedtls/ssl.h>
#include <mbedtls/x509_crt.h>

#ifdef _WIN32
#include <BaseTsd.h>
#include <winsock2.h>
#include <ws2tcpip.h>
typedef SSIZE_T ssize_t;
#pragma comment(lib, "ws2_32.lib")
typedef SOCKET socket_t;
#else
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
typedef int socket_t;
#endif

#include <cerrno>

namespace akkaradb::net {
    // ==================== Error Helper ====================

    class mbedtls_category_t : public std::error_category {
        public:
            const char* name() const noexcept override { return "mbedtls"; }

            std::string message(int ev) const override {
                char buf[256];
                mbedtls_strerror(-ev, buf, sizeof(buf));
                return buf;
            }
    };

    const std::error_category& mbedtls_category() {
        static mbedtls_category_t instance;
        return instance;
    }

    static std::error_code make_error(int ret) { return {-ret, mbedtls_category()}; }

    static std::string mbedtls_error_string(int ret) {
        char buf[256];
        mbedtls_strerror(ret, buf, sizeof(buf));
        return {buf};
    }

    static std::error_code map_mbedtls_error(int ret) {
        if (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE) { return std::make_error_code(std::errc::operation_would_block); }

        if (ret == 0) { return {}; }

        return std::error_code{-ret, mbedtls_category()};
    }

    // ==================== Context Impl ====================

    struct TlsContext::Impl {
        mbedtls_ssl_config ssl_cfg{};
        mbedtls_x509_crt ca_cert{};
        mbedtls_x509_crt cert{};
        mbedtls_pk_context key{};
        mbedtls_entropy_context entropy{};
        mbedtls_ctr_drbg_context drbg{};

        Impl() {
            mbedtls_ssl_config_init(&ssl_cfg);
            mbedtls_x509_crt_init(&ca_cert);
            mbedtls_x509_crt_init(&cert);
            mbedtls_pk_init(&key);
            mbedtls_entropy_init(&entropy);
            mbedtls_ctr_drbg_init(&drbg);
        }

        ~Impl() {
            mbedtls_ssl_config_free(&ssl_cfg);
            mbedtls_x509_crt_free(&ca_cert);
            mbedtls_x509_crt_free(&cert);
            mbedtls_pk_free(&key);
            mbedtls_ctr_drbg_free(&drbg);
            mbedtls_entropy_free(&entropy);
        }
    };

    static int to_mbed_verify(TlsVerifyMode m) {
        switch (m) {
            case TlsVerifyMode::None: return MBEDTLS_SSL_VERIFY_NONE;
            case TlsVerifyMode::Optional: return MBEDTLS_SSL_VERIFY_OPTIONAL;
            case TlsVerifyMode::Required: return MBEDTLS_SSL_VERIFY_REQUIRED;
        }
        return MBEDTLS_SSL_VERIFY_REQUIRED;
    }

    // ==================== TlsContext ====================

    TlsContext::TlsContext(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

    TlsContext::~TlsContext() = default;

    std::shared_ptr<TlsContext> TlsContext::create_server(
        const std::string& cert_pem,
        const std::string& key_pem,
        const std::string& ca_pem,
        TlsVerifyMode verify_mode
    ) {
        auto impl = std::make_unique<Impl>();

        int ret = mbedtls_ctr_drbg_seed(&impl->drbg, mbedtls_entropy_func, &impl->entropy, nullptr, 0);
        if (ret != 0) throw std::runtime_error(mbedtls_error_string(ret));

        if ((ret = mbedtls_x509_crt_parse(&impl->cert, reinterpret_cast<const unsigned char*>(cert_pem.c_str()), cert_pem.size() + 1)) != 0) throw
            std::runtime_error(mbedtls_error_string(ret));

        if ((ret = mbedtls_pk_parse_key(
            &impl->key,
            reinterpret_cast<const unsigned char*>(key_pem.c_str()),
            key_pem.size() + 1,
            nullptr,
            0,
            mbedtls_ctr_drbg_random,
            &impl->drbg
        )) != 0) throw std::runtime_error(mbedtls_error_string(ret));

        if (!ca_pem.empty()) {
            if ((ret = mbedtls_x509_crt_parse(&impl->ca_cert, reinterpret_cast<const unsigned char*>(ca_pem.c_str()), ca_pem.size() + 1)) != 0) throw
                std::runtime_error(mbedtls_error_string(ret));
        }

        if ((ret = mbedtls_ssl_config_defaults(&impl->ssl_cfg, MBEDTLS_SSL_IS_SERVER, MBEDTLS_SSL_TRANSPORT_STREAM, MBEDTLS_SSL_PRESET_DEFAULT)) != 0) throw
            std::runtime_error(mbedtls_error_string(ret));

        mbedtls_ssl_conf_rng(&impl->ssl_cfg, mbedtls_ctr_drbg_random, &impl->drbg);
        mbedtls_ssl_conf_authmode(&impl->ssl_cfg, to_mbed_verify(verify_mode));

        if (!ca_pem.empty()) { mbedtls_ssl_conf_ca_chain(&impl->ssl_cfg, &impl->ca_cert, nullptr); }

        if ((ret = mbedtls_ssl_conf_own_cert(&impl->ssl_cfg, &impl->cert, &impl->key)) != 0) throw std::runtime_error(mbedtls_error_string(ret));

        return std::shared_ptr<TlsContext>(new TlsContext(std::move(impl)));
    }

    std::shared_ptr<TlsContext> TlsContext::create_client(const std::string& ca_pem, TlsVerifyMode verify_mode) {
        auto impl = std::make_unique<Impl>();

        int ret = mbedtls_ctr_drbg_seed(&impl->drbg, mbedtls_entropy_func, &impl->entropy, nullptr, 0);
        if (ret != 0) throw std::runtime_error(mbedtls_error_string(ret));

        if (!ca_pem.empty()) {
            if ((ret = mbedtls_x509_crt_parse(&impl->ca_cert, reinterpret_cast<const unsigned char*>(ca_pem.c_str()), ca_pem.size() + 1)) != 0) throw
                std::runtime_error(mbedtls_error_string(ret));
        }

        if ((ret = mbedtls_ssl_config_defaults(&impl->ssl_cfg, MBEDTLS_SSL_IS_CLIENT, MBEDTLS_SSL_TRANSPORT_STREAM, MBEDTLS_SSL_PRESET_DEFAULT)) != 0) throw
            std::runtime_error(mbedtls_error_string(ret));

        mbedtls_ssl_conf_rng(&impl->ssl_cfg, mbedtls_ctr_drbg_random, &impl->drbg);
        mbedtls_ssl_conf_authmode(&impl->ssl_cfg, to_mbed_verify(verify_mode));

        if (!ca_pem.empty()) { mbedtls_ssl_conf_ca_chain(&impl->ssl_cfg, &impl->ca_cert, nullptr); }

        return std::shared_ptr<TlsContext>(new TlsContext(std::move(impl)));
    }

    // ==================== Stream Impl ====================

    struct TlsStream::Impl {
        mbedtls_ssl_context ssl{};
        Impl() { mbedtls_ssl_init(&ssl); }
        ~Impl() { mbedtls_ssl_free(&ssl); }
    };

    // ==================== BIO ====================

    int TlsStream::bio_send(void* ctx, const unsigned char* buf, size_t len) {
        socket_t fd = *static_cast<socket_t*>(ctx);

        for (;;) {
            #ifdef _WIN32
            int n = send(fd, reinterpret_cast<const char*>(buf), static_cast<int>(len), 0);
            if (n >= 0) return n;

            int err = WSAGetLastError();
            if (err == WSAEINTR) continue;
            if (err == WSAEWOULDBLOCK) return MBEDTLS_ERR_SSL_WANT_WRITE;
            #else
            ssize_t n = send(fd, buf, len, 0); if (n >= 0) return static_cast<int>(n); if (errno == EINTR) continue; if (errno == EAGAIN || errno ==
                EWOULDBLOCK) return MBEDTLS_ERR_SSL_WANT_WRITE;
            #endif
            return MBEDTLS_ERR_NET_SEND_FAILED;
        }
    }

    int TlsStream::bio_recv(void* ctx, unsigned char* buf, size_t len) {
        socket_t fd = *static_cast<socket_t*>(ctx);

        for (;;) {
            #ifdef _WIN32
            int n = recv(fd, reinterpret_cast<char*>(buf), static_cast<int>(len), 0);
            if (n > 0) return n;
            if (n == 0) return MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY;

            int err = WSAGetLastError();
            if (err == WSAEINTR) continue;
            if (err == WSAEWOULDBLOCK) return MBEDTLS_ERR_SSL_WANT_READ;
            #else
            ssize_t n = recv(fd, buf, len, 0); if (n > 0) return static_cast<int>(n); if (n == 0) return MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY; if (errno ==
                EINTR) continue; if (errno == EAGAIN || errno == EWOULDBLOCK) return MBEDTLS_ERR_SSL_WANT_READ;
            #endif
            return MBEDTLS_ERR_NET_RECV_FAILED;
        }
    }

    // ==================== TlsStream ====================

    TlsStream::TlsStream(socket_t fd, std::shared_ptr<TlsContext> ctx, bool is_server)
        : fd_(fd), ctx_(std::move(ctx)), is_server_(is_server) {
        if (!ctx_) return;

        ssl_ = std::make_unique<Impl>();

        int ret = mbedtls_ssl_setup(&ssl_->ssl, &ctx_->impl_->ssl_cfg);
        if (ret != 0) throw std::runtime_error(mbedtls_error_string(ret));

        mbedtls_ssl_set_bio(&ssl_->ssl, &fd_, bio_send, bio_recv, nullptr);
    }

    TlsStream::~TlsStream() = default;

    TlsStream::TlsStream(TlsStream&&) noexcept = default;
    TlsStream& TlsStream::operator=(TlsStream&&) noexcept = default;

    std::error_code TlsStream::handshake() noexcept {
        if (!ssl_) return {};

        for (;;) {
            int ret = mbedtls_ssl_handshake(&ssl_->ssl);
            if (ret == 0) return {};

            if (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE) continue;

            return make_error(ret);
        }
    }

    std::error_code TlsStream::read_some(void* buf, size_t len, size_t& out_read) noexcept {
        out_read = 0;

        // ===== Plain socket path =====
        if (!ssl_) {
            #ifdef _WIN32
            int n = recv(fd_, static_cast<char*>(buf), static_cast<int>(len), 0);
            if (n >= 0) {
                out_read = static_cast<size_t>(n);
                return {};
            }

            int err = WSAGetLastError();
            if (err == WSAEWOULDBLOCK) { return std::make_error_code(std::errc::operation_would_block); }
            return {err, std::system_category()};
            #else
            ssize_t n = recv(fd_, buf, len, 0); if (n >= 0) {
                out_read = static_cast<size_t>(n);
                return {};
            } if (errno == EAGAIN || errno == EWOULDBLOCK) { return std::make_error_code(std::errc::operation_would_block); } return {
                errno,
                std::generic_category()
            };
            #endif
        }

        // ===== TLS path =====
        int ret = mbedtls_ssl_read(&ssl_->ssl, static_cast<unsigned char*>(buf), len);

        if (ret > 0) {
            out_read = static_cast<size_t>(ret);
            return {};
        }

        if (ret == 0) {
            // orderly shutdown
            return std::make_error_code(std::errc::connection_reset);
        }

        if (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE) { return std::make_error_code(std::errc::operation_would_block); }

        return make_error(ret);
    }

    std::error_code TlsStream::write_some(const void* buf, size_t len, size_t& out_written) noexcept {
        out_written = 0;

        // ===== Plain socket path =====
        if (!ssl_) {
            #ifdef _WIN32
            int n = send(fd_, static_cast<const char*>(buf), static_cast<int>(len), 0);
            if (n >= 0) {
                out_written = static_cast<size_t>(n);
                return {};
            }

            int err = WSAGetLastError();
            if (err == WSAEWOULDBLOCK) { return std::make_error_code(std::errc::operation_would_block); }
            return {err, std::system_category()};
            #else
            ssize_t n = send(fd_, buf, len, 0); if (n >= 0) {
                out_written = static_cast<size_t>(n);
                return {};
            } if (errno == EAGAIN || errno == EWOULDBLOCK) { return std::make_error_code(std::errc::operation_would_block); } return {
                errno,
                std::generic_category()
            };
            #endif
        }

        // ===== TLS path =====
        int ret = mbedtls_ssl_write(&ssl_->ssl, static_cast<const unsigned char*>(buf), len);

        if (ret > 0) {
            out_written = static_cast<size_t>(ret);
            return {};
        }

        if (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE) { return std::make_error_code(std::errc::operation_would_block); }

        return make_error(ret);
    }

    void TlsStream::shutdown() noexcept { if (ssl_) { mbedtls_ssl_close_notify(&ssl_->ssl); } }
} // namespace akkaradb::net
