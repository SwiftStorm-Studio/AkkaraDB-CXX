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
#include "net/tls/TlsStream.hpp"

#include <mbedtls/ctr_drbg.h>
#include <mbedtls/entropy.h>
#include <mbedtls/ssl.h>
#include <mbedtls/x509_crt.h>

#include <stdexcept>
#include <system_error>

#include "mbedtls/net_sockets.h"

namespace akkaradb::net {
    // ==================== Impl ====================

    struct TlsStream::Impl {
        mbedtls_ssl_context ssl{};
        mbedtls_ssl_config cfg{};
        mbedtls_entropy_context entropy{};
        mbedtls_ctr_drbg_context drbg{};
        mbedtls_x509_crt cacert{};

        platform::Socket* sock{};

        Impl() {
            mbedtls_ssl_init(&ssl);
            mbedtls_ssl_config_init(&cfg);
            mbedtls_entropy_init(&entropy);
            mbedtls_ctr_drbg_init(&drbg);
            mbedtls_x509_crt_init(&cacert);
        }

        ~Impl() {
            mbedtls_ssl_free(&ssl);
            mbedtls_ssl_config_free(&cfg);
            mbedtls_ctr_drbg_free(&drbg);
            mbedtls_entropy_free(&entropy);
            mbedtls_x509_crt_free(&cacert);
        }
    };

    // ==================== BIO ====================

    static int bio_send(void* ctx, const unsigned char* buf, size_t len) {
        platform::Socket* sock = static_cast<platform::Socket*>(ctx);

        std::size_t sent = 0;
        std::error_code ec = sock->send_some(buf, len, sent);

        if (!ec) { return static_cast<int>(sent); }

        if (ec == std::errc::operation_would_block) { return MBEDTLS_ERR_SSL_WANT_WRITE; }

        return MBEDTLS_ERR_NET_SEND_FAILED;
    }

    static int bio_recv(void* ctx, unsigned char* buf, size_t len) {
        platform::Socket* sock = static_cast<platform::Socket*>(ctx);

        std::size_t recvd = 0;
        std::error_code ec = sock->recv_some(buf, len, recvd);

        if (!ec) { return static_cast<int>(recvd); }

        if (ec == std::errc::operation_would_block) { return MBEDTLS_ERR_SSL_WANT_READ; }

        return MBEDTLS_ERR_NET_RECV_FAILED;
    }

    // ==================== lifecycle ====================

    TlsStream::~TlsStream() { close(); }

    TlsStream::TlsStream(TlsStream&& other) noexcept : socket_(std::move(other.socket_)), impl_(other.impl_) { other.impl_ = nullptr; }

    TlsStream& TlsStream::operator=(TlsStream&& other) noexcept {
        if (this != &other) {
            close();
            socket_ = std::move(other.socket_);
            impl_ = other.impl_;
            other.impl_ = nullptr;
        }
        return *this;
    }

    // ==================== connect ====================

    void TlsStream::connect(const char* host, uint16_t port) {
        socket_ = platform::Socket::connect(host, port);

        impl_ = new Impl();
        impl_->sock = &socket_;

        int ret = mbedtls_ctr_drbg_seed(&impl_->drbg, mbedtls_entropy_func, &impl_->entropy, nullptr, 0);
        if (ret != 0) throw std::runtime_error("mbedtls_ctr_drbg_seed failed");

        ret = mbedtls_ssl_config_defaults(&impl_->cfg, MBEDTLS_SSL_IS_CLIENT, MBEDTLS_SSL_TRANSPORT_STREAM, MBEDTLS_SSL_PRESET_DEFAULT);
        if (ret != 0) throw std::runtime_error("mbedtls_ssl_config_defaults failed");

        mbedtls_ssl_conf_rng(&impl_->cfg, mbedtls_ctr_drbg_random, &impl_->drbg);

        ret = mbedtls_ssl_setup(&impl_->ssl, &impl_->cfg);
        if (ret != 0) throw std::runtime_error("mbedtls_ssl_setup failed");

        mbedtls_ssl_set_bio(&impl_->ssl, impl_->sock, bio_send, bio_recv, nullptr);

        // ==================== handshake ====================

        for (;;) {
            ret = mbedtls_ssl_handshake(&impl_->ssl);

            if (ret == 0) break;

            if (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE) { continue; }

            throw std::runtime_error("mbedtls_ssl_handshake failed");
        }
    }

    // ==================== IO ====================

    std::size_t TlsStream::send(const void* data, std::size_t size) {
        for (;;) {
            int ret = mbedtls_ssl_write(&impl_->ssl, static_cast<const unsigned char*>(data), size);

            if (ret > 0) { return static_cast<std::size_t>(ret); }

            if (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE) { continue; }

            throw std::runtime_error("tls send failed");
        }
    }

    std::size_t TlsStream::recv(void* data, std::size_t size) {
        for (;;) {
            int ret = mbedtls_ssl_read(&impl_->ssl, static_cast<unsigned char*>(data), size);

            if (ret > 0) { return static_cast<std::size_t>(ret); }

            if (ret == 0) { throw std::runtime_error("tls connection closed"); }

            if (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE) { continue; }

            throw std::runtime_error("tls recv failed");
        }
    }

    // ==================== state ====================

    void TlsStream::close() noexcept {
        if (impl_) {
            mbedtls_ssl_close_notify(&impl_->ssl);
            delete impl_;
            impl_ = nullptr;
        }
        socket_.close();
    }

    bool TlsStream::valid() const noexcept { return socket_.valid(); }
} //akkaradb::net
