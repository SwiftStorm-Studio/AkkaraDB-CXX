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

// akkara/internal/src/core/net/TlsStream.cpp
#include "core/net/TlsStream.hpp"

#include <stdexcept>
#include <cstring>

#ifdef _WIN32
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  include <winsock2.h>
#else
#  include <sys/socket.h>
#endif

#ifdef AKKARADB_TLS_ENABLED
#  ifdef _MSC_VER
#    pragma warning(push)
#    pragma warning(disable: 4061 4062 4100 4127 4200 4244 4245 4267 4389 4456 4701 4706 4996)
#  endif
#  include <mbedtls/ssl.h>
#  include <mbedtls/entropy.h>
#  include <mbedtls/ctr_drbg.h>
#  include <mbedtls/x509_crt.h>
#  include <mbedtls/pk.h>
#  include <mbedtls/error.h>
#  ifdef _MSC_VER
#    pragma warning(pop)
#  endif
#endif

namespace akkaradb::core {

// ============================================================================
// Bio callbacks for mbedTLS — use akk_sock_t* directly (safe for Windows SOCKET)
// ============================================================================

#ifdef AKKARADB_TLS_ENABLED

static int tls_bio_send(void* ctx, const unsigned char* buf, size_t len) {
    akk_sock_t fd = *static_cast<akk_sock_t*>(ctx);
    int chunk = len > 65536 ? 65536 : static_cast<int>(len);
#ifdef _WIN32
    int n = ::send(fd, reinterpret_cast<const char*>(buf), chunk, 0);
#else
    int n = static_cast<int>(::send(fd, buf, static_cast<size_t>(chunk), 0));
#endif
    return n <= 0 ? MBEDTLS_ERR_NET_SEND_FAILED : n;
}

static int tls_bio_recv(void* ctx, unsigned char* buf, size_t len) {
    akk_sock_t fd = *static_cast<akk_sock_t*>(ctx);
    int chunk = len > 65536 ? 65536 : static_cast<int>(len);
#ifdef _WIN32
    int n = ::recv(fd, reinterpret_cast<char*>(buf), chunk, 0);
#else
    int n = static_cast<int>(::recv(fd, buf, static_cast<size_t>(chunk), 0));
#endif
    return n <= 0 ? MBEDTLS_ERR_NET_RECV_FAILED : n;
}

#endif // AKKARADB_TLS_ENABLED

// ============================================================================
// TlsContext::Impl
// ============================================================================

struct TlsContext::Impl {
#ifdef AKKARADB_TLS_ENABLED
    mbedtls_entropy_context  entropy_;
    mbedtls_ctr_drbg_context drbg_;
    mbedtls_ssl_config        ssl_cfg_;
    mbedtls_x509_crt          cert_;
    mbedtls_pk_context        pk_;
    bool                      is_server_ = false;

    Impl() {
        mbedtls_entropy_init (&entropy_);
        mbedtls_ctr_drbg_init(&drbg_);
        mbedtls_ssl_config_init(&ssl_cfg_);
        mbedtls_x509_crt_init(&cert_);
        mbedtls_pk_init(&pk_);
    }

    ~Impl() {
        mbedtls_pk_free         (&pk_);
        mbedtls_x509_crt_free   (&cert_);
        mbedtls_ssl_config_free (&ssl_cfg_);
        mbedtls_ctr_drbg_free   (&drbg_);
        mbedtls_entropy_free    (&entropy_);
    }

    Impl(const Impl&)            = delete;
    Impl& operator=(const Impl&) = delete;
#endif
};

// ============================================================================
// TlsContext::make_server / make_client
// ============================================================================

std::unique_ptr<TlsContext> TlsContext::make_server(const TlsConfig& cfg) {
    if (!cfg.enabled) return nullptr;

#ifndef AKKARADB_TLS_ENABLED
    throw std::runtime_error(
        "AkkaraDB: TLS support not compiled in (AKKARADB_ENABLE_TLS=OFF)");
#else
    auto impl = std::make_unique<Impl>();
    impl->is_server_ = true;

    // Seed DRBG
    const char* pers = "akkaradb_server";
    int ret = mbedtls_ctr_drbg_seed(&impl->drbg_, mbedtls_entropy_func,
                                     &impl->entropy_,
                                     reinterpret_cast<const unsigned char*>(pers),
                                     std::strlen(pers));
    if (ret != 0) throw std::runtime_error("AkkaraDB TLS: mbedtls_ctr_drbg_seed failed");

    // Load certificate
    if (!cfg.cert_path.empty()) {
        ret = mbedtls_x509_crt_parse_file(&impl->cert_, cfg.cert_path.c_str());
        if (ret != 0) throw std::runtime_error("AkkaraDB TLS: failed to load cert: " + cfg.cert_path);
    }

    // Load private key
    if (!cfg.key_path.empty()) {
#if MBEDTLS_VERSION_MAJOR >= 3
        ret = mbedtls_pk_parse_keyfile(&impl->pk_, cfg.key_path.c_str(), nullptr,
                                        mbedtls_ctr_drbg_random, &impl->drbg_);
#else
        ret = mbedtls_pk_parse_keyfile(&impl->pk_, cfg.key_path.c_str(), nullptr);
#endif
        if (ret != 0) throw std::runtime_error("AkkaraDB TLS: failed to load key: " + cfg.key_path);
    }

    // Load CA for mTLS peer verification
    mbedtls_x509_crt ca_cert;
    mbedtls_x509_crt_init(&ca_cert);
    bool ca_loaded = false;
    if (!cfg.ca_path.empty()) {
        ret = mbedtls_x509_crt_parse_file(&ca_cert, cfg.ca_path.c_str());
        if (ret != 0) {
            mbedtls_x509_crt_free(&ca_cert);
            throw std::runtime_error("AkkaraDB TLS: failed to load CA: " + cfg.ca_path);
        }
        ca_loaded = true;
    }

    // Configure SSL defaults for server
    ret = mbedtls_ssl_config_defaults(&impl->ssl_cfg_,
                                       MBEDTLS_SSL_IS_SERVER,
                                       MBEDTLS_SSL_TRANSPORT_STREAM,
                                       MBEDTLS_SSL_PRESET_DEFAULT);
    if (ret != 0) {
        if (ca_loaded) mbedtls_x509_crt_free(&ca_cert);
        throw std::runtime_error("AkkaraDB TLS: mbedtls_ssl_config_defaults failed");
    }

    mbedtls_ssl_conf_rng(&impl->ssl_cfg_, mbedtls_ctr_drbg_random, &impl->drbg_);

    // Attach cert+key
    if (!cfg.cert_path.empty() && !cfg.key_path.empty()) {
        ret = mbedtls_ssl_conf_own_cert(&impl->ssl_cfg_, &impl->cert_, &impl->pk_);
        if (ret != 0) {
            if (ca_loaded) mbedtls_x509_crt_free(&ca_cert);
            throw std::runtime_error("AkkaraDB TLS: mbedtls_ssl_conf_own_cert failed");
        }
    }

    // Peer verification (mTLS)
    if (cfg.verify_peer && ca_loaded) {
        mbedtls_ssl_conf_ca_chain(&impl->ssl_cfg_, &ca_cert, nullptr);
        mbedtls_ssl_conf_authmode(&impl->ssl_cfg_, MBEDTLS_SSL_VERIFY_REQUIRED);
    } else {
        mbedtls_ssl_conf_authmode(&impl->ssl_cfg_, MBEDTLS_SSL_VERIFY_NONE);
        if (ca_loaded) mbedtls_x509_crt_free(&ca_cert);
    }

    return std::unique_ptr<TlsContext>(new TlsContext(std::move(impl)));
#endif
}

std::unique_ptr<TlsContext> TlsContext::make_client(const TlsConfig& cfg) {
    if (!cfg.enabled) return nullptr;

#ifndef AKKARADB_TLS_ENABLED
    throw std::runtime_error(
        "AkkaraDB: TLS support not compiled in (AKKARADB_ENABLE_TLS=OFF)");
#else
    auto impl = std::make_unique<Impl>();
    impl->is_server_ = false;

    // Seed DRBG
    const char* pers = "akkaradb_client";
    int ret = mbedtls_ctr_drbg_seed(&impl->drbg_, mbedtls_entropy_func,
                                     &impl->entropy_,
                                     reinterpret_cast<const unsigned char*>(pers),
                                     std::strlen(pers));
    if (ret != 0) throw std::runtime_error("AkkaraDB TLS: mbedtls_ctr_drbg_seed failed");

    // Load CA cert for peer verification
    if (!cfg.ca_path.empty()) {
        ret = mbedtls_x509_crt_parse_file(&impl->cert_, cfg.ca_path.c_str());
        if (ret != 0) throw std::runtime_error("AkkaraDB TLS: failed to load CA: " + cfg.ca_path);
    }

    // Load client cert+key for mTLS
    mbedtls_x509_crt client_cert;
    mbedtls_x509_crt_init(&client_cert);
    mbedtls_pk_context client_pk;
    mbedtls_pk_init(&client_pk);

    bool client_cert_loaded = false;
    if (!cfg.cert_path.empty() && !cfg.key_path.empty()) {
        ret = mbedtls_x509_crt_parse_file(&client_cert, cfg.cert_path.c_str());
        if (ret == 0) {
#if MBEDTLS_VERSION_MAJOR >= 3
            ret = mbedtls_pk_parse_keyfile(&client_pk, cfg.key_path.c_str(), nullptr,
                                            mbedtls_ctr_drbg_random, &impl->drbg_);
#else
            ret = mbedtls_pk_parse_keyfile(&client_pk, cfg.key_path.c_str(), nullptr);
#endif
        }
        if (ret == 0) client_cert_loaded = true;
    }

    // Configure SSL defaults for client
    ret = mbedtls_ssl_config_defaults(&impl->ssl_cfg_,
                                       MBEDTLS_SSL_IS_CLIENT,
                                       MBEDTLS_SSL_TRANSPORT_STREAM,
                                       MBEDTLS_SSL_PRESET_DEFAULT);
    if (ret != 0) {
        if (client_cert_loaded) {
            mbedtls_x509_crt_free(&client_cert);
            mbedtls_pk_free(&client_pk);
        }
        throw std::runtime_error("AkkaraDB TLS: mbedtls_ssl_config_defaults failed");
    }

    mbedtls_ssl_conf_rng(&impl->ssl_cfg_, mbedtls_ctr_drbg_random, &impl->drbg_);

    // Peer verification
    if (cfg.verify_peer && !cfg.ca_path.empty()) {
        mbedtls_ssl_conf_ca_chain(&impl->ssl_cfg_, &impl->cert_, nullptr);
        mbedtls_ssl_conf_authmode(&impl->ssl_cfg_, MBEDTLS_SSL_VERIFY_REQUIRED);
    } else {
        mbedtls_ssl_conf_authmode(&impl->ssl_cfg_, MBEDTLS_SSL_VERIFY_NONE);
    }

    // Client cert for mTLS
    if (client_cert_loaded) {
        ret = mbedtls_ssl_conf_own_cert(&impl->ssl_cfg_, &client_cert, &client_pk);
        if (ret != 0) {
            mbedtls_x509_crt_free(&client_cert);
            mbedtls_pk_free(&client_pk);
            throw std::runtime_error("AkkaraDB TLS: mbedtls_ssl_conf_own_cert failed");
        }
    }

    return std::unique_ptr<TlsContext>(new TlsContext(std::move(impl)));
#endif
}

TlsContext::TlsContext(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

TlsContext::~TlsContext() = default;

// ============================================================================
// TlsStream::Impl
// ============================================================================

struct TlsStream::Impl {
    akk_sock_t fd_  = {};
    bool       tls_ = false;

#ifdef AKKARADB_TLS_ENABLED
    mbedtls_ssl_context ssl_;
    bool ssl_inited_ = false;
#endif

    Impl() = default;

    ~Impl() {
#ifdef AKKARADB_TLS_ENABLED
        if (ssl_inited_) {
            mbedtls_ssl_free(&ssl_);
        }
#endif
    }

    Impl(const Impl&)            = delete;
    Impl& operator=(const Impl&) = delete;
};

// ============================================================================
// TlsStream factory
// ============================================================================

std::unique_ptr<TlsStream> TlsStream::server_wrap(akk_sock_t fd, TlsContext* ctx) {
    auto impl  = std::make_unique<Impl>();
    impl->fd_  = fd;
    impl->tls_ = (ctx != nullptr);

#ifdef AKKARADB_TLS_ENABLED
    if (ctx != nullptr) {
        mbedtls_ssl_init(&impl->ssl_);
        impl->ssl_inited_ = true;

        int ret = mbedtls_ssl_setup(&impl->ssl_, &ctx->impl()->ssl_cfg_);
        if (ret != 0) throw std::runtime_error("AkkaraDB TLS: mbedtls_ssl_setup failed (server)");

        mbedtls_ssl_set_bio(&impl->ssl_, &impl->fd_, tls_bio_send, tls_bio_recv, nullptr);

        // Handshake loop
        do {
            ret = mbedtls_ssl_handshake(&impl->ssl_);
        } while (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE);

        if (ret != 0) throw std::runtime_error("AkkaraDB TLS: server handshake failed");
    }
#else
    (void)ctx;
#endif

    return std::unique_ptr<TlsStream>(new TlsStream(std::move(impl)));
}

std::unique_ptr<TlsStream> TlsStream::client_wrap(akk_sock_t fd, const char* hostname, TlsContext* ctx) {
    auto impl  = std::make_unique<Impl>();
    impl->fd_  = fd;
    impl->tls_ = (ctx != nullptr);

#ifdef AKKARADB_TLS_ENABLED
    if (ctx != nullptr) {
        mbedtls_ssl_init(&impl->ssl_);
        impl->ssl_inited_ = true;

        int ret = mbedtls_ssl_setup(&impl->ssl_, &ctx->impl()->ssl_cfg_);
        if (ret != 0) throw std::runtime_error("AkkaraDB TLS: mbedtls_ssl_setup failed (client)");

        if (hostname != nullptr && hostname[0] != '\0') {
            ret = mbedtls_ssl_set_hostname(&impl->ssl_, hostname);
            if (ret != 0) throw std::runtime_error("AkkaraDB TLS: mbedtls_ssl_set_hostname failed");
        }

        mbedtls_ssl_set_bio(&impl->ssl_, &impl->fd_, tls_bio_send, tls_bio_recv, nullptr);

        // Handshake loop
        do {
            ret = mbedtls_ssl_handshake(&impl->ssl_);
        } while (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE);

        if (ret != 0) throw std::runtime_error("AkkaraDB TLS: client handshake failed");
    }
#else
    (void)hostname;
    (void)ctx;
#endif

    return std::unique_ptr<TlsStream>(new TlsStream(std::move(impl)));
}

// ============================================================================
// TlsStream I/O
// ============================================================================

bool TlsStream::send_all(const uint8_t* data, size_t len) noexcept {
    if (!impl_->tls_) {
        // Plain TCP path
        while (len > 0) {
            int chunk = len > 65536 ? 65536 : static_cast<int>(len);
#ifdef _WIN32
            int n = ::send(impl_->fd_, reinterpret_cast<const char*>(data), chunk, 0);
#else
            int n = static_cast<int>(::send(impl_->fd_, data, static_cast<size_t>(chunk), 0));
#endif
            if (n <= 0) return false;
            data += n;
            len  -= static_cast<size_t>(n);
        }
        return true;
    }

#ifdef AKKARADB_TLS_ENABLED
    while (len > 0) {
        int n = mbedtls_ssl_write(&impl_->ssl_, data, len);
        if (n == MBEDTLS_ERR_SSL_WANT_READ || n == MBEDTLS_ERR_SSL_WANT_WRITE) continue;
        if (n <= 0) return false;
        data += n;
        len  -= static_cast<size_t>(n);
    }
    return true;
#else
    return false;
#endif
}

bool TlsStream::recv_all(uint8_t* data, size_t len) noexcept {
    if (!impl_->tls_) {
        // Plain TCP path
        while (len > 0) {
            int chunk = len > 65536 ? 65536 : static_cast<int>(len);
#ifdef _WIN32
            int n = ::recv(impl_->fd_, reinterpret_cast<char*>(data), chunk, 0);
#else
            int n = static_cast<int>(::recv(impl_->fd_, data, static_cast<size_t>(chunk), 0));
#endif
            if (n <= 0) return false;
            data += n;
            len  -= static_cast<size_t>(n);
        }
        return true;
    }

#ifdef AKKARADB_TLS_ENABLED
    while (len > 0) {
        int n = mbedtls_ssl_read(&impl_->ssl_, data, len);
        if (n == MBEDTLS_ERR_SSL_WANT_READ || n == MBEDTLS_ERR_SSL_WANT_WRITE) continue;
        if (n <= 0) return false;
        data += n;
        len  -= static_cast<size_t>(n);
    }
    return true;
#else
    return false;
#endif
}

void TlsStream::shutdown() noexcept {
#ifdef AKKARADB_TLS_ENABLED
    if (impl_->tls_ && impl_->ssl_inited_) {
        // Best-effort close_notify; ignore return value
        int ret;
        do {
            ret = mbedtls_ssl_close_notify(&impl_->ssl_);
        } while (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE);
    }
#endif
}

TlsStream::TlsStream(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

TlsStream::~TlsStream() = default;

} // namespace akkaradb::core
