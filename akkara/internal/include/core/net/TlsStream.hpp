#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <system_error>

#ifdef _WIN32
#include <winsock2.h>
typedef SOCKET socket_t;
#else
typedef int socket_t;
#endif

namespace akkaradb::net {
    /**
     * @brief TLS verification mode.
     */
    enum class TlsVerifyMode {
        None,
        ///< No verification (INSECURE)
        Optional,
        ///< Verify if provided
        Required ///< Verification required (default for client)
    };

    class TlsContext {
        public:
            static std::shared_ptr<TlsContext> create_server(
                const std::string& cert_pem,
                const std::string& key_pem,
                const std::string& ca_pem = {},
                TlsVerifyMode verify_mode = TlsVerifyMode::None
            );

            static std::shared_ptr<TlsContext> create_client(const std::string& ca_pem, TlsVerifyMode verify_mode = TlsVerifyMode::Required);

            ~TlsContext();

            TlsContext(const TlsContext&) = delete;
            TlsContext& operator=(const TlsContext&) = delete;

        private:
            struct Impl;
            std::unique_ptr<Impl> impl_;

            explicit TlsContext(std::unique_ptr<Impl> impl) noexcept;
            friend class TlsStream;
    };

    class TlsStream {
        public:
            TlsStream(socket_t fd, std::shared_ptr<TlsContext> ctx, bool is_server);

            ~TlsStream();

            TlsStream(const TlsStream&) = delete;
            TlsStream& operator=(const TlsStream&) = delete;

            TlsStream(TlsStream&&) noexcept;
            TlsStream& operator=(TlsStream&&) noexcept;

            [[nodiscard]] std::error_code handshake() noexcept;

            [[nodiscard]] std::error_code read_some(void* buf, size_t len, size_t& out_read) noexcept;

            [[nodiscard]] std::error_code write_some(const void* buf, size_t len, size_t& out_written) noexcept;

            void shutdown() noexcept;

            [[nodiscard]] socket_t fd() const noexcept { return fd_; }

            [[nodiscard]] bool is_tls() const noexcept { return ssl_ != nullptr; }

        private:
            struct Impl;

            socket_t fd_;
            std::shared_ptr<TlsContext> ctx_;
            std::unique_ptr<Impl> ssl_;

            bool is_server_ = false;

            static int bio_send(void* ctx, const unsigned char* buf, size_t len);
            static int bio_recv(void* ctx, unsigned char* buf, size_t len);
    };
} // namespace akkaradb::net
