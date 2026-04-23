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

// internal/include/platform/socket/SocketPosix.cpp
#include "platform/socket/Socket.hpp"

#if defined(_WIN32)

#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>

#include <stdexcept>

#pragma comment(lib, "Ws2_32.lib")

namespace akkaradb::platform {
    namespace {
        // Ensure WSA is initialized once
        void EnsureWSA() {
            static bool initialized = false;
            if (!initialized) {
                WSADATA wsa{};
                if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) { throw std::runtime_error("WSAStartup failed"); }
                initialized = true;
            }
        }
    } // namespace

    // ==================== ctor / dtor ====================

    Socket::Socket() noexcept : fd_(static_cast<int>(INVALID_SOCKET)) {}

    Socket::~Socket() { if (fd_ != static_cast<int>(INVALID_SOCKET)) { ::closesocket(static_cast<SOCKET>(fd_)); } }

    // ==================== move ====================

    Socket::Socket(Socket&& other) noexcept : fd_(other.fd_) { other.fd_ = static_cast<int>(INVALID_SOCKET); }

    Socket& Socket::operator=(Socket&& other) noexcept {
        if (this != &other) {
            if (fd_ != static_cast<int>(INVALID_SOCKET)) { ::closesocket(static_cast<SOCKET>(fd_)); }
            fd_ = other.fd_;
            other.fd_ = static_cast<int>(INVALID_SOCKET);
        }
        return *this;
    }

    // ==================== state ====================

    bool Socket::valid() const noexcept { return fd_ != static_cast<int>(INVALID_SOCKET); }

    void Socket::close() noexcept {
        if (fd_ != static_cast<int>(INVALID_SOCKET)) {
            ::closesocket(static_cast<SOCKET>(fd_));
            fd_ = static_cast<int>(INVALID_SOCKET);
        }
    }

    // ==================== connect ====================

    Socket Socket::connect(const char* host, uint16_t port) {
        EnsureWSA();

        addrinfo hints{};
        addrinfo* result = nullptr;

        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;

        char port_str[6];
        std::snprintf(port_str, sizeof(port_str), "%u", port);

        if (getaddrinfo(host, port_str, &hints, &result) != 0) { throw std::runtime_error("Socket::connect: getaddrinfo failed"); }

        SOCKET sock = INVALID_SOCKET;

        for (auto* rp = result; rp != nullptr; rp = rp->ai_next) {
            sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
            if (sock == INVALID_SOCKET) { continue; }

            if (::connect(sock, rp->ai_addr, static_cast<int>(rp->ai_addrlen)) == 0) { break; }

            closesocket(sock);
            sock = INVALID_SOCKET;
        }

        freeaddrinfo(result);

        if (sock == INVALID_SOCKET) { throw std::runtime_error("Socket::connect: connection failed"); }

        Socket s;
        s.fd_ = static_cast<int>(sock);
        return s;
    }

    // ==================== IO ====================

    std::size_t Socket::send(const void* data, std::size_t size) {
        int n = ::send(static_cast<SOCKET>(fd_), static_cast<const char*>(data), static_cast<int>(size), 0);
        if (n < 0) { throw std::runtime_error("Socket::send failed"); }
        return static_cast<std::size_t>(n);
    }

    std::size_t Socket::recv(void* data, std::size_t size) {
        int n = ::recv(static_cast<SOCKET>(fd_), static_cast<char*>(data), static_cast<int>(size), 0);
        if (n < 0) { throw std::runtime_error("Socket::recv failed"); }
        return static_cast<std::size_t>(n);
    }
} // namespace akkaradb::platform

#endif
