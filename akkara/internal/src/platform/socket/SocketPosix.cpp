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

#if !defined(_WIN32)

#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include <stdexcept>

namespace akkaradb::platform {
    // ==================== ctor / dtor ====================

    Socket::Socket() noexcept : fd_(-1) {}

    Socket::~Socket() { if (fd_ >= 0) { ::close(fd_); } }

    // ==================== move ====================

    Socket::Socket(Socket&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

    Socket& Socket::operator=(Socket&& other) noexcept {
        if (this != &other) {
            if (fd_ >= 0) { ::close(fd_); }
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    // ==================== state ====================

    bool Socket::valid() const noexcept { return fd_ >= 0; }

    void Socket::close() noexcept {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

    // ==================== connect ====================

    Socket Socket::connect(const char* host, uint16_t port) {
        struct addrinfo hints{};
        struct addrinfo* result = nullptr;

        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        char port_str[6];
        std::snprintf(port_str, sizeof(port_str), "%u", port);

        if (::getaddrinfo(host, port_str, &hints, &result) != 0) { throw std::runtime_error("Socket::connect: getaddrinfo failed"); }

        int fd = -1;

        for (auto* rp = result; rp != nullptr; rp = rp->ai_next) {
            fd = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
            if (fd < 0) { continue; }

            if (::connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) { break; }

            ::close(fd);
            fd = -1;
        }

        ::freeaddrinfo(result);

        if (fd < 0) { throw std::runtime_error("Socket::connect: connection failed"); }

        Socket sock;
        sock.fd_ = fd;
        return sock;
    }

    // ==================== IO ====================

    std::size_t Socket::send(const void* data, std::size_t size) {
        ssize_t n = ::write(fd_, data, size);
        if (n < 0) { throw std::runtime_error("Socket::send failed"); }
        return static_cast<std::size_t>(n);
    }

    std::size_t Socket::recv(void* data, std::size_t size) {
        ssize_t n = ::read(fd_, data, size);
        if (n < 0) { throw std::runtime_error("Socket::recv failed"); }
        return static_cast<std::size_t>(n);
    }
} // namespace akkaradb::platform

#endif
