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

// internal/include/platform/socket/Socket.hpp
#pragma once

#include <cstddef>
#include <cstdint>

namespace akkaradb::platform {
    /**
     * @brief Minimal cross-platform TCP socket wrapper.
     *
     * Provides blocking connect/send/recv operations.
     * Platform-specific implementations are hidden in source files.
     */
    class Socket {
        public:
            Socket() noexcept;
            ~Socket();

            Socket(Socket&& other) noexcept;
            Socket& operator=(Socket&& other) noexcept;

            Socket(const Socket&) = delete;
            Socket& operator=(const Socket&) = delete;

            /**
             * @brief Check if the socket is valid.
             */
            [[nodiscard]] bool valid() const noexcept;

            /**
             * @brief Close the socket.
             */
            void close() noexcept;

            /**
             * @brief Create and connect a TCP socket.
             *
             * @param host Hostname or IP address
             * @param port TCP port
             * @return Connected socket
             *
             * @throws std::runtime_error on failure
             */
            static Socket connect(const char* host, uint16_t port);

            /**
             * @brief Send data.
             *
             * @return Number of bytes sent
             *
             * @throws std::runtime_error on failure
             */
            std::size_t send(const void* data, std::size_t size);

            /**
             * @brief Receive data.
             *
             * @return Number of bytes received
             *
             * @throws std::runtime_error on failure
             */
            std::size_t recv(void* data, std::size_t size);

        private:
            int fd_;
    };
}
