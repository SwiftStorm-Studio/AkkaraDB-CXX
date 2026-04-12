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

// internal/src/engine/cluster/ClusterManager.cpp
#include "engine/cluster/ClusterManager.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <thread>

#ifdef _WIN32
#   ifndef WIN32_LEAN_AND_MEAN
#       define WIN32_LEAN_AND_MEAN
#   endif
#include <windows.h>
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #pragma comment(lib, "ws2_32.lib")
#else
    #include <fcntl.h>
    #include <netdb.h>
    #include <netinet/in.h>
    #include <sys/file.h>
    #include <sys/socket.h>
    #include <unistd.h>
#endif

namespace akkaradb::engine::cluster {

    // ============================================================================
    // Platform-specific: lock file helpers
    // ============================================================================

    namespace {

#ifdef _WIN32
        using LockFileHandle = HANDLE;
        static const LockFileHandle INVALID_LOCK = INVALID_HANDLE_VALUE;

        LockFileHandle try_acquire_lock(const std::filesystem::path& path) {
            HANDLE h = ::CreateFileW(
                path.c_str(),
                GENERIC_READ | GENERIC_WRITE,
                0,            // no sharing
                nullptr,
                OPEN_ALWAYS,
                FILE_ATTRIBUTE_NORMAL,
                nullptr
            );
            if (h == INVALID_HANDLE_VALUE) { return INVALID_LOCK; }
            OVERLAPPED ov{};
            if (!::LockFileEx(h, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &ov)) {
                ::CloseHandle(h);
                return INVALID_LOCK;
            }
            return h;
        }

        void release_lock(LockFileHandle h) noexcept {
            if (h != INVALID_LOCK) {
                OVERLAPPED ov{};
                ::UnlockFileEx(h, 0, 1, 0, &ov);
                ::CloseHandle(h);
            }
        }

        bool write_lock_file(LockFileHandle h, uint64_t node_id, uint16_t repl_port,
                              const std::string& host) {
            const auto host_len = static_cast<uint16_t>(host.size());
            std::vector<uint8_t> buf(12 + host_len);
            // [node_id:u64][repl_port:u16][host_len:u16][host]
            for (int i = 0; i < 8; ++i) buf[i] = static_cast<uint8_t>(node_id >> (8*i));
            buf[8] = static_cast<uint8_t>(repl_port);
            buf[9] = static_cast<uint8_t>(repl_port >> 8);
            buf[10] = static_cast<uint8_t>(host_len);
            buf[11] = static_cast<uint8_t>(host_len >> 8);
            std::memcpy(buf.data() + 12, host.data(), host_len);
            ::SetFilePointer(h, 0, nullptr, FILE_BEGIN);
            DWORD written = 0;
            return ::WriteFile(h, buf.data(), static_cast<DWORD>(buf.size()), &written, nullptr) != 0;
        }

        bool read_lock_file(const std::filesystem::path& path,
                             uint64_t& node_id, uint16_t& repl_port, std::string& host) {
            HANDLE h = ::CreateFileW(path.c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
                                     nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (h == INVALID_HANDLE_VALUE) { return false; }
            uint8_t fixed[12];
            DWORD rd = 0;
            if (!::ReadFile(h, fixed, 12, &rd, nullptr) || rd < 12) { ::CloseHandle(h); return false; }
            node_id = 0; for (int i = 0; i < 8; ++i) node_id |= static_cast<uint64_t>(fixed[i]) << (8*i);
            repl_port = static_cast<uint16_t>(fixed[8]) | (static_cast<uint16_t>(fixed[9]) << 8);
            uint16_t hlen = static_cast<uint16_t>(fixed[10]) | (static_cast<uint16_t>(fixed[11]) << 8);
            host.resize(hlen);
            if (hlen > 0) {
                if (!::ReadFile(h, host.data(), hlen, &rd, nullptr) || rd < hlen) {
                    ::CloseHandle(h); return false;
                }
            }
            ::CloseHandle(h);
            return true;
        }

#else
        using LockFileHandle = int;
        static const LockFileHandle INVALID_LOCK = -1;

        LockFileHandle try_acquire_lock(const std::filesystem::path& path) {
            int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
            if (fd < 0) { return INVALID_LOCK; }
            if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
                ::close(fd);
                return INVALID_LOCK;
            }
            return fd;
        }

        void release_lock(LockFileHandle fd) noexcept {
            if (fd >= 0) {
                ::flock(fd, LOCK_UN);
                ::close(fd);
            }
        }

        bool write_lock_file(LockFileHandle fd, uint64_t node_id, uint16_t repl_port,
                              const std::string& host) {
            const auto host_len = static_cast<uint16_t>(host.size());
            std::vector<uint8_t> buf(12 + host_len);
            for (int i = 0; i < 8; ++i) buf[i] = static_cast<uint8_t>(node_id >> (8*i));
            buf[8]  = static_cast<uint8_t>(repl_port);
            buf[9]  = static_cast<uint8_t>(repl_port >> 8);
            buf[10] = static_cast<uint8_t>(host_len);
            buf[11] = static_cast<uint8_t>(host_len >> 8);
            std::memcpy(buf.data() + 12, host.data(), host_len);
            ::lseek(fd, 0, SEEK_SET);
            return ::write(fd, buf.data(), buf.size()) == static_cast<ssize_t>(buf.size());
        }

        bool read_lock_file(const std::filesystem::path& path,
                             uint64_t& node_id, uint16_t& repl_port, std::string& host) {
            int fd = ::open(path.c_str(), O_RDONLY);
            if (fd < 0) { return false; }
            uint8_t fixed[12];
            if (::read(fd, fixed, 12) < 12) { ::close(fd); return false; }
            node_id = 0; for (int i = 0; i < 8; ++i) node_id |= static_cast<uint64_t>(fixed[i]) << (8*i);
            repl_port = static_cast<uint16_t>(fixed[8]) | (static_cast<uint16_t>(fixed[9]) << 8);
            uint16_t hlen = static_cast<uint16_t>(fixed[10]) | (static_cast<uint16_t>(fixed[11]) << 8);
            host.resize(hlen);
            if (hlen > 0 && ::read(fd, host.data(), hlen) < hlen) { ::close(fd); return false; }
            ::close(fd);
            return true;
        }
#endif

        // Minimal TCP health-check: try connect to host:port, close immediately.
        bool tcp_reachable(const std::string& host, uint16_t port) noexcept {
#ifdef _WIN32
static std::once_flag wsa_once; std::call_once(
    wsa_once,
    [] {
        WSADATA wsa{};
        ::WSAStartup(MAKEWORD(2, 2), &wsa);
    }
); SOCKET s = ::socket(AF_INET, SOCK_STREAM, 0);
            if (s == INVALID_SOCKET) { return false; }
            addrinfo hints{}, *res = nullptr;
            hints.ai_family   = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            const std::string port_str = std::to_string(port);
            if (::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0) {
                ::closesocket(s); return false;
            }
            const bool ok = (::connect(s, res->ai_addr, static_cast<int>(res->ai_addrlen)) == 0);
            ::freeaddrinfo(res);
            ::closesocket(s);
            return ok;
#else
            addrinfo hints{}, *res = nullptr;
            hints.ai_family   = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            const std::string port_str = std::to_string(port);
            if (::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0) { return false; }
            int s = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
            if (s < 0) { ::freeaddrinfo(res); return false; }
            const bool ok = (::connect(s, res->ai_addr, res->ai_addrlen) == 0);
            ::freeaddrinfo(res);
            ::close(s);
            return ok;
#endif
        }

    } // anonymous namespace

    // ============================================================================
    // ClusterManager::Impl
    // ============================================================================

    class ClusterManager::Impl {
        public:
            Impl(const std::filesystem::path& db_dir,
                 const ClusterConfig& config,
                 uint64_t self_node_id)
                : db_dir_{db_dir},
                  config_{config},
                  self_node_id_{self_node_id},
                  role_{NodeRole::Standalone},
                  lock_handle_{INVALID_LOCK},
                  running_{false}
            {
                const NodeInfo* self = config_.find_by_id(self_node_id);
                if (!self && !config_.is_standalone()) {
                    throw std::runtime_error("ClusterManager: self_node_id not found in config");
                }
                if (self) { self_info_ = *self; }
            }

            ~Impl() { close(); }

            void set_role_change_callback(std::function<void(NodeRole)> cb) {
                role_change_cb_ = std::move(cb);
            }

            void start() {
                if (config_.is_standalone()) {
                    set_role(NodeRole::Standalone);
                    return;
                }
                if (running_.exchange(true)) { return; }
                elect_role();
                monitor_thread_ = std::thread([this] { run_monitor(); });
            }

            void close() {
                running_ = false;
                monitor_cv_.notify_all();
                if (monitor_thread_.joinable()) { monitor_thread_.join(); }
                release_lock(lock_handle_);
                lock_handle_ = INVALID_LOCK;
            }

            NodeRole   role()             const noexcept { return role_.load(); }
            uint64_t   self_node_id()     const noexcept { return self_node_id_; }
            bool       is_standalone()    const noexcept { return config_.is_standalone(); }

            std::string primary_host() const {
                std::lock_guard lock{primary_mutex_};
                return primary_host_;
            }
            uint16_t primary_repl_port() const {
                std::lock_guard lock{primary_mutex_};
                return primary_repl_port_;
            }

        private:
            void set_role(NodeRole r) {
                const NodeRole old = role_.exchange(r);
                if (old != r && role_change_cb_) { role_change_cb_(r); }
            }

            void elect_role() {
                const auto lock_path = db_dir_ / "PRIMARY.lock";

                // Try to become Primary
                LockFileHandle h = try_acquire_lock(lock_path);
                if (h != INVALID_LOCK) {
                    lock_handle_ = h;
                    write_lock_file(h, self_node_id_,
                                    self_info_.repl_port, self_info_.host);
                    {
                        std::lock_guard lk{primary_mutex_};
                        primary_host_      = self_info_.host;
                        primary_repl_port_ = self_info_.repl_port;
                    }
                    set_role(NodeRole::Primary);
                } else {
                    // Become Replica: read Primary address from lock file
                    uint64_t pid = 0; uint16_t pport = 0; std::string phost;
                    if (read_lock_file(lock_path, pid, pport, phost)) {
                        std::lock_guard lk{primary_mutex_};
                        primary_host_      = std::move(phost);
                        primary_repl_port_ = pport;
                    }
                    set_role(NodeRole::Replica);
                }
            }

            void run_monitor() {
                constexpr auto CHECK_INTERVAL = std::chrono::milliseconds(2000);
                constexpr int  MAX_FAILURES   = 3;

                int consecutive_failures = 0;

                while (running_) {
                    std::unique_lock lk{monitor_mutex_};
                    monitor_cv_.wait_for(lk, CHECK_INTERVAL, [this] { return !running_.load(); });
                    if (!running_) { break; }
                    lk.unlock();

                    if (role() == NodeRole::Replica) {
                        const std::string phost = primary_host();
                        const uint16_t    pport = primary_repl_port();
                        if (!phost.empty() && !tcp_reachable(phost, pport)) {
                            ++consecutive_failures;
                            if (consecutive_failures >= MAX_FAILURES) {
                                consecutive_failures = 0;
                                // Release stale lock assumption and try to become Primary
                                release_lock(lock_handle_);
                                lock_handle_ = INVALID_LOCK;
                                elect_role(); // may succeed or find a new Primary
                            }
                        } else {
                            consecutive_failures = 0;
                        }
                    }
                }
            }

            // ---- Members ----
            std::filesystem::path     db_dir_;
            ClusterConfig             config_;
            uint64_t                  self_node_id_;
            NodeInfo                  self_info_{};
            std::atomic<NodeRole>     role_;
            LockFileHandle            lock_handle_;
            std::atomic<bool>         running_;

            mutable std::mutex        primary_mutex_;
            std::string               primary_host_;
            uint16_t                  primary_repl_port_ = 0;

            std::thread               monitor_thread_;
            std::mutex                monitor_mutex_;
            std::condition_variable   monitor_cv_;

            std::function<void(NodeRole)> role_change_cb_;
    };

    // ============================================================================
    // ClusterManager public API
    // ============================================================================

    std::unique_ptr<ClusterManager> ClusterManager::create(
        const std::filesystem::path& db_dir,
        const ClusterConfig& config,
        uint64_t self_node_id
    ) {
        return std::unique_ptr<ClusterManager>(
            new ClusterManager(db_dir, config, self_node_id)
        );
    }

    ClusterManager::ClusterManager(
        const std::filesystem::path& db_dir,
        const ClusterConfig& config,
        uint64_t self_node_id
    ) : impl_{std::make_unique<Impl>(db_dir, config, self_node_id)} {}

    ClusterManager::~ClusterManager() = default;

    void ClusterManager::set_role_change_callback(std::function<void(NodeRole)> cb) {
        impl_->set_role_change_callback(std::move(cb));
    }
    void     ClusterManager::start()              { impl_->start(); }
    void     ClusterManager::close()              { impl_->close(); }
    NodeRole ClusterManager::role()         const noexcept { return impl_->role(); }
    uint64_t ClusterManager::self_node_id() const noexcept { return impl_->self_node_id(); }
    bool     ClusterManager::is_standalone()const noexcept { return impl_->is_standalone(); }
    std::string ClusterManager::primary_host()       const { return impl_->primary_host(); }
    uint16_t    ClusterManager::primary_repl_port()  const { return impl_->primary_repl_port(); }

} // namespace akkaradb::engine::cluster
