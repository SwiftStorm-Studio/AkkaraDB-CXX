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

// internal/src/engine/cluster/ReplicationServer.cpp
#include "engine/cluster/ReplicationServer.hpp"
#include "engine/cluster/ReplFraming.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include <climits>
#include <cstring>

// ============================================================================
// Platform TCP abstractions
// ============================================================================
#ifdef _WIN32
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  include <winsock2.h>
#  include <ws2tcpip.h>
   using sock_t = SOCKET;
   static constexpr sock_t BAD_SOCK = INVALID_SOCKET;
   static inline void close_sock(sock_t s) noexcept { ::closesocket(s); }
   static inline bool  sock_ok(sock_t s)   noexcept { return s != INVALID_SOCKET; }
#else
#  include <sys/socket.h>
#  include <netinet/in.h>
#  include <netinet/tcp.h>
#  include <arpa/inet.h>
#  include <unistd.h>
   using sock_t = int;
   static constexpr sock_t BAD_SOCK = -1;
   static inline void close_sock(sock_t s) noexcept { ::close(s); }
   static inline bool  sock_ok(sock_t s)   noexcept { return s >= 0; }
#endif

namespace {

    // One-time Winsock initialisation (no-op on POSIX)
    static void net_init() noexcept {
#ifdef _WIN32
        static std::once_flag once;
        std::call_once(once, [] {
            WSADATA wd{};
            WSAStartup(MAKEWORD(2, 2), &wd);
        });
#endif
    }

    // Sends exactly `len` bytes; returns false on error.
    static bool send_all(sock_t s, const uint8_t* data, size_t len) noexcept {
        while (len > 0) {
            int chunk = (len > static_cast<size_t>(INT_MAX))
                            ? INT_MAX
                            : static_cast<int>(len);
            auto n = ::send(s, reinterpret_cast<const char*>(data), chunk, 0);
            if (n <= 0) return false;
            data += n;
            len  -= static_cast<size_t>(n);
        }
        return true;
    }

    // Receives exactly `len` bytes; returns false on error / EOF.
    static bool recv_all(sock_t s, uint8_t* data, size_t len) noexcept {
        while (len > 0) {
            int chunk = (len > static_cast<size_t>(INT_MAX))
                            ? INT_MAX
                            : static_cast<int>(len);
            auto n = ::recv(s, reinterpret_cast<char*>(data), chunk, 0);
            if (n <= 0) return false;
            data += n;
            len  -= static_cast<size_t>(n);
        }
        return true;
    }

    // LE helpers (same as ReplFraming anonymous namespace, duplicated here to avoid exposing them)
    static inline uint16_t get_u16(const uint8_t* b) noexcept {
        return static_cast<uint16_t>(b[0]) | (static_cast<uint16_t>(b[1]) << 8);
    }
    static inline uint64_t get_u64(const uint8_t* b) noexcept {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(b[i]) << (8 * i);
        return v;
    }

} // anonymous namespace

namespace akkaradb::engine::cluster {

    // ============================================================================
    // Internal data structures
    // ============================================================================

    /// A fully-encoded wire message together with its WAL seq number.
    struct BufferedEntry {
        uint64_t             seq;  ///< 0 for blob messages (blob_id stored in header)
        std::vector<uint8_t> wire; ///< Complete on-wire message bytes
    };

    /// Per-replica connection state.
    struct ReplicaState {
        sock_t              sock    { BAD_SOCK };
        uint64_t            node_id { 0 };
        std::atomic<uint64_t> last_acked_seq { 0 };
        std::atomic<bool>   dead    { false };

        std::mutex              queue_mtx;
        std::condition_variable queue_cv;
        std::deque<std::vector<uint8_t>> send_queue;

        // Threads stored here; joined by close()
        std::thread send_thr;
        std::thread recv_thr;

        ReplicaState() = default;
        ~ReplicaState() {
            if (sock_ok(sock)) close_sock(sock);
        }
        ReplicaState(const ReplicaState&)            = delete;
        ReplicaState& operator=(const ReplicaState&) = delete;
    };

    // ============================================================================
    // Impl
    // ============================================================================

    struct ReplicationServer::Impl {
        uint16_t                  repl_port;
        uint64_t                  self_node_id;
        std::function<uint64_t()> get_current_seq;
        bool                      sync_mode;

        sock_t            listen_sock { BAD_SOCK };
        std::atomic<bool> running     { false };
        std::thread       accept_thr;

        // ship_mtx guards both entry_buf and replicas vector.
        // Lock order: ship_mtx → replica.queue_mtx   (never reversed)
        mutable std::mutex ship_mtx;
        std::deque<BufferedEntry>                        entry_buf;
        std::vector<std::shared_ptr<ReplicaState>>       replicas;

        // ack waiting (sync_mode)
        std::mutex              ack_mtx;
        std::condition_variable ack_cv;

        // Thread lifecycle counter for clean shutdown
        std::atomic<int>        active_threads { 0 };
        std::mutex              shutdown_mtx;
        std::condition_variable shutdown_cv;

        // ── helpers ──────────────────────────────────────────────────────────

        bool setup_listen() noexcept;
        void accept_loop();
        bool do_handshake(sock_t s, uint64_t& out_node_id, uint64_t& out_last_seq) noexcept;

        void send_loop(std::shared_ptr<ReplicaState> rs);
        void recv_loop(std::shared_ptr<ReplicaState> rs);

        void remove_dead_replica(ReplicaState* ptr);
        void wait_for_acks(uint64_t seq);

        void thread_enter() { active_threads.fetch_add(1, std::memory_order_relaxed); }
        void thread_exit()  {
            if (active_threads.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                shutdown_cv.notify_all();
            }
        }
    };

    // ── setup_listen ─────────────────────────────────────────────────────────

    bool ReplicationServer::Impl::setup_listen() noexcept {
        net_init();

        listen_sock = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (!sock_ok(listen_sock)) return false;

        int reuse = 1;
        ::setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&reuse), sizeof(reuse));

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(repl_port);

        if (::bind(listen_sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            close_sock(listen_sock); listen_sock = BAD_SOCK; return false;
        }
        if (::listen(listen_sock, 8) != 0) {
            close_sock(listen_sock); listen_sock = BAD_SOCK; return false;
        }
        return true;
    }

    // ── do_handshake ─────────────────────────────────────────────────────────

    bool ReplicationServer::Impl::do_handshake(
            sock_t s, uint64_t& out_node_id, uint64_t& out_last_seq) noexcept {
        // 1. Read ReplClientHello
        uint8_t hello_buf[ReplClientHello::SIZE];
        if (!recv_all(s, hello_buf, ReplClientHello::SIZE)) return false;

        auto hello = ReplClientHello::deserialize(hello_buf);
        if (!hello.verify_magic()) return false;

        out_node_id  = hello.node_id;
        out_last_seq = hello.last_seq;

        // 2. Send ReplServerHello
        ReplServerHello srv{};
        srv.magic       = ReplServerHello::MAGIC;
        srv.node_id     = self_node_id;
        srv.current_seq = get_current_seq();
        srv.flags       = 0;

        uint8_t srv_buf[ReplServerHello::SIZE];
        srv.serialize(srv_buf);
        return send_all(s, srv_buf, ReplServerHello::SIZE);
    }

    // ── accept_loop ──────────────────────────────────────────────────────────

    void ReplicationServer::Impl::accept_loop() {
        thread_enter();

        while (running.load(std::memory_order_relaxed)) {
            sock_t client = ::accept(listen_sock, nullptr, nullptr);
            if (!sock_ok(client)) break; // listen socket was closed

            uint64_t node_id = 0, last_seq = 0;
            if (!do_handshake(client, node_id, last_seq)) {
                close_sock(client);
                continue;
            }

            auto rs       = std::make_shared<ReplicaState>();
            rs->sock      = client;
            rs->node_id   = node_id;
            rs->last_acked_seq.store(last_seq, std::memory_order_relaxed);

            {
                // Hold ship_mtx so ship() cannot enqueue to replicas while we
                // snapshot the entry_buf and add rs to the replicas list.
                // This guarantees no gaps and no duplicates.
                std::lock_guard lk(ship_mtx);

                // Pre-populate the send queue with catch-up entries
                for (auto& e : entry_buf) {
                    if (e.seq == 0 || e.seq > last_seq) {
                        // seq==0 → blob message (always replay; idempotent on Replica)
                        // seq>last_seq → missed WAL entry
                        rs->send_queue.push_back(e.wire);
                    }
                }
                replicas.push_back(rs);
            }

            // Start per-replica threads
            rs->send_thr = std::thread([this, rs]{ send_loop(rs); });
            rs->recv_thr = std::thread([this, rs]{ recv_loop(rs); });
        }

        thread_exit();
    }

    // ── send_loop ────────────────────────────────────────────────────────────

    void ReplicationServer::Impl::send_loop(std::shared_ptr<ReplicaState> rs) {
        thread_enter();

        while (!rs->dead.load(std::memory_order_relaxed)) {
            std::vector<uint8_t> wire;

            {
                std::unique_lock lk(rs->queue_mtx);
                rs->queue_cv.wait(lk, [&]{
                    return !rs->send_queue.empty()
                        || rs->dead.load(std::memory_order_relaxed);
                });

                if (rs->dead.load(std::memory_order_relaxed) && rs->send_queue.empty())
                    break;

                wire = std::move(rs->send_queue.front());
                rs->send_queue.pop_front();
            }

            if (!send_all(rs->sock, wire.data(), wire.size())) {
                rs->dead.store(true, std::memory_order_release);
                close_sock(rs->sock);
                rs->sock = BAD_SOCK;
                break;
            }
        }

        remove_dead_replica(rs.get());
        thread_exit();
    }

    // ── recv_loop ────────────────────────────────────────────────────────────

    void ReplicationServer::Impl::recv_loop(std::shared_ptr<ReplicaState> rs) {
        thread_enter();

        while (!rs->dead.load(std::memory_order_relaxed)) {
            // ReplAck is 9 bytes: [msg_type:u8=0x03][seq:u64]
            uint8_t buf[ReplAck::SIZE];
            if (!recv_all(rs->sock, buf, ReplAck::SIZE)) break;

            if (buf[0] != static_cast<uint8_t>(ReplMsgType::Ack)) break; // unexpected

            uint64_t acked_seq = get_u64(buf + 1);
            rs->last_acked_seq.store(acked_seq, std::memory_order_release);

            // Wake up anyone waiting in wait_for_acks()
            ack_cv.notify_all();
        }

        // Signal send_loop to exit
        rs->dead.store(true, std::memory_order_release);
        if (sock_ok(rs->sock)) {
            close_sock(rs->sock);
            rs->sock = BAD_SOCK;
        }
        rs->queue_cv.notify_all();

        thread_exit();
    }

    // ── remove_dead_replica ──────────────────────────────────────────────────

    void ReplicationServer::Impl::remove_dead_replica(ReplicaState* ptr) {
        std::lock_guard lk(ship_mtx);
        replicas.erase(
            std::remove_if(replicas.begin(), replicas.end(),
                           [ptr](const auto& r){ return r.get() == ptr; }),
            replicas.end());
    }

    // ── wait_for_acks ────────────────────────────────────────────────────────

    void ReplicationServer::Impl::wait_for_acks(uint64_t seq) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            bool all_acked = true;
            {
                std::lock_guard lk(ship_mtx);
                for (auto& rs : replicas) {
                    if (!rs->dead.load(std::memory_order_acquire)
                            && rs->last_acked_seq.load(std::memory_order_acquire) < seq) {
                        all_acked = false;
                        break;
                    }
                }
            }
            if (all_acked) return;

            // Wait for a notification from recv_loop (with 50 ms timeout for safety)
            std::unique_lock lk(ack_mtx);
            ack_cv.wait_for(lk, std::chrono::milliseconds(50));
        }
        // Timeout: continue anyway (durability degraded but not blocked)
    }

    // ============================================================================
    // Public API
    // ============================================================================

    ReplicationServer::ReplicationServer(std::unique_ptr<Impl> impl)
        : impl_(std::move(impl)) {}

    ReplicationServer::~ReplicationServer() { close(); }

    std::unique_ptr<ReplicationServer> ReplicationServer::create(
            uint16_t                  repl_port,
            uint64_t                  self_node_id,
            std::function<uint64_t()> get_current_seq,
            bool                      sync_mode) {
        auto impl            = std::make_unique<Impl>();
        impl->repl_port      = repl_port;
        impl->self_node_id   = self_node_id;
        impl->get_current_seq= std::move(get_current_seq);
        impl->sync_mode      = sync_mode;
        return std::unique_ptr<ReplicationServer>(new ReplicationServer(std::move(impl)));
    }

    void ReplicationServer::start() {
        if (!impl_->setup_listen()) return;
        impl_->running.store(true, std::memory_order_relaxed);
        impl_->accept_thr = std::thread([this]{ impl_->accept_loop(); });
    }

    void ReplicationServer::ship(
            uint64_t                 seq,
            wal::WalEntryType        wal_type,
            std::span<const uint8_t> key,
            std::span<const uint8_t> val) {
        auto wire = encode_repl_entry(seq, wal_type, key, val);

        {
            std::lock_guard lk(impl_->ship_mtx);

            // Buffer for catch-up
            impl_->entry_buf.push_back({seq, wire});
            while (impl_->entry_buf.size() > ENTRY_BUFFER_SIZE)
                impl_->entry_buf.pop_front();

            // Fan-out to connected replicas
            for (auto& rs : impl_->replicas) {
                if (!rs->dead.load(std::memory_order_relaxed)) {
                    std::lock_guard qlk(rs->queue_mtx);
                    rs->send_queue.push_back(wire);
                    rs->queue_cv.notify_one();
                }
            }
        }

        if (impl_->sync_mode) impl_->wait_for_acks(seq);
    }

    void ReplicationServer::ship_blob(
            uint64_t                 blob_id,
            std::span<const uint8_t> content) {
        auto wire = encode_repl_blob_put(blob_id, content);

        std::lock_guard lk(impl_->ship_mtx);

        // Blob messages buffered with seq=0 (always replayed on reconnect)
        impl_->entry_buf.push_back({0u, wire});
        while (impl_->entry_buf.size() > ENTRY_BUFFER_SIZE)
            impl_->entry_buf.pop_front();

        for (auto& rs : impl_->replicas) {
            if (!rs->dead.load(std::memory_order_relaxed)) {
                std::lock_guard qlk(rs->queue_mtx);
                rs->send_queue.push_back(wire);
                rs->queue_cv.notify_one();
            }
        }
        // No ack waiting for blobs (acks only for WAL entries)
    }

    size_t ReplicationServer::replica_count() const noexcept {
        std::lock_guard lk(impl_->ship_mtx);
        return impl_->replicas.size();
    }

    void ReplicationServer::close() {
        if (!impl_) return;

        impl_->running.store(false, std::memory_order_relaxed);

        // Unblock accept()
        if (sock_ok(impl_->listen_sock)) {
            close_sock(impl_->listen_sock);
            impl_->listen_sock = BAD_SOCK;
        }

        // Signal all replicas to die
        std::vector<std::shared_ptr<ReplicaState>> to_join;
        {
            std::lock_guard lk(impl_->ship_mtx);
            to_join = impl_->replicas;
            impl_->replicas.clear();
        }
        for (auto& rs : to_join) {
            rs->dead.store(true, std::memory_order_release);
            if (sock_ok(rs->sock)) {
                close_sock(rs->sock);
                rs->sock = BAD_SOCK;
            }
            rs->queue_cv.notify_all();
        }

        // Wait for all per-replica threads to finish
        // (active_threads counts accept_thr + per-replica send/recv threads)
        {
            std::unique_lock lk(impl_->shutdown_mtx);
            impl_->shutdown_cv.wait_for(lk, std::chrono::seconds(10), [&]{
                return impl_->active_threads.load(std::memory_order_acquire) == 0;
            });
        }

        // join() per-replica threads
        for (auto& rs : to_join) {
            if (rs->send_thr.joinable()) rs->send_thr.join();
            if (rs->recv_thr.joinable()) rs->recv_thr.join();
        }
        if (impl_->accept_thr.joinable()) impl_->accept_thr.join();
    }

} // namespace akkaradb::engine::cluster
