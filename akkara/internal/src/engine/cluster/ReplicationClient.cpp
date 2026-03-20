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

// internal/src/engine/cluster/ReplicationClient.cpp
#include "engine/cluster/ReplicationClient.hpp"
#include "engine/cluster/ReplFraming.hpp"
#include "core/CRC32C.hpp"

#include <atomic>
#include <chrono>
#include <climits>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

// ============================================================================
// Platform TCP abstractions (same pattern as ReplicationServer.cpp)
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
#  include <netdb.h>
#  include <unistd.h>
   using sock_t = int;
   static constexpr sock_t BAD_SOCK = -1;
   static inline void close_sock(sock_t s) noexcept { ::close(s); }
   static inline bool  sock_ok(sock_t s)   noexcept { return s >= 0; }
#endif

namespace {

    static void net_init() noexcept {
#ifdef _WIN32
        static std::once_flag once;
        std::call_once(once, []{
            WSADATA wd{};
            WSAStartup(MAKEWORD(2, 2), &wd);
        });
#endif
    }

    /// Attempts to open a TCP connection; returns BAD_SOCK on failure.
    static sock_t tcp_connect(const std::string& host, uint16_t port) noexcept {
        net_init();

        addrinfo hints{};
        hints.ai_family   = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;

        char port_str[8];
#ifdef _WIN32
        _itoa_s(port, port_str, sizeof(port_str), 10);
#else
        std::snprintf(port_str, sizeof(port_str), "%u", port);
#endif

        addrinfo* res = nullptr;
        if (::getaddrinfo(host.c_str(), port_str, &hints, &res) != 0) return BAD_SOCK;

        sock_t s = BAD_SOCK;
        for (addrinfo* p = res; p; p = p->ai_next) {
            s = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
            if (!sock_ok(s)) continue;
            if (::connect(s, p->ai_addr, static_cast<int>(p->ai_addrlen)) == 0) break;
            close_sock(s); s = BAD_SOCK;
        }
        ::freeaddrinfo(res);
        return s;
    }

} // anonymous namespace

namespace akkaradb::engine::cluster {

    // ============================================================================
    // Impl
    // ============================================================================

    struct ReplicationClient::Impl {
        std::string                          primary_host;
        uint16_t                             primary_port;
        uint64_t                             self_node_id;
        ReplicationClient::GetLastSeqFn      get_last_seq;
        ReplicationClient::BlobCallback      blob_cb;
        ReplicationClient::ApplyCallback     apply_cb;

        // TLS configuration — shared context created in start()
        core::TlsConfig tls_cfg;
        std::unique_ptr<core::TlsContext> tls_ctx;

        std::atomic<bool>   running    { false };
        std::atomic<bool>   is_connected { false };
        sock_t              sock       { BAD_SOCK };
        mutable std::mutex  sock_mtx;   // guards `sock` for close()

        std::thread recv_thr;

        // ── helpers ──────────────────────────────────────────────────────────

        bool do_handshake(core::TlsStream& stream) noexcept;
        void recv_loop();
        bool handle_entry(core::TlsStream& stream);
        bool handle_blob(core::TlsStream& stream);
        bool send_ack(core::TlsStream& stream, uint64_t seq) noexcept;
    };

    // ── do_handshake ─────────────────────────────────────────────────────────

    bool ReplicationClient::Impl::do_handshake(core::TlsStream& stream) noexcept {
        // 1. Send ReplClientHello
        ReplClientHello hello{};
        hello.magic    = ReplClientHello::MAGIC;
        hello.node_id  = self_node_id;
        hello.last_seq = get_last_seq();
        hello.flags    = 0;

        uint8_t buf[ReplClientHello::SIZE];
        hello.serialize(buf);
        if (!stream.send_all(buf, ReplClientHello::SIZE)) return false;

        // 2. Read ReplServerHello
        uint8_t srv_buf[ReplServerHello::SIZE];
        if (!stream.recv_all(srv_buf, ReplServerHello::SIZE)) return false;

        auto srv = ReplServerHello::deserialize(srv_buf);
        return srv.verify_magic();
        // current_seq from ServerHello could be used for progress reporting
        // but is not strictly needed by the Replica logic.
    }

    // ── send_ack ─────────────────────────────────────────────────────────────

    bool ReplicationClient::Impl::send_ack(core::TlsStream& stream, uint64_t seq) noexcept {
        auto wire = encode_repl_ack(seq);
        return stream.send_all(wire.data(), wire.size());
    }

    // ── handle_entry ─────────────────────────────────────────────────────────

    bool ReplicationClient::Impl::handle_entry(core::TlsStream& stream) {
        // Already consumed msg_type byte; read the rest of the header.
        // Total header = 20 bytes; we read 19 remaining bytes (1 already read as msg_type).
        uint8_t hdr_rest[ReplEntryHeader::SIZE - 1];
        if (!stream.recv_all(hdr_rest, sizeof(hdr_rest))) return false;

        // Reconstruct full header buffer
        uint8_t full_hdr[ReplEntryHeader::SIZE];
        full_hdr[0] = static_cast<uint8_t>(ReplMsgType::Entry);
        std::memcpy(full_hdr + 1, hdr_rest, sizeof(hdr_rest));

        auto hdr = ReplEntryHeader::deserialize(full_hdr);

        // Read key + val
        std::vector<uint8_t> key(hdr.key_len);
        std::vector<uint8_t> val(hdr.val_len);
        if (hdr.key_len > 0 && !stream.recv_all(key.data(), hdr.key_len)) return false;
        if (hdr.val_len > 0 && !stream.recv_all(val.data(), hdr.val_len)) return false;

        // Verify CRC32C
        std::vector<uint8_t> combined(key.size() + val.size());
        if (!key.empty()) std::memcpy(combined.data(), key.data(), key.size());
        if (!val.empty()) std::memcpy(combined.data() + key.size(), val.data(), val.size());
        uint32_t expected = core::CRC32C::compute(combined.data(), combined.size());
        if (expected != hdr.crc32c) return false; // CRC mismatch → connection error

        // Apply
        if (apply_cb) {
            apply_cb(
                hdr.seq,
                static_cast<wal::WalEntryType>(hdr.wal_type),
                std::span<const uint8_t>(key.data(), key.size()),
                std::span<const uint8_t>(val.data(), val.size())
            );
        }

        // ACK
        return send_ack(stream, hdr.seq);
    }

    // ── handle_blob ──────────────────────────────────────────────────────────

    bool ReplicationClient::Impl::handle_blob(core::TlsStream& stream) {
        // Already consumed msg_type byte; read remaining header bytes.
        uint8_t hdr_rest[ReplBlobPutHeader::SIZE - 1];
        if (!stream.recv_all(hdr_rest, sizeof(hdr_rest))) return false;

        uint8_t full_hdr[ReplBlobPutHeader::SIZE];
        full_hdr[0] = static_cast<uint8_t>(ReplMsgType::BlobPut);
        std::memcpy(full_hdr + 1, hdr_rest, sizeof(hdr_rest));

        auto hdr = ReplBlobPutHeader::deserialize(full_hdr);

        // Sanity-check content length (reject obviously bad messages)
        constexpr uint64_t MAX_BLOB = 1ULL << 32; // 4 GiB hard cap
        if (hdr.content_len > MAX_BLOB) return false;

        std::vector<uint8_t> content(static_cast<size_t>(hdr.content_len));
        if (hdr.content_len > 0 && !stream.recv_all(content.data(), content.size())) return false;

        // Verify CRC32C
        uint32_t expected = core::CRC32C::compute(content.data(), content.size());
        if (expected != hdr.crc32c) return false;

        // Deliver
        if (blob_cb) {
            blob_cb(hdr.blob_id,
                    std::span<const uint8_t>(content.data(), content.size()));
        }
        // No ACK for blob messages
        return true;
    }

    // ── recv_loop ────────────────────────────────────────────────────────────

    void ReplicationClient::Impl::recv_loop() {
        while (running.load(std::memory_order_relaxed)) {
            // ── connect ──────────────────────────────────────────────────────
            sock_t s = tcp_connect(primary_host, primary_port);
            if (!sock_ok(s)) {
                // Retry after 500 ms
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                continue;
            }

            // ── TLS wrap ─────────────────────────────────────────────────────
            std::unique_ptr<core::TlsStream> stream;
            try { stream = core::TlsStream::client_wrap(s, primary_host.c_str(), tls_ctx.get()); }
            catch (...) {
                close_sock(s);
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                continue;
            }

            // ── handshake ────────────────────────────────────────────────────
            if (!do_handshake(*stream)) {
                stream->shutdown();
                close_sock(s);
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                continue;
            }

            {
                std::lock_guard lk(sock_mtx);
                sock = s;
            }
            is_connected.store(true, std::memory_order_release);

            // ── receive messages ─────────────────────────────────────────────
            bool ok = true;
            while (ok && running.load(std::memory_order_relaxed)) {
                uint8_t type_byte = 0;
                if (!stream->recv_all(&type_byte, 1)) {
                    ok = false;
                    break;
                }

                switch (static_cast<ReplMsgType>(type_byte)) {
                    case ReplMsgType::Entry: ok = handle_entry(*stream);
                        break;
                    case ReplMsgType::BlobPut: ok = handle_blob(*stream);
                        break;
                    default:
                        ok = false; // Unknown message type
                        break;
                }
            }

            // ── disconnect ───────────────────────────────────────────────────
            is_connected.store(false, std::memory_order_release);
            stream->shutdown();
            {
                std::lock_guard lk(sock_mtx);
                close_sock(s);
                sock = BAD_SOCK;
            }

            if (running.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }
    }

    // ============================================================================
    // Public API
    // ============================================================================

    ReplicationClient::ReplicationClient(std::unique_ptr<Impl> impl)
        : impl_(std::move(impl)) {}

    ReplicationClient::~ReplicationClient() { close(); }

    std::unique_ptr<ReplicationClient> ReplicationClient::create(
            std::string     primary_host,
            uint16_t        primary_repl_port,
            uint64_t        self_node_id,
            GetLastSeqFn get_last_seq,
            core::TlsConfig tls
    ) {
        auto impl              = std::make_unique<Impl>();
        impl->primary_host     = std::move(primary_host);
        impl->primary_port     = primary_repl_port;
        impl->self_node_id     = self_node_id;
        impl->get_last_seq     = std::move(get_last_seq);
        impl->tls_cfg = std::move(tls);
        return std::unique_ptr<ReplicationClient>(new ReplicationClient(std::move(impl)));
    }

    void ReplicationClient::set_blob_callback(BlobCallback cb) {
        impl_->blob_cb = std::move(cb);
    }

    void ReplicationClient::set_apply_callback(ApplyCallback cb) {
        impl_->apply_cb = std::move(cb);
    }

    void ReplicationClient::start() {
        // Initialise TLS context if enabled
        impl_->tls_ctx = core::TlsContext::make_client(impl_->tls_cfg);

        impl_->running.store(true, std::memory_order_relaxed);
        impl_->recv_thr = std::thread([this]{ impl_->recv_loop(); });
    }

    void ReplicationClient::close() {
        if (!impl_) return;
        impl_->running.store(false, std::memory_order_relaxed);

        // Wake up blocked recv_all by closing the socket
        {
            std::lock_guard lk(impl_->sock_mtx);
            if (sock_ok(impl_->sock)) {
                close_sock(impl_->sock);
                impl_->sock = BAD_SOCK;
            }
        }
        impl_->is_connected.store(false, std::memory_order_release);

        if (impl_->recv_thr.joinable()) impl_->recv_thr.join();
    }

    bool ReplicationClient::connected() const noexcept {
        return impl_->is_connected.load(std::memory_order_acquire);
    }

} // namespace akkaradb::engine::cluster
