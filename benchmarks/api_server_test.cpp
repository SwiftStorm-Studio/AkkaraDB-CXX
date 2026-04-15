/*
 * AkkaraDB — API Server smoke test / interactive runner
 *
 * Starts an AkkEngine with the public API server (HTTP and/or TCP),
 * pre-loads a handful of test key/value pairs, then blocks until you
 * type "stop" (or Ctrl-C).
 *
 * Usage:
 *   akkaradb_api_server_test [--http] [--tcp] [--both]
 *                            [--http-port N] [--tcp-port N]
 *
 *   --http        HTTP/REST only  (default)
 *   --tcp         Binary TCP only
 *   --both        Both backends simultaneously
 *   --http-port N HTTP listen port  (default 7070)
 *   --tcp-port  N TCP  listen port  (default 7071)
 *
 * Example curl commands (HTTP mode):
 *   PUT:    curl -s -X POST "http://localhost:7070/v1/put?key=hello" --data "world"
 *   GET:    curl -s "http://localhost:7070/v1/get?key=hello"
 *   DELETE: curl -s -X DELETE "http://localhost:7070/v1/remove?key=hello"
 *   PING:   curl -s "http://localhost:7070/v1/ping"
 */

#include "engine/AkkEngine.hpp"

#include <cstring>
#include <format>
#include <iostream>
#include <span>
#include <string>
#include <string_view>

using namespace std::string_view_literals;

using namespace akkaradb::engine;

// ── helpers ───────────────────────────────────────────────────────────────────

static std::span<const uint8_t> as_bytes(std::string_view sv) {
    return { reinterpret_cast<const uint8_t*>(sv.data()), sv.size() };
}

static std::string to_str(const std::vector<uint8_t>& v) {
    return { reinterpret_cast<const char*>(v.data()), v.size() };
}

static bool arg_flag(int argc, char** argv, const char* flag) {
    for (int i = 1; i < argc; ++i)
        if (std::strcmp(argv[i], flag) == 0) return true;
    return false;
}

static uint16_t arg_uint16(int argc, char** argv, const char* flag, uint16_t def) {
    for (int i = 1; i + 1 < argc; ++i)
        if (std::strcmp(argv[i], flag) == 0)
            return static_cast<uint16_t>(std::stoul(argv[i + 1]));
    return def;
}

// ── seed data ─────────────────────────────────────────────────────────────────

static void seed(AkkEngine& eng) {
    struct KV { std::string_view k, v; };
    static constexpr KV PAIRS[] = {
        { "hello",              "world"                         },
        { "akkaradb",           "blazing fast KV store"         },
        { "version",            "4.0.0"                         },
        { "author",             "Swift Storm Studio"             },
        { "greeting",           "Konnichiwa!"                   },
        { "binary_key\x00"sv,   "value with embedded null byte" }, // sv: includes \x00 (length=11)
    };
    for (const auto& [k, v] : PAIRS)
        eng.put(as_bytes(k), as_bytes(v));

    std::printf("  Seeded %zu key/value pairs.\n", std::size(PAIRS));
}

// ── main ──────────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
    // ── Parse arguments ───────────────────────────────────────────────────────
    const bool want_http = arg_flag(argc, argv, "--http");
    const bool want_tcp  = arg_flag(argc, argv, "--tcp");
    const bool want_both = arg_flag(argc, argv, "--both");

    const uint16_t http_port = arg_uint16(argc, argv, "--http-port", 7070);
    const uint16_t tcp_port  = arg_uint16(argc, argv, "--tcp-port",  7071);

    // Default: HTTP only
    const bool use_http = want_both || want_http || (!want_tcp);
    const bool use_tcp  = want_both || want_tcp;

    // ── Engine options ────────────────────────────────────────────────────────
    AkkEngineOptions opts;
    opts.wal_enabled = false;   // memory-only for the test

    opts.api.enabled = true;
    opts.api.backends.clear();
    if (use_http) opts.api.backends.push_back(AkkEngineOptions::ApiBackend::Http);
    if (use_tcp)  opts.api.backends.push_back(AkkEngineOptions::ApiBackend::Tcp);
    opts.api.http_port = http_port;
    opts.api.tcp_port  = tcp_port;

    // ── Start engine + API server ─────────────────────────────────────────────
    std::printf("\n=== AkkaraDB API Server Test ===\n\n");

    std::unique_ptr<AkkEngine> eng;
    try {
        eng = AkkEngine::open(opts);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "ERROR: Failed to start engine: %s\n", e.what());
        return 1;
    }

    // ── Seed test data ────────────────────────────────────────────────────────
    seed(*eng);

    // ── Print endpoint info ───────────────────────────────────────────────────
    std::printf("\n");
    if (use_http) {
        std::printf("  [HTTP]  http://localhost:%u\n", http_port);
        std::printf("\n");
        std::printf("  curl -s -X POST \"http://localhost:%u/v1/put?key=foo\" --data \"bar\"\n", http_port);
        std::printf("  curl -s \"http://localhost:%u/v1/get?key=hello\"\n", http_port);
        std::printf("  curl -s -X DELETE \"http://localhost:%u/v1/remove?key=hello\"\n", http_port);
        std::printf("  curl -s \"http://localhost:%u/v1/ping\"\n", http_port);
    }
    if (use_tcp) {
        std::printf("  [TCP]   localhost:%u  (binary AkkaraDB protocol)\n", tcp_port);
    }
    std::printf("\n");

    // ── Verify seed data is accessible ───────────────────────────────────────
    {
        std::vector<uint8_t> val;
        if (eng->get_into(as_bytes("hello"), val)) {
            std::printf("  Verify: GET hello = \"%s\"  ✓\n", to_str(val).c_str());
        } else {
            std::printf("  Verify: GET hello = (not found)  ✗\n");
        }
    }

    // ── Block until "stop" ────────────────────────────────────────────────────
    std::printf("\n  Type \"stop\" and press Enter to shut down.\n\n");

    std::string line;
    while (std::getline(std::cin, line)) {
        if (line == "stop") break;
        std::printf("  (type \"stop\" to exit)\n");
    }

    // ── Graceful shutdown ─────────────────────────────────────────────────────
    std::printf("\n  Shutting down...\n");
    eng->close();
    std::printf("  Done.\n\n");
    return 0;
}
