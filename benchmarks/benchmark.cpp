/*
 * AkkaraDB — Engine correctness tests + throughput benchmark
 *
 * Covers:
 *   1. Memory-only mode  (wal_enabled=false)
 *   2. WAL persistence   (wal_enabled=true, Sync)
 *   3. BLOB path         (value >= 16 KiB threshold)
 *   4. SyncMode::Off     (wal_enabled=true overridden to memory-only)
 *   5. Throughput        (write + read, in-memory)
 *
 * No external test framework — failures abort via CHECK().
 */

#include "engine/AkkEngine.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <format>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace fs = std::filesystem;
using namespace akkaradb::engine;

// ============================================================================
// Helpers
// ============================================================================

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(expr)                                                         \
    do {                                                                    \
        if (!(expr)) {                                                      \
            std::fprintf(stderr, "  FAIL  %s:%d  %s\n",                    \
                         __FILE__, __LINE__, #expr);                        \
            ++g_fail;                                                       \
        } else {                                                            \
            ++g_pass;                                                       \
        }                                                                   \
    } while (0)

#define CHECK_EQ(a, b)                                                      \
    do {                                                                    \
        if ((a) != (b)) {                                                   \
            std::fprintf(stderr, "  FAIL  %s:%d  %s != %s\n",              \
                         __FILE__, __LINE__, #a, #b);                       \
            ++g_fail;                                                       \
        } else {                                                            \
            ++g_pass;                                                       \
        }                                                                   \
    } while (0)

// ── Span helpers ─────────────────────────────────────────────────────────────

static std::span<const uint8_t> as_span(std::string_view sv) {
    return {reinterpret_cast<const uint8_t*>(sv.data()), sv.size()};
}

static std::span<const uint8_t> as_span(const std::vector<uint8_t>& v) {
    return {v.data(), v.size()};
}

static std::string to_str(const std::vector<uint8_t>& v) {
    return {reinterpret_cast<const char*>(v.data()), v.size()};
}

// ── Temp directory (unique per run) ──────────────────────────────────────────

static fs::path make_temp_dir(std::string_view name) {
    auto base = fs::temp_directory_path() / "akkaradb_bench";
    auto dir  = base / name;
    fs::remove_all(dir);
    fs::create_directories(dir);
    return dir;
}

// ── Section banner ────────────────────────────────────────────────────────────

static void section(const char* title) {
    std::printf("\n── %s ──\n", title);
}

// ── Timing helper ─────────────────────────────────────────────────────────────

using Clock = std::chrono::steady_clock;

static double elapsed_ms(Clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

// ============================================================================
// 1. Memory-only mode
// ============================================================================

static void test_memory_basic() {
    section("Memory-only: basic put / get / remove");

    AkkEngineOptions opts;
    opts.wal_enabled = false;
    auto eng = AkkEngine::open(opts);

    // put + get
    eng->put(as_span("hello"), as_span("world"));
    auto v1 = eng->get(as_span("hello"));
    CHECK(v1.has_value());
    CHECK(to_str(*v1) == "world");

    // overwrite
    eng->put(as_span("hello"), as_span("earth"));
    auto v2 = eng->get(as_span("hello"));
    CHECK(v2.has_value());
    CHECK(to_str(*v2) == "earth");

    // missing key
    auto v3 = eng->get(as_span("missing"));
    CHECK(!v3.has_value());

    // remove → tombstone
    eng->remove(as_span("hello"));
    auto v4 = eng->get(as_span("hello"));
    CHECK(!v4.has_value());

    // remove non-existent key (must not crash)
    eng->remove(as_span("ghost"));

    eng->close();
    std::printf("  ok\n");
}

static void test_memory_many_keys() {
    section("Memory-only: 1 000 keys");

    AkkEngineOptions opts;
    opts.wal_enabled = false;
    auto eng = AkkEngine::open(opts);

    constexpr int N = 1000;
    for (int i = 0; i < N; ++i) {
        auto k = std::format("key_{:06d}", i);
        auto v = std::format("val_{:06d}", i);
        eng->put(as_span(k), as_span(v));
    }

    int found = 0;
    for (int i = 0; i < N; ++i) {
        auto k   = std::format("key_{:06d}", i);
        auto exp = std::format("val_{:06d}", i);
        auto got = eng->get(as_span(k));
        if (got && to_str(*got) == exp) ++found;
    }
    CHECK_EQ(found, N);

    // remove half, verify
    for (int i = 0; i < N; i += 2) {
        auto k = std::format("key_{:06d}", i);
        eng->remove(as_span(k));
    }
    int alive = 0;
    for (int i = 0; i < N; ++i) {
        auto k = std::format("key_{:06d}", i);
        if (eng->get(as_span(k)).has_value()) ++alive;
    }
    CHECK_EQ(alive, N / 2);

    eng->close();
    std::printf("  ok\n");
}

// ============================================================================
// 2. SyncMode::Off  (wal_enabled=true is ignored → in-memory)
// ============================================================================

static void test_sync_off() {
    section("SyncMode::Off overrides wal_enabled");

    auto dir = make_temp_dir("sync_off");

    AkkEngineOptions opts;
    opts.data_dir   = dir;
    opts.wal_enabled = true;           // should be overridden
    opts.sync_mode  = SyncMode::Off;   // → memory-only

    auto eng = AkkEngine::open(opts);
    eng->put(as_span("k"), as_span("v"));
    CHECK(eng->get(as_span("k")).has_value());

    // No WAL directory should have been created
    CHECK(!fs::exists(dir / "wal"));
    eng->close();

    fs::remove_all(dir);
    std::printf("  ok\n");
}

// ============================================================================
// 3. WAL persistence — put, close, reopen, verify
// ============================================================================

static void test_wal_recovery() {
    section("WAL: crash-recovery (close + reopen)");

    auto dir = make_temp_dir("wal_recovery");

    // ── Write phase ───────────────────────────────────────────────────────────
    {
        AkkEngineOptions opts;
        opts.data_dir   = dir;
        opts.wal_enabled = true;
        opts.sync_mode  = SyncMode::Sync;
        auto eng = AkkEngine::open(opts);

        for (int i = 0; i < 100; ++i) {
            auto k = std::format("rk_{:04d}", i);
            auto v = std::format("rv_{:04d}", i);
            eng->put(as_span(k), as_span(v));
        }
        // Remove some
        for (int i = 0; i < 100; i += 3) {
            auto k = std::format("rk_{:04d}", i);
            eng->remove(as_span(k));
        }
        eng->close();
    }

    // ── Recovery phase ────────────────────────────────────────────────────────
    {
        AkkEngineOptions opts;
        opts.data_dir   = dir;
        opts.wal_enabled = true;
        opts.sync_mode  = SyncMode::Sync;
        auto eng = AkkEngine::open(opts);

        int ok = 0, missing = 0, deleted = 0;
        for (int i = 0; i < 100; ++i) {
            auto k   = std::format("rk_{:04d}", i);
            auto exp = std::format("rv_{:04d}", i);
            auto got = eng->get(as_span(k));

            if (i % 3 == 0) {
                // was removed → should be absent
                if (!got.has_value()) ++deleted;
            } else {
                if (got && to_str(*got) == exp) ++ok;
                else ++missing;
            }
        }

        // 100 keys, i%3==0 → 34 deleted (0,3,6,...,99), 66 alive
        CHECK_EQ(deleted, 34);
        CHECK_EQ(ok,      66);
        CHECK_EQ(missing,  0);

        eng->close();
    }

    fs::remove_all(dir);
    std::printf("  ok\n");
}

// ============================================================================
// 4. WAL: force_sync + Async mode
// ============================================================================

static void test_wal_async() {
    section("WAL: Async mode + force_sync");

    auto dir = make_temp_dir("wal_async");

    AkkEngineOptions opts;
    opts.data_dir   = dir;
    opts.wal_enabled = true;
    opts.sync_mode  = SyncMode::Async;
    auto eng = AkkEngine::open(opts);

    for (int i = 0; i < 50; ++i) {
        auto k = std::format("ak_{:04d}", i);
        auto v = std::format("av_{:04d}", i);
        eng->put(as_span(k), as_span(v));
    }
    eng->force_sync(); // explicitly flush

    int found = 0;
    for (int i = 0; i < 50; ++i) {
        auto k   = std::format("ak_{:04d}", i);
        auto exp = std::format("av_{:04d}", i);
        auto got = eng->get(as_span(k));
        if (got && to_str(*got) == exp) ++found;
    }
    CHECK_EQ(found, 50);

    eng->close();
    fs::remove_all(dir);
    std::printf("  ok\n");
}

// ============================================================================
// 5. BLOB path  (value >= DEFAULT_THRESHOLD = 16 KiB)
// ============================================================================

static void test_blob_roundtrip() {
    section("BLOB: large-value roundtrip (>= 16 KiB)");

    auto dir = make_temp_dir("blob_rt");

    // Build a 64 KiB payload with a recognizable pattern
    constexpr size_t BLOB_SIZE = 64 * 1024;
    std::vector<uint8_t> payload(BLOB_SIZE);
    for (size_t i = 0; i < BLOB_SIZE; ++i)
        payload[i] = static_cast<uint8_t>(i & 0xFF);

    {
        AkkEngineOptions opts;
        opts.data_dir   = dir;
        opts.wal_enabled = true;
        opts.sync_mode  = SyncMode::Sync;
        auto eng = AkkEngine::open(opts);

        eng->put(as_span("big_key"), as_span(payload));

        // Inline get (before close)
        auto got = eng->get(as_span("big_key"));
        CHECK(got.has_value());
        CHECK(got->size() == BLOB_SIZE);
        CHECK(*got == payload);

        // A .blob file should have been created
        bool blob_file_found = false;
        for (auto& e : fs::recursive_directory_iterator(dir / "blobs"))
            if (e.path().extension() == ".blob") { blob_file_found = true; break; }
        CHECK(blob_file_found);

        eng->close();
    }

    // Recovery: blob file survives restart, get() still works
    {
        AkkEngineOptions opts;
        opts.data_dir   = dir;
        opts.wal_enabled = true;
        opts.sync_mode  = SyncMode::Sync;
        auto eng = AkkEngine::open(opts);

        auto got = eng->get(as_span("big_key"));
        CHECK(got.has_value());
        CHECK(got->size() == BLOB_SIZE);
        CHECK(*got == payload);

        eng->close();
    }

    // remove() schedules blob for GC
    {
        AkkEngineOptions opts;
        opts.data_dir   = dir;
        opts.wal_enabled = true;
        opts.sync_mode  = SyncMode::Sync;
        auto eng = AkkEngine::open(opts);

        eng->remove(as_span("big_key"));
        auto got = eng->get(as_span("big_key"));
        CHECK(!got.has_value());

        eng->close();
    }

    fs::remove_all(dir);
    std::printf("  ok\n");
}

// ============================================================================
// 6. BLOB: inline values near the threshold are NOT treated as BlobRefs
// ============================================================================

static void test_blob_no_false_positive() {
    section("BLOB: 20-byte inline value is NOT misread as BlobRef");

    auto dir = make_temp_dir("blob_fp");

    // Exactly BLOB_REF_SIZE (20) bytes — stored inline (< 16 KiB threshold)
    // total_size field would parse as whatever the first 16 bytes encode,
    // but that value is < 16 KiB, so the disambiguation check correctly
    // returns the raw bytes without touching the blob directory.
    std::vector<uint8_t> inline20(20, 0xAB);

    AkkEngineOptions opts;
    opts.data_dir   = dir;
    opts.wal_enabled = true;
    opts.sync_mode  = SyncMode::Sync;
    auto eng = AkkEngine::open(opts);

    eng->put(as_span("tiny"), as_span(inline20));
    auto got = eng->get(as_span("tiny"));
    CHECK(got.has_value());
    CHECK(*got == inline20);

    // No blob files should exist
    bool any_blob = false;
    if (fs::exists(dir / "blobs")) {
        for (auto& e : fs::recursive_directory_iterator(dir / "blobs"))
            if (e.path().extension() == ".blob") { any_blob = true; break; }
    }
    CHECK(!any_blob);

    eng->close();
    fs::remove_all(dir);
    std::printf("  ok\n");
}

// ============================================================================
// 7. Idempotent close
// ============================================================================

static void test_idempotent_close() {
    section("Lifecycle: double-close is safe");

    AkkEngineOptions opts;
    opts.wal_enabled = false;
    auto eng = AkkEngine::open(opts);
    eng->put(as_span("x"), as_span("y"));
    eng->close();
    eng->close(); // must not crash or double-free
    std::printf("  ok\n");
    ++g_pass;
}

// ============================================================================
// 8. Throughput benchmark
// ============================================================================

static void bench_throughput() {
    section("Throughput benchmark");

    constexpr int N = 100'000;

    // ── In-memory write ───────────────────────────────────────────────────────
    {
        AkkEngineOptions opts;
        opts.wal_enabled = false;
        auto eng = AkkEngine::open(opts);

        auto t0 = Clock::now();
        for (int i = 0; i < N; ++i) {
            auto k = std::format("bk_{:08d}", i);
            auto v = std::format("bv_{:08d}", i);
            eng->put(as_span(k), as_span(v));
        }
        double ms = elapsed_ms(t0);
        std::printf("  [mem]  write  %6d ops in %7.1f ms  →  %7.0f ops/s\n",
                    N, ms, N / (ms / 1000.0));

        t0 = Clock::now();
        int found = 0;
        for (int i = 0; i < N; ++i) {
            auto k = std::format("bk_{:08d}", i);
            if (eng->get(as_span(k)).has_value()) ++found;
        }
        ms = elapsed_ms(t0);
        std::printf("  [mem]  read   %6d ops in %7.1f ms  →  %7.0f ops/s  (found %d)\n",
                    N, ms, N / (ms / 1000.0), found);

        eng->close();
    }

    // ── WAL Async write ───────────────────────────────────────────────────────
    {
        auto dir = make_temp_dir("bench_wal");

        AkkEngineOptions opts;
        opts.data_dir   = dir;
        opts.wal_enabled = true;
        opts.sync_mode  = SyncMode::Async;
        opts.wal.group_n      = 1024;
        opts.wal.group_micros = 200;
        auto eng = AkkEngine::open(opts);

        auto t0 = Clock::now();
        for (int i = 0; i < N; ++i) {
            auto k = std::format("wk_{:08d}", i);
            auto v = std::format("wv_{:08d}", i);
            eng->put(as_span(k), as_span(v));
        }
        eng->force_sync();
        double ms = elapsed_ms(t0);
        std::printf("  [wal]  write  %6d ops in %7.1f ms  →  %7.0f ops/s  (async+fsync)\n",
                    N, ms, N / (ms / 1000.0));

        t0 = Clock::now();
        auto found = 0;
        for (int i = 0; i < N; ++i) {
            auto k = std::format("wk_{:08d}", i);
            if (eng->get(as_span(k)).has_value()) ++found;
        }
        ms = elapsed_ms(t0);
        std::printf("  [wal]  read   %6d ops in %7.1f ms  →  %7.0f ops/s  (found %d)\n",
                    N, ms, N / (ms / 1000.0), found);

        eng->close();
        fs::remove_all(dir);
    }
}

// ============================================================================
// main
// ============================================================================

int main() {
    std::printf("AkkaraDB Engine — correctness + benchmark\n");
    std::printf("==========================================\n");

    // ── Correctness tests ────────────────────────────────────────────────────
    test_memory_basic();
    test_memory_many_keys();
    test_sync_off();
    test_wal_recovery();
    test_wal_async();
    test_blob_roundtrip();
    test_blob_no_false_positive();
    test_idempotent_close();
    // ── Throughput ───────────────────────────────────────────────────────────
    bench_throughput();

    // ── Summary ──────────────────────────────────────────────────────────────
    std::printf("\n==========================================\n");
    if (g_fail == 0) {
        std::printf("PASSED  %d / %d checks\n", g_pass, g_pass + g_fail);
    } else {
        std::printf("FAILED  %d checks,  passed %d / %d\n",
                    g_fail, g_pass, g_pass + g_fail);
    }

    return g_fail == 0 ? 0 : 1;
}
