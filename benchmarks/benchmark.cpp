/*
 * AkkaraDB — Engine correctness tests + throughput benchmark
 *
 * Covers:
 *   1. Memory-only mode  (wal_enabled=false)
 *   2. WAL persistence   (wal_enabled=true, Sync)
 *   3. BLOB path         (value >= 16 KiB threshold)
 *   4. SyncMode::Off     (WAL enabled, OS page cache only — no fdatasync)
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
// 2. SyncMode::Off  (WAL enabled, OS page cache only — no fdatasync)
// ============================================================================

static void test_sync_off() {
    section("SyncMode::Off: WAL written, no fdatasync, data survives reopen");

    auto dir = make_temp_dir("sync_off");

    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off; // write to OS page cache, no fdatasync

        auto eng = AkkEngine::open(opts);
        eng->put(as_span("hello"), as_span("world"));
        CHECK(eng->get(as_span("hello")).has_value());

        // WAL directory must exist — Off still writes WAL entries
        CHECK(fs::exists(dir / "wal"));
        eng->close();
    }

    // Reopen: WAL recovery should restore the entry
    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off;

        auto eng = AkkEngine::open(opts);
        CHECK(eng->get(as_span("hello")).has_value());
        eng->close();
    }

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
        opts.wal.sync_mode = SyncMode::Sync;
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
        opts.wal.sync_mode = SyncMode::Sync;
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
    opts.wal.sync_mode = SyncMode::Async;
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
        opts.wal.sync_mode = SyncMode::Sync;
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
        opts.wal.sync_mode = SyncMode::Sync;
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
        opts.wal.sync_mode = SyncMode::Sync;
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
    opts.wal.sync_mode = SyncMode::Sync;
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

// ── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Generate a benchmark key of exactly `key_size` bytes.
 *
 * Layout: 'k' × (key_size - 6)  +  zero-padded 6-digit index
 * Example (key_size=11, i=42): "kkkkk000042"
 * Uniqueness guaranteed for N ≤ 999,999 (6 digits).
 */
static std::string make_bench_key(int i, int key_size) {
    const int pad = std::max(0, key_size - 6);
    return std::string(pad, 'k') + std::format("{:06d}", i);
}

/**
 * Generate a benchmark value of exactly `val_size` bytes.
 *
 * Layout: 'v' × (val_size - 6)  +  zero-padded 6-digit index  (if val_size ≥ 6)
 * Values are intentionally non-identical to avoid trivial branch prediction.
 */
static std::string make_bench_val(int i, int val_size) {
    std::string v(val_size, 'v');
    if (val_size >= 6) {
        auto num = std::format("{:06d}", i);
        std::copy(num.begin(), num.end(), v.end() - 6);
    }
    return v;
}

struct SizeCase {
    const char* label;
    int key_size; ///< Key bytes
    int val_size; ///< Value bytes
};

/**
 * Run one mem-only write+read benchmark for a given key/value size.
 *
 * Keys and values are pre-generated before the timed section so that
 * std::format / string-allocation overhead is excluded from the measurement.
 *
 * A warmup pass (untimed) runs before measurement to put all cases on equal footing:
 *   - CRT heap: arena slab pools and SmallBuffer heap chunks are pre-cached so that
 *     the first timed case doesn't pay cold-start malloc costs that later cases avoid.
 *   - CPU I-cache / branch predictors: engine hot paths are warmed before timing.
 *   - TLB: pages touched in warmup fill TLB entries ahead of the timed run.
 */
static void bench_mem_case(const SizeCase& sc, int N) {
    // ── Pre-generate keys and values (outside timed section) ──────────────────
    std::vector<std::string> keys(N), vals(N);
    for (int i = 0; i < N; ++i) {
        keys[i] = make_bench_key(i, sc.key_size);
        vals[i] = make_bench_val(i, sc.val_size);
    }

    // ── Warmup pass (untimed) ─────────────────────────────────────────────────
    {
        AkkEngineOptions wo;
        wo.wal_enabled = false;
        auto eng = AkkEngine::open(wo);
        for (int i = 0; i < N; ++i) eng->put(as_span(keys[i]), as_span(vals[i]));
        eng->close(); // releases arena/SmallBuffer memory back to CRT heap free-lists
    }

    // ── Timed measurement ─────────────────────────────────────────────────────
    AkkEngineOptions opts;
    opts.wal_enabled = false;
    auto eng = AkkEngine::open(opts);

    // Write
    auto t0 = Clock::now();
    for (int i = 0; i < N; ++i) eng->put(as_span(keys[i]), as_span(vals[i]));
    const double w_ops = N / (elapsed_ms(t0) / 1000.0);

    // Read — get_into() reuses a single buffer each iteration (no per-call malloc).
    // Pre-reserve so vector::assign() is always an in-place memcpy.
    t0 = Clock::now();
    std::vector<uint8_t> rbuf;
    rbuf.reserve(static_cast<size_t>(sc.val_size));
    int found = 0;
    for (int i = 0; i < N; ++i) if (eng->get_into(as_span(keys[i]), rbuf)) ++found;
    const double r_ops = N / (elapsed_ms(t0) / 1000.0);

    eng->close();

    const int total = sc.key_size + sc.val_size;
    const bool inl = total <= 22; // SmallBuffer::INLINE_CAP
    std::printf("  [mem] %-20s  total=%4dB (%s)   write %8.0f   read %8.0f ops/s\n", sc.label, total, inl ? "inline" : "heap  ", w_ops, r_ops);
}

static void bench_throughput() {
    section("Throughput benchmark  (std::format excluded — keys/vals pre-generated)");

    constexpr int N = 100'000;

    // ── 5 key/value size cases (mem-only) ────────────────────────────────────
    //
    // SmallBuffer::INLINE_CAP = 22 bytes.
    //   total ≤ 22  →  zero heap alloc per record (inline path)
    //   total > 22  →  one new uint8_t[total] per record (heap path)
    //
    static constexpr SizeCase CASES[] = {
        {"k= 8  v=  8", 8, 8},
        // total= 16  inline  ← headroom
        {"k=11  v= 11", 11, 11},
        // total= 22  inline  ← original benchmark (max)
        {"k=16  v= 64", 16, 64},
        // total= 80  heap    ← first heap tier
        {"k=32  v=256", 32, 256},
        // total=288  heap    ← medium
        {"k=32  v=1k ", 32, 1024},
        // total=1056 heap    ← large
    };

    std::printf("  %-20s  %-18s   %13s  %13s\n", "case", "data", "write", "read");
    std::printf("  %-20s  %-18s   %13s  %13s\n", "────────────────────", "──────────────────", "─────────────", "─────────────");
    for (const auto& sc : CASES) bench_mem_case(sc, N);

    // ── WAL Async (k=11, v=11 — inline, same as original benchmark) ──────────
    std::printf("\n");
    {
        auto dir = make_temp_dir("bench_wal");

        std::vector<std::string> keys(N), vals(N);
        for (int i = 0; i < N; ++i) {
            keys[i] = make_bench_key(i, 11);
            vals[i] = make_bench_val(i, 11);
        }

        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal_enabled = true;
        opts.wal.sync_mode = SyncMode::Async;
        opts.wal.shard_count = 1; // single-threaded: 1 shard minimises fdatasync interrupt overhead
        opts.wal.group_n = 1024;
        opts.wal.group_micros = 500;
        auto eng = AkkEngine::open(opts);

        auto t0 = Clock::now();
        for (int i = 0; i < N; ++i) eng->put(as_span(keys[i]), as_span(vals[i]));
        double ms = elapsed_ms(t0);
        std::printf("  [wal] k=11  v= 11       total= 22B (inline)   write %8.0f ops/s  (async)\n", N / (ms / 1000.0));

        t0 = Clock::now();
        int found = 0;
        for (int i = 0; i < N; ++i) if (eng->get(as_span(keys[i])).has_value()) ++found;
        ms = elapsed_ms(t0);
        std::printf(
            "  [wal] k=11  v= 11       total= 22B (inline)   read  %8.0f ops/s  (found %d)\n",
            N / (ms / 1000.0),
            found
        );

        eng->close();
        fs::remove_all(dir);
    }
}

// ============================================================================
// 9. VersionLog: basic history + get_at
// ============================================================================

static void test_vlog_basic() {
    section("VersionLog: history and get_at");

    auto dir = make_temp_dir("vlog_basic");

    AkkEngineOptions opts;
    opts.data_dir = dir;
    opts.wal.sync_mode = SyncMode::Off;
    opts.version_log_enabled = true;

    auto eng = AkkEngine::open(opts);

    // Write three versions of the same key
    eng->put(as_span("fruit"), as_span("apple"));
    eng->put(as_span("fruit"), as_span("banana"));
    eng->put(as_span("fruit"), as_span("cherry"));

    // history() must return 3 entries in ascending seq order
    auto hist = eng->history(as_span("fruit"));
    CHECK_EQ(static_cast<int>(hist.size()), 3);
    if (hist.size() == 3) {
        CHECK(to_str(hist[0].value) == "apple");
        CHECK(to_str(hist[1].value) == "banana");
        CHECK(to_str(hist[2].value) == "cherry");
        CHECK(hist[0].seq < hist[1].seq);
        CHECK(hist[1].seq < hist[2].seq);
    }

    // get_at: value at the seq of the first write
    if (hist.size() >= 2) {
        const uint64_t seq_after_first = hist[0].seq;
        auto v = eng->get_at(as_span("fruit"), seq_after_first);
        CHECK(v.has_value());
        if (v)
            CHECK(to_str(*v) == "apple");
    }

    // get_at: value at seq of second write
    if (hist.size() >= 3) {
        const uint64_t seq_after_second = hist[1].seq;
        auto v = eng->get_at(as_span("fruit"), seq_after_second);
        CHECK(v.has_value());
        if (v)
            CHECK(to_str(*v) == "banana");
    }

    // history() of unknown key → empty
    auto empty = eng->history(as_span("no_such_key"));
    CHECK(empty.empty());

    // get_at before any write → nullopt
    auto none = eng->get_at(as_span("fruit"), 0);
    CHECK(!none.has_value());

    eng->close();
    std::printf("  ok\n");
}

// ============================================================================
// 10. VersionLog: rollback_key
// ============================================================================

static void test_vlog_rollback_key() {
    section("VersionLog: rollback_key restores previous value");

    auto dir = make_temp_dir("vlog_rollback_key");

    AkkEngineOptions opts;
    opts.data_dir = dir;
    opts.wal.sync_mode = SyncMode::Off;
    opts.version_log_enabled = true;

    auto eng = AkkEngine::open(opts);

    eng->put(as_span("k"), as_span("v1"));
    auto hist_after_v1 = eng->history(as_span("k"));
    CHECK_EQ(static_cast<int>(hist_after_v1.size()), 1);
    const uint64_t seq_v1 = hist_after_v1.empty() ? 0 : hist_after_v1[0].seq;

    eng->put(as_span("k"), as_span("v2"));

    // Current value is v2
    auto cur = eng->get(as_span("k"));
    CHECK(cur.has_value());
    if (cur)
        CHECK(to_str(*cur) == "v2");

    // Rollback to the seq of v1 → should restore "v1"
    eng->rollback_key(as_span("k"), seq_v1);

    auto after = eng->get(as_span("k"));
    CHECK(after.has_value());
    if (after)
        CHECK(to_str(*after) == "v1");

    eng->close();
    std::printf("  ok\n");
}

// ============================================================================
// 11. VersionLog: rollback_to (full-DB rollback)
// ============================================================================

static void test_vlog_rollback_to() {
    section("VersionLog: rollback_to restores entire DB");

    auto dir = make_temp_dir("vlog_rollback_to");

    AkkEngineOptions opts;
    opts.data_dir = dir;
    opts.wal.sync_mode = SyncMode::Off;
    opts.version_log_enabled = true;

    auto eng = AkkEngine::open(opts);

    // Baseline: write two keys
    eng->put(as_span("a"), as_span("a1"));
    eng->put(as_span("b"), as_span("b1"));

    // Record a snapshot point via seq of the last write in the baseline
    auto hist_b = eng->history(as_span("b"));
    const uint64_t snap_seq = hist_b.empty() ? 0 : hist_b.back().seq;

    // More writes after the snapshot
    eng->put(as_span("a"), as_span("a2")); // overwrite
    eng->put(as_span("c"), as_span("c1")); // new key
    eng->remove(as_span("b")); // delete existing key

    // Verify state before rollback
    CHECK(to_str(*eng->get(as_span("a"))) == "a2");
    CHECK(!eng->get(as_span("b")).has_value());
    CHECK(eng->get(as_span("c")).has_value());

    // Full rollback to snap_seq
    eng->rollback_to(snap_seq);

    // "a" should be restored to "a1"
    auto va = eng->get(as_span("a"));
    CHECK(va.has_value());
    if (va)
        CHECK(to_str(*va) == "a1");

    // "b" should be restored (was alive at snap_seq)
    auto vb = eng->get(as_span("b"));
    CHECK(vb.has_value());
    if (vb)
        CHECK(to_str(*vb) == "b1");

    // "c" should be removed (did not exist at snap_seq)
    CHECK(!eng->get(as_span("c")).has_value());

    eng->close();
    std::printf("  ok\n");
}

// ============================================================================
// 12. VersionLog: persistence across restart
// ============================================================================

static void test_vlog_persist() {
    section("VersionLog: history survives close + reopen");

    auto dir = make_temp_dir("vlog_persist");

    // First session: write some data
    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off;
        opts.version_log_enabled = true;

        auto eng = AkkEngine::open(opts);
        eng->put(as_span("p"), as_span("p1"));
        eng->put(as_span("p"), as_span("p2"));
        eng->close();
    }

    // Second session: history must still be available
    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off;
        opts.version_log_enabled = true;

        auto eng = AkkEngine::open(opts);
        auto hist = eng->history(as_span("p"));
        CHECK_EQ(static_cast<int>(hist.size()), 2);
        if (hist.size() == 2) {
            CHECK(to_str(hist[0].value) == "p1");
            CHECK(to_str(hist[1].value) == "p2");
        }
        eng->close();
    }

    std::printf("  ok\n");
}

// ============================================================================
// 13. SST: basic write → flush → close → reopen → read from SST
// ============================================================================

static void test_sst_basic() {
    section("SST: write / flush / reopen reads from SST");

    auto dir = make_temp_dir("sst_basic");
    constexpr int N = 1000;

    // ── Session 1: write N keys, flush, close ──────────────────────────────
    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off;

        auto eng = AkkEngine::open(opts);
        for (int i = 0; i < N; ++i) {
            auto key = std::format("skey{:06d}", i);
            auto val = std::format("sval{:06d}", i);
            eng->put(as_span(key), as_span(val));
        }
        eng->force_flush(); // MemTable → SST
        eng->close(); // WAL truncated after flush
    }

    // ── Verify SST directory has .aksst files ──────────────────────────────
    bool has_sst = false;
    if (fs::exists(dir / "sst")) {
        for (const auto& entry : fs::directory_iterator(dir / "sst")) {
            if (entry.path().extension() == ".aksst") {
                has_sst = true;
                break;
            }
        }
    }
    CHECK(has_sst);

    // ── Session 2: reopen; WAL was truncated so data must come from SST ────
    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off;

        auto eng = AkkEngine::open(opts);

        int found = 0;
        for (int i = 0; i < N; ++i) {
            auto key = std::format("skey{:06d}", i);
            auto val = std::format("sval{:06d}", i);
            auto r = eng->get(as_span(key));
            if (r && to_str(*r) == val) ++found;
        }
        CHECK_EQ(found, N);

        eng->close();
    }

    std::printf("  ok\n");
}

// ============================================================================
// 14. SST: tombstone — deleted key absent after flush + reopen
// ============================================================================

static void test_sst_tombstone() {
    section("SST: deleted key absent after flush + reopen");

    auto dir = make_temp_dir("sst_tombstone");

    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off;

        auto eng = AkkEngine::open(opts);

        // Write keys, then delete some.
        for (int i = 0; i < 20; ++i) {
            auto key = std::format("tk{:03d}", i);
            auto val = std::format("tv{:03d}", i);
            eng->put(as_span(key), as_span(val));
        }
        // Delete every other key.
        for (int i = 0; i < 20; i += 2) {
            auto key = std::format("tk{:03d}", i);
            eng->remove(as_span(key));
        }

        eng->force_flush();
        eng->close();
    }

    // Session 2: verify presence / absence.
    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off;

        auto eng = AkkEngine::open(opts);

        int alive = 0, dead = 0;
        for (int i = 0; i < 20; ++i) {
            auto key = std::format("tk{:03d}", i);
            auto r = eng->get(as_span(key));
            if (i % 2 == 0) {
                // deleted keys
                if (!r.has_value()) ++dead;
            }
            else {
                // live keys
                auto val = std::format("tv{:03d}", i);
                if (r && to_str(*r) == val) ++alive;
            }
        }
        CHECK_EQ(alive, 10); // 10 odd-indexed keys alive
        CHECK_EQ(dead, 10); // 10 even-indexed keys deleted

        eng->close();
    }

    std::printf("  ok\n");
}

// ============================================================================
// 15. SST: compaction — L0 files merged into L1, all data accessible
// ============================================================================

static void test_sst_compaction() {
    section("SST: L0 → L1 compaction, all data survives");

    auto dir = make_temp_dir("sst_compaction");
    constexpr int BATCHES = 4;
    constexpr int PER_BATCH = 25;

    // ── Session 1: write 4 batches, each followed by force_flush ──────────
    // With shard_count=2 and max_l0_sst_files=2 each force_flush produces
    // 2 L0 files, immediately triggering compaction (2 >= 2).
    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off;
        opts.max_l0_sst_files = 2; // compact every 2 L0 files
        opts.memtable.shard_count = 2; // exactly 2 shards → 2 L0 per flush

        auto eng = AkkEngine::open(opts);

        for (int b = 0; b < BATCHES; ++b) {
            for (int i = 0; i < PER_BATCH; ++i) {
                auto key = std::format("ck{:01d}{:03d}", b, i);
                auto val = std::format("cv{:01d}{:03d}", b, i);
                eng->put(as_span(key), as_span(val));
            }
            eng->force_flush(); // creates 2 L0 files → compaction
        }

        eng->close();
    }

    // ── Verify L1 files exist ──────────────────────────────────────────────
    bool has_l1 = false;
    if (fs::exists(dir / "sst")) {
        for (const auto& entry : fs::directory_iterator(dir / "sst")) {
            if (entry.path().filename().string().starts_with("L1_")) {
                has_l1 = true;
                break;
            }
        }
    }
    CHECK(has_l1);

    // ── Session 2: reopen, all keys accessible ────────────────────────────
    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off;
        opts.max_l0_sst_files = 2;
        opts.memtable.shard_count = 2;

        auto eng = AkkEngine::open(opts);

        int found = 0;
        for (int b = 0; b < BATCHES; ++b) {
            for (int i = 0; i < PER_BATCH; ++i) {
                auto key = std::format("ck{:01d}{:03d}", b, i);
                auto val = std::format("cv{:01d}{:03d}", b, i);
                auto r = eng->get(as_span(key));
                if (r && to_str(*r) == val) ++found;
            }
        }
        CHECK_EQ(found, BATCHES * PER_BATCH);

        eng->close();
    }

    std::printf("  ok\n");
}

// ============================================================================
// 16. SST: overwrite deduplication — only latest value survives compaction
// ============================================================================

static void test_sst_overwrite() {
    section("SST: overwrite deduplication across compaction");

    auto dir = make_temp_dir("sst_overwrite");

    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off;
        opts.max_l0_sst_files = 2;
        opts.memtable.shard_count = 2;

        auto eng = AkkEngine::open(opts);

        // Write v1, flush → SST
        for (int i = 0; i < 10; ++i) {
            auto key = std::format("ok{:03d}", i);
            eng->put(as_span(key), as_span("v1"));
        }
        eng->force_flush();

        // Write v2 (overwrites), flush → new SST + compaction
        for (int i = 0; i < 10; ++i) {
            auto key = std::format("ok{:03d}", i);
            eng->put(as_span(key), as_span("v2"));
        }
        eng->force_flush();

        eng->close();
    }

    // All keys must return "v2" (the newer version wins).
    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off;
        opts.max_l0_sst_files = 2;
        opts.memtable.shard_count = 2;

        auto eng = AkkEngine::open(opts);

        int correct = 0;
        for (int i = 0; i < 10; ++i) {
            auto key = std::format("ok{:03d}", i);
            auto r = eng->get(as_span(key));
            if (r && to_str(*r) == "v2") ++correct;
        }
        CHECK_EQ(correct, 10);

        eng->close();
    }

    std::printf("  ok\n");
}

// ============================================================================
// 17. SST: WAL truncation — WAL files are empty after close()
// ============================================================================

static void test_wal_truncation() {
    section("SST: WAL files truncated after close()");

    auto dir = make_temp_dir("sst_wal_trunc");

    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off;

        auto eng = AkkEngine::open(opts);
        for (int i = 0; i < 200; ++i) {
            auto key = std::format("wk{:04d}", i);
            eng->put(as_span(key), as_span("wval"));
        }
        eng->force_flush();
        eng->close(); // close() truncates all WAL shard files
    }

    // All WAL shard files must be very small (truncated to 0 or header-only).
    bool wal_truncated = true;
    const auto wal_dir = dir / "wal";
    if (fs::exists(wal_dir)) {
        for (const auto& entry : fs::directory_iterator(wal_dir)) {
            if (entry.path().extension() == ".akwal") {
                // After truncation, file should be 0 bytes (no data, no header).
                // Allow up to 64 bytes to account for any implementation variation.
                if (entry.file_size() > 64) {
                    wal_truncated = false;
                    break;
                }
            }
        }
    }
    CHECK(wal_truncated);

    // Reopen: with truncated WAL, all data must still be accessible via SST.
    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off;

        auto eng = AkkEngine::open(opts);

        int found = 0;
        for (int i = 0; i < 200; ++i) {
            auto key = std::format("wk{:04d}", i);
            auto r = eng->get(as_span(key));
            if (r && to_str(*r) == "wval") ++found;
        }
        CHECK_EQ(found, 200);

        eng->close();
    }

    std::printf("  ok\n");
}

// ============================================================================
// 18. SST: negative lookup throughput (bloom filter)
// ============================================================================

/**
 * Measures how fast exists() rejects keys that are not in the database.
 *
 * Setup:
 *   - N keys are written and flushed to SST (so bloom filters are built).
 *   - Negative keys interleave with existing keys (same lexicographic range)
 *     so every lookup passes the key_in_range() check and exercises the
 *     bloom filter rather than being short-circuited by a range miss.
 *
 * Layout:
 *   existing : "nlkey_XXXXXXXXXX"  where X = i * 2         (even indices)
 *   negative : "nlkey_XXXXXXXXXX"  where X = i * 2 + 1     (odd  indices)
 *
 * A false-positive occurs when bloom_check() returns true for a negative key,
 * causing a full disk scan. The count is reported as FP rate.
 */
static void bench_sst_negative_lookup() {
    section("SST: negative lookup throughput  (1 M keys, bloom filter)");

    auto dir = make_temp_dir("bench_sst_neg");
    constexpr int N = 1'000'000;

    // ── Pre-generate keys (outside timed section) ─────────────────────────
    std::vector<std::string> exist_keys(N), neg_keys(N);
    for (int i = 0; i < N; ++i) {
        exist_keys[i] = std::format("nlkey_{:010d}", i * 2);
        neg_keys[i] = std::format("nlkey_{:010d}", i * 2 + 1);
    }

    // ── Insert N keys, flush to SST, then benchmark in the same session ──────
    // Note: WAL truncation is Phase 5 (pending). Reopening would replay the WAL
    // and restore all 1 M keys into the MemTable, adding BPTree overhead to
    // every exists() call. Instead we flush and benchmark in the same session:
    // after force_flush() the MemTable is empty, so exists() hits only the SST
    // bloom filter — which is what we want to measure.
    {
        AkkEngineOptions opts;
        opts.data_dir = dir;
        opts.wal.sync_mode = SyncMode::Off;
        // Use a single shard so all keys land in one SST after flush
        opts.memtable.shard_count = 1;

        auto eng = AkkEngine::open(opts);
        for (int i = 0; i < N; ++i) eng->put(as_span(exist_keys[i]), as_span("v"));
        eng->force_flush();
        // MemTable is now empty; SST holds all N keys.

        // Warmup (10 K lookups, untimed) — warms up bloom filter in L3 cache
        {
            int dummy = 0;
            constexpr int WARM = 10'000;
            for (int i = 0; i < WARM; ++i) if (eng->exists(as_span(neg_keys[i]))) ++dummy;
        }

        // Timed: 1 M negative lookups
        const auto t0 = Clock::now();
        int false_pos = 0;
        for (int i = 0; i < N; ++i) if (eng->exists(as_span(neg_keys[i]))) ++false_pos;
        const double ops = static_cast<double>(N) / (elapsed_ms(t0) / 1000.0);

        std::printf(
            "  [sst] negative lookup  N=%7d   %9.0f ops/s   " "FP: %d / %d (%.3f%%)\n",
            N,
            ops,
            false_pos,
            N,
            100.0 * static_cast<double>(false_pos) / static_cast<double>(N)
        );

        eng->close();
    }

    fs::remove_all(dir);
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
    // ── VersionLog tests ─────────────────────────────────────────────────────
    test_vlog_basic();
    test_vlog_rollback_key();
    test_vlog_rollback_to();
    test_vlog_persist();
    // ── SST tests (Phase 5) ───────────────────────────────────────────────────
    test_sst_basic();
    test_sst_tombstone();
    test_sst_compaction();
    test_sst_overwrite();
    test_wal_truncation();
    // ── Throughput ───────────────────────────────────────────────────────────
    bench_throughput();
    // ── SST negative lookup (bloom filter) ───────────────────────────────────
    bench_sst_negative_lookup();

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
