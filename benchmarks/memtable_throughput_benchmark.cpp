/*
 * Sharded MemTable throughput benchmark (size sweep).
 *
 * Measures:
 *  - PUT throughput (unique keys)
 *  - GET throughput (existing keys)
 *
 * Runs one backend with auto-resolved shard_count from writer count.
 *
 * Usage:
 *   akkaradb_memtable_throughput_benchmark [ops_per_case] [--writers=N] [--prehash] [--backend=skiplist|bptree]
 *
 * Default:
 *   ops_per_case = 200000
 *   writers      = 0 (auto)
 */

#include "engine/memtable/MemTable.hpp"
#include "engine/memtable/BPTreeMemTable.hpp"
#include "engine/memtable/SkipListMemTable.hpp"
#include "core/record/SSTHdr32.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <format>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;
using namespace akkaradb::engine;
using namespace akkaradb::core;

namespace {
    struct CaseSpec {
        int key_size;
        int value_size;
    };

    struct ThroughputResult {
        double put_ops_per_sec;
        double get_ops_per_sec;
    };

    enum class BackendKind {
        SkipList,
        BPTree
    };

    [[nodiscard]] static uint32_t next_pow2_clamped(uint64_t n, uint32_t min_value, uint32_t max_value) {
        uint32_t p = 1;
        while (p < n && p < max_value) {
            p <<= 1;
        }
        if (p < min_value) {
            p = min_value;
        }
        if (p > max_value) {
            p = max_value;
        }
        return p;
    }

    [[nodiscard]] static uint32_t resolve_auto_shard_count(size_t expected_concurrent_writers) {
        const size_t n = expected_concurrent_writers > 0
            ? expected_concurrent_writers
            : std::max<size_t>(2, std::thread::hardware_concurrency());
        const long double estimate = std::ceil(
            static_cast<long double>(n) *
            static_cast<long double>(n - 1) *
            2.25L
        );
        const uint64_t target = estimate < 2.0L ? 2ULL : static_cast<uint64_t>(estimate);
        return next_pow2_clamped(target, 2, 256);
    }

    [[nodiscard]] static int resolve_writer_threads(int requested) {
        if (requested > 0) {
            return requested;
        }
        const unsigned hw = std::thread::hardware_concurrency();
        return static_cast<int>(hw == 0 ? 1u : hw);
    }

    static std::span<const uint8_t> as_u8(const std::string& s) {
        return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
    }

    static std::string make_fixed_bytes(int size, uint64_t seed) {
        std::string out;
        out.resize(static_cast<size_t>(size));
        uint64_t x = seed ^ 0x9e3779b97f4a7c15ULL;
        for (int i = 0; i < size; ++i) {
            x ^= (x << 13);
            x ^= (x >> 7);
            x ^= (x << 17);
            out[static_cast<size_t>(i)] = static_cast<char>('a' + (x % 26));
        }
        return out;
    }

    static memtable::MemTable::Options make_options(uint32_t shard_count, BackendKind backend) {
        memtable::MemTable::Options opts;
        opts.shard_count = shard_count;
        opts.threshold_bytes_per_shard = (1ULL << 62); // effectively disable auto flush in bench
        if (backend == BackendKind::BPTree) {
            opts.backend_factory = []() {
                return std::make_unique<BPTreeMemTable>();
            };
        } else {
            opts.backend_factory = []() {
                return std::make_unique<SkipListMemTable>();
            };
        }
        return opts;
    }

    static void run_put_parallel(
        memtable::MemTable& memtable,
        const std::vector<std::string>& keys,
        const std::string& value,
        const std::vector<uint64_t>* key_fp64,
        const std::vector<uint64_t>* key_mk,
        int writer_threads
    ) {
        if (writer_threads <= 1) {
            for (size_t i = 0; i < keys.size(); ++i) {
                const uint64_t seq = memtable.next_seq();
                memtable.put(
                    as_u8(keys[i]),
                    as_u8(value),
                    seq,
                    0,
                    key_fp64 ? (*key_fp64)[i] : 0ULL,
                    key_mk ? (*key_mk)[i] : 0ULL
                );
            }
            return;
        }

        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(writer_threads));

        for (int tid = 0; tid < writer_threads; ++tid) {
            threads.emplace_back([&, tid]() {
                for (size_t i = static_cast<size_t>(tid); i < keys.size(); i += static_cast<size_t>(writer_threads)) {
                    const uint64_t seq = memtable.next_seq();
                    memtable.put(
                        as_u8(keys[i]),
                        as_u8(value),
                        seq,
                        0,
                        key_fp64 ? (*key_fp64)[i] : 0ULL,
                        key_mk ? (*key_mk)[i] : 0ULL
                    );
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }
    }

    static void run_get_parallel(
        memtable::MemTable& memtable,
        const std::vector<std::string>& keys,
        uint64_t snapshot,
        int reader_threads
    ) {
        if (reader_threads <= 1) {
            RecordView out;
            for (size_t i = 0; i < keys.size(); ++i) {
                if (!memtable.get(as_u8(keys[i]), snapshot, &out)) {
                    std::fprintf(stderr, "GET miss at i=%zu\n", i);
                    std::exit(3);
                }
            }
            return;
        }

        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(reader_threads));

        for (int tid = 0; tid < reader_threads; ++tid) {
            threads.emplace_back([&, tid]() {
                RecordView out;
                for (size_t i = static_cast<size_t>(tid); i < keys.size(); i += static_cast<size_t>(reader_threads)) {
                    if (!memtable.get(as_u8(keys[i]), snapshot, &out)) {
                        std::fprintf(stderr, "GET miss at i=%zu (tid=%d)\n", i, tid);
                        std::exit(3);
                    }
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }
    }

    static ThroughputResult run_case(
        const CaseSpec spec,
        int ops_per_case,
        bool use_prehash,
        uint32_t shard_count,
        int writer_threads,
        BackendKind backend
    ) {
        const int warmup_ops = std::min(ops_per_case, 50000);

        std::vector<std::string> keys;
        keys.reserve(static_cast<size_t>(ops_per_case));
        for (int i = 0; i < ops_per_case; ++i) {
            std::string key = make_fixed_bytes(spec.key_size, static_cast<uint64_t>(i) + 1);
            if (spec.key_size >= 10) {
                const auto tail = std::format("{:010d}", i);
                std::memcpy(key.data() + (spec.key_size - 10), tail.data(), 10);
            }
            keys.emplace_back(std::move(key));
        }

        const std::string value = make_fixed_bytes(spec.value_size, 0xA11CEULL);

        std::vector<uint64_t> key_fp64;
        std::vector<uint64_t> key_mk;
        const std::vector<uint64_t>* fp_ptr = nullptr;
        const std::vector<uint64_t>* mk_ptr = nullptr;

        if (use_prehash) {
            key_fp64.resize(static_cast<size_t>(ops_per_case));
            key_mk.resize(static_cast<size_t>(ops_per_case));
            for (int i = 0; i < ops_per_case; ++i) {
                const auto& key = keys[static_cast<size_t>(i)];
                const uint8_t* ptr = reinterpret_cast<const uint8_t*>(key.data());
                const size_t len = key.size();
                key_fp64[static_cast<size_t>(i)] = SSTHdr32::compute_key_fp64(ptr, len);
                key_mk[static_cast<size_t>(i)] = SSTHdr32::build_mini_key(ptr, len);
            }
            fp_ptr = &key_fp64;
            mk_ptr = &key_mk;
        }

        {
            auto warmup = memtable::MemTable::create(make_options(shard_count, backend));
            const std::vector<std::string> warmup_keys(keys.begin(), keys.begin() + warmup_ops);
            run_put_parallel(*warmup, warmup_keys, value, fp_ptr, mk_ptr, writer_threads);

            const uint64_t snapshot = warmup->last_seq();
            RecordView out;
            for (int i = 0; i < warmup_ops; ++i) {
                if (!warmup->get(as_u8(keys[static_cast<size_t>(i)]), snapshot, &out)) {
                    std::fprintf(stderr, "WARMUP GET miss at i=%d\n", i);
                    std::exit(1);
                }
            }
        }

        auto memtable = memtable::MemTable::create(make_options(shard_count, backend));

        const auto put_t0 = Clock::now();
        run_put_parallel(*memtable, keys, value, fp_ptr, mk_ptr, writer_threads);
        const auto put_ms = std::chrono::duration<double, std::milli>(Clock::now() - put_t0).count();

        const uint64_t snapshot = memtable->last_seq();
        const auto get_t0 = Clock::now();
        run_get_parallel(*memtable, keys, snapshot, writer_threads);
        const auto get_ms = std::chrono::duration<double, std::milli>(Clock::now() - get_t0).count();

        return {
            .put_ops_per_sec = static_cast<double>(ops_per_case) * 1000.0 / put_ms,
            .get_ops_per_sec = static_cast<double>(ops_per_case) * 1000.0 / get_ms
        };
    }
} // namespace

int main(int argc, char** argv) {
    int ops_per_case = 200000;
    int writer_threads = 16;
    bool use_prehash = false;
    BackendKind backend = BackendKind::BPTree;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--prehash") {
            use_prehash = true;
            continue;
        }
        if (arg.rfind("--backend=", 0) == 0) {
            const std::string kind = arg.substr(10);
            if (kind == "skiplist") {
                backend = BackendKind::SkipList;
                continue;
            }
            if (kind == "bptree") {
                backend = BackendKind::BPTree;
                continue;
            }
            std::fprintf(stderr, "Unknown backend: %s (use skiplist|bptree)\n", kind.c_str());
            return 2;
        }
        if (arg.rfind("--writers=", 0) == 0) {
            writer_threads = std::max(0, std::atoi(arg.substr(10).c_str()));
            continue;
        }
        ops_per_case = std::max(1000, std::atoi(arg.c_str()));
    }

    const std::array<CaseSpec, 6> cases{{
        {8, 16},
        {16, 64},
        {16, 256},
        {32, 1024},
        {32, 4096},
        {64, 16384}
    }};

    const int effective_writers = resolve_writer_threads(writer_threads);
    const uint32_t shard_count = resolve_auto_shard_count(static_cast<size_t>(effective_writers));

    std::printf("Sharded MemTable throughput benchmark\n");
    std::printf("ops_per_case = %d\n", ops_per_case);
    std::printf("backend = %s\n", backend == BackendKind::BPTree ? "bptree" : "skiplist");
    if (writer_threads == 0) {
        std::printf("writer_threads = auto (%d from hw_threads)\n", effective_writers);
    } else {
        std::printf("writer_threads = %d\n", writer_threads);
    }
    std::printf("resolved_shards = %u (MemTable same heuristic)\n\n", shard_count);
    std::printf("warmup_ops   = %d\n\n", std::min(ops_per_case, 50000));
    std::printf("prehash_mode = %s\n\n", use_prehash ? "ON (fp64/mk precomputed)" : "OFF (hash inside put)");
    std::printf("%-10s %-12s %-8s %-8s %-14s %-14s\n",
                "key", "value", "shards", "writers", "put(ops/s)", "get(ops/s)");
    std::printf("------------------------------------------------------------------------------------------------\n");

    for (const auto& spec : cases) {
        const ThroughputResult result = run_case(spec, ops_per_case, use_prehash, shard_count, effective_writers, backend);
        std::printf("%-10d %-12d %-8u %-8d %-14.0f %-14.0f\n",
                    spec.key_size,
                    spec.value_size,
                    shard_count,
                    effective_writers,
                    result.put_ops_per_sec,
                    result.get_ops_per_sec);
        std::printf("------------------------------------------------------------------------------------------------\n");
        std::fflush(stdout);
    }

    return 0;
}
