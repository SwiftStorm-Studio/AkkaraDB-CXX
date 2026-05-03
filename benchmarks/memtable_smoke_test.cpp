/*
 * AkkaraDB - MemTable smoke test
 */

#include "core/record/RecordView.hpp"
#include "core/types/ByteView.hpp"
#include "core/utils/ArenaGenerator.hpp"
#include "engine/memtable/SkipListMemTable.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace akkaradb::core;
using namespace akkaradb::engine;

namespace {
    static std::span<const uint8_t> as_u8(std::string_view sv) {
        return {reinterpret_cast<const uint8_t*>(sv.data()), sv.size()};
    }

    static ByteView as_bv(std::string_view sv) {
        return {reinterpret_cast<const std::byte*>(sv.data()), sv.size()};
    }

    static std::string to_string(std::span<const uint8_t> value) {
        return {reinterpret_cast<const char*>(value.data()), value.size()};
    }

    void test_put_get_and_snapshot() {
        SkipListMemTable memtable;
        RecordView out;

        assert(memtable.put(as_bv("k"), as_bv("v1"), 1, 0).ok());
        assert(memtable.put(as_bv("k"), as_bv("v2"), 2, 0).ok());

        assert(memtable.get(as_bv("k"), 1, &out));
        assert(to_string(out.value()) == "v1");

        assert(memtable.get(as_bv("k"), 2, &out));
        assert(to_string(out.value()) == "v2");
    }

    void test_four_version_ring() {
        SkipListMemTable memtable;
        RecordView out;

        assert(memtable.put(as_bv("key"), as_bv("v1"), 1, 0).ok());
        assert(memtable.put(as_bv("key"), as_bv("v2"), 2, 0).ok());
        assert(memtable.put(as_bv("key"), as_bv("v3"), 3, 0).ok());
        assert(memtable.put(as_bv("key"), as_bv("v4"), 4, 0).ok());
        assert(memtable.put(as_bv("key"), as_bv("v5"), 5, 0).ok());

        assert(!memtable.get(as_bv("key"), 1, &out)); // v1 is evicted.
        assert(memtable.get(as_bv("key"), 2, &out));
        assert(to_string(out.value()) == "v2");
        assert(memtable.get(as_bv("key"), 5, &out));
        assert(to_string(out.value()) == "v5");
        assert(memtable.entryCount() == 4);
    }

    void test_tombstone_visibility() {
        SkipListMemTable memtable;
        RecordView out;

        assert(memtable.put(as_bv("dead"), as_bv("alive"), 1, 0).ok());
        assert(memtable.put(as_bv("dead"), ByteView{}, 2, RecordView::FLAG_TOMBSTONE).ok());

        assert(memtable.get(as_bv("dead"), 2, &out));
        assert(out.is_tombstone());
    }

    void test_freeze() {
        SkipListMemTable memtable;
        memtable.freeze();
        const Status st = memtable.put(as_bv("k"), as_bv("v"), 1, 0);
        assert(!st.ok());
    }

    void test_iterator_order_and_visibility() {
        SkipListMemTable memtable;
        assert(memtable.put(as_bv("b"), as_bv("1"), 1, 0).ok());
        assert(memtable.put(as_bv("a"), as_bv("2"), 2, 0).ok());
        assert(memtable.put(as_bv("c"), as_bv("3"), 3, 0).ok());

        std::vector<std::string> keys;
        for (const RecordView& rec : memtable.iterator(3)) {
            keys.emplace_back(reinterpret_cast<const char*>(rec.key().data()), rec.key().size());
        }

        assert(keys.size() == 3);
        assert(keys[0] == "a");
        assert(keys[1] == "b");
        assert(keys[2] == "c");
    }

    void test_generator_yield_all() {
        BufferArena arena{16 * 1024, 128 * 1024};

        auto gen1 = ArenaGenerator<int>::with_arena(arena, []() -> ArenaGenerator<int> {
            co_yield 1;
            co_yield 2;
        });
        auto gen2 = ArenaGenerator<int>::with_arena(arena, []() -> ArenaGenerator<int> {
            co_yield 3;
            co_yield 4;
        });

        auto merged = ArenaGenerator<int>::yieldAll(arena, std::move(gen1), std::move(gen2));
        std::vector<int> values;
        for (int value : merged) {
            values.push_back(value);
        }

        assert(values.size() == 4);
        assert(values[0] == 1);
        assert(values[1] == 2);
        assert(values[2] == 3);
        assert(values[3] == 4);
    }

    void test_single_writer_multi_reader_stress() {
        SkipListMemTable memtable;
        std::atomic<bool> stop{false};
        std::atomic<uint64_t> latest_seq{0};
        std::atomic<uint64_t> read_hits{0};

        std::thread writer([&]() {
            for (uint64_t seq = 1; seq <= 2000; ++seq) {
                std::string value = "v" + std::to_string(seq);
                const Status st = memtable.put(as_bv("shared"), as_bv(value), seq, 0);
                assert(st.ok());
                latest_seq.store(seq, std::memory_order_release);
            }
            stop.store(true, std::memory_order_release);
        });

        std::vector<std::thread> readers;
        readers.reserve(2);
        for (int i = 0; i < 2; ++i) {
            readers.emplace_back([&]() {
                RecordView out;
                size_t spin = 0;
                while (!stop.load(std::memory_order_acquire)) {
                    const uint64_t snapshot = latest_seq.load(std::memory_order_acquire);
                    if (snapshot == 0) {
                        continue;
                    }
                    if (memtable.get(as_bv("shared"), snapshot, &out)) {
                        read_hits.fetch_add(1, std::memory_order_relaxed);
                    }
                    if ((++spin & 0x3FF) == 0) {
                        std::this_thread::yield();
                    }
                }
            });
        }

        writer.join();
        for (auto& th : readers) {
            th.join();
        }

        assert(read_hits.load(std::memory_order_relaxed) > 0);
    }
} // namespace

int main() {
    std::puts("[1/7] put/get + snapshot");
    std::fflush(stdout);
    test_put_get_and_snapshot();

    std::puts("[2/7] ring(4 versions)");
    std::fflush(stdout);
    test_four_version_ring();

    std::puts("[3/7] tombstone visibility");
    std::fflush(stdout);
    test_tombstone_visibility();

    std::puts("[4/7] freeze");
    std::fflush(stdout);
    test_freeze();

    std::puts("[5/7] iterator order");
    std::fflush(stdout);
    test_iterator_order_and_visibility();

    std::puts("[6/7] ArenaGenerator::yield_all");
    std::fflush(stdout);
    test_generator_yield_all();

    std::puts("[7/7] single-writer/multi-reader stress");
    std::fflush(stdout);
    test_single_writer_multi_reader_stress();

    std::puts("memtable smoke test passed");
    return 0;
}
