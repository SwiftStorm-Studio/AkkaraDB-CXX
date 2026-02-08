/*
 * AkkaraDB C++ Benchmark Suite - Part 3
 * Multi-thread Benchmark and Main Program
 */

#pragma once

#include "akkaradb_benchmark_part2.hpp"
#include <thread>
#include <latch>

namespace akkaradb::benchmark {
    // 8. Multi-threaded Scalability Benchmark
    class MultiThreadBenchmark {
        public:
            void run(size_t ops_per_thread = 100'000, size_t value_size = 64) {
                std::cout << "\n";
                print_separator();
                std::cout << "📊 Multi-threaded Scalability Benchmark" << std::endl;
                print_separator();

                const std::vector<int> thread_counts = {1, 2, 4, 8, 16};
                std::vector<std::pair<int, double>> results;

                for (int threads : thread_counts) {
                    auto base_dir = create_temp_dir("akkdb-mt-bench-");

                    try {
                        auto db = akkaradb::AkkaraDB::open(
                            {
                                .base_dir = base_dir.string(),
                                .memtable_shard_count = 4,
                                .memtable_threshold_bytes = 64 * 1024 * 1024,
                                .wal_group_n = 512,
                                .wal_group_micros = 50'000,
                                .wal_fast_mode = true,
                                .bloom_fp_rate = 0.01
                            }
                        );

                        std::latch latch(threads);
                        std::atomic<size_t> total_ops{0};

                        auto start_time = std::chrono::high_resolution_clock::now();

                        std::vector<std::thread> workers;
                        for (int tid = 0; tid < threads; ++tid) {
                            workers.emplace_back(
                                [&, tid]() {
                                    try {
                                        for (size_t i = 0; i < ops_per_thread; ++i) {
                                            auto key = generate_key(tid * ops_per_thread + i);
                                            auto value = generate_value(value_size);
                                            db->put(key, value);
                                            total_ops.fetch_add(1, std::memory_order_relaxed);
                                        }
                                    }
                                    catch (const std::exception& e) { std::cerr << "Thread " << tid << " error: " << e.what() << std::endl; }
                                    latch.count_down();
                                }
                            );
                        }

                        latch.wait();
                        auto end_time = std::chrono::high_resolution_clock::now();

                        for (auto& worker : workers) { if (worker.joinable()) { worker.join(); } }

                        double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
                        double ops_per_sec = total_ops.load() / elapsed_sec;

                        db->close();

                        results.emplace_back(threads, ops_per_sec);

                        std::cout << "✓ " << std::setw(2) << threads << " threads: "
                            << std::setw(12) << static_cast<size_t>(ops_per_sec) << " ops/sec"
                            << std::endl;
                    }
                    catch (const std::exception& e) { std::cout << "✗ " << threads << " threads - Error: " << e.what() << std::endl; }

                    std::filesystem::remove_all(base_dir);
                }

                const double baseline = results.empty()
                                            ? 1.0
                                            : results[0].second;

                std::cout << "\n📋 Summary:" << std::endl;
                print_dash(60);
                std::cout << std::left << std::setw(12) << "| Threads"
                    << std::right << std::setw(16) << "ops/sec"
                    << std::setw(14) << "Scale"
                    << std::setw(12) << "Efficiency |" << std::endl;
                print_dash(60);

                for (const auto& [threads, ops] : results) {
                    double scale = ops / baseline;
                    double efficiency = (scale / threads) * 100.0;

                    std::cout << std::left << "| " << std::setw(10) << threads
                        << std::right << std::setw(16) << static_cast<size_t>(ops)
                        << std::fixed << std::setprecision(2)
                        << std::setw(14) << scale << "x"
                        << std::setw(11) << efficiency << "% |" << std::endl;
                }
                print_dash(60);
            }
    };

    // ==================== Benchmark Suite Runner ====================

    class BenchmarkSuite {
        public:
            static void print_header() {
                std::cout << R"(
╔══════════════════════════════════════════════════════════════════════╗
║           AkkaraDB C++ Benchmark Suite                               ║
╠══════════════════════════════════════════════════════════════════════╣
║  Usage:                                                              ║
║    --benchmark all         Run all benchmarks                        ║
║    --benchmark write       Write benchmarks                          ║
║    --benchmark read        Read benchmarks                           ║
║    --benchmark bloom       Bloom filter benchmark                    ║
║    --benchmark range       Range scan benchmark                      ║
║    --benchmark mixed       Mixed workload benchmark                  ║
║    --benchmark mt          Multi-threaded benchmark                  ║
╚══════════════════════════════════════════════════════════════════════╝
)" << std::endl;
            }

            static void run_all() {
                std::cout << "\n🚀 Running all benchmarks..." << std::endl;

                WriteWalGroupBenchmark().run();
                WriteScalabilityBenchmark().run();
                WriteValueSizeBenchmark().run();
                ReadBenchmark().run();
                BloomFilterBenchmark().run();
                RangeScanBenchmark().run();
                MixedWorkloadBenchmark().run();
                MultiThreadBenchmark().run();

                std::cout << "\n✅ All benchmarks completed!" << std::endl;
            }

            static void run_write_benchmarks() {
                std::cout << "\n🚀 Running write benchmarks..." << std::endl;

                WriteWalGroupBenchmark().run();
                WriteScalabilityBenchmark().run();
                WriteValueSizeBenchmark().run();

                std::cout << "\n✅ Write benchmarks completed!" << std::endl;
            }

            static void run_read_benchmarks() {
                std::cout << "\n🚀 Running read benchmarks..." << std::endl;
                ReadBenchmark().run();
                std::cout << "\n✅ Read benchmarks completed!" << std::endl;
            }

            static void run_bloom_benchmark() {
                std::cout << "\n🚀 Running Bloom filter benchmark..." << std::endl;
                BloomFilterBenchmark().run();
                std::cout << "\n✅ Bloom filter benchmark completed!" << std::endl;
            }

            static void run_range_benchmark() {
                std::cout << "\n🚀 Running range scan benchmark..." << std::endl;
                RangeScanBenchmark().run();
                std::cout << "\n✅ Range scan benchmark completed!" << std::endl;
            }

            static void run_mixed_benchmark() {
                std::cout << "\n🚀 Running mixed workload benchmark..." << std::endl;
                MixedWorkloadBenchmark().run();
                std::cout << "\n✅ Mixed workload benchmark completed!" << std::endl;
            }

            static void run_mt_benchmark() {
                std::cout << "\n🚀 Running multi-threaded benchmark..." << std::endl;
                MultiThreadBenchmark().run();
                std::cout << "\n✅ Multi-threaded benchmark completed!" << std::endl;
            }
    };
} // namespace akkaradb::benchmark