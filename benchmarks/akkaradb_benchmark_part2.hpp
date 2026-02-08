/*
 * AkkaraDB C++ Benchmark Suite - Part 2
 * Read, Bloom Filter, Range, Mixed Workload Benchmarks
 */

#pragma once

#include "akkaradb_benchmark.hpp"

namespace akkaradb::benchmark {
    // 4. Read Benchmark - MemTable vs SST
    class ReadBenchmark {
        public:
            void run(size_t key_count = 100'000, size_t value_size = 64) {
                std::cout << "\n";
                print_separator();
                std::cout << "📊 Read Benchmark - MemTable vs SST" << std::endl;
                print_separator();

                run_memtable_read(key_count, value_size);
                run_sst_read(key_count, value_size);
            }

        private:
            void run_memtable_read(size_t key_count, size_t value_size) {
                std::cout << "\n--- MemTable Read ---" << std::endl;
                auto base_dir = create_temp_dir("akkdb-read-mem-");
                LatencyStats stats("MemTable Read");
                std::vector<int64_t> latencies(key_count);

                try {
                    auto db = akkaradb::AkkaraDB::open(
                        {
                            .base_dir = base_dir.string(),
                            .memtable_shard_count = 4,
                            .memtable_threshold_bytes = 256 * 1024 * 1024,
                            // Large to keep in MemTable
                            .wal_group_n = 512,
                            .wal_group_micros = 50'000,
                            .wal_fast_mode = true,
                            .bloom_fp_rate = 0.01
                        }
                    );

                    // Insert data (keep in MemTable by not flushing)
                    for (size_t i = 0; i < key_count; ++i) {
                        auto key = generate_key(i);
                        auto value = generate_value(value_size);
                        db->put(key, value);
                    }

                    // Warmup
                    std::random_device rd;
                    std::mt19937 gen(rd());
                    std::uniform_int_distribution<size_t> dist(0, key_count - 1);

                    for (size_t i = 0; i < std::min(BenchConfig::WARMUP_COUNT, key_count / 10); ++i) {
                        auto key = generate_key(dist(gen));
                        db->get(key);
                    }

                    // Benchmark (random reads)
                    std::vector<size_t> indices(key_count);
                    std::iota(indices.begin(), indices.end(), 0);
                    std::shuffle(indices.begin(), indices.end(), gen);

                    for (size_t i = 0; i < key_count; ++i) {
                        auto key = generate_key(indices[i]);

                        auto start = std::chrono::high_resolution_clock::now();
                        db->get(key);
                        auto end = std::chrono::high_resolution_clock::now();

                        latencies[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                    }

                    db->close();
                    stats.record_all(latencies);
                    auto report = stats.report();

                    std::cout << report.to_string() << std::endl;
                }
                catch (const std::exception& e) { std::cout << "Error: " << e.what() << std::endl; }

                std::filesystem::remove_all(base_dir);
            }

            void run_sst_read(size_t key_count, size_t value_size) {
                std::cout << "\n--- SST Read (Bloom Filter Enabled) ---" << std::endl;
                auto base_dir = create_temp_dir("akkdb-read-sst-");
                LatencyStats stats("SST Read");
                std::vector<int64_t> latencies(key_count);

                try {
                    // First DB instance: write and flush
                    {
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

                        for (size_t i = 0; i < key_count; ++i) {
                            auto key = generate_key(i);
                            auto value = generate_value(value_size);
                            db->put(key, value);
                        }
                        db->flush(); // Force to SST
                        db->close();
                    }

                    // Second DB instance: read from SST
                    auto db2 = akkaradb::AkkaraDB::open(
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

                    // Warmup
                    std::random_device rd;
                    std::mt19937 gen(rd());
                    std::uniform_int_distribution<size_t> dist(0, key_count - 1);

                    for (size_t i = 0; i < std::min(BenchConfig::WARMUP_COUNT, key_count / 10); ++i) {
                        auto key = generate_key(dist(gen));
                        db2->get(key);
                    }

                    // Benchmark (random reads)
                    std::vector<size_t> indices(key_count);
                    std::iota(indices.begin(), indices.end(), 0);
                    std::shuffle(indices.begin(), indices.end(), gen);

                    for (size_t i = 0; i < key_count; ++i) {
                        auto key = generate_key(indices[i]);

                        auto start = std::chrono::high_resolution_clock::now();
                        db2->get(key);
                        auto end = std::chrono::high_resolution_clock::now();

                        latencies[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                    }

                    db2->close();
                    stats.record_all(latencies);
                    auto report = stats.report();

                    std::cout << report.to_string() << std::endl;
                }
                catch (const std::exception& e) { std::cout << "Error: " << e.what() << std::endl; }

                std::filesystem::remove_all(base_dir);
            }
    };

    // 5. Bloom Filter Effectiveness Benchmark
    class BloomFilterBenchmark {
        public:
            void run(size_t key_count = 100'000) {
                std::cout << "\n";
                print_separator();
                std::cout << "📊 Bloom Filter Effectiveness Benchmark" << std::endl;
                print_separator();

                auto base_dir = create_temp_dir("akkdb-bloom-bench-");

                try {
                    // Write data
                    {
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

                        // Insert keys: key:00000000 ~ key:00099999
                        for (size_t i = 0; i < key_count; ++i) {
                            auto key = generate_key(i);
                            auto value = generate_value(64);
                            db->put(key, value);
                        }
                        db->flush();
                        db->close();
                    }

                    // Reopen and search for non-existent keys
                    auto db2 = akkaradb::AkkaraDB::open(
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

                    LatencyStats stats("Negative lookup (Bloom enabled)");
                    std::vector<int64_t> latencies(key_count);
                    size_t false_positives = 0;

                    // Search for non-existent keys: key:10000000 ~ key:10099999
                    for (size_t i = 0; i < key_count; ++i) {
                        auto key = generate_key(i + 10'000'000); // Non-existent

                        auto start = std::chrono::high_resolution_clock::now();
                        auto result = db2->get(key);
                        auto end = std::chrono::high_resolution_clock::now();

                        if (result.has_value()) { false_positives++; }

                        latencies[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                    }

                    db2->close();
                    stats.record_all(latencies);
                    auto report = stats.report();

                    std::cout << report.to_string();
                    std::cout << "  False Positive Count: " << false_positives << " / " << key_count << std::endl;
                    std::cout << "  False Positive Rate:  " << std::fixed << std::setprecision(2)
                        << (false_positives * 100.0 / key_count) << "%" << std::endl;
                }
                catch (const std::exception& e) { std::cout << "Error: " << e.what() << std::endl; }

                std::filesystem::remove_all(base_dir);
            }
    };

    // 6. Range Scan Benchmark
    class RangeScanBenchmark {
        public:
            void run(size_t key_count = 100'000) {
                std::cout << "\n";
                print_separator();
                std::cout << "📊 Range Scan Benchmark" << std::endl;
                print_separator();

                const std::vector<size_t> range_sizes = {100, 1'000, 10'000, 100'000};
                std::vector<std::tuple<size_t, int64_t, double>> results;

                auto base_dir = create_temp_dir("akkdb-range-bench-");

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

                    // Insert data
                    for (size_t i = 0; i < key_count; ++i) {
                        auto key = generate_key(i);
                        auto value = generate_value(64);
                        db->put(key, value);
                    }
                    db->flush();

                    for (size_t range_size : range_sizes) {
                        if (range_size > key_count) continue;

                        auto start_key = generate_key(0);
                        auto end_key = generate_key(range_size);

                        size_t count = 0;
                        auto start = std::chrono::high_resolution_clock::now();

                        auto records = db->range(start_key, end_key);
                        for (const auto& rec : records) { count++; }

                        auto end = std::chrono::high_resolution_clock::now();
                        int64_t elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                        int64_t elapsed_ms = elapsed_ns / 1'000'000;
                        double avg_per_entry = static_cast<double>(elapsed_ns) / count / 1000.0; // µs

                        results.emplace_back(range_size, elapsed_ms, avg_per_entry);

                        std::cout << "✓ Range size " << std::setw(6) << range_size << ": "
                            << std::setw(6) << elapsed_ms << " ms total, "
                            << std::fixed << std::setprecision(1) << avg_per_entry << " µs/entry"
                            << std::endl;
                    }

                    db->close();

                    std::cout << "\n📋 Summary:" << std::endl;
                    print_dash(60);
                    std::cout << std::left << std::setw(15) << "| Range Size"
                        << std::right << std::setw(14) << "Time(ms)"
                        << std::setw(20) << "Avg/Entry(µs) |" << std::endl;
                    print_dash(60);

                    for (const auto& [size, time_ms, avg_us] : results) {
                        std::cout << std::left << "| " << std::setw(13) << size
                            << std::right << std::setw(14) << time_ms
                            << std::fixed << std::setprecision(1)
                            << std::setw(20) << avg_us << " |" << std::endl;
                    }
                    print_dash(60);
                }
                catch (const std::exception& e) { std::cout << "Error: " << e.what() << std::endl; }

                std::filesystem::remove_all(base_dir);
            }
    };

    // 7. Mixed Workload Benchmark
    class MixedWorkloadBenchmark {
        public:
            struct MixedResult {
                int read_pct;
                int write_pct;
                double total_ops_per_sec;
                double read_p99;
                double write_p99;
            };

            void run(size_t total_ops = 1'000'000, size_t value_size = 64) {
                std::cout << "\n";
                print_separator();
                std::cout << "📊 Mixed Workload Benchmark" << std::endl;
                print_separator();

                const std::vector<std::pair<int, int>> ratios = {
                    {100, 0},
                    // 100% read
                    {80, 20},
                    // 80% read, 20% write
                    {50, 50},
                    // 50/50
                    {20, 80},
                    // 20% read, 80% write
                    {0, 100} // 100% write
                };

                std::vector<MixedResult> results;

                for (const auto& [read_pct, write_pct] : ratios) {
                    auto base_dir = create_temp_dir("akkdb-mixed-bench-");

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

                        // Seed data
                        const size_t seed_count = 100'000;
                        for (size_t i = 0; i < seed_count; ++i) {
                            auto key = generate_key(i);
                            auto value = generate_value(value_size);
                            db->put(key, value);
                        }

                        LatencyStats read_stats("Read (" + std::to_string(read_pct) + "%)");
                        LatencyStats write_stats("Write (" + std::to_string(write_pct) + "%)");
                        std::vector<int64_t> read_latencies;
                        std::vector<int64_t> write_latencies;

                        std::mt19937 gen(42);
                        std::uniform_int_distribution<int> ratio_dist(0, 99);
                        std::uniform_int_distribution<size_t> key_dist(0, seed_count - 1);

                        size_t write_key_idx = seed_count;

                        auto start_time = std::chrono::high_resolution_clock::now();

                        for (size_t i = 0; i < total_ops; ++i) {
                            if (ratio_dist(gen) < read_pct) {
                                // Read operation
                                auto key = generate_key(key_dist(gen));

                                auto op_start = std::chrono::high_resolution_clock::now();
                                db->get(key);
                                auto op_end = std::chrono::high_resolution_clock::now();

                                read_latencies.push_back(
                                    std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count()
                                );
                            }
                            else {
                                // Write operation
                                auto key = generate_key(write_key_idx++);
                                auto value = generate_value(value_size);

                                auto op_start = std::chrono::high_resolution_clock::now();
                                db->put(key, value);
                                auto op_end = std::chrono::high_resolution_clock::now();

                                write_latencies.push_back(
                                    std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count()
                                );
                            }
                        }

                        auto end_time = std::chrono::high_resolution_clock::now();
                        double total_time = std::chrono::duration<double>(end_time - start_time).count();
                        double total_ops_per_sec = total_ops / total_time;

                        db->close();

                        read_stats.record_all(read_latencies);
                        write_stats.record_all(write_latencies);

                        auto read_report = read_stats.report();
                        auto write_report = write_stats.report();

                        results.push_back(
                            {
                                read_pct,
                                write_pct,
                                total_ops_per_sec,
                                read_report.p99,
                                write_report.p99
                            }
                        );

                        std::cout << "✓ Read " << std::setw(3) << read_pct << "% / Write " << std::setw(3) << write_pct << "%: "
                            << std::setw(10) << static_cast<size_t>(total_ops_per_sec) << " ops/sec, "
                            << "Read p99: " << std::fixed << std::setprecision(1) << read_report.p99 << " µs, "
                            << "Write p99: " << write_report.p99 << " µs" << std::endl;
                    }
                    catch (const std::exception& e) {
                        std::cout << "✗ Read " << read_pct << "% / Write " << write_pct << "% - Error: " << e.what() << std::endl;
                    }

                    std::filesystem::remove_all(base_dir);
                }

                std::cout << "\n📋 Summary:" << std::endl;
                print_dash(80);
                std::cout << std::left << std::setw(10) << "| Read%"
                    << std::setw(10) << "Write%"
                    << std::right << std::setw(16) << "Total ops/sec"
                    << std::setw(16) << "Read p99(µs)"
                    << std::setw(16) << "Write p99(µs) |" << std::endl;
                print_dash(80);

                for (const auto& r : results) {
                    std::cout << std::left << "| " << std::setw(8) << r.read_pct
                        << std::setw(10) << r.write_pct
                        << std::right << std::setw(16) << static_cast<size_t>(r.total_ops_per_sec)
                        << std::fixed << std::setprecision(1)
                        << std::setw(16) << r.read_p99
                        << std::setw(16) << r.write_p99 << " |" << std::endl;
                }
                print_dash(80);
            }
    };
} // namespace akkaradb::benchmark