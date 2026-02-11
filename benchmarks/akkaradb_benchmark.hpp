/*
 * AkkaraDB C++ Benchmark Suite
 *
 * Based on JVM version (AkkaraDBBenchmark.kt)
 * Provides performance measurement tools and benchmark implementations
 */

#pragma once

#include <akkaradb/AkkaraDB.hpp>
#include <akkaradb/typed/PackedTable.hpp>
#include <vector>
#include <string>
#include <algorithm>
#include <numeric>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <random>
#include <filesystem>
#include <thread>
#include <atomic>
#include <future>

namespace akkaradb::benchmark {
    // ==================== Configuration ====================

    struct BenchConfig {
        static constexpr size_t DEFAULT_KEY_COUNT = 1'000'000;
        static constexpr size_t DEFAULT_VALUE_SIZE = 64;
        static constexpr size_t WARMUP_COUNT = 10'000;
        static constexpr int WARMUP_ITERATIONS = 3;
    };

    // ==================== Latency Statistics ====================

    struct StatsReport {
        std::string name;
        size_t count;
        double ops_per_sec;
        double p50; // microseconds
        double p75;
        double p90;
        double p95;
        double p99;
        double p999;
        double p9999;
        double max;

        std::string to_string() const {
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(1);
            oss << "=== " << name << " ===" << std::endl;
            oss << "  Total ops:    " << std::setw(12) << count << std::endl;
            oss << "  ops/sec:      " << std::setw(12) << static_cast<size_t>(ops_per_sec) << std::endl;
            oss << "  p50:          " << std::setw(8) << p50 << " µs" << std::endl;
            oss << "  p75:          " << std::setw(8) << p75 << " µs" << std::endl;
            oss << "  p90:          " << std::setw(8) << p90 << " µs" << std::endl;
            oss << "  p95:          " << std::setw(8) << p95 << " µs" << std::endl;
            oss << "  p99:          " << std::setw(8) << p99 << " µs" << std::endl;
            oss << "  p99.9:        " << std::setw(8) << p999 << " µs" << std::endl;
            oss << "  p99.99:       " << std::setw(8) << p9999 << " µs" << std::endl;
            oss << "  max:          " << std::setw(8) << max << " µs" << std::endl;
            return oss.str();
        }
    };

    class LatencyStats {
        public:
            explicit LatencyStats(std::string name) : name_{std::move(name)} { latencies_.reserve(BenchConfig::DEFAULT_KEY_COUNT); }

            void record(int64_t nanos) { latencies_.push_back(nanos); }

            void record_all(const std::vector<int64_t>& data) { latencies_.insert(latencies_.end(), data.begin(), data.end()); }

            [[nodiscard]] StatsReport report() const {
                if (latencies_.empty()) { return StatsReport{name_, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}; }

                auto sorted = latencies_;
                std::sort(sorted.begin(), sorted.end());

                const size_t count = sorted.size();
                const int64_t sum = std::accumulate(sorted.begin(), sorted.end(), 0LL);
                //const double mean = static_cast<double>(sum) / count;

                // Calculate ops/sec
                const double total_sec = static_cast<double>(sum) / 1'000'000'000.0;
                const double ops_per_sec = count / total_sec;

                // Percentile helper (nanos -> micros)
                auto percentile = [&](double p) -> double {
                    const size_t idx = std::min(
                        static_cast<size_t>((p / 100.0) * (count - 1)),
                        count - 1
                    );
                    return sorted[idx] / 1000.0; // nanos -> micros
                };

                return StatsReport{
                    .name = name_,
                    .count = count,
                    .ops_per_sec = ops_per_sec,
                    .p50 = percentile(50.0),
                    .p75 = percentile(75.0),
                    .p90 = percentile(90.0),
                    .p95 = percentile(95.0),
                    .p99 = percentile(99.0),
                    .p999 = percentile(99.9),
                    .p9999 = percentile(99.99),
                    .max = sorted.back() / 1000.0
                };
            }

        private:
            std::string name_;
            std::vector<int64_t> latencies_;
    };

    // ==================== Utility Functions ====================

    inline std::filesystem::path create_temp_dir(const std::string& prefix) {
        auto temp_dir = std::filesystem::temp_directory_path() /
            (prefix + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(temp_dir);
        return temp_dir;
    }

    inline std::vector<uint8_t> generate_key(size_t index) {
        std::string key_str = "key:" + std::string(8 - std::min(size_t(8), std::to_string(index).length()), '0') + std::to_string(index);
        return std::vector<uint8_t>(key_str.begin(), key_str.end());
    }

    inline std::vector<uint8_t> generate_value(size_t size) {
        std::vector<uint8_t> value(size);
        for (size_t i = 0; i < size; ++i) { value[i] = static_cast<uint8_t>(i % 256); }
        return value;
    }

    inline void print_separator(size_t length = 70) { std::cout << std::string(length, '=') << std::endl; }

    inline void print_dash(size_t length = 70) { std::cout << std::string(length, '-') << std::endl; }

    // ==================== Benchmark Implementations ====================

    // 1. Write Benchmark - WAL Group Commit Tuning
    class WriteWalGroupBenchmark {
        public:
            struct WalGroupConfig {
                size_t wal_group_n;
                uint64_t wal_group_micros;
                std::string description;
            };

            void run(size_t key_count = 100'000, size_t value_size = 64) {
                std::cout << "\n";
                print_separator();
                std::cout << "📊 Write Benchmark - WAL Group Commit Tuning" << std::endl;
                print_separator();
                std::cout << "Key count: " << key_count << ", Value size: " << value_size << " bytes" << std::endl;
                std::cout << std::endl;

                const std::vector<WalGroupConfig> configs = {
                    {64, 1'000, "① WalGroupN=64, Micros=1000 (excessive fsync)"},
                    {128, 5'000, "② WalGroupN=128, Micros=5000"},
                    {128, 10'000, "③ WalGroupN=128, Micros=10000"},
                    {256, 1'000, "④ WalGroupN=256, Micros=1000"},
                    {256, 10'000, "⑤ WalGroupN=256, Micros=10000"},
                    {512, 10'000, "⑥ WalGroupN=512, Micros=10000 (peak throughput)"},
                    {512, 50'000, "⑦ WalGroupN=512, Micros=50000 (recommended)"}
                };

                std::vector<std::pair<WalGroupConfig, StatsReport>> results;

                for (const auto& cfg : configs) {
                    auto base_dir = create_temp_dir("akkdb-wal-bench-");
                    LatencyStats stats(cfg.description);
                    std::vector<int64_t> latencies(key_count);

                    try {
                        auto db = AkkaraDB::open(
                            {
                                .base_dir = base_dir.string(),
                                .memtable_shard_count = 4,
                                .memtable_threshold_bytes = 64 * 1024 * 1024,
                                .wal_group_n = cfg.wal_group_n,
                                .wal_group_micros = cfg.wal_group_micros,
                                .wal_fast_mode = true,
                                .bloom_fp_rate = 0.01
                            }
                        );

                        // Warmup
                        for (size_t i = 0; i < std::min(BenchConfig::WARMUP_COUNT, key_count / 10); ++i) {
                            auto key = generate_key(i);
                            auto value = generate_value(value_size);
                            db->put(key, value);
                        }
                        db->flush();

                        // Benchmark
                        for (size_t i = 0; i < key_count; ++i) {
                            auto key = generate_key(i + 1'000'000);
                            auto value = generate_value(value_size);

                            auto start = std::chrono::high_resolution_clock::now();
                            db->put(key, value);
                            auto end = std::chrono::high_resolution_clock::now();

                            latencies[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                        }

                        db->close();
                        stats.record_all(latencies);
                        auto report = stats.report();
                        results.emplace_back(cfg, report);

                        std::cout << "✓ " << cfg.description << std::endl;
                        std::cout << "  ops/sec: " << std::setw(10) << static_cast<size_t>(report.ops_per_sec)
                            << ", p50: " << std::fixed << std::setprecision(1) << report.p50
                            << " µs, p99: " << report.p99 << " µs" << std::endl;
                        std::cout << std::endl;
                    }
                    catch (const std::exception& e) { std::cout << "✗ " << cfg.description << " - Error: " << e.what() << std::endl; }

                    std::filesystem::remove_all(base_dir);
                }

                // Summary table
                std::cout << "\n📋 Summary Table:" << std::endl;
                print_dash(100);
                std::cout << std::left << std::setw(45) << "| Configuration"
                    << std::right << std::setw(12) << "ops/sec"
                    << std::setw(10) << "p50(µs)"
                    << std::setw(10) << "p90(µs)"
                    << std::setw(10) << "p99(µs) |" << std::endl;
                print_dash(100);

                for (const auto& [cfg, report] : results) {
                    std::cout << std::left << "| " << std::setw(43) << cfg.description
                        << std::right << std::setw(12) << static_cast<size_t>(report.ops_per_sec)
                        << std::fixed << std::setprecision(1)
                        << std::setw(10) << report.p50
                        << std::setw(10) << report.p90
                        << std::setw(10) << report.p99 << " |" << std::endl;
                }
                print_dash(100);
            }
    };

    // 2. Write Benchmark - Scalability by Key Count
    class WriteScalabilityBenchmark {
        public:
            void run(size_t value_size = 64) {
                std::cout << "\n";
                print_separator();
                std::cout << "📊 Write Benchmark - Scalability by Key Count" << std::endl;
                print_separator();

                const std::vector<size_t> key_counts = {10'000, 100'000, 1'000'000};
                std::vector<std::tuple<size_t, double, StatsReport>> results;

                for (size_t key_count : key_counts) {
                    auto base_dir = create_temp_dir("akkdb-scale-bench-");
                    LatencyStats stats(std::to_string(key_count) + " keys");
                    std::vector<int64_t> latencies(key_count);

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

                        auto start_time = std::chrono::high_resolution_clock::now();

                        for (size_t i = 0; i < key_count; ++i) {
                            auto key = generate_key(i);
                            auto value = generate_value(value_size);

                            auto op_start = std::chrono::high_resolution_clock::now();
                            db->put(key, value);
                            auto op_end = std::chrono::high_resolution_clock::now();

                            latencies[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count();
                        }

                        auto end_time = std::chrono::high_resolution_clock::now();
                        double total_time = std::chrono::duration<double>(end_time - start_time).count();

                        db->close();

                        stats.record_all(latencies);
                        auto report = stats.report();
                        results.emplace_back(key_count, total_time, report);

                        std::cout << "✓ " << std::setw(10) << key_count << " keys: "
                            << std::fixed << std::setprecision(2) << total_time << "s, "
                            << std::setw(10) << static_cast<size_t>(report.ops_per_sec) << " ops/sec, "
                            << "p99: " << std::setprecision(1) << report.p99 << " µs" << std::endl;
                    }
                    catch (const std::exception& e) { std::cout << "✗ " << key_count << " keys - Error: " << e.what() << std::endl; }

                    std::filesystem::remove_all(base_dir);
                }

                std::cout << "\n📋 Summary:" << std::endl;
                print_dash(80);
                std::cout << std::left << std::setw(15) << "| Key Count"
                    << std::right << std::setw(12) << "Time(s)"
                    << std::setw(14) << "ops/sec"
                    << std::setw(12) << "p50(µs)"
                    << std::setw(12) << "p99(µs) |" << std::endl;
                print_dash(80);

                for (const auto& [count, time, report] : results) {
                    std::cout << std::left << "| " << std::setw(13) << count
                        << std::right << std::fixed << std::setprecision(2)
                        << std::setw(12) << time
                        << std::setw(14) << static_cast<size_t>(report.ops_per_sec)
                        << std::setprecision(1)
                        << std::setw(12) << report.p50
                        << std::setw(12) << report.p99 << " |" << std::endl;
                }
                print_dash(80);
            }
    };

    // 3. Write Benchmark - Impact of Value Size
    class WriteValueSizeBenchmark {
        public:
            void run(size_t key_count = 100'000) {
                std::cout << "\n";
                print_separator();
                std::cout << "📊 Write Benchmark - Impact of Value Size" << std::endl;
                print_separator();

                const std::vector<size_t> value_sizes = {16, 64, 256, 1024, 4096, 16384};
                std::vector<std::pair<size_t, StatsReport>> results;

                for (size_t value_size : value_sizes) {
                    auto base_dir = create_temp_dir("akkdb-valsize-bench-");
                    LatencyStats stats(std::to_string(value_size) + " bytes");
                    std::vector<int64_t> latencies(key_count);

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

                        // Warmup
                        for (size_t i = 0; i < std::min(BenchConfig::WARMUP_COUNT, key_count / 10); ++i) {
                            auto key = generate_key(i);
                            auto value = generate_value(value_size);
                            db->put(key, value);
                        }
                        db->flush();

                        // Benchmark
                        for (size_t i = 0; i < key_count; ++i) {
                            auto key = generate_key(i + 1'000'000);
                            auto value = generate_value(value_size);

                            auto start = std::chrono::high_resolution_clock::now();
                            db->put(key, value);
                            auto end = std::chrono::high_resolution_clock::now();

                            latencies[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                        }

                        db->close();
                        stats.record_all(latencies);
                        auto report = stats.report();
                        results.emplace_back(value_size, report);

                        double throughput_mbps = (key_count * value_size) /
                            (key_count / report.ops_per_sec) / (1024.0 * 1024.0);

                        std::cout << "✓ " << std::setw(6) << value_size << " B: "
                            << std::setw(10) << static_cast<size_t>(report.ops_per_sec) << " ops/sec, "
                            << "p99: " << std::fixed << std::setprecision(1) << std::setw(8) << report.p99 << " µs, "
                            << throughput_mbps << " MB/s" << std::endl;
                    }
                    catch (const std::exception& e) { std::cout << "✗ " << value_size << " B - Error: " << e.what() << std::endl; }

                    std::filesystem::remove_all(base_dir);
                }

                std::cout << "\n📋 Summary:" << std::endl;
                print_dash(80);
                std::cout << std::left << std::setw(15) << "| Value Size"
                    << std::right << std::setw(14) << "ops/sec"
                    << std::setw(12) << "p50(µs)"
                    << std::setw(12) << "p99(µs)"
                    << std::setw(18) << "Throughput(MB/s) |" << std::endl;
                print_dash(80);

                for (const auto& [size, report] : results) {
                    double throughput_mbps = (100'000 * size) /
                        (100'000.0 / report.ops_per_sec) / (1024.0 * 1024.0);

                    std::cout << std::left << "| " << std::setw(13) << size
                        << std::right << std::setw(14) << static_cast<size_t>(report.ops_per_sec)
                        << std::fixed << std::setprecision(1)
                        << std::setw(12) << report.p50
                        << std::setw(12) << report.p99
                        << std::setw(18) << throughput_mbps << " |" << std::endl;
                }
                print_dash(80);
            }
    };
} // namespace akkaradb::benchmark