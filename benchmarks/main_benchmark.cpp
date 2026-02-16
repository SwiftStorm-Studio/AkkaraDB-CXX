/*
 * AkkaraDB C++ Benchmark Suite - Main Program
 *
 * Compile:
 *   g++ -std=c++20 -O3 -o akkaradb_benchmark main_benchmark.cpp \
 *       -I/path/to/akkaradb/include -L/path/to/akkaradb/lib -lakkaradb \
 *       -lpthread -lstdc++fs
 *
 * Run:
 *   ./akkaradb_benchmark --benchmark all
 *   ./akkaradb_benchmark --benchmark write
 *   ./akkaradb_benchmark --benchmark read
 *   ./akkaradb_benchmark --benchmark bloom
 *   ./akkaradb_benchmark --benchmark range
 *   ./akkaradb_benchmark --benchmark mixed
 *   ./akkaradb_benchmark --benchmark mt
 */

#include "akkaradb_benchmark_part3.hpp"
#include <cstring>
#include <ctime>
#include <iomanip>

using namespace akkaradb::benchmark;

void print_current_time() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::cout << "⏱️  " << std::put_time(std::localtime(&time_t_now), "%Y-%m-%d %H:%M:%S") << std::endl;
}

int main(int argc, char** argv) {
    BenchmarkSuite::print_header();

    std::string benchmark_type = "mixed";

    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--benchmark") == 0 && i + 1 < argc) {
            benchmark_type = argv[i + 1];
            break;
        }
    }

    std::cout << "\n🚀 Running benchmark: " << benchmark_type << std::endl;
    std::cout << "Start time: ";
    print_current_time();
    std::cout << std::endl;

    // Warmup (simulate JVM warmup)
    std::cout << "☕ Warming up..." << std::endl;
    for (int i = 0; i < BenchConfig::WARMUP_ITERATIONS; ++i) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
    std::cout << std::endl;

    try {
        if (benchmark_type == "all") { BenchmarkSuite::run_all(); }
        else if (benchmark_type == "write") { BenchmarkSuite::run_write_benchmarks(); }
        else if (benchmark_type == "read") { BenchmarkSuite::run_read_benchmarks(); }
        else if (benchmark_type == "bloom") { BenchmarkSuite::run_bloom_benchmark(); }
        else if (benchmark_type == "range") { BenchmarkSuite::run_range_benchmark(); }
        else if (benchmark_type == "mixed") { BenchmarkSuite::run_mixed_benchmark(); }
        else if (benchmark_type == "mt") { BenchmarkSuite::run_mt_benchmark(); }
        else {
            std::cerr << "Unknown benchmark type: " << benchmark_type << std::endl;
            std::cerr << "Available: all, write, read, bloom, range, mixed, mt" << std::endl;
            return 1;
        }
    }
    catch (const std::exception& e) {
        std::cerr << "\n❌ Benchmark failed with exception: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "\n✅ Benchmark completed successfully!" << std::endl;
    std::cout << "End time: ";
    print_current_time();
    std::cout << std::endl;

    return 0;
}