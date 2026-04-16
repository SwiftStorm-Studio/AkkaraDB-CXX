# AkkaraDB - Benchmark Notes

> Version 0.5.0 - C++23 - Copyright 2026 Swift Storm Studio - AGPL-3.0

---

## Table of Contents

1. [Overview](#1-overview)
2. [What the Benchmark Binary Covers](#2-what-the-benchmark-binary-covers)
3. [How to Build and Run](#3-how-to-build-and-run)
4. [Methodology](#4-methodology)
5. [Current Results](#5-current-results)
6. [Result Interpretation](#6-result-interpretation)
7. [Reproducibility Notes](#7-reproducibility-notes)

---

## 1. Overview

`akkaradb_benchmark` is a combined correctness + microbenchmark executable.
It serves two purposes:

- Verify that core engine features still behave correctly after code changes.
- Measure the cost of the hot paths that matter most for AkkaraDB:
  in-memory put/get, WAL-backed writes, SST lookups, and typed API overhead.

---

## 2. What the Benchmark Binary Covers

The executable currently runs **20 steps**: **17 correctness tests** and
**3 benchmark sections**.

### 2.1 Correctness Coverage

| Area       | Checks                                                        |
|------------|---------------------------------------------------------------|
| MemTable   | Basic put/get/remove, many keys                               |
| WAL        | `SyncMode::Off`, crash recovery, async mode, WAL truncation   |
| Blob       | Large-value roundtrip, no false BlobRef detection             |
| Lifecycle  | Idempotent `close()`                                          |
| VersionLog | History, `get_at`, `rollback_key`, `rollback_to`, persistence |
| SST        | Flush/reopen reads, tombstones, compaction, overwrite dedup   |

The benchmark binary is therefore useful as a fast smoke test even when the
performance numbers themselves are not the primary goal.

### 2.2 Benchmark Coverage

| Benchmark              | Purpose                                                                                                |
|------------------------|--------------------------------------------------------------------------------------------------------|
| `bench_throughput`     | Measure raw in-memory throughput across inline and heap-backed record sizes, plus WAL async throughput |
| `bench_sst_lookup`     | Measure SST negative lookups, positive `exists()`, and positive `get()` throughput                     |
| `bench_api_comparison` | Quantify overhead of the typed `PackedTable` API relative to raw `AkkEngine` access                    |

---

## 3. How to Build and Run

### 3.1 Build

The benchmark target is always built as `akkaradb_benchmark`.

```bash
cmake -S . -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release --target akkaradb_benchmark --config Release
```

### 3.2 Run

```bash
./cmake-build-release/bin/akkaradb_benchmark
```

On Windows with a multi-config generator, the executable is typically:

```text
cmake-build-release/bin/akkaradb_benchmark.exe
```

The benchmark prints each correctness step first, then the performance tables,
then a final pass/fail summary.

---

## 4. Methodology

### 4.1 General Rules

- Timed sections exclude one-time key/value generation where practical.
- Warmup loops are used before timing to reduce cold-cache distortion.
- Temporary benchmark directories are created per scenario and removed after use.
- `ops/s` is computed from wall-clock elapsed time of the timed loop only.
- A non-zero `wrong_answers` count is treated as a correctness bug, not as a
  performance artifact.

### 4.2 Throughput Benchmark

The in-memory throughput benchmark measures single-process engine performance
across several key/value sizes.

`SmallBuffer::INLINE_CAP = 22 bytes`, so the benchmark deliberately includes:

- Inline cases: total key+value payload `<= 22 B`
- Heap cases: total key+value payload `> 22 B`

The current size cases are:

| Case         | Key  | Value  | Total  | Storage Path | Operations |
|--------------|------|--------|--------|--------------|------------|
| `k=8  v=8`   | 8 B  | 8 B    | 16 B   | inline       | 5.0 M      |
| `k=11 v=11`  | 11 B | 11 B   | 22 B   | inline       | 5.0 M      |
| `k=16 v=64`  | 16 B | 64 B   | 80 B   | heap         | 5.0 M      |
| `k=32 v=256` | 32 B | 256 B  | 288 B  | heap         | 2.0 M      |
| `k=32 v=1k`  | 32 B | 1024 B | 1056 B | heap         | 1.0 M      |

The same section also measures WAL-backed throughput in `SyncMode::Async`
using the `k=11 / v=11` inline-sized workload.

### 4.3 SST Lookup Benchmark

The SST benchmark isolates read-path behavior from a single flushed SST.

Setup:

- `1,000,000` keys are inserted and flushed to SST.
- `sst_preload_data = true` keeps the SST data section in RAM.
- The benchmark does **not** reopen the database after flush, because a reopen
  would replay the WAL and repopulate MemTable, which would invalidate the SST-only path.

Three lookup modes are measured:

| Mode                | Meaning                                                    |
|---------------------|------------------------------------------------------------|
| Negative `exists()` | Bloom filter rejects absent keys                           |
| Positive `exists()` | Bloom pass + index seek + record scan, no value copy       |
| Positive `get()`    | Same as positive `exists()` plus CRC/value materialization |

Negative keys are interleaved with existing keys in the same lexical range so
they still exercise the bloom filter instead of short-circuiting on a range miss.

### 4.4 API Comparison Benchmark

This benchmark compares three access patterns over `1,000,000` in-memory ops:

| Path         | Meaning                                                                  |
|--------------|--------------------------------------------------------------------------|
| `raw-inline` | `AkkEngine::put/get_into` with 8 B key + 8 B value                       |
| `raw-heap`   | `AkkEngine::put/get_into` with 16 B key + 16 B value                     |
| `typed`      | `PackedTable<&KvRecord::id>::put/get` with runtime BinPack encode/decode |

The benchmark is designed to separate:

- Heap allocation overhead: `raw-heap` vs `raw-inline`
- Typed API overhead: `typed` vs `raw-heap`
- Total abstraction cost: `typed` vs `raw-inline`

---

## 5. Current Results

The following results are from the current `akkaradb-native` benchmark run
shared in this repository context.

### 5.1 Correctness Summary

| Metric    | Result            |
|-----------|-------------------|
| Steps     | 20 / 20 completed |
| Checks    | 71 / 71 passed    |
| Exit code | 0                 |

### 5.2 In-Memory Throughput

| Case               | Total Payload | Storage | N     | Write ops/s | Read ops/s |
|--------------------|---------------|---------|-------|-------------|------------|
| `[mem] k=8  v=8`   | 16 B          | inline  | 5.0 M | 7,631,672   | 5,918,729  |
| `[mem] k=11 v=11`  | 22 B          | inline  | 5.0 M | 6,720,298   | 4,472,465  |
| `[mem] k=16 v=64`  | 80 B          | heap    | 5.0 M | 4,528,993   | 3,746,110  |
| `[mem] k=32 v=256` | 288 B         | heap    | 2.0 M | 2,998,134   | 3,174,423  |
| `[mem] k=32 v=1k`  | 1056 B        | heap    | 1.0 M | 1,494,787   | 2,734,551  |

### 5.3 WAL Async Throughput

| Case              | Total Payload | N     | Write ops/s | Read ops/s |
|-------------------|---------------|-------|-------------|------------|
| `[wal] k=11 v=11` | 22 B inline   | 5.0 M | 4,393,075   | 3,626,907  |

Read validation reported `found = 5,000,000`.

### 5.4 SST Lookup Throughput

| Case                | Keys      | Probes    | Throughput       | Wrong Answers |
|---------------------|-----------|-----------|------------------|---------------|
| Negative `exists()` | 1,000,000 | 5,000,000 | 12,891,446 ops/s | 0             |
| Positive `exists()` | 1,000,000 | 5,000,000 | 1,747,877 ops/s  | 0             |
| Positive `get()`    | 1,000,000 | 5,000,000 | 1,583,018 ops/s  | 0             |

### 5.5 Raw API vs Typed API

| Case         | Key  | Value | N     | Write ops/s | Read ops/s |
|--------------|------|-------|-------|-------------|------------|
| `raw-inline` | 8 B  | 8 B   | 1.0 M | 7,203,319   | 6,204,952  |
| `raw-heap`   | 16 B | 16 B  | 1.0 M | 5,630,593   | 4,189,970  |
| `typed`      | 16 B | 16 B  | 1.0 M | 3,234,418   | 2,050,399  |

Derived slowdown ratios:

| Comparison      | Write        | Read         |
|-----------------|--------------|--------------|
| Heap vs inline  | 1.28x slower | 1.48x slower |
| Typed vs heap   | 1.74x slower | 2.04x slower |
| Typed vs inline | 2.23x slower | 3.03x slower |

---

## 6. Result Interpretation

### 6.1 Memory Path

- The inline path is the fastest configuration, as expected.
- Crossing the `22 B` inline threshold causes a clear drop from inline to heap-backed records.
- Read throughput degrades more gently than write throughput as values grow, which is
  consistent with allocation and copy costs dominating the write path first.

### 6.2 WAL Path

- Async WAL preserves a large fraction of the in-memory throughput.
- The current `k=11 / v=11` inline workload reaches about `65%` of the pure in-memory
  write throughput (`4.39M / 6.72M`), which is a strong result for durable logging.

### 6.3 SST Path

- Negative lookups are extremely fast because bloom rejection avoids deeper SST work.
- Positive `exists()` is much slower than negative `exists()` because it must pass bloom,
  seek the index, and scan the data path.
- Positive `get()` is slower than positive `exists()` because it includes value retrieval
  and buffer materialization, not only existence testing.

### 6.4 Typed API Cost

- The typed API is not free, and the benchmark makes that visible.
- Most of the additional cost comes from BinPack encode/decode and the extra object/value
  transformation work relative to the raw byte API.
- This is an acceptable trade-off when schema convenience matters more than absolute throughput,
  but it should not be mistaken for a zero-overhead wrapper.

---

## 7. Reproducibility Notes

Benchmark numbers are environment-dependent. When comparing runs, keep the following stable:

- Build type: use `Release`
- Compiler and version
- SIMD setting (`AKKARADB_ENABLE_SIMD`)
- CPU model, clock behavior, and thermal state
- Storage device and filesystem
- Background system load

The benchmark is best used for:

- Before/after comparisons on the same machine
- Regression detection after engine changes
- Quantifying the cost of a new feature or abstraction

It is not designed to be a universal cross-machine leaderboard.
