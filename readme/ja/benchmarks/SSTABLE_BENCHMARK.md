# SSTable ベンチマーク

このメモは SSTable 系ベンチマークの説明と、ローカルで実行した参照値だけを残したものです。

SST の block size、cache size、Bloom filter bits-per-key、codec などの調整方針はここでは扱いません。実運用での調整は `AkkEngineOptions` など、AkkEngine の起動オプション側で管理します。

## 対象ベンチマーク

Release build で次のターゲットを使います。

```powershell
cmake --build cmake-build-release --target akkaradb_sstable_throughput_benchmark --config Release
cmake --build cmake-build-release --target akkaradb_sstable_bloom_negative_lookup_benchmark --config Release
```

`akkaradb_sstable_throughput_benchmark` は SSTWriter の write、SSTReader の point get、full scan を測ります。

`akkaradb_sstable_bloom_negative_lookup_benchmark` は、SST の key range 内にある absent key を `SSTReader::contains()` で引き、Bloom filter の negative rejection path を測ります。

## 実行例

SSTable throughput:

```powershell
cmake-build-release\bin\akkaradb_sstable_throughput_benchmark.exe 500000 --writers=16 --codec=zstd --block-size=32KiB --cache-bytes=64MiB
```

SSTable Bloom negative lookup:

```powershell
cmake-build-release\bin\akkaradb_sstable_bloom_negative_lookup_benchmark.exe 1000000 5000000 --writers=16 --bits-per-key=10 --codec=zstd --cache-bytes=64MiB
```

## SSTable throughput の出力項目

| 項目 | 意味 |
|---|---|
| `write(ops/s)` | SSTWriter が sorted records を SST file に書き出す throughput。 |
| `get(ops/s)` | 既存 key の point read throughput。 |
| `scan(ops/s)` | SSTReader の full scan throughput。 |
| `sst_bytes` | 生成された SST file bytes。 |
| `get_smp` / `scan_smp` | latency percentile 用の sample 数。 |
| `timings(ms)` | write / open / get / scan にかかった wall-clock time。 |
| `file(MiB/s)` | `sst_bytes` を各フェーズ時間で割った byte throughput。 |
| `payload(MiB/s)` | key + value payload bytes を各フェーズ時間で割った参考値。 |

## ローカル参照値: SSTable throughput

実行条件は次の通りです。

```text
ops_per_case = 500000
codec = zstd
block_size = 32768
block_cache_bytes = 64.00 MiB
max_case_payload_bytes = 512.00 MiB
reader_threads = 16
warmup_ops = 50000
```

`max_case_payload_bytes` が有効なので、大きい value のケースでは実行 ops 数が 500000 より少なくなります。

| Key | Value | Ops | Readers | Write ops/s | Get ops/s | Scan ops/s | SST bytes | Get P99/P999 | Scan P99/P999 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 500,000 | 16 | 2,335,118 | 1,979,279 | 8,907,083 | 11,878,856 | 104.40 / 1,184.90 us | 0.21 / 0.63 us |
| 16 | 64 | 500,000 | 16 | 2,115,993 | 1,987,865 | 4,562,897 | 15,246,632 | 148.90 / 375.20 us | 2.01 / 3.10 us |
| 16 | 256 | 500,000 | 16 | 1,649,446 | 1,601,752 | 4,579,337 | 17,199,432 | 187.10 / 483.70 us | 0.85 / 1.49 us |
| 32 | 1024 | 500,000 | 16 | 922,845 | 780,439 | 2,322,289 | 37,808,512 | 470.10 / 1,026.30 us | 0.91 / 2.02 us |
| 32 | 4096 | 130,055 | 16 | 284,683 | 260,575 | 573,939 | 58,003,088 | 797.60 / 1,522.20 us | 3.29 / 5.96 us |
| 64 | 16384 | 32,640 | 16 | 25,099 | 46,673 | 52,879 | 333,609,296 | 1,462.50 / 2,389.70 us | 29.19 / 31.65 us |

| Key | Value | Write ms | Open ms | Get ms | Scan ms | Write file MiB/s | Get file MiB/s | Scan file MiB/s | Write payload MiB/s | Get payload MiB/s | Scan payload MiB/s |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 214.12 | 13.56 | 252.62 | 56.14 | 52.91 | 44.84 | 201.81 | 53.45 | 45.30 | 203.87 |
| 16 | 64 | 236.30 | 7.80 | 251.53 | 109.58 | 61.53 | 57.81 | 132.69 | 161.44 | 151.66 | 348.12 |
| 16 | 256 | 303.13 | 10.63 | 312.16 | 109.19 | 54.11 | 52.55 | 150.23 | 427.87 | 415.49 | 1,187.88 |
| 32 | 1024 | 541.80 | 8.45 | 640.66 | 215.30 | 66.55 | 56.28 | 167.47 | 929.38 | 785.96 | 2,338.73 |
| 32 | 4096 | 456.84 | 10.22 | 499.11 | 226.60 | 121.08 | 110.83 | 244.11 | 1,120.73 | 1,025.82 | 2,259.46 |
| 64 | 16384 | 1,300.43 | 10.31 | 699.34 | 617.25 | 244.65 | 454.94 | 515.44 | 393.71 | 732.11 | 829.47 |

## Bloom negative lookup の出力項目

| 項目 | 意味 |
|---|---|
| `negative(ops/s)` | in-range absent key を `SSTReader::contains()` で reject する throughput。 |
| `total_ms` | negative lookup probes 全体にかかった時間。 |
| `samples` | latency percentile 用の sample 数。 |
| `wrong_present` | absent key が present と判定された回数。通常は 0。 |
| `expected_absent` | absent として処理された probe 数。 |
| `latency(us)` | negative lookup latency の percentile。 |

## ローカル参照値: Bloom negative lookup

実行条件は次の通りです。

```text
keys = 1000000
negative_keys = 999999
probes = 5000000
codec = zstd
bits_per_key = 10
block_size = 32768
block_cache_bytes = 64.00 MiB
value_size = 1
reader_threads = 16
```

| Keys | Probes | Readers | Negative ops/s | Total ms | Samples | Wrong present | Expected absent |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000,000 | 5,000,000 | 16 | 38,311,681 | 130.51 | 78,125 | 0 | 5,000,000 |

| Write ms | Open ms | SST bytes | Write file MiB/s | P50 | P90 | P99 | P999 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 331.55 | 6.52 | 18,234,768 | 52.45 | 0.20 us | 0.40 us | 0.60 us | 5.30 us |
