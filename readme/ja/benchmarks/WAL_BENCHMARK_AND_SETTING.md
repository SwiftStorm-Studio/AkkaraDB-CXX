# WAL ベンチマーク

このメモは `akkaradb_wal_throughput_benchmark` の説明と、ローカルで実行した参照値だけを残したものです。

WAL の group size、pending bytes、sync mode、shard 数などの調整方針はここでは扱いません。実運用での調整は `AkkEngineOptions` など、AkkEngine の起動オプション側で管理します。

## 対象ベンチマーク

Release build で次のターゲットを使います。

```powershell
cmake --build cmake-build-release --target akkaradb_wal_throughput_benchmark --config Release
```

このベンチマークは `WalWriter` 単体で、固定された key/value サイズのケースを走らせます。測定対象は append、close まで含めた finish、WAL recovery です。

## 実行例

```powershell
cmake-build-release\bin\akkaradb_wal_throughput_benchmark.exe 200000 --writers=16 --sync=async
```

ローカル参照値として残している実行は次の条件です。

```powershell
cmake-build-release\bin\akkaradb_wal_throughput_benchmark.exe 200000 --writers=16 --sync=async --group-n=8192 --group-micros=2000 --group-bytes=4MiB --max-pending-bytes=64MiB
```

## 出力項目

| 項目 | 意味 |
|---|---|
| `append(ops/s)` | `WalWriter::append()` 呼び出しの throughput。 |
| `finish(ops/s)` | append 開始から `close()` 完了までを含めた throughput。 |
| `recover(ops/s)` | 生成された WAL を `WalRecovery::recover()` で replay する throughput。 |
| `wal_bytes` | 実際に生成された WAL file bytes の合計。 |
| `app_smp` | append latency percentile 用の sample 数。 |
| `append_ms` | append loop にかかった時間。 |
| `close_ms` | append 後、`close()` が queue drain / header update / sync を終えるまでの時間。 |
| `total_ms` | append 開始から close 完了までの時間。`finish(ops/s)` の分母。 |
| `recover_ms` | recovery にかかった時間。 |
| `throughput(MiB/s)` | `wal_bytes` を各フェーズ時間で割った byte throughput。 |

## ローカル参照値

実行条件は次の通りです。

```text
ops_per_case = 200000
sync_mode = async
writer_threads = 16
resolved_shards = 16
group_n = 8192
group_micros = 2000
group_bytes = 4.00 MiB
max_pending_bytes = 64.00 MiB
prehash_mode = OFF
```

| Key | Value | WAL bytes | Append ops/s | Finish ops/s | Recover ops/s | Append ms | Close ms | Total ms | Recover ms | Append MiB/s | Finish MiB/s | Recover MiB/s | P99/P999 append latency |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 11,200,768 | 10,071,406 | 360,365 | 3,844,978 | 19.86 | 535.13 | 554.99 | 52.02 | 537.91 | 19.25 | 205.36 | 10.40 / 129.30 us |
| 16 | 64 | 22,400,768 | 9,574,325 | 652,831 | 2,235,554 | 20.89 | 285.47 | 306.36 | 89.46 | 1,022.68 | 69.73 | 238.79 | 18.20 / 166.50 us |
| 16 | 256 | 60,800,768 | 7,540,824 | 456,964 | 580,491 | 26.52 | 411.15 | 437.67 | 344.54 | 2,186.24 | 132.48 | 168.30 | 12.10 / 230.30 us |
| 32 | 1024 | 217,600,768 | 3,799,529 | 184,473 | 937,425 | 52.64 | 1,031.53 | 1,084.17 | 213.35 | 3,942.40 | 191.41 | 972.67 | 15.90 / 696.00 us |
| 32 | 4096 | 832,000,768 | 314,435 | 103,059 | 297,584 | 636.06 | 1,304.57 | 1,940.63 | 672.08 | 1,247.45 | 408.87 | 1,180.60 | 105.50 / 1,914.70 us |
| 64 | 16384 | 3,296,003,072 | 8,734 | 8,513 | 82,534 | 22,897.77 | 594.87 | 23,492.64 | 2,423.25 | 137.28 | 133.80 | 1,297.15 | 1,030.90 / 5,795.90 us |
