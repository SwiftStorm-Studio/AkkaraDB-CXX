# MemTable B+Tree / ART ベンチマーク

このメモは `akkaradb_memtable_throughput_benchmark` の説明と、ローカルで実行した参照値だけを残したものです。

MemTable の並列度、shard 数、flush threshold などの調整方針はここでは扱いません。実運用での調整は `AkkEngineOptions` など、AkkEngine の起動オプション側で管理します。

## 対象ベンチマーク

Release build で次のターゲットを使います。

```powershell
cmake --build cmake-build-release --target akkaradb_memtable_throughput_benchmark --config Release
```

このベンチマークは MemTable backend ごとに、固定された key/value サイズのケースを走らせます。測定対象は `put`、既存 key の `get`、全件 `scan` です。

## 実行例

MemTable 全体の通常ケースを見る実行例です。

```powershell
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 500000 --backend=art
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 500000 --backend=bptree
```

backend 単体の傾向を見るための実行例です。

```powershell
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 50000 --writers=1 --backend=art --shards=1 --prehash
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 50000 --writers=1 --backend=bptree --shards=1 --prehash
```

## 出力項目

| 項目 | 意味 |
|---|---|
| `put(ops/s)` | MemTable への insert / update throughput。 |
| `get(ops/s)` | 既存 key の point lookup throughput。 |
| `scan(ops/s)` | snapshot iterator による全件 scan throughput。 |
| `mem_bytes` | backend が報告する概算メモリ使用量。 |
| `put_smp` / `get_smp` / `scan_smp` | latency percentile 用の sample 数。 |
| `timings(ms)` | 各フェーズにかかった wall-clock time。 |
| `payload(MiB/s)` | key + value payload bytes を処理時間で割った参考値。 |

## ローカル参照値: 通常ケース

実行条件は次の通りです。

```text
ops_per_case = 500000
writer_threads = 16
resolved_shards = 64
threshold_bytes_per_shard = disabled
flush_after_scan = OFF
prehash_mode = OFF
```

### ART

| Key | Value | Put ops/s | Get ops/s | Scan ops/s | mem_bytes | Put P99/P999 | Get P99/P999 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 3,576,615 | 8,827,917 | 2,701,548 | 897,815,387 | 59.60 / 236.40 us | 1.70 / 2.60 us |
| 16 | 64 | 3,783,199 | 15,215,281 | 3,150,088 | 930,800,379 | 39.30 / 169.50 us | 1.60 / 44.00 us |
| 16 | 256 | 3,573,491 | 11,593,181 | 2,663,092 | 1,026,788,029 | 39.00 / 166.60 us | 1.80 / 5.40 us |
| 32 | 1024 | 3,241,920 | 12,825,742 | 2,374,379 | 1,428,767,499 | 42.30 / 150.20 us | 1.90 / 10.20 us |
| 32 | 4096 | 1,962,011 | 7,290,851 | 1,950,706 | 2,964,810,774 | 50.00 / 175.30 us | 2.60 / 13.30 us |
| 64 | 16384 | 768,491 | 8,173,648 | 1,729,525 | 9,144,890,899 | 110.80 / 281.80 us | 2.30 / 13.90 us |

### B+Tree

| Key | Value | Put ops/s | Get ops/s | Scan ops/s | mem_bytes | Put P99/P999 | Get P99/P999 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 5,644,781 | 6,052,111 | 4,222,763 | 86,157,440 | 21.90 / 131.00 us | 2.80 / 20.00 us |
| 16 | 64 | 3,928,075 | 3,783,714 | 4,401,110 | 114,152,808 | 28.50 / 172.90 us | 3.30 / 34.80 us |
| 16 | 256 | 4,163,218 | 5,205,980 | 3,841,272 | 210,098,768 | 28.10 / 151.70 us | 3.50 / 66.00 us |
| 32 | 1024 | 3,131,976 | 4,434,625 | 4,005,624 | 602,089,504 | 53.00 / 245.10 us | 4.20 / 171.50 us |
| 32 | 4096 | 1,651,952 | 3,507,726 | 3,206,004 | 2,138,063,256 | 90.60 / 357.80 us | 5.50 / 307.00 us |
| 64 | 16384 | 618,492 | 3,773,220 | 3,111,175 | 8,298,072,520 | 221.10 / 922.80 us | 4.80 / 58.20 us |

## ローカル参照値: backend 単体ケース

実行条件は次の通りです。

```text
ops_per_case = 50000
writer_threads = 1
resolved_shards = 1
threshold_bytes_per_shard = disabled
flush_after_scan = OFF
prehash_mode = ON
```

### ART

| Key | Value | Put ops/s | Get ops/s | Scan ops/s | mem_bytes | Put P99/P999 | Get P99/P999 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 774,196 | 2,861,591 | 3,093,983 | 111,896,727 | 5.20 / 9.10 us | 1.20 / 12.30 us |
| 16 | 64 | 752,904 | 2,303,755 | 1,457,739 | 115,223,506 | 6.10 / 11.30 us | 2.20 / 3.80 us |
| 16 | 256 | 743,617 | 2,649,358 | 2,971,998 | 124,823,506 | 6.20 / 9.30 us | 1.10 / 6.20 us |
| 32 | 1024 | 622,537 | 2,474,121 | 2,431,528 | 165,077,127 | 6.40 / 18.80 us | 1.10 / 7.20 us |
| 32 | 4096 | 414,275 | 1,632,674 | 1,745,591 | 318,677,127 | 9.90 / 14.80 us | 1.60 / 6.10 us |
| 64 | 16384 | 199,541 | 1,559,065 | 1,905,691 | 936,784,327 | 14.50 / 34.60 us | 1.40 / 6.10 us |

### B+Tree

| Key | Value | Put ops/s | Get ops/s | Scan ops/s | mem_bytes | Put P99/P999 | Get P99/P999 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 2,382,121 | 1,451,400 | 3,421,330 | 8,581,776 | 1.20 / 6.00 us | 1.80 / 4.40 us |
| 16 | 64 | 1,324,117 | 1,101,909 | 2,736,547 | 11,381,776 | 2.00 / 6.80 us | 5.00 / 13.90 us |
| 16 | 256 | 1,317,252 | 1,267,896 | 3,072,687 | 20,981,776 | 5.20 / 8.20 us | 1.90 / 6.60 us |
| 32 | 1024 | 800,941 | 1,172,248 | 3,792,648 | 60,181,776 | 7.30 / 8.30 us | 1.90 / 7.00 us |
| 32 | 4096 | 558,795 | 1,075,778 | 2,818,553 | 213,781,776 | 6.90 / 9.50 us | 2.30 / 6.50 us |
| 64 | 16384 | 152,231 | 824,541 | 2,319,464 | 829,781,776 | 19.50 / 43.00 us | 4.10 / 6.40 us |
