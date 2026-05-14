# MemTable B+Tree / ART ベンチマークの読み方

このメモは、AkkaraDB の MemTable backend を B+Tree と ART で比較するときの見方を整理したものです。
対象は `akkaradb_memtable_throughput_benchmark` です。

今回の結果は、2種類の条件を分けて読みます。

1つ目は、通常の実行に近い default 条件です。`writers=16`、`shards=64`、`prehash=OFF` で、MemTable 全体としての並列性能を見ます。

2つ目は、backend 単体の性格を見るための分離条件です。`writers=1`、`shards=1`、`prehash=ON` で、sharding や put 中の hash 計算をできるだけ外して、B+Tree / ART 自体の傾向を見ます。

## 対象ベンチマーク

Release build で次の target を使います。

```powershell
cmake --build cmake-build-release --target akkaradb_memtable_throughput_benchmark --config Release
```

default 条件は次の実行です。

```powershell
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 500000 --backend=art
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 500000 --backend=bptree
```

backend 単体の比較は次の実行です。

```powershell
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 50000 --writers=1 --backend=art --shards=1 --prehash
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 50000 --writers=1 --backend=bptree --shards=1 --prehash
```

## 主なオプション

| オプション | 意味 |
|---|---|
| `--backend=art` | ARTMemTable を使います。point get の性能を見るときの主対象です。 |
| `--backend=bptree` | BPTreeMemTable を使います。put と scan の安定性を見やすい backend です。 |
| `--writers=N` | writer thread 数です。default は現在の実行環境で 16 でした。 |
| `--shards=N` | MemTable shard 数です。指定しない場合は writer 数から自動解決されます。今回の default では 64 でした。 |
| `--prehash` | put 中の key hash 計算を測定から外します。backend 構造そのものを比較しやすくするための設定です。 |
| `--threshold-bytes=N` | shard ごとの flush threshold です。今回の測定では disabled です。 |
| `--flush-after-scan` | scan 後に force flush します。backend 比較では通常混ぜません。 |

## 出力項目の意味

| 項目 | 意味 |
|---|---|
| `put(ops/s)` | MemTable への insert / update throughput です。record 作成、node 更新、version chain 更新を含みます。 |
| `get(ops/s)` | 既存 key の point lookup throughput です。ART ではここが最重要です。 |
| `scan(ops/s)` | snapshot iterator による全件 scan throughput です。 |
| `mem_bytes` | backend が報告する概算メモリ使用量です。node / record / arena allocation を含みます。 |
| `put_smp` / `get_smp` / `scan_smp` | latency percentile 用の sample 数です。 |
| `payload(MiB/s)` | key + value payload bytes を処理時間で割った参考値です。大きい value では ops/s より読みやすいことがあります。 |

## Default 条件

```text
ops_per_case = 500000
writer_threads = 16
resolved_shards = 64
threshold_bytes_per_shard = disabled
flush_after_scan = OFF
prehash_mode = OFF
```

この条件は、backend 単体というより「現在の MemTable 実装としてどれくらい出るか」を見るための値です。
shard 分散、writer 並列、put 中の hash 計算、active / immutable 探索のコストが含まれます。

### ART default

| Key | Value | Put ops/s | Get ops/s | Scan ops/s | mem_bytes | Put P99/P999 | Get P99/P999 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 3,576,615 | 8,827,917 | 2,701,548 | 897,815,387 | 59.60 / 236.40 us | 1.70 / 2.60 us |
| 16 | 64 | 3,783,199 | 15,215,281 | 3,150,088 | 930,800,379 | 39.30 / 169.50 us | 1.60 / 44.00 us |
| 16 | 256 | 3,573,491 | 11,593,181 | 2,663,092 | 1,026,788,029 | 39.00 / 166.60 us | 1.80 / 5.40 us |
| 32 | 1024 | 3,241,920 | 12,825,742 | 2,374,379 | 1,428,767,499 | 42.30 / 150.20 us | 1.90 / 10.20 us |
| 32 | 4096 | 1,962,011 | 7,290,851 | 1,950,706 | 2,964,810,774 | 50.00 / 175.30 us | 2.60 / 13.30 us |
| 64 | 16384 | 768,491 | 8,173,648 | 1,729,525 | 9,144,890,899 | 110.80 / 281.80 us | 2.30 / 13.90 us |

### B+Tree default

| Key | Value | Put ops/s | Get ops/s | Scan ops/s | mem_bytes | Put P99/P999 | Get P99/P999 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 5,644,781 | 6,052,111 | 4,222,763 | 86,157,440 | 21.90 / 131.00 us | 2.80 / 20.00 us |
| 16 | 64 | 3,928,075 | 3,783,714 | 4,401,110 | 114,152,808 | 28.50 / 172.90 us | 3.30 / 34.80 us |
| 16 | 256 | 4,163,218 | 5,205,980 | 3,841,272 | 210,098,768 | 28.10 / 151.70 us | 3.50 / 66.00 us |
| 32 | 1024 | 3,131,976 | 4,434,625 | 4,005,624 | 602,089,504 | 53.00 / 245.10 us | 4.20 / 171.50 us |
| 32 | 4096 | 1,651,952 | 3,507,726 | 3,206,004 | 2,138,063,256 | 90.60 / 357.80 us | 5.50 / 307.00 us |
| 64 | 16384 | 618,492 | 3,773,220 | 3,111,175 | 8,298,072,520 | 221.10 / 922.80 us | 4.80 / 58.20 us |

default 条件では、ART は get がかなり強く出ています。特に 16 byte key / 64 byte value では約 15.2M get ops/s です。
B+Tree は put と scan が強く、8 byte key / 16 byte value では約 5.6M put ops/s、scan も多くの case で ART より高く出ています。

一方で ART は `mem_bytes` が大きく出ます。これは ART の node / prefix / copy-on-write 更新のための arena allocation が効いているためです。

## Backend 分離条件

```text
ops_per_case = 50000
writer_threads = 1
resolved_shards = 1
threshold_bytes_per_shard = disabled
flush_after_scan = OFF
prehash_mode = ON
```

この条件は、default 条件より throughput が小さく見えます。これは遅い結果ではなく、意図的に並列化を外して backend 単体を読みやすくした結果です。
Doc や実装判断で使うときは、default 条件と混ぜて比較しないでください。

### ART backend 分離

| Key | Value | Put ops/s | Get ops/s | Scan ops/s | mem_bytes | Put P99/P999 | Get P99/P999 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 774,196 | 2,861,591 | 3,093,983 | 111,896,727 | 5.20 / 9.10 us | 1.20 / 12.30 us |
| 16 | 64 | 752,904 | 2,303,755 | 1,457,739 | 115,223,506 | 6.10 / 11.30 us | 2.20 / 3.80 us |
| 16 | 256 | 743,617 | 2,649,358 | 2,971,998 | 124,823,506 | 6.20 / 9.30 us | 1.10 / 6.20 us |
| 32 | 1024 | 622,537 | 2,474,121 | 2,431,528 | 165,077,127 | 6.40 / 18.80 us | 1.10 / 7.20 us |
| 32 | 4096 | 414,275 | 1,632,674 | 1,745,591 | 318,677,127 | 9.90 / 14.80 us | 1.60 / 6.10 us |
| 64 | 16384 | 199,541 | 1,559,065 | 1,905,691 | 936,784,327 | 14.50 / 34.60 us | 1.40 / 6.10 us |

### B+Tree backend 分離

| Key | Value | Put ops/s | Get ops/s | Scan ops/s | mem_bytes | Put P99/P999 | Get P99/P999 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 2,382,121 | 1,451,400 | 3,421,330 | 8,581,776 | 1.20 / 6.00 us | 1.80 / 4.40 us |
| 16 | 64 | 1,324,117 | 1,101,909 | 2,736,547 | 11,381,776 | 2.00 / 6.80 us | 5.00 / 13.90 us |
| 16 | 256 | 1,317,252 | 1,267,896 | 3,072,687 | 20,981,776 | 5.20 / 8.20 us | 1.90 / 6.60 us |
| 32 | 1024 | 800,941 | 1,172,248 | 3,792,648 | 60,181,776 | 7.30 / 8.30 us | 1.90 / 7.00 us |
| 32 | 4096 | 558,795 | 1,075,778 | 2,818,553 | 213,781,776 | 6.90 / 9.50 us | 2.30 / 6.50 us |
| 64 | 16384 | 152,231 | 824,541 | 2,319,464 | 829,781,776 | 19.50 / 43.00 us | 4.10 / 6.40 us |

## 読み分け

| 観点 | 向いている backend | 理由 |
|---|---|---|
| point get 重視 | ART | default / backend 分離のどちらでも get が B+Tree より高いです。 |
| write-heavy ingest | B+Tree | 小さい record から中程度の value まで put が高く、tail も読みやすいです。 |
| scan 重視 | B+Tree | default 条件では多くの case で scan が ART より高いです。 |
| memory footprint | B+Tree | 今回の実測では ART よりかなり小さく出ています。 |
| read-mostly shard | ART | get の優位が大きく出ます。 |
| mixed workload | 要測定 | get 比率が高ければ ART、put/scan 比率が高ければ B+Tree が候補です。 |

## 注意点

### default と backend 分離は数字を混ぜない

default 条件は 16 writers / 64 shards なので、単純な ops/s は backend 分離条件より大きく出ます。
backend 分離条件は、ART と B+Tree の内部構造差を見るために single writer / single shard にしています。

そのため、default の ART get が 8M から 15M ops/s 出ている一方で、backend 分離の ART get が 1.5M から 2.8M ops/s なのは自然です。
これは性能退化ではなく、測っているものが違います。

### ART の get は raw backend だけの数字ではない

このベンチの `get(ops/s)` は `MemTable::get()` 経由です。
backend の lookup だけでなく、shard 選択用の hash 計算、published table 取得、active / immutable 探索も含まれます。
ART 単体の get をさらに細かく見るなら、`ARTMemTable` を直接叩く microbench を別に切るのが正確です。

### `--prehash` は backend 比較用

`--prehash` を付けると、put 中の key hash 計算を測定から外します。
backend の tree update コストを比較するには便利ですが、実運用では record ごとに `fp64` / `mini_key` は必要です。

### 大きい value は payload も見る

4 KiB から 16 KiB value では、ops/s だけでなく `payload(MiB/s)` も見てください。
大きい payload では record copy / arena allocation / memory bandwidth の影響が混ざります。

## 代表的な実行

default の MemTable 全体性能を見る場合です。

```powershell
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 500000 --backend=art
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 500000 --backend=bptree
```

backend 単体の性格を見る場合です。

```powershell
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 200000 --writers=1 --shards=1 --backend=art --prehash
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 200000 --writers=1 --shards=1 --backend=bptree --prehash
```

## 今回の結論

default 条件では、ART は get が強く、B+Tree は put と scan が強いです。
特に ART の get は、parallel shard 条件で大きく伸びています。

backend 分離条件では、ART の point lookup 優位と B+Tree の put / scan 優位がより見やすくなります。
ただし throughput の絶対値は default 条件より小さく出るため、実運用寄りの数字として読むべきではありません。
