# WAL 設定とベンチマークの読み方

このメモは、AkkaraDB の WAL Writer をチューニングするときに、各設定が何に効くかを整理したものです。

特に `WalSyncMode::Async` は、`append()` が返る速度と、実際に WAL を書き切る速度が一致しません。ベンチ結果を見るときは、`append(ops/s)` だけで判断せず、`finish(ops/s)`、`close_ms`、`total_ms`、`throughput(MiB/s)` を合わせて見てください。

## 対象ベンチマーク

WAL 単体の測定には、次のターゲットを使います。

```powershell
cmake --build cmake-build-release --target akkaradb_wal_throughput_benchmark --config Release
cmake-build-release\bin\akkaradb_wal_throughput_benchmark.exe 200000 --writers=16 --sync=async
```

主なオプションは次の通りです。

| オプション                   | 意味                                                                   |
|-------------------------|----------------------------------------------------------------------|
| `--writers=N`           | producer thread 数です。`0` なら hardware concurrency から自動解決します。           |
| `--shards=N`            | WAL shard 数です。`1..16` を指定できます。`0` なら writer thread 数に合わせて自動解決します。 |
| `--sync=off`            | `fwrite` と `fflush` は行いますが、通常 write ごとの `fdatasync` は行いません。          |
| `--sync=async`          | producer は queue に積み、background flusher が batch 単位で `fdatasync` します。 |
| `--sync=sync`           | append ごとに `fdatasync` します。最も durable ですが、かなり遅くなります。                 |
| `--group-n=N`           | Async flusher が batch を切る entry 数の目安です。                              |
| `--group-micros=N`      | batch が十分に溜まらない場合に待つ時間です。                                            |
| `--group-bytes=N`       | Async flusher が batch を切る byte 数の目安です。例: `4MiB`。                     |
| `--max-pending-bytes=N` | queued + in-flight の未完了 WAL bytes 上限です。超えると producer 側で待ちます。         |
| `--prehash`             | key hash を事前計算し、append 内の hash cost を除外します。                          |

## 出力項目の意味

| 項目                  | 意味                                                                               |
|---------------------|----------------------------------------------------------------------------------|
| `append(ops/s)`     | `WalWriter::append()` 呼び出しが戻る速度です。Async では主に serialize + queue push の速度です。       |
| `finish(ops/s)`     | append 開始から `close()` 完了までを含めた速度です。Async WAL の実効 write throughput としてはこちらを重視します。 |
| `recover(ops/s)`    | 書かれた WAL を `WalRecovery::recover()` で replay する速度です。                             |
| `wal_bytes`         | 実際に生成された WAL file bytes の合計です。segment header も含みます。                              |
| `app_smp`           | append latency percentile の sample 数です。                                          |
| `append_ms`         | append loop の時間です。Async では producer 側がどれだけ待たされたかも含みます。                           |
| `close_ms`          | append 後、`close()` が queue drain / header update / sync を終えるまでの時間です。             |
| `total_ms`          | append 開始から close 完了までです。`finish(ops/s)` の分母です。                                  |
| `recover_ms`        | recovery にかかった時間です。                                                              |
| `throughput(MiB/s)` | `wal_bytes` を各時間で割った byte throughput です。大きい value では ops/s よりこちらが読みやすいです。        |

Async では、`append_ms` が短く `close_ms` が長い場合、producer が flusher より大きく先行しています。これは queue に未書き込みデータが溜まり、最後の `close()` でまとめて精算している状態です。

逆に `append_ms` が長く `close_ms` が短い場合、`max_pending_bytes` による backpressure が効いています。producer は append 中に待たされますが、`close()` での後払いは小さくなります。WAL としては後者の方が実態に近い測定です。

## 各設定の効き方

### `group_n`

`group_n` は、Async flusher が一度に処理する entry 数の目安です。

小さい record では、`group_n` を大きくしすぎると batch が溜まるまで待ちやすくなります。`group_micros` が長い場合、特に `close_ms` や tail latency に出ます。

大きい record では、entry 数より byte 数が支配的になります。例えば 16 KiB value で `group_n=8192` にしても、entry 数の上限に届く前に `group_bytes` が効くことが多いです。

目安として、小さい record を重視するなら `512` から `2048`、大きい record を重視するなら `2048` から `8192` が候補です。ただし `group_bytes` を併用する前提です。

### `group_micros`

`group_micros` は、batch が小さいときに flusher が追加の entry を待つ時間です。

短くすると latency は下がりやすいですが、flush / sync 回数が増えて throughput は落ちやすくなります。長くすると throughput は上がることがありますが、小さい record では `close_ms` や P99/P999 が悪化しやすくなります。

低 latency 寄りなら `100` から `500`、throughput 寄りなら `1000` から `2000` が候補です。

### `group_bytes`

`group_bytes` は、大きい value でかなり重要です。

entry 数だけで batch を切ると、8 B value と 16 KiB value が同じ `group_n` で扱われます。これは自然ではありません。`group_bytes` を入れることで、「最大 8192 entries または 4 MiB のどちらかに到達したら flush」のようにできます。

大きい value では `group_bytes=4MiB` が比較的扱いやすいです。さらに大きくすると write batch は太くなりますが、pending memory と tail latency が増えやすくなります。

### `max_pending_bytes`

`max_pending_bytes` は、Async queue の未完了 WAL bytes 上限です。実装上は queued bytes と flusher が持っている in-flight bytes を合わせて見ます。

この値が大きいと、producer は先行しやすくなります。`append(ops/s)` は高く見えますが、`close_ms` が長くなりやすいです。つまり、最後にまとめて待つ測定になります。

この値が小さいと、producer は append 中に待たされます。`append(ops/s)` は下がりますが、`close_ms` は短くなり、`finish(ops/s)` は実際の書き切り速度に近づきます。

現在の目安は `64MiB` です。大きい value で producer をもう少し先行させたい場合は `128MiB` も候補です。ただし、メモリ使用量と tail latency は増えます。

### `shard_count`

WAL shard 数は最大 16 です。`shard_count=0` の auto では、hardware thread 数またはベンチの writer thread 数を `1..16` に clamp して使います。つまり 16 writer なら 16 shard、8 writer なら 8 shard です。

writer が多い場合、shard を増やすと queue / file lock の競合は減ります。

ただし shard が増えると、close / force sync 時に同期対象 file も増えます。小さい record では 16 shard が有利なことが多いですが、大きい value では 8 shard の方が安定する可能性があります。

16 writer なら auto で `16` になります。巨大 value の `close_ms` や tail latency が気になる場合は、明示的に `--shards=8` なども比較してください。

以前は shard 選択に bit mask を使っていたため、`1,2,4,8,16` のみを許していました。現在は `fp64 % shard_count` で選ぶため、`1..16` の任意 shard 数を使えます。2 のべき乗に限定する必要はありません。

### `sync_mode`

`off` は OS page cache への write cost を見る設定です。crash-safe durability の測定ではありません。

`async` は通常の high-throughput ingest 向けです。`group_*` と `max_pending_bytes` の設定が効きます。

`sync` は append ごとに `fdatasync` するため、非常に遅くなります。10 件程度でも数秒かかる環境があります。durability の最悪ケース確認には使えますが、throughput 比較には小さい `ops_per_case` を使ってください。

## よくある読み違い

### `append(ops/s)` が高いので WAL が速い、とは限らない

Async の `append(ops/s)` は queue に積む速度です。未書き込みデータが大量に残っている場合、`close_ms` が大きくなります。

実効性能を見るなら、まず `finish(ops/s)` と `finish MiB/s` を見てください。

### `close_ms` が長いのは必ず悪い、とは限らない

`max_pending_bytes` を大きくして producer を先行させる設定では、`close_ms` が長くなるのは自然です。

ただし運用上、shutdown / force_sync / checkpoint の待ち時間が問題になるなら、`max_pending_bytes` を下げるか、`group_bytes` を調整して append 中に backpressure をかける方が安定します。

### 16 KiB value の WAL 単体ベンチは engine write path そのものではない

WAL 単体ベンチは、`WalWriter` に value payload を直接渡します。エンジン経由では大きい value が Blob 化され、WAL には BlobRef 相当の小さい payload だけが載る設計になる場合があります。

そのため `64 / 16384` の結果は、「raw WAL writer に巨大 payload を直接投げた場合の限界試験」として見てください。実運用寄りに見るなら、巨大 payload ケースと BlobRef 相当の小 payload ケースを分けて測る必要があります。

## 代表的な設定例

### バランス型

小さい record と 4 KiB 程度の value を両方見る設定です。

```powershell
cmake-build-release\bin\akkaradb_wal_throughput_benchmark.exe 200000 --writers=16 --sync=async --group-n=2048 --group-micros=500 --group-bytes=4MiB --max-pending-bytes=64MiB
```

期待される傾向は、`append` が極端に先行しすぎず、`close_ms` も抑えられることです。全サイズで最速を狙うというより、読みやすい比較用の設定です。

### 大きい value 寄り

4 KiB から 16 KiB value の byte throughput を見たい場合の設定です。

```powershell
cmake-build-release\bin\akkaradb_wal_throughput_benchmark.exe 200000 --writers=16 --sync=async --group-n=8192 --group-micros=2000 --group-bytes=4MiB --max-pending-bytes=64MiB
```

この設定では大きい value の `finish MiB/s` が見やすくなります。一方で、小さい record では `close_ms` や tail latency が悪化する場合があります。

### Producer を先行させる設定

append 側の enqueue 限界を見たい場合の設定です。

```powershell
cmake-build-release\bin\akkaradb_wal_throughput_benchmark.exe 200000 --writers=16 --sync=async --group-n=4096 --group-micros=1000 --group-bytes=4MiB --max-pending-bytes=128MiB
```

`append(ops/s)` は高く見えやすくなりますが、`close_ms` が伸びる可能性があります。実効性能は `finish MiB/s` で判断してください。

### Backpressure を強める設定

pending memory を抑え、`close()` の後払いを減らす設定です。

```powershell
cmake-build-release\bin\akkaradb_wal_throughput_benchmark.exe 200000 --writers=16 --sync=async --group-n=2048 --group-micros=500 --group-bytes=2MiB --max-pending-bytes=16MiB
```

`append(ops/s)` は下がりやすいですが、`close_ms` は短くなりやすいです。shutdown latency や force sync latency を重視する場合に向いています。

## 今回の測定から見えた傾向

`group_n=8192 / group_micros=2000 / group_bytes=4MiB / max_pending_bytes=64MiB` では、4 KiB value の `finish` がかなり良くなりました。大きい batch が効き、`finish MiB/s` が読みやすい形になります。

一方で 16 KiB value では、`append_ms` が長く、`close_ms` は短くなりました。これは backpressure が効いて producer が flusher に合わせて待っている状態です。以前のように `append` が速く見えて `close()` で長く待つより、実態に近い測定です。

この状態で劇的に伸ばすには、設定だけでなく実装変更が必要です。具体的には `PendingEntry::bytes` の buffer pool 化、shard-local arena、batch write の連続 buffer 化、または BlobRef path の分離が候補です。

### 実測例: 大きい value 寄りの設定

次の設定で測った例です。

```powershell
cmake-build-release\bin\akkaradb_wal_throughput_benchmark.exe 200000 --writers=16 --sync=async --group-n=8192 --group-micros=2000 --group-bytes=4MiB --max-pending-bytes=64MiB
```

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

結果の要約です。

| Key | Value | WAL bytes | Append ops/s | Finish ops/s | Recover ops/s | Append ms | Close ms | Total ms | Recover ms | Append MiB/s | Finish MiB/s | Recover MiB/s | P99/P999 append latency |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 11,200,768 | 10,071,406 | 360,365 | 3,844,978 | 19.86 | 535.13 | 554.99 | 52.02 | 537.91 | 19.25 | 205.36 | 10.40 / 129.30 us |
| 16 | 64 | 22,400,768 | 9,574,325 | 652,831 | 2,235,554 | 20.89 | 285.47 | 306.36 | 89.46 | 1,022.68 | 69.73 | 238.79 | 18.20 / 166.50 us |
| 16 | 256 | 60,800,768 | 7,540,824 | 456,964 | 580,491 | 26.52 | 411.15 | 437.67 | 344.54 | 2,186.24 | 132.48 | 168.30 | 12.10 / 230.30 us |
| 32 | 1024 | 217,600,768 | 3,799,529 | 184,473 | 937,425 | 52.64 | 1,031.53 | 1,084.17 | 213.35 | 3,942.40 | 191.41 | 972.67 | 15.90 / 696.00 us |
| 32 | 4096 | 832,000,768 | 314,435 | 103,059 | 297,584 | 636.06 | 1,304.57 | 1,940.63 | 672.08 | 1,247.45 | 408.87 | 1,180.60 | 105.50 / 1,914.70 us |
| 64 | 16384 | 3,296,003,072 | 8,734 | 8,513 | 82,534 | 22,897.77 | 594.87 | 23,492.64 | 2,423.25 | 137.28 | 133.80 | 1,297.15 | 1,030.90 / 5,795.90 us |

この例では、4 KiB value の `finish MiB/s` が約 `409 MiB/s` まで出ています。大きい byte batch が効いており、このサイズ帯ではかなり良い結果です。

一方、16 KiB value では `append_ms=22897.77`、`close_ms=594.87` です。これは `close()` で詰まっているのではなく、append 中に `max_pending_bytes` の backpressure が効き、producer が flusher の実効書き込み速度に合わせて待っている状態です。`append MiB/s` と `finish MiB/s` が近いことからも、その傾向が分かります。

小さい record では `finish MiB/s` が低く見えますが、これは byte 数が小さいためです。小さい record の評価では MiB/s より ops/s と tail latency を重視してください。この設定は大きい value 寄りなので、8 B / 16 B のような小さい payload では `close_ms` が相対的に目立ちます。

## チューニング時の見方

まず `finish MiB/s` を見ます。大きい value では ops/s より byte throughput が重要です。

次に `append_ms` と `close_ms` の比率を見ます。`close_ms` が支配的なら producer が先行しすぎています。`append_ms` が支配的なら backpressure が効いています。

最後に P99/P999 を見ます。小さい record で tail latency が悪化する場合は、`group_micros` を短くするか、`group_n` を下げます。大きい value で tail latency が悪化する場合は、`max_pending_bytes` を下げるか、`group_bytes` を小さくします。

単一の設定で全 payload size に勝つのは難しいです。小さい record 向け、4 KiB 前後向け、16 KiB 以上向けで設定を分けて考える方が現実的です。
