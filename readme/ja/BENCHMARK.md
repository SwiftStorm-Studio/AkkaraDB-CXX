# AkkaraDB - ベンチマークノート

> Version 0.5.0 - C++23 - Copyright 2026 Swift Storm Studio - AGPL-3.0

---

## 目次

1. [概要](#1-概要)
2. [ベンチマーク実行バイナリが対象とする範囲](#2-ベンチマーク実行バイナリが対象とする範囲)
3. [ビルドと実行方法](#3-ビルドと実行方法)
4. [測定方法](#4-測定方法)
5. [現在の結果](#5-現在の結果)
6. [結果の読み方](#6-結果の読み方)
7. [再現性に関する注意](#7-再現性に関する注意)

---

## 1. 概要

`akkaradb_benchmark` は、正当性検証とマイクロベンチマークを
一体化した実行ファイルです。
目的は次の 2 つです。

- コード変更後も、エンジンの主要機能が正しく動作していることを確認する
- AkkaraDB における重要なホットパスのコストを測定する。
  具体的には、インメモリの put/get、WAL 付き書き込み、SST 参照、
  型付き API のオーバーヘッドを対象とする

---

## 2. ベンチマーク実行バイナリが対象とする範囲

この実行ファイルは現在 **20 ステップ** を実行します。
内訳は **17 個の正しさテスト** と **3 個のベンチマークセクション** です。

### 2.1 正当性検証の対象

| 領域         | 内容                                                       |
|------------|----------------------------------------------------------|
| MemTable   | 基本的な put/get/remove、多数キー                                 |
| WAL        | `SyncMode::Off`、クラッシュリカバリ、async モード、WAL truncate         |
| Blob       | 大きい値の往復確認、誤った BlobRef 判定が起きないこと                          |
| Lifecycle  | `close()` の多重呼び出し安全性                                     |
| VersionLog | history、`get_at`、`rollback_key`、`rollback_to`、永続化        |
| SST        | flush/reopen 後の読み出し、tombstone、compaction、overwrite dedup |

そのため、このバイナリは性能測定だけでなく、
変更後の高速なスモークテストとしても使えます。

### 2.2 ベンチマークの対象

| ベンチマーク                 | 目的                                                                               |
|------------------------|----------------------------------------------------------------------------------|
| `bench_throughput`     | inline と heap の両パスを含むインメモリ throughput と、WAL async throughput を測定する               |
| `bench_sst_lookup`     | SST に対する negative lookup、positive `exists()`、positive `get()` の throughput を測定する |
| `bench_api_comparison` | 型付き `PackedTable` API が raw `AkkEngine` に対してどの程度のオーバーヘッドを持つかを定量化する               |

---

## 3. ビルドと実行方法

### 3.1 ビルド

ベンチマークターゲット名は常に `akkaradb_benchmark` です。

```bash
cmake -S . -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release --target akkaradb_benchmark --config Release
```

### 3.2 実行

```bash
./cmake-build-release/bin/akkaradb_benchmark
```

Windows で multi-config generator を使っている場合、実行ファイルは通常:

```text
cmake-build-release/bin/akkaradb_benchmark.exe
```

実行時には、まず各正しさテストの結果が表示され、
その後に性能テーブル、最後に pass/fail の集計が表示されます。

---

## 4. 測定方法

### 4.1 共通ルール

- 可能な箇所では、キーや値の一度きりの生成コストを計測区間から除外する
- 計測前に warmup ループを入れ、コールドキャッシュの影響を減らす
- シナリオごとに一時ディレクトリを作成し、終了後に削除する
- `ops/s` は、計測対象ループの実時間から算出する
- `wrong_answers` が 0 以外であれば、性能差ではなく正当性の欠如として扱う

### 4.2 Throughput ベンチマーク

インメモリ throughput ベンチマークでは、単一プロセス内での
エンジン性能を複数の key/value サイズで測定します。

`SmallBuffer::INLINE_CAP = 22 bytes` なので、意図的に次の両方を含めています。

- inline ケース: key+value の合計ペイロードが `<= 22 B`
- heap ケース: key+value の合計ペイロードが `> 22 B`

現在のサイズケースは次の通りです。

| Case         | Key  | Value  | Total  | 格納パス   | 操作数   |
|--------------|------|--------|--------|--------|-------|
| `k=8  v=8`   | 8 B  | 8 B    | 16 B   | inline | 5.0 M |
| `k=11 v=11`  | 11 B | 11 B   | 22 B   | inline | 5.0 M |
| `k=16 v=64`  | 16 B | 64 B   | 80 B   | heap   | 5.0 M |
| `k=32 v=256` | 32 B | 256 B  | 288 B  | heap   | 2.0 M |
| `k=32 v=1k`  | 32 B | 1024 B | 1056 B | heap   | 1.0 M |

同じセクション内で、`SyncMode::Async` の WAL 付き throughput も測定します。
対象ワークロードは `k=11 / v=11` の inline ケースです。

### 4.3 SST Lookup ベンチマーク

SST ベンチマークは、1 つの flush 済み SST に対する read path を切り出して測ります。

前提条件:

- `1,000,000` 個のキーを挿入して SST に flush する
- `sst_preload_data = true` により SST の data section を RAM 上に保持する
- flush 後に DB を reopen しない
  reopen すると WAL replay により MemTable が再構築され、
  SST 専用パスの測定にならなくなるため

測定する lookup モードは 3 種類です。

| モード                 | 意味                                           |
|---------------------|----------------------------------------------|
| Negative `exists()` | Bloom filter により absent key を弾く              |
| Positive `exists()` | bloom pass + index seek + record scan。値コピーなし |
| Positive `get()`    | positive `exists()` に加えて CRC と値の取り出しを含む      |

negative key は、既存キーと同じ辞書順レンジ内に交互配置されています。
これにより、range miss による早期終了ではなく、
きちんと bloom filter を通るパスを測定できます。

### 4.4 API 比較ベンチマーク

このベンチマークは、`1,000,000` 回のインメモリ操作に対して
3 つのアクセスパターンを比較します。

| パス           | 意味                                                                      |
|--------------|-------------------------------------------------------------------------|
| `raw-inline` | 8 B key + 8 B value を使う `AkkEngine::put/get_into`                       |
| `raw-heap`   | 16 B key + 16 B value を使う `AkkEngine::put/get_into`                     |
| `typed`      | runtime BinPack encode/decode を伴う `PackedTable<&KvRecord::id>::put/get` |

この比較により、次を分離して見られるようにしています。

- heap 確保コスト: `raw-heap` と `raw-inline` の差
- 型付き API コスト: `typed` と `raw-heap` の差
- 抽象化の総コスト: `typed` と `raw-inline` の差

---

## 5. 現在の結果

以下の結果は、この `akkaradb-native` の現在のベンチマーク実行結果に基づきます。

### 5.1 正当性検証サマリ

| 指標        | 結果           |
|-----------|--------------|
| Steps     | 20 / 20 完了   |
| Checks    | 71 / 71 pass |
| Exit code | 0            |

### 5.2 インメモリ Throughput

| Case               | 合計ペイロード | 格納     | N     | Write ops/s | Read ops/s |
|--------------------|---------|--------|-------|-------------|------------|
| `[mem] k=8  v=8`   | 16 B    | inline | 5.0 M | 7,631,672   | 5,918,729  |
| `[mem] k=11 v=11`  | 22 B    | inline | 5.0 M | 6,720,298   | 4,472,465  |
| `[mem] k=16 v=64`  | 80 B    | heap   | 5.0 M | 4,528,993   | 3,746,110  |
| `[mem] k=32 v=256` | 288 B   | heap   | 2.0 M | 2,998,134   | 3,174,423  |
| `[mem] k=32 v=1k`  | 1056 B  | heap   | 1.0 M | 1,494,787   | 2,734,551  |

### 5.3 WAL Async Throughput

| Case              | 合計ペイロード     | N     | Write ops/s | Read ops/s |
|-------------------|-------------|-------|-------------|------------|
| `[wal] k=11 v=11` | 22 B inline | 5.0 M | 4,393,075   | 3,626,907  |

Read 検証では `found = 5,000,000` でした。

### 5.4 SST Lookup Throughput

| Case                | Keys      | Probes    | Throughput       | Wrong Answers |
|---------------------|-----------|-----------|------------------|---------------|
| Negative `exists()` | 1,000,000 | 5,000,000 | 12,891,446 ops/s | 0             |
| Positive `exists()` | 1,000,000 | 5,000,000 | 1,747,877 ops/s  | 0             |
| Positive `get()`    | 1,000,000 | 5,000,000 | 1,583,018 ops/s  | 0             |

### 5.5 Raw API と Typed API の比較

| Case         | Key  | Value | N     | Write ops/s | Read ops/s |
|--------------|------|-------|-------|-------------|------------|
| `raw-inline` | 8 B  | 8 B   | 1.0 M | 7,203,319   | 6,204,952  |
| `raw-heap`   | 16 B | 16 B  | 1.0 M | 5,630,593   | 4,189,970  |
| `typed`      | 16 B | 16 B  | 1.0 M | 3,234,418   | 2,050,399  |

導出される slowdown 比は次の通りです。

| 比較              | Write        | Read         |
|-----------------|--------------|--------------|
| Heap vs inline  | 1.28x slower | 1.48x slower |
| Typed vs heap   | 1.74x slower | 2.04x slower |
| Typed vs inline | 2.23x slower | 3.03x slower |

---

## 6. 結果の読み方

### 6.1 Memory Path

- もっとも速いのは想定通り inline path です
- `22 B` の inline 閾値を超えると、inline から heap-backed record に切り替わり、
  性能が明確に落ちます
- 値サイズの増大に対して、read throughput は write throughput より緩やかに低下します
  これはまず write path 側で allocation と copy のコストが支配的になることと整合します

### 6.2 WAL Path

- Async WAL でも、インメモリ throughput のかなり大きな割合を維持できています
- 現在の `k=11 / v=11` inline ワークロードでは、
  純粋なインメモリ write throughput の約 `65%` に達しています
  これは durable logging としては強い結果です

### 6.3 SST Path

- Negative lookup は bloom rejection により深い SST 処理を避けられるため非常に高速です
- Positive `exists()` は、bloom pass、index seek、data scan が必要なため、
  negative `exists()` より大きく遅くなります
- Positive `get()` は、存在確認に加えて値の取り出しとバッファ materialization を含むため、
  positive `exists()` よりさらに遅くなります

### 6.4 Typed API のコスト

- typed API は無料ではなく、このベンチマークはその点を明確に示します
- 追加コストの多くは、BinPack の encode/decode と、
  raw byte API に対するオブジェクト変換処理から来ています
- スキーマの扱いやすさを優先する場面では妥当なトレードオフですが、
  zero-overhead なラッパーと見なすべきではありません

---

## 7. 再現性に関する注意

ベンチマーク値は実行環境に依存します。比較時には次を揃えるべきです。

- Build type は `Release` を使う
- コンパイラとそのバージョン
- SIMD 設定 (`AKKARADB_ENABLE_SIMD`)
- CPU モデル、クロック挙動、熱状態
- ストレージデバイスとファイルシステム
- バックグラウンド負荷

このベンチマークは、次の用途に向いています。

- 同一マシン上での before/after 比較
- エンジン変更後の回帰検出
- 新機能や抽象化のコスト定量化

一方で、異なるマシン間で順位を競うための汎用ベンチマークではありません。
