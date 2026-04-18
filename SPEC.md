# AkkaraDB — Technical Specification v4

> Version 0.5.0 · C++23 · Copyright 2026 Swift Storm Studio · AGPL-3.0

---

## Table of Contents

1. [Overview & Design Goals](#1-overview--design-goals)
2. [Architecture](#2-architecture)
3. [Record & Key Formats](#3-record--key-formats)
4. [Memory Management](#4-memory-management)
5. [MemTable](#5-memtable)
6. [Write-Ahead Log (WAL)](#6-write-ahead-log-wal)
7. [Blob Manager](#7-blob-manager)
8. [Sorted String Tables (SST)](#8-sorted-string-tables-sst)
9. [Manifest](#9-manifest)
10. [Version Log](#10-version-log)
11. [API Servers](#11-api-servers)
12. [Cluster & Replication](#12-cluster--replication)
13. [TLS Support](#13-tls-support)
14. [Configuration Reference](#14-configuration-reference)
15. [Public API Reference](#15-public-api-reference)
16. [File Format Reference](#16-file-format-reference)
17. [Threading & Concurrency Model](#17-threading--concurrency-model)
18. [Error Handling & Recovery](#18-error-handling--recovery)
19. [Build & Integration](#19-build--integration)

---

## 1. Overview & Design Goals

AkkaraDB is a C++ key-value storage engine designed to scale from a tiny embedded cache to a large-scale distributed database without changing application code.
It follows an LSM-tree (Log-Structured Merge-tree) architecture with a multi-shard MemTable, optional WAL, optional value externalization (BlobManager), and
leveled SST compaction.

### Core Properties

| Property     | Guarantee                                                |
|--------------|----------------------------------------------------------|
| Durability   | WAL + fsync (configurable: Sync / Async / Off)           |
| Crash safety | Atomic writes via tmp+rename; CRC32C on all records      |
| Ordering     | Lexicographic on raw key bytes                           |
| Concurrency  | All engine operations fully thread-safe                  |
| Keys         | Binary-safe, arbitrary bytes, up to 65535 bytes          |
| Values       | Binary-safe, arbitrary bytes, unlimited (≥16 KiB → Blob) |
| Compression  | Per-file, self-describing: `None` or `Zstd`              |

### Non-Goals

- SQL or query language

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Public API                         │
│      AkkaraDB   PackedTable<&T::field>   AkkEngine        │
└───────────────────────────┬─────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────┐
│                      AkkEngine                          │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │               MemTable (N shards)                │   │
│  │    ┌────────┐  ┌────────┐      ┌────────┐       │   │
│  │    │ Shard0 │  │ Shard1 │  ... │ ShardN │       │   │
│  │    │BPTree/ │  │BPTree/ │      │BPTree/ │       │   │
│  │    │SkipList│  │SkipList│      │SkipList│       │   │
│  │    └───┬────┘  └───┬────┘      └───┬────┘       │   │
│  └────────┼───────────┼───────────────┼────────────┘   │
│           │           │               │                  │
│     ┌─────▼───┐  ┌────▼────┐   ┌─────▼────┐           │
│     │  WAL    │  │  Blob   │   │   SST    │            │
│     │ Writer  │  │ Manager │   │ Manager  │            │
│     │(sharded)│  │(.blob)  │   │(L0..L6)  │            │
│     └─────────┘  └─────────┘   └──────────┘            │
│                                      │                   │
│                                 ┌────▼─────┐            │
│                                 │ Manifest │            │
│                                 └──────────┘            │
│                                                         │
│     ┌───────────┐   ┌────────────┐                     │
│     │VersionLog │   │  Cluster   │                     │
│     │(optional) │   │  Manager   │                     │
│     └───────────┘   └────────────┘                     │
│                                                         │
│     ┌──────────────────────────────┐                   │
│     │ API Servers (TCP / HTTP)     │                   │
│     │  Optional TLS via mbedTLS    │                   │
│     └──────────────────────────────┘                   │
└─────────────────────────────────────────────────────────┘
```

### Write Path

```
put(key, value)
  │
  ├─ compute key_fp64 (SipHash-2-4)
  ├─ select shard: fp64 & (shard_count - 1)
  │
  ├─ [if value.size >= blob_threshold]
  │    ├─ enqueue WAL entry (FLAG_BLOB, BlobRef 20B as value)
  │    ├─ BlobManager::write(seq, value)
  │    └─ MemTable::put(key, BlobRef, FLAG_BLOB, fp64)
  │
  └─ [else]
       ├─ enqueue WAL entry (FLAG_NORMAL, value)
       └─ MemTable::put(key, value, FLAG_NORMAL, fp64)
```

### Read Path

```
get(key)
  │
  ├─ compute key_fp64
  ├─ select shard
  ├─ MemTable::find_into(key, out)
  │    ├─ found, FLAG_NORMAL  → return value bytes
  │    ├─ found, FLAG_BLOB    → BlobManager::read(blob_id) → return
  │    ├─ found, TOMBSTONE    → return nullopt (do not check SST)
  │    └─ not found          ↓
  └─ SSTManager::get(key)
       ├─ L0 files (newest first, overlapping ranges)
       ├─ L1..L6 (binary search on level file list)
       └─ bloom_check → index_seek → scan ≤128 records → return
```

---

## 3. Record & Key Formats

### 3.1 AKHdr32 — 32-Byte Record Header

All records (MemTable, WAL, SST) share this header. Layout is little-endian, `#pragma pack(1)`.

```
Offset  Size  Field        Description
──────  ────  ───────────  ──────────────────────────────────────────
  0      2    k_len        Key length in bytes (0–65535)
  2      2    v_len        Value length in bytes (0–65535); inline ≤ blob_threshold, BlobRef = 20 B
  4      2    reserved1    Reserved (0); reclaimed from formerly-wasted high 2 bytes of old u32 v_len
  6      8    seq          Monotonic write sequence number
 14      1    flags        Record type: NORMAL=0x00, TOMBSTONE=0x01, BLOB=0x02
 15      1    pad0         Reserved (0)
 16      8    key_fp64     SipHash-2-4 of key (seed 0x5AD6DCD676D23C25)
 24      8    mini_key     First 8 bytes of key, little-endian padded
──────────────────────────────────────────────────────────────────────
Total  32 bytes
```

**`seq`** is globally monotonic across all shards, assigned atomically.
**`key_fp64`** is used for:

- Shard selection (`fp64 & (shard_count - 1)`)
- Bloom filter hashing in SST
- Fast-rejection comparison in BPTree

### 3.2 MemRecord — Owning In-Memory Record

```
┌──────────────────┬─────────────────────────────┐
│   AKHdr32 (32B)  │      SmallBuffer (32B)       │
│                  │  active_ptr(8) meta(2) inl(22)│
└──────────────────┴─────────────────────────────┘
Total: 64 bytes (both Debug and Release builds)
```

`SmallBuffer` provides SSO-like inline storage: values ≤ 22 bytes are stored inline with no heap allocation.

### 3.3 BlobRef — Inline Blob Reference

When a value is externalized to a `.blob` file, the MemTable/WAL stores a 20-byte `BlobRef` as the value:

```
Offset  Size  Field       Description
──────  ────  ──────────  ──────────────────────────
  0      8    blob_id     WAL seq of the creating write
  8      8    value_size  Original uncompressed size
 16      4    crc32c      CRC32C of uncompressed content
──────────────────────────────────────────────────
Total  20 bytes
```

The `FLAG_BLOB` flag in `AKHdr32.flags` signals that the value bytes are a `BlobRef`, not the raw value.

### 3.4 PackedTable Key Layout

**Primary key entry** (entity value):

```
┌──────────────────────┬──────────────────────────────┐
│   NS Prefix (8 B)    │     Encoded PK bytes          │
│  FNV-1a-64(name)     │  BinPack::encode(entity.pk)   │
└──────────────────────┴──────────────────────────────┘
→ value: BinPack::encode(entity)   (all fields, declaration order)
```

**Secondary index entry** (PK reference):
```
┌──────────────────────┬──────────────────────────────┐
│  Index Prefix (8 B)  │  Encoded field value bytes    │
│  FNV-1a-64(name +    │  BinPack::encode(entity.field)│
│  ":index:" +         │                               │
│  field_name)         │                               │
└──────────────────────┴──────────────────────────────┘
→ value: BinPack::encode(pk)
```

**Namespace prefix** is deterministic: same table name → same 8 bytes across all processes and restarts.

**Index prefix** is derived from `FNV-1a-64("<table_name>:index:<field_name>")`, where
`field_name` is the C++ member name extracted at **compile time** via `member_name<MPtr>()`
(parsing `__FUNCSIG__` on MSVC, `__PRETTY_FUNCTION__` on Clang/GCC).
Name-based prefixes are **stable across struct layout changes** — adding, removing, or reordering
unrelated fields does not shift the index prefix, unlike an offset-based scheme.
The prefix changes only when the field is renamed.

**PK encoding ordering rules** (BinPack wire format):

- Unsigned integers: big-endian bytes (naturally ordered)
- Signed integers: stored as-is in big-endian (use unsigned types for ordered scans)
- `std::string`: raw UTF-8 bytes (lexicographic)

---

## 4. Memory Management

### 4.1 MonotonicArena

Bump-pointer allocator used for BPTree node allocation. Block size: **256 KiB**.

- **Allocation**: O(1), ~3–5 ns. No per-object free.
- **Deallocation**: Entire arena freed as a unit when MemTable shard is flushed.
- **Alignment**: 64-byte default for cache-line alignment.

### 4.2 PerThreadArena

Three-tier buffer pool for WAL and SST I/O buffers. Each tier is per-thread, no cross-thread sharing.

| Tier           | Latency | Notes                               |
|----------------|---------|-------------------------------------|
| Clean pool     | ~10 ns  | Zero-filled buffers ready to use    |
| Dirty queue    | ~15 ns  | Returned buffers awaiting zero-fill |
| Bump allocator | ~15 ns  | New blocks from OS                  |

Default block size: 32 KiB, 4 KiB aligned.

### 4.3 OwnedBuffer

RAII-owning aligned buffer. Uses `_aligned_malloc`/`_aligned_free` on Windows, `posix_memalign`/`free` on POSIX. Move-only; copying deleted.

---

## 5. MemTable

### 5.1 Sharding

- **Shard count**: Always a power of 2, range \[2, 256\]. Auto-derived from `expected_concurrent_writers` using birthday-paradox formula:
  `S = next_pow2(⌈N×(N−1)×2.25⌉)`, minimum 2.
- **Shard selection**: `key_fp64 & (shard_count - 1)` — branchless, ~1 ns, no integer division.
- Each shard has its own `shared_mutex` (readers shared, writers exclusive).

### 5.2 Backends

#### BPTree (default)

B+ tree with leaf nodes allocated from `MonotonicArena`.

| Property          | Value                                           |
|-------------------|-------------------------------------------------|
| Leaf order        | 503 (for 64B MemRecord keys)                    |
| Leaf node size    | ~32 KiB                                         |
| Point lookup      | O(log N)                                        |
| Range scan        | O(log N + K) via leaf linked list               |
| Fingerprint check | Mini-key 8B prefix shortcut before full compare |

#### SkipList

Probabilistic ordered map with `p = 0.25`, `MAX_HEIGHT = 12`. Nodes allocated from `MonotonicArena`.

| Property        | Value                                    |
|-----------------|------------------------------------------|
| Expected height | ~log₄(N)                                 |
| Point lookup    | O(log N) expected                        |
| Node size       | 64B (MemRecord) + 4B (height) + pointers |
| PRNG            | `std::mt19937` seeded at construction    |

### 5.3 Flush Lifecycle

1. Shard accumulates writes into active `IMemMap`.
2. When `size_bytes > threshold_bytes_per_shard`: shard is sealed.
3. Sealed map collected as `std::shared_ptr<IMemMap>`.
4. Per-shard flusher thread calls `collect_sorted()` → sorted `vector<MemRecord>`.
5. `FlushCallback` invoked with sorted records (typically writes an SST).
6. `on_flushed(shard_id)` called to release the sealed map.

### 5.4 Sequence Generation

Atomic monotonic counter (`seq_gen_`), shared across all shards. On WAL recovery, `seq_gen_` is advanced to `max(current, recovered_max + 1)`.

### 5.5 `find_into` Return Semantics

```
return value        meaning
─────────────────   ──────────────────────────────────────────
std::nullopt        Key not found in MemTable; continue to SST
false               Key found, is TOMBSTONE; skip SST lookup
true                Key found, value written into `out`
```

---

## 6. Write-Ahead Log (WAL)

### 6.1 Segment File Layout

Each shard writes to its own segment file: `{wal_dir}/{shard_id:04d}_{segment_id:016x}.akwal`

**Segment header** (32 bytes, little-endian):

```
Offset  Size  Field        Description
──────  ────  ───────────  ──────────────────────────────────────────
  0      4    magic        0x414B5741 ("AKWA" in memory, LE on disk)
  4      2    version      0x0001
  6      2    shard_id     0-based shard index
  8      8    segment_id   Monotonic per-shard counter
 16      8    created_at   Microseconds since Unix epoch
 24      4    crc32c       CRC32C of header (computed with crc32c=0)
 28      4    reserved     0
──────────────────────────────────────────────────────────────────────
Total  32 bytes
```

**Per-entry layout** (immediately follows previous entry or segment header):

```
┌────────────────┬────────────┬─────────────┬──────────┐
│  AKHdr32 (32B) │  key bytes │  value bytes│ crc32c(4)│
└────────────────┴────────────┴─────────────┴──────────┘
                               ↑ 0 bytes for tombstone
```

CRC32C covers: entire `AKHdr32` + key bytes + value bytes (not including the trailing CRC field).

### 6.2 Sharding

Shard count: always power-of-2 in {2, 4, 8, 16}. Auto-derived from `expected_concurrent_writers`.
Shard for a write: `key_fp64 & (shard_count - 1)`.

### 6.3 Sync Modes

| Mode    | Behaviour                                                          | Latency | Durability                                 |
|---------|--------------------------------------------------------------------|---------|--------------------------------------------|
| `Sync`  | `fdatasync` after every batch                                      | High    | Full                                       |
| `Async` | Background flusher thread with `group_n`/`group_micros` thresholds | Low     | Best-effort (power-safe after group flush) |
| `Off`   | No `fdatasync`; page cache only                                    | Minimal | None (call `force_sync()` manually)        |

### 6.4 Double-Buffered Entry Arena

Each `ShardWriter` maintains two entry arenas (`entry_buf_[2]`).

- `enqueue()`: serializes entry into **active write arena** under `queue_mutex_` (no heap allocation).
- `flusher_loop`: **atomically swaps** arenas, then writes to disk without holding the queue lock.

Result: `schedule_delete` / `enqueue` never blocks on disk I/O.

### 6.5 WAL Recovery (`WalRecovery`)

1. Enumerate all `.akwal` files in `wal_dir`, grouped by shard.
2. For each file: validate segment header CRC, then read entries in order.
3. Per entry: validate entry CRC; on mismatch, stop reading that segment (rest is truncated/corrupt).
4. Apply valid entries to MemTable via `mt.put(key, value, seq, flags, fp64)`.
5. Advance `seq_gen_` to `max_recovered_seq + 1`.
6. WAL files from before the last Manifest checkpoint may be pruned.

---

## 7. Blob Manager

### 7.1 Purpose

Values whose size meets or exceeds `blob_threshold_bytes` (default: **16 384 bytes = 16 KiB**) are externalized to the blob store. The MemTable and WAL store a
**20-byte `BlobRef`** instead of the raw value bytes.

### 7.2 Directory Layout

```
{data_dir}/blobs/
    00/   ← high byte of blob_id = 0x00
        0000000000000001.blob
        0000000000000002.blob
    01/
        0100000000000042.blob
    ...
    ff/
```

All 256 subdirectories are pre-created at `start()` time to eliminate `create_directories` calls on the hot write path.

**Blob ID** equals the WAL sequence number of the write that created the blob, guaranteeing global uniqueness without a separate counter.

### 7.3 Blob File Format

```
Offset  Size  Field          Description
──────  ────  ─────────────  ──────────────────────────────────────────
  0      4    magic          "AKBF"
  4      2    version        0x0001
  6      1    codec          0=None, 1=Zstd
  7      1    flags          Reserved (0)
  8      8    blob_id        WAL seq of creating write
 16      8    total_size     Uncompressed content size (always)
 24      4    compressed_sz  Stored payload size (= total_size if None)
 28      4    crc32c         CRC32C of header (with crc32c=0)
──────────────────────────────────────────────────────────────────────
 32    var    payload        Raw or Zstd-compressed content
```

`total_size` always reflects the **uncompressed** size so the reader can pre-allocate the decompression buffer without reading the payload first.

### 7.4 Write Path (Crash-Safe)

1. Write to `{path}.tmp` (platform file I/O, fsync).
2. Rename `.tmp` → `.blob` (atomic on all supported filesystems).
3. If the process crashes between steps 1 and 2, leftover `.tmp` files are harmless and can be purged at startup.

### 7.5 Delete Path (Crash-Safe, Two-Phase)

1. `schedule_delete(blob_id)` → pushed to GC queue.
2. GC worker renames `.blob` → `.blob.del` (atomic rename, crash-safe).
3. GC worker deletes `.blob.del`.
4. On startup, `startup_cleanup()` deletes any leftover `*.blob.del` files.

### 7.6 GC Worker

- Single background thread, wakes on `del_cv` or 200 ms timeout.
- Batch drain: swaps entire queue in one lock acquisition, processes batch **outside** the lock.
- Callers (`schedule_delete`, `scan_orphans`) never block on disk I/O.

### 7.7 Compression

If `blob_codec = Codec::Zstd`:

- Compress with `ZSTD_CLEVEL_DEFAULT`.
- Accept compressed result only if: no error AND `cz < original_size` AND `cz ≤ UINT32_MAX`.
- Incompressible data is stored as `Codec::None` regardless.

---

## 8. Sorted String Tables (SST)

### 8.1 File Naming

`{data_dir}/sstable/L{level}/{seq:016x}.aksst`

### 8.2 File Format

**File header** (64 bytes, little-endian):

```
Offset  Size  Field          Description
──────  ────  ─────────────  ──────────────────────────────────────────
  0      4    magic          "AKSS"
  4      2    version        0x0001
  6      1    level          0–6
  7      1    flags          Reserved (0)
  8      8    entry_count    Total records (including tombstones)
 16      8    data_size      Bytes in data section
 24      8    index_offset   Absolute file offset of sparse index
 32      4    index_count    Sparse index entry count
 36      4    bloom_offset   File offset of bloom filter (0 = none)
 40      4    bloom_size     Bloom filter byte size (0 = none)
 44      8    min_seq        Minimum seq in file
 52      8    max_seq        Maximum seq in file
 60      4    crc32c         CRC32C of header (with crc32c=0)
──────────────────────────────────────────────────────────────────────
Total  64 bytes
```

**Sections** (in order):

| Section      | Offset         | Content                                                     |
|--------------|----------------|-------------------------------------------------------------|
| Data         | 64             | `[AKHdr32 \| key bytes \| value bytes \| crc32c(4)]` × N    |
| Sparse index | `index_offset` | `[data_offset(8) \| key_len(2) \| key_bytes]` × index_count |
| Bloom filter | `bloom_offset` | Variable-length bit array                                   |

**Sparse index stride**: One entry every 128 data records. First and last records always indexed.

### 8.3 Bloom Filter

- Hash function: double-hashing derived from `key_fp64`
- Default: **10 bits per key** → ~1% false-positive rate
- On positive bloom hit: seek to nearest index entry, scan ≤128 records

### 8.4 Compaction Levels

| Level | Key range                               | Trigger                                 |
|-------|-----------------------------------------|-----------------------------------------|
| L0    | Overlapping (hash-sharded flush output) | `file_count ≥ max_l0_files` (default 8) |
| L1–L6 | Non-overlapping, sorted by `first_key`  | `level_bytes > budget(n)`               |

**Level budget**: `l1_max_bytes × level_multiplier^(n−1)`. Default: L1=10 MiB, L2=100 MiB, L3=1 GiB, …

**Tombstone elision**: Tombstones are dropped only when compacting into the **bottom level (L6)** and no older SST files remain that could contain the key.

**Target file size**: 4 MiB. Files larger than this are split at compaction output boundaries.

### 8.5 Read-Through Promotion

When `sst_promote_reads = true`, a key read from SST is re-inserted into the MemTable. Subsequent reads of the same hot key are served from RAM.

---

## 9. Manifest

### 9.1 Purpose

The Manifest tracks the lifecycle of SST files: which files exist at each level, which have been deleted, and periodic checkpoints. It enables crash recovery
without scanning the entire SST directory.

### 9.2 File Format

**File**: `{data_dir}/manifest.akmf`

**File header** (32 bytes, little-endian):

```
Offset  Size  Field     Description
──────  ────  ────────  ──────────────────────────────────────────
  0      4    magic     "AKMF"
  4      2    version   0x0001
  6      2    flags     0 = full; 1 = delta-only
  8      8    created   Microseconds since epoch
 16      8    seq       Monotonic manifest record counter
 24      4    crc32c    CRC32C of header (with crc32c=0)
 28      4    reserved  0
──────────────────────────────────────────────────────────────────
Total  32 bytes
```

**Record header** (immediately follows each payload):

```
Offset  Size  Field      Description
──────  ────  ─────────  ──────────────────────────────────────────
  0      4    magic      "AKRH"
  4      1    type       ManifestRecordType (SSTFlush, SSTDelete, StripeCommit, Checkpoint, SSTSeal)
  5      2    pay_len    Payload length
  7      4    crc32c     CRC32C of header + payload (with crc32c=0)
 11      1    reserved   0
──────────────────────────────────────────────────────────────────
 12    var    payload    Record-type-specific data
```

### 9.3 Async Write Mode

When `fast_mode = true`, records are queued and written by a background flusher thread. The queue uses `std::vector` with `swap` to drain atomically without
holding the lock during disk I/O.

### 9.4 Checkpoint Pruning

On each checkpoint, `deleted_sst_` is cleared and `sst_seals_` entries for files no longer in `live_sst_` are purged. This bounds in-memory state growth without
time-based heuristics.

### 9.5 Rotation

When the manifest file exceeds 32 MiB, it is rotated: a new file is written with a full checkpoint (all live SST files), then the old file is deleted.

---

## 10. Version Log

### 10.1 Purpose

Optional per-key write history. When enabled, every `put` and `remove` is appended to a `.akvlog` file alongside the WAL. Enables:

- Point-in-time reads (`get_at(key, seq)`)
- Full write history (`history(key)`)
- Engine-level rollback (`rollback_to(seq)`, `rollback_key(key, seq)`)

Enabled via `AkkEngineOptions::VersionLogOptions::enabled = true` or `StartupMode::DURABLE`.

### 10.2 File Format

**File**: `{data_dir}/history.akvlog`

**File header** (32 bytes):

```
Offset  Size  Field     Description
──────  ────  ────────  ──────────────────────────────────
  0      4    magic     "AKVL"
  4      2    version   0x0001
  6      2    flags     Reserved
  8      8    created   Nanoseconds since Unix epoch
 16      8    reserved  0
 24      4    crc32c    CRC32C of header (with crc32c=0)
 28      4    reserved  0
──────────────────────────────────────────────────────────
Total  32 bytes
```

**Per-entry layout**:

```
Offset  Size  Field        Description
──────  ────  ───────────  ──────────────────────────────────────────
  0      4    entry_len    Total entry size (including this field)
  4      8    seq          Write sequence number
 12      8    node_id      Source node (0 = local/unknown)
 20      8    timestamp_ns Wall-clock nanoseconds since epoch
 28      1    flags        0x00=NORMAL, 0x01=TOMBSTONE, 0x02=BLOB
 29      8    fp64         SipHash-2-4 of key
 37      2    key_len      Key length
 39      4    val_len      Value length (0 for tombstone)
 43   key_len key          Raw key bytes
  +   val_len value        Raw value bytes (or BlobRef if FLAG_BLOB)
  +      4    crc32c       CRC32C of entire entry (with crc32c=0)
──────────────────────────────────────────────────────────────────────
```

### 10.3 VersionEntry

```cpp
struct VersionEntry {
    uint64_t seq;
    uint64_t source_node_id;  // 0 = local write
    uint64_t timestamp_ns;
    uint8_t  flags;           // same as AKHdr32 flags
    std::vector<uint8_t> value;
};
```

### 10.4 Rollback Semantics

`rollback_to(seq)`: Identifies all keys that have writes with `seq > target_seq` and re-applies their last state at or before `target_seq` (re-issues `put` or
`remove`). Both WAL and VersionLog are updated by the rollback writes.

`rollback_key(key, seq)`: Same as above for a single key.

Rollback entries are written as normal put/remove operations; they are indistinguishable from user writes at the WAL level.

### 10.5 Recovery

On `open()`, the `.akvlog` is replayed from the beginning. Entries with CRC32C mismatches are skipped (treated as truncated). The in-memory index (
`key → list of VersionEntry`) is rebuilt from valid entries.

---

## 11. API Servers

### 11.1 Binary TCP Protocol

**Request frame** (16-byte header + variable payload):

```
Offset  Size  Field       Description
──────  ────  ──────────  ──────────────────────────────────────────
  0      4    magic       "AKRQ"
  4      1    version     0x01
  5      1    opcode      ApiOp (see below)
  6      4    request_id  Echo'd in response (pipelining)
 10      2    key_len     Key length
 12      4    val_len     Value length (0 for Get/Remove)
──────────────────────────────────────────────────────────────────
 16  key_len  key         Raw key bytes
  +  val_len  value       Raw value bytes
  +       4   crc32c      CRC32C of key + value
```

**Response frame** (13-byte header + variable payload):

```
Offset  Size  Field       Description
──────  ────  ──────────  ──────────────────────────────────────────
  0      4    magic       "AKRS"
  4      1    status      ApiStatus (Ok=0x00, NotFound=0x01, Error=0xFF)
  5      4    request_id  Echoed from request
  9      4    val_len     Value length (0 for NotFound/Error)
──────────────────────────────────────────────────────────────────
 13  val_len  value       Response value bytes
  +       4   crc32c      CRC32C of value
```

**Opcodes** (`ApiOp`):

| Value | Name     | Description                                      |
|-------|----------|--------------------------------------------------|
| 0x01  | `Get`    | Point lookup                                     |
| 0x02  | `Put`    | Insert or replace                                |
| 0x03  | `Remove` | Insert tombstone                                 |
| 0x04  | `GetAt`  | Point-in-time read (val = seq as big-endian u64) |

### 11.2 HTTP REST API

| Method   | Path                               | Request        | Response              |
|----------|------------------------------------|----------------|-----------------------|
| `GET`    | `/v1/get?key=<encoded>`            | —              | 200 (raw bytes) / 404 |
| `POST`   | `/v1/put?key=<encoded>`            | raw bytes body | 204                   |
| `DELETE` | `/v1/remove?key=<encoded>`         | —              | 204                   |
| `GET`    | `/v1/get_at?key=<encoded>&seq=<n>` | —              | 200 / 404             |

- Content-Type: `application/octet-stream`
- Keys: URL percent-encoded, binary-safe
- Keep-alive: supported
- One thread per connection

### 11.3 Threading

Both servers run one dedicated accept thread. Each accepted connection runs on its own thread. Optional TLS wrapping via `TlsStream` (see §13).

---

## 12. Cluster & Replication

### 12.1 Cluster Modes

| Mode         | Description                                                    |
|--------------|----------------------------------------------------------------|
| `Standalone` | Single node; no replication                                    |
| `Mirror`     | Full copies on all replicas (RAID-1 semantics)                 |
| `Stripe`     | Data sharded across replicas (RAID-0 semantics; no redundancy) |

### 12.2 Node Identity

Each node is assigned a unique 64-bit `node_id`. When `data_dir` is persistent, the ID is persisted to `{data_dir}/node.id` (plain text, hex). This ensures
consistent identity across restarts, even in non-cluster mode (used as `source_node_id` in VersionLog entries).

### 12.3 Replication Framing

**Handshake — Client Hello** (22 bytes):

```
Offset  Size  Field      Description
──────  ────  ─────────  ──────────────────────────────────────────
  0      4    magic      "AKRH"
  4      8    node_id    Sender's node ID
 12      8    last_seq   Last replicated seq (resume point)
 20      2    flags      Reserved
──────────────────────────────────────────────────────────────────
Total  22 bytes
```

**Handshake — Server Hello** (22 bytes):

```
Offset  Size  Field       Description
──────  ────  ──────────  ──────────────────────────────────────────
  0      4    magic       "AKPH"
  4      8    node_id     Server's node ID
 12      8    current_seq Server's current sequence
 20      2    flags       Reserved
──────────────────────────────────────────────────────────────────
Total  22 bytes
```

**Message types** (`ReplMsgType`):

| Value | Name      | Description               |
|-------|-----------|---------------------------|
| 0x01  | `Entry`   | WAL entry (put or delete) |
| 0x02  | `BlobPut` | Blob file transfer        |
| 0x03  | `Ack`     | Replica acknowledgement   |

### 12.4 Cluster Config File

**File**: `{data_dir}/cluster.akcc`

Binary format: 32-byte file header (`"AKCC"`) + per-node entries, each containing `node_id(8) + host + data_port(2) + repl_port(2)`.

---

## 13. TLS Support

### 13.1 Build-Time Option

TLS support is **disabled by default** and compiled in only when the CMake option is set:

```cmake
cmake -DAKKARADB_ENABLE_TLS=ON ..
```

This fetches **mbedTLS 3.6.2** (static) and defines `AKKARADB_TLS_ENABLED`. When not compiled in, all TLS config is accepted but `cfg.enabled=true` throws
`std::runtime_error` at startup. `cfg.enabled=false` has **zero overhead** regardless.

### 13.2 TlsConfig

```cpp
struct TlsConfig {
    bool        enabled     = false;
    std::string cert_path;        // PEM certificate (server cert / client cert for mTLS)
    std::string key_path;         // PEM private key
    std::string ca_path;          // PEM CA cert for peer verification (mTLS)
    bool        verify_peer = false;  // require peer certificate (enables mTLS)
};
```

### 13.3 TlsContext

Shared per server/client. Thread-safe for concurrent `server_wrap`/`client_wrap` calls.

- **Server**: loads `cert_path` + `key_path`; optionally `ca_path` for mTLS.
- **Client**: loads `ca_path` for server certificate verification.

### 13.4 TlsStream

Per-connection I/O wrapper. Created after `accept()` / `connect()`:

```cpp
// Server side (performs TLS handshake if ctx != nullptr):
auto stream = TlsStream::server_wrap(fd, ctx.get());

// Client side (performs TLS handshake if ctx != nullptr):
auto stream = TlsStream::client_wrap(fd, hostname, ctx.get());

stream->send_all(data, len);
stream->recv_all(buf, len);
stream->shutdown();  // TLS close_notify; does NOT close the underlying fd
```

**Plain TCP** (`ctx == nullptr`): `send_all`/`recv_all` delegate directly to `::send`/`::recv`. Zero overhead.

### 13.5 Scope

TLS wrapping is applied to all three TCP endpoints: `TcpApiServer`, `HttpApiServer`, and `ReplicationServer`/`ReplicationClient`. All share the same `TlsConfig`
in `AkkEngineOptions::ApiOptions::tls`.

---

## 14. Configuration Reference

### 14.1 StartupMode Presets

| Mode         | WAL   | Sync      | VersionLog | MemTable threshold |
|--------------|-------|-----------|------------|--------------------|
| `ULTRA_FAST` | Off   | —         | Off        | 256 MiB/shard      |
| `FAST`       | Async | group     | Off        | 128 MiB/shard      |
| `NORMAL`     | Async | group     | Off        | 64 MiB/shard       |
| `DURABLE`    | Sync  | per-write | On         | 32 MiB/shard       |

Individual options can be overridden via `AkkaraDB::Options::Overrides`.

### 14.2 AkkEngineOptions Fields

#### Storage

| Field              | Default                  | Description                            |
|--------------------|--------------------------|----------------------------------------|
| `data_dir`         | (required)               | Root directory for all files           |
| `wal_enabled`      | `true`                   | Disable WAL entirely (ULTRA_FAST mode) |
| `blob_enabled`     | `true`                   | Enable large-value externalization     |
| `manifest_enabled` | `true`                   | Enable SST lifecycle tracking          |
| `blob_dir`         | `data_dir/blobs`         | Blob storage directory                 |
| `manifest_path`    | `data_dir/manifest.akmf` | Manifest file path                     |

#### MemTable

| Field                         | Default  | Description                                                           |
|-------------------------------|----------|-----------------------------------------------------------------------|
| `shard_count`                 | auto     | Power-of-2, \[2, 256\]; 0 = derive from `expected_concurrent_writers` |
| `threshold_bytes_per_shard`   | 64 MiB   | Flush trigger per shard                                               |
| `backend`                     | `BPTree` | `BPTree` or `SkipList`                                                |
| `expected_concurrent_writers` | 0        | Used to auto-derive shard count                                       |

#### WAL

| Field               | Default        | Description                    |
|---------------------|----------------|--------------------------------|
| `wal_dir`           | `data_dir/wal` | WAL segment directory          |
| `sync_mode`         | `Sync`         | `Sync`, `Async`, or `Off`      |
| `shard_count`       | auto           | Power-of-2 in {2,4,8,16}       |
| `group_n`           | 128            | Async: batch size trigger      |
| `group_micros`      | 100            | Async: batch time trigger (µs) |
| `max_segment_bytes` | 512 MiB        | Roll to new segment file       |

#### Blob

| Field                  | Default | Description                    |
|------------------------|---------|--------------------------------|
| `blob_threshold_bytes` | 16384   | Values ≥ this are externalized |
| `blob_codec`           | `None`  | `None` or `Zstd`               |

#### SST

| Field                    | Default | Description                                |
|--------------------------|---------|--------------------------------------------|
| `max_level`              | 7       | Number of SST levels (1-based, so L0..L6)  |
| `max_l0_files`           | 8       | L0→L1 compaction trigger                   |
| `l1_max_bytes`           | 10 MiB  | L1 budget; each level × `level_multiplier` |
| `level_multiplier`       | 10      | Exponential growth factor                  |
| `target_file_size_bytes` | 4 MiB   | Compaction output file size                |
| `bloom_bits_per_key`     | 10      | ~1% false-positive rate                    |
| `sst_codec`              | `None`  | `None` or `Zstd`                           |
| `sst_promote_reads`      | `false` | Re-insert SST reads into MemTable          |

#### Version Log

| Field       | Default                   | Description                   |
|-------------|---------------------------|-------------------------------|
| `enabled`   | `false`                   | Enable write history tracking |
| `log_path`  | `data_dir/history.akvlog` | History file path             |
| `sync_mode` | `Async`                   | `Sync`, `Async`, or `Off`     |

#### API Servers

| Field          | Default  | Description                       |
|----------------|----------|-----------------------------------|
| `http_enabled` | `false`  | Enable HTTP REST server           |
| `http_port`    | 8080     | HTTP listen port                  |
| `tcp_enabled`  | `false`  | Enable binary TCP server          |
| `tcp_port`     | 9090     | TCP listen port                   |
| `tls`          | disabled | `TlsConfig` for all TCP endpoints |

---

## 15. Public API Reference

### 15.1 AkkaraDB (High-Level Entry Point)

```cpp
// Open with a preset
auto db = AkkaraDB::open("/path/to/data", StartupMode::NORMAL);

// Open with full control
AkkaraDB::Options opts;
opts.data_dir = "/path/to/data";
opts.mode = StartupMode::FAST;
opts.overrides.blob_threshold_bytes = 32 * 1024;
auto db = AkkaraDB::open(opts);

// Access underlying engine directly
AkkEngine& eng = db->engine();

// Create a typed table (PK field inferred from member pointer)
auto users = db->table<&User::id>("users");

// With secondary indexes
auto users = db->table<&User::id>("users")
                 .index<&User::email>()
                 .index<&User::age>();
```

### 15.2 AkkEngine (Core KV API)

```cpp
// Basic CRUD
eng.put(key_span, value_span);
auto val = eng.get(key_span);            // std::optional<vector<uint8_t>>
eng.remove(key_span);
bool ok = eng.exists(key_span);

// Range scan
auto it = eng.scan(start_key, end_key);
while (it.has_next()) {
    auto [k, v] = *it.next();
}

size_t n = eng.count(start_key, end_key);

// Version log (requires enabled=true)
auto entry = eng.get_at(key_span, seq);            // optional<VersionEntry>
auto hist  = eng.history(key_span);                // vector<VersionEntry>
eng.rollback_to(seq);
eng.rollback_key(key_span, seq);

// Lifecycle
eng.force_sync();  // flush WAL + VersionLog to disk
eng.close();
```

### 15.3 PackedTable<PrimaryKeyPtr>

```cpp
// Open — entity type and key type inferred from member pointer
auto users = db->table<&User::id>("users");

// With secondary indexes (fluent, rvalue chain)
auto users = db->table<&User::id>("users")
                 .index<&User::email>()
                 .index<&User::age>();

// CRUD
users.put(user);                       // id extracted automatically
auto u  = users.get(42ULL);            // optional<User>
users.remove(42ULL);
bool ok = users.exists(42ULL);
users.upsert(42ULL, [](User& u) { u.age++; });

// Secondary index lookup (index must be registered at construction)
auto u = users.find_by<&User::email>("alice@example.com");  // optional<User>

// Count
size_t n = users.count();

// Range scan
auto it = users.scan_all();
auto it = users.scan(start_id);
auto it = users.scan(start_id, end_id);  // [start, end)
while (it.has_next()) {
    auto [id, user] = it.next();
}

// Zero-overhead single-predicate query (FilterView<Pred> — template, inlined by compiler)
auto results = users
    .query([](const User& u) { return u.age > 21; })
    .limit(100)
    .to_vector();

// Multi-predicate builder query (Query — chains .where() calls, uses std::function)
auto results = users
    .query()
    .where([](const User& u) { return u.age > 21; })
    .where([](const User& u) { return u.active; })
    .limit(100)
    .to_vector();

// Range-for (works with both FilterView and Query)
for (const auto& [id, user] : users.query([](const User& u) { return u.active; })) { ... }

// Terminal operators (available on both FilterView and Query)
auto first = users.query([](const User& u) { return u.age > 18; }).first();  // optional<Entry{id, value}>
bool any   = users.query([](const User& u) { return u.age > 18; }).any();
size_t cnt = users.query([](const User& u) { return u.age > 18; }).count();
```

**Thread-safety**: `PackedTable` is **not thread-safe**. `AkkEngine` (accessed via `engine()`) is.

### 15.4 BinPack — Serialization Layer

Serialization is zero-config. All fields of an aggregate struct are serialized in
declaration order via Boost.PFR. No specialization is required for:

| Category    | Types                                                                                                                                      |
|-------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| Primitives  | `bool`, `int8_t`–`int64_t`, `uint8_t`–`uint64_t`, `float`, `double`                                                                        |
| Strings     | `std::string` (write+read), `std::string_view` (write only)                                                                                |
| Byte array  | `std::vector<uint8_t>` (optimized bulk path)                                                                                               |
| Collections | `std::optional<T>`, `std::vector<T>`, `std::array<T,N>`, `std::map<K,V>`, `std::unordered_map<K,V>`, `std::pair<A,B>`, `std::tuple<Ts...>` |
| Enums       | Serialized as `underlying_type`                                                                                                            |
| Aggregates  | Any `struct` with no user-provided constructor — fields serialized recursively                                                             |

For custom types, specialize `TypeAdapter<T>`:

```cpp
template<>
struct akkaradb::binpack::TypeAdapter<MyType> {
    static void   write(const MyType& v, std::vector<uint8_t>& out);
    static MyType read(std::span<const uint8_t>& in);   // must advance span
    static size_t estimate_size(const MyType& v);
};
```

Direct BinPack API:

```cpp
auto bytes = BinPack::encode(value);          // → vector<uint8_t>
auto val   = BinPack::decode<T>(bytes);       // → T
BinPack::encode_into(value, buf);             // append to existing buffer
size_t n   = BinPack::estimate_size(value);   // upper-bound estimate
```

---

## 16. File Format Reference

### Magic Numbers Summary

| Extension | Magic (ASCII) | Magic (hex LE) | Version  |
|-----------|---------------|----------------|----------|
| `.akwal`  | `AKWA`        | `0x414B5741`   | `0x0001` |
| `.aksst`  | `AKSS`        | `0x414B5353`   | `0x0001` |
| `.blob`   | `AKBF`        | `0x4642414B`   | `0x0001` |
| `.akvlog` | `AKVL`        | `0x4C564B41`   | `0x0001` |
| `.akmf`   | `AKMF`        | `0x464D4B41`   | `0x0001` |
| `.akcc`   | `AKCC`        | `0x4343414B`   | `0x0001` |

### Checksum Policy

All file headers contain a `crc32c` field computed over the **entire header** with the `crc32c` field zeroed during computation. All data records contain a
trailing `crc32c(4)` covering the record payload. The CRC32C implementation uses the Castagnoli polynomial (0x82F63B78) with hardware acceleration (SSE4.2) when
available.

### Endianness

All multi-byte integer fields are **little-endian** on disk, matching x86/x64 native byte order. Exception: `PackedTable` key encodings use **big-endian** for
lexicographic ordering of ID types.

---

## 17. Threading & Concurrency Model

### 17.1 Thread-Safe Components

| Component     | Guarantee                                              |
|---------------|--------------------------------------------------------|
| `AkkEngine`   | All public methods: fully thread-safe                  |
| `MemTable`    | Thread-safe; per-shard `shared_mutex`                  |
| `WalWriter`   | Thread-safe; per-shard lock-free queue                 |
| `BlobManager` | `write`/`read`/`schedule_delete`: thread-safe          |
| `VersionLog`  | Thread-safe; separate `index_mutex_` + `file_mutex_`   |
| `SSTManager`  | Thread-safe; `shared_mutex` for flush/get/compaction   |
| `TlsContext`  | Thread-safe for concurrent `server_wrap`/`client_wrap` |

### 17.2 NOT Thread-Safe

| Component             | Note                                                |
|-----------------------|-----------------------------------------------------|
| `PackedTable<MPtr>`   | Create one per thread, or synchronize externally    |
| `SSTReader`           | One reader instance per thread                      |
| `BPTree` / `SkipList` | Synchronized by MemTable's per-shard `shared_mutex` |
| `MonotonicArena`      | Per-shard, accessed only under shard lock           |

### 17.3 Background Threads

| Thread                 | Count                        | Owner                                     |
|------------------------|------------------------------|-------------------------------------------|
| MemTable shard flusher | 1 per shard                  | `MemTable`                                |
| WAL shard flusher      | 1 per WAL shard (Async mode) | `WalWriter`                               |
| SST compaction         | 1                            | `SSTManager`                              |
| Blob GC                | 1                            | `BlobManager`                             |
| Manifest flusher       | 1 (fast_mode)                | `Manifest`                                |
| VersionLog flusher     | 1 (Async mode)               | `VersionLog`                              |
| TCP/HTTP accept        | 1 per server                 | `TcpApiServer` / `HttpApiServer`          |
| TCP/HTTP connection    | 1 per connection             | `TcpApiServer` / `HttpApiServer`          |
| Replication accept     | 1                            | `ReplicationServer`                       |
| Replication connection | 1 per peer                   | `ReplicationServer` / `ReplicationClient` |

---

## 18. Error Handling & Recovery

### 18.1 CRC32C Validation

Every record on disk has a `crc32c` field. On read:

- **WAL recovery**: CRC mismatch stops reading the current segment (remaining bytes are truncated/corrupt). Earlier valid entries are applied.
- **SST read**: CRC mismatch throws `std::runtime_error`; the affected SST is considered corrupt.
- **Blob read**: CRC mismatch throws `std::runtime_error`.
- **VersionLog recovery**: CRC mismatch skips the entry; recovery continues from next entry boundary.
- **Manifest**: CRC mismatch on a record skips that record.

### 18.2 Startup Cleanup

On `BlobManager::start()`:

- Deletes all leftover `*.blob.del` files (from crashes between rename and delete).

On `WalRecovery`:

- Re-applies all valid WAL entries to a fresh MemTable.
- Advances `seq_gen_` past recovered maximum.

### 18.3 Exception Policy

All public methods that perform I/O throw `std::runtime_error` on unrecoverable errors (corrupt files, OS errors). Operations that are expected to sometimes
fail (key not found, bloom false positive) return `std::optional` or `bool`.

---

## 19. Build & Integration

### 19.1 Requirements

| Item         | Minimum                                                    |
|--------------|------------------------------------------------------------|
| C++ Standard | C++23                                                      |
| CMake        | 4.1+                                                       |
| Windows      | MSVC 19.38+ (VS 2022 17.8+)                                |
| Linux        | GCC 13+ or Clang 17+                                       |
| Zstd         | Fetched automatically (v1.5.6)                             |
| Boost.PFR    | Fetched automatically (boost-1.84.0, header-only)          |
| mbedTLS      | Fetched automatically if `AKKARADB_ENABLE_TLS=ON` (v3.6.2) |

### 19.2 CMake Options

| Option                 | Default | Description                       |
|------------------------|---------|-----------------------------------|
| `BUILD_SHARED_LIBS`    | `ON`    | Build shared library (.dll / .so) |
| `AKKARADB_BUILD_TESTS` | `OFF`   | Build unit tests                  |
| `AKKARADB_ENABLE_SIMD` | `ON`    | Enable SSE4.2 / AVX2              |
| `AKKARADB_ENABLE_TLS`  | `OFF`   | Enable TLS via mbedTLS 3.x        |

### 19.3 Linking

**Static zstd** is always linked (`libzstd_static`). On Windows, `ws2_32` is linked for Winsock2.

```cmake
target_link_libraries(my_app PRIVATE akkaradb)
target_include_directories(my_app PRIVATE ${AKKARADB_INCLUDE_DIR})
```

### 19.4 Key Headers (Public API)

| Header                                        | Contents                                                         |
|-----------------------------------------------|------------------------------------------------------------------|
| `akkaradb/AkkaraDB.hpp`                       | `AkkaraDB`, `StartupMode`, `Options`                             |
| `akkaradb/PackedTable.hpp`                    | `PackedTable<MPtr>`, `FilterView<Pred>`, `Query`, `Iterator`     |
| `akkaradb/binpack/BinPack.hpp`                | `BinPack::encode`, `decode`, `encode_into`, `estimate_size`      |
| `akkaradb/binpack/TypeAdapter.hpp`            | `TypeAdapter<T>` — specialize for custom types                   |
| `akkaradb/binpack/detail/MemberPtrTraits.hpp` | `class_of<MPtr>`, `member_of<MPtr>`, `field_byte_offset<MPtr>()`, `member_name<MPtr>()` |
| `akkaradb/detail/Hash.hpp`                    | `fnv1a_64`, `write_be64`, `read_be64`                            |

Internal headers under `akkara/internal/include/` are part of the build interface but not the public API surface.
