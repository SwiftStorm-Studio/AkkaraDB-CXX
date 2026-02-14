# AkkaraDB Native - Technical Specification

**Version:** 1.0.0
**Last Updated:** 2026-02-15

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Data Structures](#data-structures)
4. [File Formats](#file-formats)
5. [Performance Characteristics](#performance-characteristics)
6. [Configuration](#configuration)
7. [Limitations](#limitations)

---

## Overview

AkkaraDB Native is a high-performance embedded key-value store written in C++23, designed for low-latency random access and high-throughput writes. It implements an LSM-Tree (Log-Structured Merge-Tree) architecture optimized for modern hardware.

### Key Features

- **High Performance**: 1.1M+ ops/sec for point reads
- **Durability**: Write-Ahead Log (WAL) with group commit
- **Crash Safety**: Atomic operations with manifest-based recovery
- **Memory Efficiency**: Custom B+ Tree with 32KB nodes
- **Zero-Copy Operations**: Optimized record handling
- **SIMD Optimizations**: AVX2 for hashing and comparisons

### Design Goals

1. **Latency**: Sub-microsecond p50 read latency
2. **Throughput**: Multi-million ops/sec on commodity hardware
3. **Durability**: No data loss on crashes
4. **Simplicity**: Embedded library, no external dependencies (except nlohmann/json)

---

## Architecture

### LSM-Tree Structure

```
┌─────────────────────────────────────────────────────────┐
│                    AkkaraDB Engine                      │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐      ┌──────────────┐               │
│  │   MemTable   │──────│     WAL      │               │
│  │   (BPTree)   │      │  (Append)    │               │
│  └──────┬───────┘      └──────────────┘               │
│         │ Flush                                        │
│         ▼                                              │
│  ┌──────────────────────────────────────┐             │
│  │         SSTable Levels               │             │
│  ├──────────────────────────────────────┤             │
│  │  L0: [SST] [SST] [SST] ...          │             │
│  │  L1: [SST──────] [SST──────] ...    │             │
│  │  L2: [SST──────────────────] ...    │             │
│  └──────────────────────────────────────┘             │
│         │ Compaction                                   │
│         ▼                                              │
│  ┌──────────────┐                                     │
│  │   Manifest   │ (Metadata + Rotation)               │
│  └──────────────┘                                     │
└─────────────────────────────────────────────────────────┘
```

### Components

#### 1. MemTable (In-Memory Index)

- **Implementation**: Custom B+ Tree with 32KB nodes
- **Node Configuration**:
  - LEAF_ORDER: 368 (for 88-byte MemRecord)
  - INTERNAL_ORDER: 341
  - Prefetching enabled for memory latency reduction
- **Sharding**: Configurable (default: 4 shards) for parallel writes
- **Flush Trigger**: Configurable threshold (default: 32 MB)

**Performance**:
- Point read: ~1.1M ops/sec
- p50 latency: 0.8 µs
- p99 latency: 4.9 µs

#### 2. Write-Ahead Log (WAL)

- **Format**: Custom framing with CRC32C checksums
- **Group Commit**: Batches writes (default: 512 ops or 50ms)
- **Fast Mode**: Background flusher with periodic fsync
- **Rotation**: Creates new WAL segment on MemTable flush

**Frame Format**:
```
[length: u32] [op_type: u8] [payload] [crc32c: u32]
```

#### 3. SSTable (Sorted String Table)

- **Format**: AKK (Akkara) format with blocks
- **Block Size**: 16 KB (configurable)
- **Compression**: Optional (LZ4/Zstd)
- **Index**: Sparse index with binary search
- **Bloom Filter**: Configurable FP rate (default: 1%)

**Structure**:
```
┌─────────────────────────────────────┐
│         Data Blocks                 │
│  [Block 0] [Block 1] ... [Block N]  │
├─────────────────────────────────────┤
│         Index Block                 │
│  (Sparse index to data blocks)      │
├─────────────────────────────────────┤
│         Bloom Filter                │
│  (Probabilistic membership test)    │
├─────────────────────────────────────┤
│         Footer                      │
│  (Metadata: offsets, counts, etc.)  │
└─────────────────────────────────────┘
```

**Performance**:
- Point read: ~1.15M ops/sec (with Bloom filter)
- p50 latency: 0.8 µs

#### 4. Manifest (Metadata Log)

- **Purpose**: Tracks database state (SST files, checkpoints, etc.)
- **Format**: JSON records with CRC32C checksums
- **Rotation**: Automatic at 32 MiB threshold
- **Naming**: `MANIFEST`, `MANIFEST.1`, `MANIFEST.2`, ...
- **Recovery**: Replays all manifest files in order

**Record Types**:
- `StripeCommit`: WAL stripe committed
- `SSTSeal`: New SSTable created
- `SSTDelete`: SSTable deleted
- `CompactionStart/End`: Compaction lifecycle
- `Checkpoint`: Durable state marker
- `Truncate`: WAL truncation event

---

## Data Structures

### MemRecord (88 bytes)

```cpp
struct MemRecord {
    AKHdr32 header;              // 32 bytes
    std::vector<uint8_t> key;    // 24 bytes (pointer + size + capacity)
    std::vector<uint8_t> value;  // 24 bytes
    size_t approx_size;          // 8 bytes
};
```

**AKHdr32 Layout**:
```
┌─────────────────────────────────────────────────────┐
│ flags (1) │ k_len (2) │ v_len (4) │ seq (8)         │
│ key_fp64 (8) │ mini_key (8) │ reserved (1)         │
└─────────────────────────────────────────────────────┘
Total: 32 bytes
```

### BPTree Node (32 KB)

```cpp
struct LeafNode {
    NodeType type;                      // 1 byte
    uint16_t count;                     // 2 bytes
    uint8_t padding[5];                 // Alignment
    LeafNode* next;                     // 8 bytes
    std::array<K, 368> keys;            // 368 * 88 = 32384 bytes
    std::array<V, 368> values;          // 368 * 1 = 368 bytes
};
// Total: ~32752 bytes (fits in 32KB target)
```

**Optimizations**:
1. No virtual functions (no vptr overhead)
2. Fixed-size arrays (no heap allocation)
3. Iterative tree destruction (no stack overflow)
4. Prefetching for memory latency hiding

---

## File Formats

### 1. WAL Format

```
File: <base_dir>/wal/wal_<segment>.log

Frame:
┌────────────────────────────────────────┐
│ length: u32 (LE)                       │
├────────────────────────────────────────┤
│ op_type: u8                            │
│   0x01 = Put                           │
│   0x02 = Delete                        │
├────────────────────────────────────────┤
│ payload:                               │
│   - key_len: u32                       │
│   - key: [u8; key_len]                 │
│   - value_len: u32                     │
│   - value: [u8; value_len]             │
│   - seq: u64                           │
├────────────────────────────────────────┤
│ crc32c: u32 (LE)                       │
│   (checksum of op_type + payload)      │
└────────────────────────────────────────┘
```

### 2. SSTable Format (AKK)

```
File: <base_dir>/L<level>/sst_<id>.sst

Header:
┌────────────────────────────────────────┐
│ magic: [u8; 4] = "AKKS"                │
│ version: u16 = 1                       │
│ flags: u16                             │
└────────────────────────────────────────┘

Data Blocks (16 KB each):
┌────────────────────────────────────────┐
│ entries: [Record; N]                   │
│   Each record: [AKHdr32][key][value]   │
├────────────────────────────────────────┤
│ block_crc: u32                         │
└────────────────────────────────────────┘

Index Block:
┌────────────────────────────────────────┐
│ entries: [(first_key, offset, size)]   │
│   Sparse index pointing to data blocks │
└────────────────────────────────────────┘

Bloom Filter:
┌────────────────────────────────────────┐
│ num_bits: u64                          │
│ num_hashes: u32                        │
│ bits: [u8; num_bits / 8]               │
└────────────────────────────────────────┘

Footer (AKSSFooter):
┌────────────────────────────────────────┐
│ num_entries: u64                       │
│ index_offset: u64                      │
│ index_size: u64                        │
│ bloom_offset: u64                      │
│ bloom_size: u64                        │
│ min_seq: u64                           │
│ max_seq: u64                           │
│ footer_crc: u32                        │
└────────────────────────────────────────┘
```

### 3. Manifest Format

```
File: <base_dir>/MANIFEST[.N]

Record:
┌────────────────────────────────────────┐
│ length: u32 (LE)                       │
├────────────────────────────────────────┤
│ json_payload: JSON string              │
│   {                                    │
│     "type": "SSTSeal",                 │
│     "level": 0,                        │
│     "file": "L0/sst_001.sst",          │
│     "entries": 10000,                  │
│     "firstKeyHex": "...",              │
│     "lastKeyHex": "...",               │
│     "ts": 1234567890                   │
│   }                                    │
├────────────────────────────────────────┤
│ crc32c: u32 (LE)                       │
│   (checksum of json_payload)           │
└────────────────────────────────────────┘

Rotation:
- Threshold: 32 MiB per file
- Naming: MANIFEST (base), MANIFEST.1, MANIFEST.2, ...
- Recovery: Replay all files in order
```

---

## Performance Characteristics

### Read Performance

| Operation | Throughput | p50 Latency | p99 Latency |
|-----------|-----------|-------------|-------------|
| MemTable Read | 1.13M ops/sec | 0.8 µs | 4.9 µs |
| SST Read (Bloom) | 1.15M ops/sec | 0.8 µs | 2.9 µs |
| Range Scan | TBD | TBD | TBD |

### Write Performance

| Operation | Throughput | p50 Latency | p99 Latency |
|-----------|-----------|-------------|-------------|
| Put (WAL enabled) | TBD | TBD | TBD |
| Put (WAL disabled) | TBD | TBD | TBD |
| Batch Write | TBD | TBD | TBD |

### Space Amplification

- **Best Case**: ~1.1x (with aggressive compaction)
- **Worst Case**: ~10x (with no compaction)
- **Typical**: ~2-3x (with default settings)

### Write Amplification

- **L0 → L1**: ~10x (size-tiered compaction)
- **L1 → L2**: ~10x
- **Total**: O(log N) where N = data size

---

## Configuration

### AkkaraDB Options

```cpp
struct AkkaraDBOptions {
    // Base directory
    std::string base_dir;

    // MemTable configuration
    size_t memtable_shard_count = 4;
    size_t memtable_threshold_bytes = 32 * 1024 * 1024; // 32 MB

    // WAL configuration
    size_t wal_group_n = 512;              // Batch size
    size_t wal_group_micros = 50'000;      // 50ms timeout
    bool wal_fast_mode = true;             // Background flusher

    // SSTable configuration
    double bloom_fp_rate = 0.01;           // 1% false positive rate
    size_t block_size = 16384;             // 16 KB

    // Compaction configuration
    bool disable_background_compaction = false;
    size_t compaction_threads = 2;

    // Advanced
    bool enable_statistics = false;
    bool paranoid_checks = false;
};
```

### BPTree Configuration (Compile-Time)

```cpp
// Current optimal settings (in BPTree.hpp)
constexpr size_t target_size = 32768;      // 32 KB nodes
constexpr size_t LEAF_ORDER = 368;         // ~368 entries per node
constexpr size_t INTERNAL_ORDER = 341;     // ~341 entries per node
```

**Tuning Guidelines**:
- Smaller nodes (4-16 KB): Better cache locality, higher tree
- Larger nodes (32-64 KB): Shallower tree, more memory bandwidth
- **Optimal**: 32 KB for 88-byte MemRecord

---

## Limitations

### Current Limitations

1. **Key Size**: Maximum 65,535 bytes (u16 limit in AKHdr32)
2. **Value Size**: Maximum 4,294,967,295 bytes (u32 limit)
3. **Sequence Number**: Maximum 2^64 - 1 operations
4. **Single Process**: No multi-process access (file locking not implemented)
5. **No Snapshots**: Snapshot isolation not yet implemented
6. **No Transactions**: Multi-key transactions not supported

### Platform Support

- **Tested**: Windows (MSVC), Linux (GCC/Clang), macOS (Clang)
- **Required**: C++23 compiler with `<ranges>`, `<span>` support
- **Optional**: AVX2 for SIMD optimizations

### Memory Requirements

- **Minimum**: 128 MB
- **Recommended**: 1 GB+ for MemTable + page cache
- **Per MemTable Shard**: ~32 MB (default threshold)
- **BPTree Overhead**: 32 KB per node

---

## Future Work

### Planned Features

1. **Snapshots**: MVCC-based snapshot isolation
2. **Transactions**: Multi-key ACID transactions
3. **Replication**: Master-slave replication
4. **Compression**: LZ4/Zstd for SSTable blocks
5. **Secondary Indexes**: Fast secondary key lookups
6. **Range Deletes**: Efficient range tombstones

### Optimization Opportunities

1. **SIMD Comparisons**: AVX2/AVX-512 for key comparison
2. **Async I/O**: io_uring (Linux) for non-blocking I/O
3. **Adaptive Radix Tree (ART)**: Replace BPTree for even faster lookups
4. **Tiered Storage**: Hot/cold data separation (SSD/HDD)

---

## References

1. [LevelDB Design](https://github.com/google/leveldb/blob/main/doc/impl.md)
2. [RocksDB Architecture](https://github.com/facebook/rocksdb/wiki/RocksDB-Basics)
3. [The Log-Structured Merge-Tree (LSM-Tree)](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
4. [B+ Tree Data Structure](https://en.wikipedia.org/wiki/B%2B_tree)

---

**Document Version**: 1.0.0
**Last Updated**: 2026-02-15
**Author**: AkkaraDB Team
