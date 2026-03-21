# AkkaraDB

**The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database.**

> C++23 · LSM-tree · WAL · Multi-shard · Optional TLS · AGPL-3.0

---

## Features

| Category         | Capability                                                           |
|------------------|----------------------------------------------------------------------|
| **Storage**      | LSM-tree (MemTable + WAL + SST), Bloom filters, leveled compaction   |
| **Durability**   | CRC32C on every record; crash-safe atomic writes (tmp+rename)        |
| **Large values** | Automatic blob externalization for values ≥ 16 KiB                   |
| **Compression**  | Per-file Zstandard (Zstd), self-describing, mixed-codec safe         |
| **Concurrency**  | Multi-shard MemTable; all engine operations fully thread-safe        |
| **History**      | Optional per-key version log; point-in-time reads; rollback          |
| **API servers**  | HTTP REST + binary TCP, both with optional TLS (mbedTLS 3.x)         |
| **Clustering**   | Standalone / Mirror / Stripe replication modes                       |
| **ORM layer**    | `PackedTable<T,ID>`: typed tables with query builder, lazy iterators |
| **Portability**  | Windows (MSVC) and Linux (GCC/Clang)                                 |

---

## Quick Start

### Build

```cmake
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

Optional flags:

```cmake
-DAKKARADB_ENABLE_TLS=ON      # Enable TLS support via mbedTLS 3.x
-DAKKARADB_ENABLE_SIMD=OFF    # Disable SSE4.2/AVX2
-DBUILD_SHARED_LIBS=OFF       # Build as static library
```

> **Windows**: Build from within a Visual Studio / MSVC environment. Do not use plain `bash` without initializing the MSVC toolchain.

### Minimal Usage (C++ API)

```cpp
#include <akkaradb/AkkaraDB.hpp>

auto db = AkkaraDB::open("/path/to/data", StartupMode::NORMAL);
auto& eng = db->engine();

// Raw bytes API
std::vector<uint8_t> key   = {'h','e','l','l','o'};
std::vector<uint8_t> value = {'w','o','r','l','d'};

eng.put(key, value);
auto result = eng.get(key);  // std::optional<std::vector<uint8_t>>

eng.remove(key);
db->close();
```

### Typed Tables (ORM Layer)

```cpp
#include <akkaradb/AkkaraDB.hpp>
#include <akkaradb/PackedTable.hpp>
#include <akkaradb/Codec.hpp>

struct User { std::string name; uint32_t age; };

// Specialize EntityCodec<User>
template<> struct akkaradb::EntityCodec<User> {
    static void serialize(const User& u, std::vector<uint8_t>& out) { /* ... */ }
    static User deserialize(std::span<const uint8_t> bytes) { /* ... */ }
};

auto db    = AkkaraDB::open("/path/to/data", StartupMode::FAST);
auto users = db->table<User, uint64_t>("users");

users.put(1ULL, User{"Alice", 30});
users.put(2ULL, User{"Bob",   25});

auto alice = users.get(1ULL);  // std::optional<User>
users.upsert(1ULL, [](User& u) { u.age++; });

// Range scan
for (const auto& [id, user] : users.scan_all()) {
    // ...
}

// Query builder
auto adults = users
    .query([](const User& u) { return u.age >= 18; })
    .limit(100)
    .to_vector();
```

---

## Startup Modes

| Mode         | WAL   | Sync        | Version History | Use case                      |
|--------------|-------|-------------|-----------------|-------------------------------|
| `ULTRA_FAST` | Off   | —           | Off             | In-memory cache, test harness |
| `FAST`       | Async | group flush | Off             | High-throughput ingestion     |
| `NORMAL`     | Async | group flush | Off             | General-purpose (default)     |
| `DURABLE`    | Sync  | per-write   | **On**          | Financial records, audit logs |

Fine-grained overrides are available via `AkkaraDB::Options::Overrides`.

---

## Architecture Overview

```
put(key, value)
  │
  ├─ [value ≥ 16 KiB]  →  BlobManager (.blob file, atomic write)
  │                          MemTable stores 20-byte BlobRef
  │
  └─ [value < 16 KiB]  →  WAL append (CRC32C, optional fdatasync)
                           MemTable insert (BPTree or SkipList, per-shard)
                                │
                         [shard full] → flush to SST → compaction
```

Read path: MemTable → SSTManager (L0 → L6, bloom filter + sparse index).

---

## Configuration Highlights

```cpp
AkkaraDB::Options opts;
opts.data_dir = "/var/lib/akkaradb";
opts.mode     = StartupMode::FAST;

// Fine-tune
opts.overrides.blob_threshold_bytes         = 32 * 1024;   // 32 KiB externalize threshold
opts.overrides.sst_codec                    = Codec::Zstd;  // compress SST files
opts.overrides.blob_codec                   = Codec::Zstd;  // compress blob files
opts.overrides.memtable_threshold_per_shard = 128ULL << 20; // 128 MiB/shard
opts.overrides.sst_bloom_bits_per_key       = 10;           // ~1% false-positive

auto db = AkkaraDB::open(opts);
```

For the full configuration reference, see [SPEC.md §14](SPEC.md#14-configuration-reference).

---

## API Servers

```cpp
// Enable via engine options
AkkEngineOptions eng_opts;
eng_opts.api.http_enabled = true;   // REST on :8080
eng_opts.api.tcp_enabled  = true;   // Binary protocol on :9090

// Optional TLS (requires AKKARADB_ENABLE_TLS=ON at build time)
eng_opts.api.tls.enabled     = true;
eng_opts.api.tls.cert_path   = "/etc/akkaradb/server.crt";
eng_opts.api.tls.key_path    = "/etc/akkaradb/server.key";
eng_opts.api.tls.ca_path     = "/etc/akkaradb/ca.crt";    // mTLS
eng_opts.api.tls.verify_peer = true;                       // mTLS
```

**HTTP endpoints**: `GET/POST/DELETE /v1/get|put|remove?key=<percent-encoded>`

**Binary protocol**: 16-byte framed request + 13-byte framed response. CRC32C on payload.

---

## Version History & Rollback

Enabled in `DURABLE` mode or via `opts.overrides.version_log_enabled = true`.

```cpp
// Point-in-time read
auto entry = eng.get_at(key, target_seq);

// Full write history for a key
auto hist = eng.history(key);   // vector<VersionEntry>

// Rollback all keys to a prior state
eng.rollback_to(checkpoint_seq);

// Rollback a single key
eng.rollback_key(key, checkpoint_seq);
```

---

## Dependencies

| Library   | Version | License    | Notes                                 |
|-----------|---------|------------|---------------------------------------|
| Zstandard | 1.5.6   | BSD        | Always included (static)              |
| mbedTLS   | 3.6.2   | Apache-2.0 | Optional (`-DAKKARADB_ENABLE_TLS=ON`) |

All dependencies are fetched automatically via CMake `FetchContent`.

---

## File Layout

```
{data_dir}/
├── wal/                        Write-Ahead Log segments (.akwal)
├── sstable/
│   ├── L0/                     Level-0 SST files (.aksst)
│   ├── L1/ … L6/
├── blobs/
│   ├── 00/ … ff/               Large-value blob files (.blob)
├── history.akvlog              Version log (if enabled)
├── manifest.akmf               SST lifecycle manifest
├── cluster.akcc                Cluster topology (if enabled)
└── node.id                     Persistent node identity
```

---

## Performance Notes

- **MemTable put**: ~100–200 ns (arena alloc + BPTree insert, no WAL in `ULTRA_FAST`)
- **WAL enqueue** (Async): ~10–20 ns (double-buffered arena, fire-and-forget)
- **WAL enqueue** (Sync): blocked until `fdatasync` completes
- **SST point read**: ~1–5 µs (bloom check + sparse index seek + ≤128 record scan)
- **Blob read**: disk I/O latency + optional Zstd decompression
- **Shard selection**: `fp64 & mask` — ~1 ns, branchless

For benchmark setup and results, see `benchmarks/benchmark.cpp`.

---

## License

AkkaraDB is free software licensed under the **GNU Affero General Public License v3.0** (AGPL-3.0).
Copyright © 2026 Swift Storm Studio.

See [LICENSE](LICENSE) for the full license text.
