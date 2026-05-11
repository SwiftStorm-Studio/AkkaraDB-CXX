# AkkaraDB Benchmark Notes

This file is an index of the benchmark and smoke-test targets that currently exist and build in `akkaradb-native`.

The older `akkaradb_benchmark`-centered notes described SST, Blob, `AkkEngine`, and typed API paths that do not currently match the native implementation. Those details have been removed for now. Re-add them as separate notes when those components are implemented again.

## Current Targets

| Target | Purpose |
|---|---|
| `akkaradb_memtable_smoke_test` | Smoke test for MemTable backends, snapshots, iterators, and flush callbacks. |
| `akkaradb_memtable_throughput_benchmark` | Measures sharded MemTable put / get / scan throughput and sampled latency. |
| `akkaradb_wal_smoke_test` | Checks WAL append, recovery, tombstones, async force sync, corruption handling, and rotation. |
| `akkaradb_wal_throughput_benchmark` | Measures sharded WAL append, close drain, and recovery throughput. See `readme/ja/WAL_SETUP.md` for the detailed tuning notes. |
| `akkaradb_manifest_smoke_test` | Checks Manifest SST lifecycle records, compaction commit, and CRC replay behavior. |
| `akkaradb_versionlog_smoke_test` | Checks VersionLog append, history, get-at, and rollback behavior. |

## Build Examples

Build individual Release targets:

```powershell
cmake --build cmake-build-release --target akkaradb_memtable_throughput_benchmark --config Release
cmake --build cmake-build-release --target akkaradb_wal_throughput_benchmark --config Release
```

Build the smoke tests:

```powershell
cmake --build cmake-build-release --target akkaradb_memtable_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_wal_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_manifest_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_versionlog_smoke_test --config Release
```

If a normal Windows PowerShell session cannot find MSVC standard headers, run through the Visual Studio developer environment:

```powershell
cmd /c "call ""C:\Program Files\Microsoft Visual Studio\18\Community\Common7\Tools\VsDevCmd.bat"" -arch=x64 -host_arch=x64 >nul && cmake --build cmake-build-release --target akkaradb_wal_smoke_test --config Release"
```

## Run Examples

```powershell
cmake-build-release\bin\akkaradb_memtable_smoke_test.exe
cmake-build-release\bin\akkaradb_wal_smoke_test.exe
cmake-build-release\bin\akkaradb_manifest_smoke_test.exe
cmake-build-release\bin\akkaradb_versionlog_smoke_test.exe
```

MemTable throughput:

```powershell
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 200000 --writers=16 --backend=art
```

WAL throughput:

```powershell
cmake-build-release\bin\akkaradb_wal_throughput_benchmark.exe 200000 --writers=16 --sync=async --group-n=2048 --group-micros=500 --group-bytes=4MiB --max-pending-bytes=64MiB
```