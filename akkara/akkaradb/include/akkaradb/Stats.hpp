/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable.
 */

#pragma once

#include "akkaradb/Export.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace akkaradb::engine {
    struct LevelStats {
        int level = 0;
        size_t file_count = 0;
        uint64_t bytes = 0;
        uint64_t budget_bytes = 0;
    };

    struct AKDB_API EngineStats {
        uint64_t current_seq = 0;
        uint64_t node_id = 0;

        uint64_t puts_total = 0;
        uint64_t removes_total = 0;
        uint64_t gets_total = 0;
        uint64_t gets_memtable_hit = 0;
        uint64_t gets_sst_hit = 0;
        uint64_t gets_miss = 0;
        uint64_t exists_total = 0;
        uint64_t scans_total = 0;
        uint64_t blob_puts_total = 0;

        struct MemTableStats {
            uint32_t shard_count = 0;
            uint64_t threshold_bytes_per_shard = 0;
            uint64_t approx_bytes = 0;
            uint64_t puts_applied = 0;
            uint64_t removes_applied = 0;
            uint64_t flushes_completed = 0;
            uint64_t bytes_flushed = 0;
        } memtable;

        struct WalStats {
            bool enabled = false;
            uint32_t shard_count = 0;
            uint64_t entries_written = 0;
            uint64_t bytes_written = 0;
            uint64_t batches_flushed = 0;
            uint64_t syncs_executed = 0;
            uint64_t segment_rotations = 0;
        } wal;

        struct BlobStats {
            bool enabled = false;
            uint64_t threshold_bytes = 0;
            uint64_t blobs_written = 0;
            uint64_t bytes_uncompressed = 0;
            uint64_t bytes_on_disk = 0;
            uint64_t blobs_deleted = 0;
            uint64_t gc_cycles = 0;
        } blob;

        struct SstStats {
            bool enabled = false;
            std::vector<LevelStats> levels;
            size_t file_count = 0;
            uint64_t bytes = 0;
            size_t l0_file_count = 0;
            bool compaction_pending = false;
            uint64_t compactions_completed = 0;
            uint64_t files_compacted = 0;
            uint64_t bytes_compacted_in = 0;
            uint64_t bytes_compacted_out = 0;
            uint64_t l0_stalls = 0;
        } sst;

        struct VLogStats {
            bool enabled = false;
        } vlog;
    };
} // namespace akkaradb::engine
