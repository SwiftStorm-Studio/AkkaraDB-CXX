/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 */

// internal/include/engine/sstable/SSTWriter.hpp
#pragma once

#include <cstdint>
#include <filesystem>
#include <span>
#include <vector>

#include "core/record/RecordView.hpp"
#include "engine/sstable/SSTFormat.hpp"

namespace akkaradb::engine::sst {

    class SSTWriter {
        public:
            enum class Codec : uint8_t {
                None = 0,
                Zstd = 1,
            };

            struct Options {
                int level = 0;
                uint32_t block_size = SST_DEFAULT_BLOCK_SIZE;
                uint64_t target_file_size = SST_DEFAULT_TARGET_FILE_SIZE;
                uint32_t bloom_bits_per_key = SST_DEFAULT_BLOOM_BITS_PER_KEY;
                Codec codec = Codec::Zstd;
            };

            struct Result {
                std::filesystem::path path;
                uint64_t entry_count = 0;
                uint64_t file_size_bytes = 0;
                uint64_t min_seq = UINT64_MAX;
                uint64_t max_seq = 0;
                std::vector<uint8_t> first_key;
                std::vector<uint8_t> last_key;
            };

            [[nodiscard]] static Result write(
                const std::filesystem::path& path,
                std::span<const core::RecordView> records,
                const Options& options = {}
            );

        private:
            SSTWriter() = delete;
    };

} // namespace akkaradb::engine::sst
