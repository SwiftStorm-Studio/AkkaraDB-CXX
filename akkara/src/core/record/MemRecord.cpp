// MemRecord.cpp
#include "akkara/core/record/MemRecord.hpp"
#include <algorithm>
#include <cstring>

namespace akkaradb::core {
    int MemRecord::lex_compare(std::string_view a, std::string_view b) noexcept {
        const size_t min_len = std::min(a.size(), b.size());

        // Bytewise comparison
        if (const int cmp = std::memcmp(a.data(), b.data(), min_len); cmp != 0) {
            return cmp;
        }

        // If prefixes are equal, shorter key comes first
        if (a.size() < b.size()) return -1;
        if (a.size() > b.size()) return 1;
        return 0;
    }

    bool MemRecord::should_replace(const MemRecord* existing, const MemRecord& candidate) noexcept {
        if (!existing) {
            return true; // No existing record
        }

        // Rule 1: Higher seq wins
        if (candidate.seq > existing->seq) return true;
        if (candidate.seq < existing->seq) return false;

        // Rule 2: Equal seq
        const bool candidate_tombstone = candidate.is_tombstone();
        const bool existing_tombstone = existing->is_tombstone();

        // Tombstone wins over non-tombstone
        if (candidate_tombstone && !existing_tombstone) return true;
        if (!candidate_tombstone && existing_tombstone) return false;

        // Rule 3: Same state -> keep existing (avoid churn)
        return false;
    }

    uint32_t MemRecord::hash_key(std::string_view key) noexcept {
        // FNV-1a hash (32-bit)
        constexpr uint32_t FNV_OFFSET = 0x811C9DC5u;

        uint32_t hash = FNV_OFFSET;
        for (unsigned char c : key) {
            constexpr uint32_t FNV_PRIME = 0x01000193u;
            hash ^= c;
            hash *= FNV_PRIME;
        }
        return hash;
    }

    uint32_t MemRecord::estimate_size(std::string_view key, std::string_view value) noexcept {
        // Rough estimate: struct overhead + key + value + padding
        constexpr uint32_t STRUCT_OVERHEAD = 64; // MemRecord + allocator metadata
        return STRUCT_OVERHEAD + static_cast<uint32_t>(key.size()) + static_cast<uint32_t>(value.size());
    }
} // namespace akkaradb::core