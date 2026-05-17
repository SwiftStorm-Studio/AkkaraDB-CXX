/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable.
 */

#pragma once

#include <cstdint>
#include <string_view>

namespace akkaradb::detail {
    [[nodiscard]] constexpr uint64_t fnv1a_64(std::string_view s) noexcept {
        uint64_t h = 14695981039346656037ULL;
        for (const unsigned char c : s) {
            h ^= static_cast<uint64_t>(c);
            h *= 1099511628211ULL;
        }
        return h;
    }

    inline void write_be32(uint32_t v, uint8_t* dst) noexcept {
        dst[0] = static_cast<uint8_t>(v >> 24);
        dst[1] = static_cast<uint8_t>(v >> 16);
        dst[2] = static_cast<uint8_t>(v >> 8);
        dst[3] = static_cast<uint8_t>(v);
    }

    inline void write_be64(uint64_t v, uint8_t* dst) noexcept {
        dst[0] = static_cast<uint8_t>(v >> 56);
        dst[1] = static_cast<uint8_t>(v >> 48);
        dst[2] = static_cast<uint8_t>(v >> 40);
        dst[3] = static_cast<uint8_t>(v >> 32);
        dst[4] = static_cast<uint8_t>(v >> 24);
        dst[5] = static_cast<uint8_t>(v >> 16);
        dst[6] = static_cast<uint8_t>(v >> 8);
        dst[7] = static_cast<uint8_t>(v);
    }

    [[nodiscard]] inline bool increment_be_bytes(uint8_t* data, size_t size) noexcept {
        for (size_t i = size; i > 0; --i) { if (++data[i - 1] != 0) { return true; } }
        return false;
    }
} // namespace akkaradb::detail

