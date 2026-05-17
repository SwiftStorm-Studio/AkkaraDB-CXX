/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable.
 */

#pragma once

#include <cstdint>
#include <span>
#include <stdexcept>

namespace akkaradb::binpack::detail {
    template <typename Out>
    inline void write_u8(uint8_t v, Out& out) { out.push_back(v); }

    template <typename Out>
    inline void write_u16(uint16_t v, Out& out) {
        out.push_back(static_cast<uint8_t>(v >> 8));
        out.push_back(static_cast<uint8_t>(v));
    }

    template <typename Out>
    inline void write_u32(uint32_t v, Out& out) {
        out.push_back(static_cast<uint8_t>(v >> 24));
        out.push_back(static_cast<uint8_t>(v >> 16));
        out.push_back(static_cast<uint8_t>(v >> 8));
        out.push_back(static_cast<uint8_t>(v));
    }

    template <typename Out>
    inline void write_u64(uint64_t v, Out& out) {
        out.push_back(static_cast<uint8_t>(v >> 56));
        out.push_back(static_cast<uint8_t>(v >> 48));
        out.push_back(static_cast<uint8_t>(v >> 40));
        out.push_back(static_cast<uint8_t>(v >> 32));
        out.push_back(static_cast<uint8_t>(v >> 24));
        out.push_back(static_cast<uint8_t>(v >> 16));
        out.push_back(static_cast<uint8_t>(v >> 8));
        out.push_back(static_cast<uint8_t>(v));
    }

    [[nodiscard]] inline uint8_t read_u8(std::span<const uint8_t>& in) {
        if (in.size() < 1) { throw std::runtime_error("BinPack: buffer underflow (u8)"); }
        const uint8_t v = in[0];
        in = in.subspan(1);
        return v;
    }

    [[nodiscard]] inline uint16_t read_u16(std::span<const uint8_t>& in) {
        if (in.size() < 2) { throw std::runtime_error("BinPack: buffer underflow (u16)"); }
        const uint16_t v = (static_cast<uint16_t>(in[0]) << 8) | in[1];
        in = in.subspan(2);
        return v;
    }

    [[nodiscard]] inline uint32_t read_u32(std::span<const uint8_t>& in) {
        if (in.size() < 4) { throw std::runtime_error("BinPack: buffer underflow (u32)"); }
        const uint32_t v = (static_cast<uint32_t>(in[0]) << 24) | (static_cast<uint32_t>(in[1]) << 16) | (static_cast<uint32_t>(in[2]) << 8) | static_cast<
            uint32_t>(in[3]);
        in = in.subspan(4);
        return v;
    }

    [[nodiscard]] inline uint64_t read_u64(std::span<const uint8_t>& in) {
        if (in.size() < 8) { throw std::runtime_error("BinPack: buffer underflow (u64)"); }
        const uint64_t v = (static_cast<uint64_t>(in[0]) << 56) | (static_cast<uint64_t>(in[1]) << 48) | (static_cast<uint64_t>(in[2]) << 40) | (static_cast<
                uint64_t>(in[3]) << 32) | (static_cast<uint64_t>(in[4]) << 24) | (static_cast<uint64_t>(in[5]) << 16) | (static_cast<uint64_t>(in[6]) << 8) |
            static_cast<uint64_t>(in[7]);
        in = in.subspan(8);
        return v;
    }
} // namespace akkaradb::binpack::detail
