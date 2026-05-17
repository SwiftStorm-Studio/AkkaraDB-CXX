/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable.
 */

#pragma once

#include "TypeAdapter.hpp"

#include <span>
#include <vector>

namespace akkaradb::binpack {
    struct BinPack {
        template <typename T>
        [[nodiscard]] static std::vector<uint8_t> encode(const T& value) {
            std::vector<uint8_t> out;
            out.reserve(TypeAdapter<T>::estimate_size(value));
            TypeAdapter<T>::write(value, out);
            return out;
        }

        template <typename T, typename Out>
        static void encode_into(const T& value, Out& out) { TypeAdapter<T>::write(value, out); }

        template <typename T>
        [[nodiscard]] static T decode(std::span<const uint8_t> bytes) { return TypeAdapter<T>::read(bytes); }

        template <typename T>
        [[nodiscard]] static T decode(const std::vector<uint8_t>& bytes) { return decode<T>(std::span<const uint8_t>{bytes.data(), bytes.size()}); }

        template <typename T>
        static bool decode_into(std::span<const uint8_t> bytes, T& out) { return TypeAdapter<T>::read_into(bytes, out); }

        template <typename T>
        [[nodiscard]] static size_t estimate_size(const T& value) { return TypeAdapter<T>::estimate_size(value); }

        BinPack() = delete;
    };
} // namespace akkaradb::binpack
