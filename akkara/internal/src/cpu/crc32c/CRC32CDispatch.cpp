/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/include/cpu/crc32c/CRC32CArmCRC.cpp
#include "cpu/CRC32C.hpp"

#include <cstddef>
#include <cstdint>

#if defined(_MSC_VER)
#  include <intrin.h>
#endif

#if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)
#  if defined(__GNUC__) || defined(__clang__)
#    include <cpuid.h>
#  endif
#endif

#if defined(__aarch64__) && defined(__linux__)
#  include <sys/auxv.h>
#  include <asm/hwcap.h>
#endif

namespace akkaradb::cpu {
    uint32_t CRC32C_Ref(const std::byte* data, size_t length) noexcept;

    #if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)
    uint32_t CRC32C_X86_SSE42(const std::byte* data, size_t length) noexcept;
    #endif

    #if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    uint32_t CRC32C_ARM_CRC(const std::byte* data, size_t length) noexcept;
    #endif

    namespace {
        using Fn = uint32_t (*)(const std::byte*, size_t) noexcept;

        #if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)
        /**
         * @brief Checks whether SSE4.2 CRC instructions are available.
         *
         * @return true if SSE4.2 CRC instructions can be used.
         */
        [[nodiscard]] bool SupportsSSE42() noexcept {
            #if defined(__GNUC__) || defined(__clang__)
            __builtin_cpu_init(); return __builtin_cpu_supports("sse4.2");
            #elif defined(_MSC_VER)
            int regs[4]{};
            __cpuid(regs, 1);
            return (regs[2] & (1 << 20)) != 0;
            #else
            return false;
            #endif
        }
        #endif

        #if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
        /**
         * @brief Checks whether AArch64 CRC instructions are available.
         *
         * @return true if CRC instructions can be used.
         */
        [[nodiscard]] bool SupportsARMCRC() noexcept {
        #if defined(__linux__)
        return (getauxval(AT_HWCAP) &HWCAP_CRC32) != 0;
        #else
        return true;
        #endif
        }
        #endif

        /**
         * @brief Resolves the best available CRC32C implementation.
         *
         * @return Function pointer to the selected implementation.
         */
        [[nodiscard]] Fn Resolve() noexcept {
            #if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)
            if (SupportsSSE42()) { return &CRC32C_X86_SSE42; }
            #endif

            #if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
            if (SupportsARMCRC()) { return &CRC32C_ARM_CRC; }
            #endif

            return &CRC32C_Ref;
        }
    } // namespace

    /**
     * @brief Public CRC32C entry point.
     *
     * The selected implementation is cached after the first call.
     *
     * @param data Pointer to the input bytes.
     * @param length Number of bytes to process.
     * @return CRC32C checksum for the input.
     */
    [[nodiscard]] uint32_t CRC32C(const std::byte* data, size_t length) noexcept {
        static const Fn fn = Resolve();
        return fn(data, length);
    }
} // namespace akkaradb::cpu
