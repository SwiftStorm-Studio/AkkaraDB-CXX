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

namespace akkaradb::cpu {

    // ---- external implementations ----

    uint32_t CRC32C_Ref(const std::byte*, size_t) noexcept;

    #if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)
    uint32_t CRC32C_X86_SSE42(const std::byte*, size_t) noexcept;
    #endif

    #if defined(__aarch64__)
    uint32_t CRC32C_ARM_CRC(const std::byte*, size_t) noexcept;
    #endif

    namespace {

        // ---- function pointer ----

        typedef uint32_t(*Fn)(const std::byte*, size_t) noexcept;

        // ---- CPU feature detection ----

        #if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)

        bool SupportsSSE42() noexcept {
            #if defined(__GNUC__) || defined(__clang__)
            return __builtin_cpu_supports("sse4.2");
            #elif defined(_MSC_VER)
            int regs[4]{};
            __cpuid(regs, 1);
            return (regs[2] & (1 << 20)) != 0;
            #else
            return false;
            #endif
        }

        #endif

        #if defined(__aarch64__)

        bool SupportsARMCRC() noexcept {
            #if defined(__linux__)
            return (getauxval(AT_HWCAP) & HWCAP_CRC32) != 0;
            #else
            return true;
            #endif
        }

        #endif

        // ---- resolver ----

        Fn Resolve() noexcept {

            #if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)
            if (SupportsSSE42()) {
                return &CRC32C_X86_SSE42;
            }
            #endif

            #if defined(__aarch64__)
            if (SupportsARMCRC()) {
                return &CRC32C_ARM_CRC;
            }
            #endif

            return &CRC32C_Ref;
        }

    } // namespace

    // ---- public entry ----

    uint32_t CRC32C(const std::byte* data, size_t length) noexcept {
        static Fn fn = Resolve();
        return fn(data, length);
    }

} // namespace akkaradb::cpu