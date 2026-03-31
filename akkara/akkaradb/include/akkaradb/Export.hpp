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

// akkaradb/Export.hpp
#pragma once

/**
 * AKDB_API — symbol visibility annotation for the AkkaraDB public API.
 *
 * Rules:
 *   Building the shared library  (AKKARADB_BUILD_SHARED defined by CMake):
 *     Windows/MSVC:  __declspec(dllexport)
 *     GCC/Clang:     __attribute__((visibility("default")))
 *
 *   Consuming the shared library (default when AKKARADB_STATIC is NOT defined):
 *     Windows/MSVC:  __declspec(dllimport)
 *     GCC/Clang:     (nothing — symbol visibility is set per-DSO at link time)
 *
 *   Static build (AKKARADB_STATIC defined by the consumer):
 *     All platforms: (empty)
 *
 * Usage:
 *   class AKDB_API MyClass { ... };
 *   AKDB_API void my_function();
 */

#if defined(_WIN32) || defined(__CYGWIN__)
#   if defined(AKKARADB_BUILD_SHARED)
#       define AKDB_API __declspec(dllexport)
#   elif !defined(AKKARADB_STATIC)
#       define AKDB_API __declspec(dllimport)
#   else
#       define AKDB_API
#   endif
#elif defined(__GNUC__) && __GNUC__ >= 4
#   if defined(AKKARADB_BUILD_SHARED)
#       define AKDB_API __attribute__((visibility("default")))
#   else
#       define AKDB_API
#   endif
#else
#   define AKDB_API
#endif
