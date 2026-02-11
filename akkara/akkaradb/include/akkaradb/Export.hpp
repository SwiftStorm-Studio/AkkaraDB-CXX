/*
* AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

// akkara/akkaradb/include/akkaradb/Export.hpp
#pragma once

/**
 * Symbol visibility macros for shared library export/import.
 *
 * - AKKARADB_API: Used for public API classes and functions
 * - AKKARADB_LOCAL: Used for internal symbols (hidden visibility)
 */

#if defined(_WIN32) || defined(_WIN64)
// Windows DLL export/import
    #ifdef AKKARADB_BUILD_SHARED
// Building the DLL - export symbols
        #define AKKARADB_API __declspec(dllexport)
    #else
// Using the DLL - import symbols
        #define AKKARADB_API __declspec(dllimport)
    #endif
    #define AKKARADB_LOCAL
#elif defined(__GNUC__) || defined(__clang__)
// GCC/Clang visibility attributes
    #ifdef AKKARADB_BUILD_SHARED
        #define AKKARADB_API __attribute__((visibility("default")))
        #define AKKARADB_LOCAL __attribute__((visibility("hidden")))
    #else
        #define AKKARADB_API
        #define AKKARADB_LOCAL
    #endif
#else
// Unknown compiler - no visibility control
    #define AKKARADB_API
    #define AKKARADB_LOCAL
#endif

#ifdef _MSC_VER
    #pragma warning(push)
    #pragma warning(disable: 4251) // needs to have dll-interface
#endif
