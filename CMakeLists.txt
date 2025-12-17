# AkkaraDB C++ - Shared Library Build Configuration
cmake_minimum_required(VERSION 4.1)
project(AkkaraDB VERSION 0.1.0 LANGUAGES CXX)

# ==================== ! Build Options ! ====================
option(BUILD_SHARED_LIBS "Build shared libraries (.so/.dll)" ON)
option(AKKARADB_BUILD_TESTS "Build unit tests" OFF)
option(AKKARADB_ENABLE_SIMD "Enable SIMD optimizations (SSE4.2/AVX2)" ON)

# ==================== C++ Standard ====================
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ==================== Directory Configuration ====================
set(AKKARA_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/akkara)
set(AKKARADB_DIR ${AKKARA_ROOT}/akkaradb)
set(INTERNAL_DIR ${AKKARA_ROOT}/internal)

set(AKKARADB_INCLUDE_DIR ${AKKARADB_DIR}/include)
set(AKKARADB_SRC_DIR ${AKKARADB_DIR}/src)

set(INTERNAL_INCLUDE_DIR ${INTERNAL_DIR}/include)
set(INTERNAL_SRC_DIR ${INTERNAL_DIR}/src)

# ==================== Compiler Settings ====================
if (MSVC)
    add_compile_options(/W4 /WX /permissive-)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
else ()
    add_compile_options(-Wall -Wextra -Werror -pedantic)
endif ()

# Enable SIMD if requested
if (AKKARADB_ENABLE_SIMD)
    if (MSVC)
        add_compile_options(/arch:AVX2)
    else ()
        add_compile_options(-msse4.2 -mavx2)
    endif ()
endif ()

# ==================== Main Library ====================
add_library(akkaradb)

# Public headers (C++ API - User-facing)
target_sources(akkaradb PUBLIC FILE_SET HEADERS
        BASE_DIRS ${AKKARADB_INCLUDE_DIR}
        FILES

)

# Public implementation (akkaradb/src)
target_sources(akkaradb PRIVATE

)

# Internal headers (not exposed to users)
target_sources(akkaradb PRIVATE
        # Core - Buffer
        ${INTERNAL_INCLUDE_DIR}/core/buffer/BufferView.hpp
        ${INTERNAL_INCLUDE_DIR}/core/buffer/OwnedBuffer.hpp
        ${INTERNAL_INCLUDE_DIR}/core/buffer/BufferPool.hpp

        # Core - Record
        ${INTERNAL_INCLUDE_DIR}/core/record/AkHdr32.hpp
        ${INTERNAL_INCLUDE_DIR}/core/record/RecordView.hpp
        ${INTERNAL_INCLUDE_DIR}/core/record/MemRecord.hpp

        # Format - API
        ${INTERNAL_INCLUDE_DIR}/format-api/BlockPacker.hpp
        ${INTERNAL_INCLUDE_DIR}/format-api/BlockUnpacker.hpp
        ${INTERNAL_INCLUDE_DIR}/format-api/RecordCursor.hpp
        ${INTERNAL_INCLUDE_DIR}/format-api/StripeReader.hpp
        ${INTERNAL_INCLUDE_DIR}/format-api/StripeWriter.hpp
        ${INTERNAL_INCLUDE_DIR}/format-api/FlushPolicy.hpp
        ${INTERNAL_INCLUDE_DIR}/format-api/ParityCoder.hpp

        # Format - Implementation
        ${INTERNAL_INCLUDE_DIR}/format-akk/AkkBlockPacker.hpp
        ${INTERNAL_INCLUDE_DIR}/format-akk/AkkBlockUnpacker.hpp

        # Engine

)

# Internal implementation (internal/src)
target_sources(akkaradb PRIVATE
        # Core - Buffer
        ${INTERNAL_SRC_DIR}/core/buffer/BufferView.cpp
        ${INTERNAL_SRC_DIR}/core/buffer/OwnedBuffer.cpp
        ${INTERNAL_SRC_DIR}/core/buffer/BufferPool.cpp

        # Core - Record


        # Format - Implementation
        ${INTERNAL_SRC_DIR}/format-akk/AkkBlockPacker.cpp
        ${INTERNAL_SRC_DIR}/format-akk/AkkBlockUnpacker.cpp


        # Engine

)

# Include directories
target_include_directories(akkaradb
        PUBLIC
        $<BUILD_INTERFACE:${AKKARADB_INCLUDE_DIR}>
        $<INSTALL_INTERFACE:include>
        PRIVATE
        ${INTERNAL_INCLUDE_DIR}
)

set_target_properties(akkaradb PROPERTIES LINKER_LANGUAGE CXX)

# Shared library configuration
if (BUILD_SHARED_LIBS)
    target_compile_definitions(akkaradb PRIVATE AKKARADB_BUILD_SHARED)
    set_target_properties(akkaradb PROPERTIES
            CXX_VISIBILITY_PRESET hidden
            VISIBILITY_INLINES_HIDDEN ON
            VERSION ${PROJECT_VERSION}
            SOVERSION ${PROJECT_VERSION_MAJOR}
    )
endif ()

# Windows-specific: explicit symbol export
if (WIN32 AND BUILD_SHARED_LIBS)
    set_target_properties(akkaradb PROPERTIES
            WINDOWS_EXPORT_ALL_SYMBOLS OFF
    )
endif ()

# ==================== Installation ====================
install(TARGETS akkaradb
        EXPORT AkkaraDBTargets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        FILE_SET HEADERS DESTINATION include
)

install(EXPORT AkkaraDBTargets
        FILE AkkaraDBTargets.cmake
        NAMESPACE AkkaraDB::
        DESTINATION lib/cmake/AkkaraDB
)

# ==================== Tests ====================
if (AKKARADB_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif ()