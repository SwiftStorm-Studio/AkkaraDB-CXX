# AkkaraDB C++ - Shared Library Build Configuration
cmake_minimum_required(VERSION 4.1)
project(AkkaraDB
        DESCRIPTION "Low-latency, crash-safe JVM KV store with WAL & stripe parity"
        VERSION 0.1.0
        LANGUAGES CXX
)

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
    add_compile_options(/W4 /WX /permissive- /utf-8)
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

# ==================== Windows-specific definitions ====================
if (WIN32)
    add_compile_definitions(NOMINMAX WIN32_LEAN_AND_MEAN)
endif ()

# ==================== Main Library ====================
add_library(akkaradb)

# Public headers (C++ API - User-facing)
target_sources(akkaradb PUBLIC FILE_SET HEADERS

)

# Public implementation (akkaradb/src)
target_sources(akkaradb PRIVATE

)

# Internal headers (not exposed to users)
target_sources(akkaradb PRIVATE
        # Core - Buffer
        ${INTERNAL_INCLUDE_DIR}/core/buffer/BufferView.hpp
        ${INTERNAL_INCLUDE_DIR}/core/buffer/OwnedBuffer.hpp
        ${INTERNAL_INCLUDE_DIR}/core/buffer/PerThreadArena.hpp

        # Core - Record
        ${INTERNAL_INCLUDE_DIR}/core/record/AKHdr32.hpp
        ${INTERNAL_INCLUDE_DIR}/core/record/RecordView.hpp
        ${INTERNAL_INCLUDE_DIR}/core/record/MemRecord.hpp

        # Core - CRC
        ${INTERNAL_INCLUDE_DIR}/core/CRC32C.hpp

        # Engine
        ${INTERNAL_INCLUDE_DIR}/engine/AkkEngine.hpp

        # Engine - MemTable
        ${INTERNAL_INCLUDE_DIR}/engine/memtable/BPTree.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/memtable/MemTable.hpp

        # Engine - WAL
        ${INTERNAL_INCLUDE_DIR}/engine/wal/WalOp.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/wal/WalFraming.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/wal/WalWriter.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/wal/WalReader.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/wal/WalRecovery.hpp
)

# Internal implementation (internal/src)
target_sources(akkaradb PRIVATE
        # Core - Buffer
        ${INTERNAL_SRC_DIR}/core/buffer/BufferView.cpp
        ${INTERNAL_SRC_DIR}/core/buffer/OwnedBuffer.cpp
        ${INTERNAL_SRC_DIR}/core/buffer/PerThreadArena.cpp

        # Core - Record
        ${INTERNAL_SRC_DIR}/core/record/AKHdr32.cpp
        ${INTERNAL_SRC_DIR}/core/record/RecordView.cpp
        ${INTERNAL_SRC_DIR}/core/record/MemRecord.cpp

        # Core - CRC
        ${INTERNAL_SRC_DIR}/core/CRC32C.cpp

        # Engine - MemTable
        ${INTERNAL_SRC_DIR}/engine/memtable/MemTable.cpp

        # Engine - WAL
        ${INTERNAL_SRC_DIR}/engine/wal/WalOp.cpp
        ${INTERNAL_SRC_DIR}/engine/wal/WalFraming.cpp
        ${INTERNAL_SRC_DIR}/engine/wal/WalWriter.cpp
        ${INTERNAL_SRC_DIR}/engine/wal/WalReader.cpp
        ${INTERNAL_SRC_DIR}/engine/wal/WalRecovery.cpp
)

# Include directories
target_include_directories(akkaradb
        PUBLIC
        $<BUILD_INTERFACE:${AKKARADB_INCLUDE_DIR}>
        $<INSTALL_INTERFACE:include>
        PRIVATE
        ${INTERNAL_INCLUDE_DIR}
)

# Specify C++ linker
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

# ==================== Benchmarks ====================
message(STATUS "Building AkkaraDB benchmarks")

# Benchmark executable
add_executable(akkaradb_benchmark
        benchmarks/benchmark.cpp
)

if (MSVC)
    target_compile_options(akkaradb_benchmark PRIVATE /WX-)
else ()
    get_target_property(CURRENT_OPTS akkaradb_benchmark COMPILE_OPTIONS)
    if (CURRENT_OPTS)
        list(REMOVE_ITEM CURRENT_OPTS "-Werror")
        set_target_properties(akkaradb_benchmark PROPERTIES COMPILE_OPTIONS "${CURRENT_OPTS}")
    endif ()
endif ()

# Link against akkaradb library
target_link_libraries(akkaradb_benchmark
        PRIVATE
        akkaradb
)

# Include benchmark headers
target_include_directories(akkaradb_benchmark
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${INTERNAL_DIR}/include
)

# Set output directory
set_target_properties(akkaradb_benchmark PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# Install benchmark executable
install(TARGETS akkaradb_benchmark
        RUNTIME DESTINATION bin
)

message(STATUS "Benchmark executable will be built at: ${CMAKE_BINARY_DIR}/bin/akkaradb_benchmark")

if (WIN32 AND BUILD_SHARED_LIBS)
    add_custom_command(TARGET akkaradb_benchmark POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:akkaradb>
            $<TARGET_FILE_DIR:akkaradb_benchmark>
            COMMENT "Copying akkaradb.dll to benchmark executable directory"
    )
endif ()