# AkkaraDB C++ - Shared Library Build Configuration
cmake_minimum_required(VERSION 4.1)
project(AkkaraDB VERSION 0.1.0 LANGUAGES CXX)

include(FetchContent)

FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG v3.12.0
)

FetchContent_MakeAvailable(nlohmann_json)

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

# ==================== Main Library ====================
add_library(akkaradb)

# Public headers (C++ API - User-facing)
target_sources(akkaradb PUBLIC FILE_SET HEADERS
        BASE_DIRS ${AKKARADB_INCLUDE_DIR}
        FILES
        ${AKKARADB_INCLUDE_DIR}/akkaradb/Export.hpp

        ${AKKARADB_INCLUDE_DIR}/akkaradb/AkkaraDB.hpp

        ${AKKARADB_INCLUDE_DIR}/akkaradb/typed/EntityMacros.hpp
        ${AKKARADB_INCLUDE_DIR}/akkaradb/typed/PackedTable.hpp
        ${AKKARADB_INCLUDE_DIR}/akkaradb/typed/Query.hpp

        ${AKKARADB_INCLUDE_DIR}/akkaradb/typed/serialization/Serialization.hpp
        ${AKKARADB_INCLUDE_DIR}/akkaradb/typed/serialization/Core.hpp

        ${AKKARADB_INCLUDE_DIR}/akkaradb/typed/serialization/serializers/Primitives.hpp
        ${AKKARADB_INCLUDE_DIR}/akkaradb/typed/serialization/serializers/String.hpp
        ${AKKARADB_INCLUDE_DIR}/akkaradb/typed/serialization/serializers/Bool.hpp
        ${AKKARADB_INCLUDE_DIR}/akkaradb/typed/serialization/serializers/Vector.hpp
        ${AKKARADB_INCLUDE_DIR}/akkaradb/typed/serialization/serializers/Optional.hpp
        ${AKKARADB_INCLUDE_DIR}/akkaradb/typed/serialization/serializers/Map.hpp
        ${AKKARADB_INCLUDE_DIR}/akkaradb/typed/serialization/serializers/Set.hpp
        ${AKKARADB_INCLUDE_DIR}/akkaradb/typed/serialization/serializers/Pair.hpp
        ${AKKARADB_INCLUDE_DIR}/akkaradb/typed/serialization/serializers/Array.hpp
)

# Public implementation (akkaradb/src)
target_sources(akkaradb PRIVATE
        ${AKKARADB_SRC_DIR}/AkkaraDB.cpp
)

# Internal headers (not exposed to users)
target_sources(akkaradb PRIVATE
        # Core - Buffer
        ${INTERNAL_INCLUDE_DIR}/core/buffer/BufferView.hpp
        ${INTERNAL_INCLUDE_DIR}/core/buffer/OwnedBuffer.hpp
        ${INTERNAL_INCLUDE_DIR}/core/buffer/BufferPool.hpp
        ${INTERNAL_INCLUDE_DIR}/core/buffer/PooledBuffer.hpp

        # Core - Record
        ${INTERNAL_INCLUDE_DIR}/core/record/AKHdr32.hpp
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
        ${INTERNAL_INCLUDE_DIR}/format-akk/AkkStripeWriter.hpp
        ${INTERNAL_INCLUDE_DIR}/format-akk/AkkStripeReader.hpp

        ${INTERNAL_INCLUDE_DIR}/format-akk/parity/NoParityCoder.hpp
        ${INTERNAL_INCLUDE_DIR}/format-akk/parity/XorParityCoder.hpp
        ${INTERNAL_INCLUDE_DIR}/format-akk/parity/DualXorParityCoder.hpp

        # Engine
        ${INTERNAL_INCLUDE_DIR}/engine/wal/WalOp.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/wal/WalFraming.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/wal/WalWriter.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/wal/WalReader.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/wal/WalRecovery.hpp

        ${INTERNAL_INCLUDE_DIR}/engine/memtable/MemTable.hpp

        ${INTERNAL_INCLUDE_DIR}/engine/manifest/Manifest.hpp

        ${INTERNAL_INCLUDE_DIR}/engine/sstable/AKSSFooter.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/sstable/IndexBlock.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/sstable/BloomFilter.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/sstable/SSTableWriter.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/sstable/SSTableReader.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/sstable/SSTCompactor.hpp

        ${INTERNAL_INCLUDE_DIR}/engine/AkkEngine.hpp
        ${INTERNAL_INCLUDE_DIR}/engine/StripeLaneWriter.hpp
)

# Internal implementation (internal/src)
target_sources(akkaradb PRIVATE
        # Core - Buffer
        ${INTERNAL_SRC_DIR}/core/buffer/BufferView.cpp
        ${INTERNAL_SRC_DIR}/core/buffer/OwnedBuffer.cpp
        ${INTERNAL_SRC_DIR}/core/buffer/BufferPool.cpp

        # Core - Record
        ${INTERNAL_SRC_DIR}/core/record/AKHdr32.cpp
        ${INTERNAL_SRC_DIR}/core/record/RecordView.cpp
        ${INTERNAL_SRC_DIR}/core/record/MemRecord.cpp

        # Format - Implementation
        ${INTERNAL_SRC_DIR}/format-akk/AkkBlockPacker.cpp
        ${INTERNAL_SRC_DIR}/format-akk/AkkBlockUnpacker.cpp
        ${INTERNAL_SRC_DIR}/format-akk/AkkStripeWriter.cpp
        ${INTERNAL_SRC_DIR}/format-akk/AkkStripeReader.cpp

        ${INTERNAL_SRC_DIR}/format-akk/parity/NoParityCoder.cpp
        ${INTERNAL_SRC_DIR}/format-akk/parity/XorParityCoder.cpp
        ${INTERNAL_SRC_DIR}/format-akk/parity/DualXorParityCoder.cpp

        # Engine
        ${INTERNAL_SRC_DIR}/engine/wal/WalOp.cpp
        ${INTERNAL_SRC_DIR}/engine/wal/WalFraming.cpp
        ${INTERNAL_SRC_DIR}/engine/wal/WalWriter.cpp
        ${INTERNAL_SRC_DIR}/engine/wal/WalReader.cpp
        ${INTERNAL_SRC_DIR}/engine/wal/WalRecovery.cpp

        ${INTERNAL_SRC_DIR}/engine/memtable/MemTable.cpp

        ${INTERNAL_SRC_DIR}/engine/manifest/Manifest.cpp

        ${INTERNAL_SRC_DIR}/engine/sstable/AKSSFooter.cpp
        ${INTERNAL_SRC_DIR}/engine/sstable/IndexBlock.cpp
        ${INTERNAL_SRC_DIR}/engine/sstable/BloomFilter.cpp
        ${INTERNAL_SRC_DIR}/engine/sstable/SSTableWriter.cpp
        ${INTERNAL_SRC_DIR}/engine/sstable/SSTableReader.cpp
        ${INTERNAL_SRC_DIR}/engine/sstable/SSTCompactor.cpp

        ${INTERNAL_SRC_DIR}/engine/AkkEngine.cpp
        ${INTERNAL_SRC_DIR}/engine/StripeLaneWriter.cpp
)

# Include directories
target_include_directories(akkaradb
        PUBLIC
        $<BUILD_INTERFACE:${AKKARADB_INCLUDE_DIR}>
        $<INSTALL_INTERFACE:include>
        PRIVATE
        ${INTERNAL_INCLUDE_DIR}
)

# Link dependencies
target_link_libraries(akkaradb
        PRIVATE
        nlohmann_json::nlohmann_json
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
        benchmarks/main_benchmark.cpp
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