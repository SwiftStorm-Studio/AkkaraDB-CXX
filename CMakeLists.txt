cmake_minimum_required(VERSION 4.0)
project(AkkaraDB VERSION 0.0.1 LANGUAGES CXX)

# C++26
set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# =========================
# Common include root
# =========================
set(AKKARA_INCLUDE_DIR
        ${CMAKE_CURRENT_SOURCE_DIR}/akkara/include
)

# =========================
# core (implementation)
# =========================
add_library(akkara_core STATIC)

target_sources(akkara_core
        PUBLIC
        akkara/include/akkara/core/buf/BufferView.hpp
        akkara/include/akkara/core/buf/OwnedBuffer.hpp
        akkara/include/akkara/core/buf/BufferPool.hpp

        akkara/include/akkara/core/record/MemRecord.hpp
        akkara/include/akkara/core/record/AKHdr32.hpp
        akkara/include/akkara/core/record/RecordView.hpp
        PRIVATE
        akkara/src/core/buf/BufferView.cpp
        akkara/src/core/buf/OwnedBuffer.cpp
        akkara/src/core/buf/BufferPool.cpp

        akkara/src/core/record/MemRecord.cpp
        akkara/src/core/record/AKHdr.cpp
        akkara/src/core/record/RecordView.cpp
)

target_include_directories(akkara_core
        PUBLIC ${AKKARA_INCLUDE_DIR}
)

set_target_properties(akkara_core
        PROPERTIES
        LINKER_LANGUAGE CXX
)

# =========================
# engine (implementation, depends on core)
# =========================
#add_library(akkara_engine STATIC
#
#)
#
#target_include_directories(akkara_engine
#        PUBLIC ${AKKARA_INCLUDE_DIR}
#)
#
#target_link_libraries(akkara_engine
#        PUBLIC akkara_core
#)

# =========================
# format-api (header-only)
# =========================
#add_library(akkara_format_api INTERFACE)
#
#target_include_directories(akkara_format_api
#        INTERFACE ${AKKARA_INCLUDE_DIR}
#)

# =========================
# format-akk (implementation of format-api)
# =========================
#add_library(akkara_format_akk STATIC
#
#)
#
#target_include_directories(akkara_format_akk
#        PUBLIC ${AKKARA_INCLUDE_DIR}
#)
#
#target_link_libraries(akkara_format_akk
#        PUBLIC
#        akkara_format_api
#        akkara_core
#)

# =========================
# umbrella target
# =========================
add_library(akkaradb INTERFACE)

target_link_libraries(akkaradb
        INTERFACE
        akkara_core
        #akkara_engine
        #akkara_format_api
        #akkara_format_akk
)
