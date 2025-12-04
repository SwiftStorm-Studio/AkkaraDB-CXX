add_library(akkaradb_common STATIC
        include/akkaradb/common/ByteBufferL.h
        include/akkaradb/common/AKHdr32.h

        src/ByteBufferL.cpp
        src/AKHdr32.cpp
)

target_include_directories(akkaradb_common
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# CRC32c用（後で追加）
# target_link_libraries(akkaradb_common PRIVATE crc32c)