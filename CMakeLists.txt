cmake_minimum_required(VERSION 4.0)
project(akkaradb_native VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

if(CMAKE_BUILD_TYPE MATCHES Debug)
    if(NOT MSVC)
        message(STATUS "Enabling sanitizers for Debug build")
        add_compile_options(-fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer -g)
        add_link_options(-fsanitize=address -fsanitize=undefined)
    endif()
endif()

add_subdirectory(akkara/common)
add_subdirectory(akkara/format-api)
add_subdirectory(akkara/format-akk)
add_subdirectory(akkara/engine)

# 統合ライブラリ（Kotlin版の akkaradb に相当）
#add_library(akkaradb_native STATIC)
#target_link_libraries(akkaradb_native
#        PUBLIC
#        akkaradb_engine
#)