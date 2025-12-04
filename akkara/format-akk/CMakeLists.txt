#add_library(akkaradb_format_akk STATIC
#        #src/akk_stripe_reader.cpp
#        #src/akk_stripe_writer.cpp
#        #src/akk_block_packer.cpp
#        #src/parity/no_parity_coder.cpp
#        #src/parity/xor_parity_coder.cpp
#        #src/parity/dual_xor_parity_coder.cpp
#        #src/parity/rs_parity_coder.cpp
#)
#
#target_include_directories(akkaradb_format_akk
#        PUBLIC
#        ${CMAKE_CURRENT_SOURCE_DIR}/include
#)
#
#target_link_libraries(akkaradb_format_akk
#        PUBLIC
#        akkaradb_common
#        akkaradb_format_api
#)