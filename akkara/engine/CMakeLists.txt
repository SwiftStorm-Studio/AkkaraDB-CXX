#add_library(akkaradb_engine STATIC
#        #src/akkara_db.cpp
#        #src/packed_table.cpp
#        #src/memtable/mem_table.cpp
#        #src/wal/wal_writer.cpp
#        #src/wal/wal_replay.cpp
#        #src/wal/wal_op.cpp
#        #src/sstable/sstable_reader.cpp
#        #src/sstable/sstable_writer.cpp
#        #src/sstable/sst_compactor.cpp
#        #src/manifest/akk_manifest.cpp
#)
#
#target_include_directories(akkaradb_engine
#        PUBLIC
#        ${CMAKE_CURRENT_SOURCE_DIR}/include
#)
#
#target_link_libraries(akkaradb_engine
#        PUBLIC
#        akkaradb_common
#        akkaradb_format_api
#        akkaradb_format_akk
#)