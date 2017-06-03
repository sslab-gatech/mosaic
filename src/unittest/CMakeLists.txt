# set(SOURCES_MAIN_MMAP_TEST
#   main-mmap-test.cc
# )
# 
# set(SOURCES_BOOL_ARRAY_TEST
#   bool-array-test.cc
# )
# 
# set(SOURCES_UTIL_TEST
#   util-test.cc
# )
# SET(SOURCES_RB_TEST
#   test-rb.cc
# )
# 
# SET(SOURCES_OFFSET_TEST
#   offset-test.cc
# )

set(SOURCES_SMALL_END_TO_END_TEST_LOAD
  end-to-end-test-small-load.cc
)

set(SOURCES_PARTITION_TEST
  partition-test.cc
)

# add_executable(test_mmap ${SOURCES_MAIN_MMAP_TEST})
# add_executable(test_bool_array ${SOURCES_BOOL_ARRAY_TEST})
# add_executable(test_util ${SOURCES_UTIL_TEST})
# add_executable(test_rb ${SOURCES_RB_TEST})
# add_executable(test_offset ${SOURCES_OFFSET_TEST})

add_executable(test_end_to_end_small_load ${SOURCES_SMALL_END_TO_END_TEST_LOAD})
add_executable(test_partitions ${SOURCES_PARTITION_TEST})

find_package(Threads)
TARGET_LINK_LIBRARIES(test_end_to_end_small_load util core ${CMAKE_THREAD_LIBS_INIT})
TARGET_LINK_LIBRARIES(test_partitions util core)
