set(SOURCES_TILE_PROCESSOR_TEST
  main.cc
  tile-processor-test.cc
)

set(SOURCES_BOOL_ARRAY_TEST
  main.cc
  bool-array-test.cc
)

set(SOURCES_PARTITION_TEST
  main.cc
  partition-test.cc
)

set(SOURCES_TRAVERSAL_TEST
  main.cc
  traversal-test.cc
)

add_executable(bool_array_test ${SOURCES_BOOL_ARRAY_TEST})
add_executable(tile_processor_test ${SOURCES_TILE_PROCESSOR_TEST})
add_executable(partition_test ${SOURCES_PARTITION_TEST})
add_executable(traversal_test ${SOURCES_TRAVERSAL_TEST})

find_package(Threads)
find_package(GTest REQUIRED)

target_link_libraries(tile_processor_test util core pci_ring_buffer ${GTEST_BOTH_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(bool_array_test util ${GTEST_BOTH_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(partition_test core util ${GTEST_BOTH_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(traversal_test core util ${GTEST_BOTH_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
include_directories(${GTEST_INCLUDE_DIRS})
