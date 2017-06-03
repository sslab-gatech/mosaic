set(SOURCES_HOST
  main-vertex.cc
  util.cc
)

set(SOURCES_XEON_PHI
  main-edge.cc
  util.cc
)

set(SOURCES_COMBINED
  main-combined.cc
  util.cc
)

add_library(core STATIC util.cc)

find_package(Threads)

IF($ENV{TARGET_ARCH} MATCHES "k1om")
ELSE()
  add_executable(vertex-engine ${SOURCES_HOST})
  target_link_libraries(vertex-engine util ${CMAKE_THREAD_LIBS_INIT})
ENDIF()

add_executable(edge-engine ${SOURCES_XEON_PHI})
target_link_libraries(edge-engine util ${CMAKE_THREAD_LIBS_INIT})

add_executable(mosaic ${SOURCES_COMBINED})
target_link_libraries(mosaic util ${CMAKE_THREAD_LIBS_INIT})
