set(SOURCES_HOST
  host.cc
)

set(SOURCE_CLIENT
  client.cc
)

set(SOURCE_LOCAL
  local.cc
)

add_executable(host ${SOURCES_HOST})
add_executable(client ${SOURCE_CLIENT})
add_executable(local ${SOURCE_LOCAL})

find_package(Threads)
TARGET_LINK_LIBRARIES(host util core ${CMAKE_THREAD_LIBS_INIT})
TARGET_LINK_LIBRARIES(client util core ${CMAKE_THREAD_LIBS_INIT})
TARGET_LINK_LIBRARIES(local util core ${CMAKE_THREAD_LIBS_INIT})
