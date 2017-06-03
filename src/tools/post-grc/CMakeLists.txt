set(SOURCES_TILE_INDEXER
    main-tile-indexer.cc
    index-reader.cc
)

find_package(Threads)

add_executable (post-grc-indexer ${SOURCES_TILE_INDEXER})
target_link_libraries(post-grc-indexer util core ${CMAKE_THREAD_LIBS_INIT})
