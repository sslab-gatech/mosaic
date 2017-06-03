set(SOURCES_PARTITIONER
    main-partitioner.cc
    abstract-partition-manager.cc
    partition-manager.cc
    list-partition-store.cc
    ipartition-store.cc
    partition-store.cc
    write-worker.cc
    rmat-context.cc
)

set(SOURCES_TILER
    main-tiler.cc
    abstract-partition-manager.cc
    partition-manager.cc
    list-partition-store.cc
    ipartition-store.cc
    partition-store.cc
)

set(SOURCES_RMAT_GENERATOR
    main-rmat-generator.cc
    remote-rmat-generator.cc
    rmat-context.cc
)

set(SOURCES_RMAT_TILER
    main-rmat-tiler.cc
    abstract-partition-manager.cc
    in-memory-partition-manager.cc
    in-memory-partition-store.cc
    rmat-edge-receiver.cc
)

set(SOURCES_IN_MEMORY_CONVERTER
    main-in-memory.cc
    abstract-partition-manager.cc
    partition-manager.cc
    ipartition-store.cc
    partition-store.cc
    list-partition-store.cc
    rmat-context.cc
    )

find_package(Threads)

add_executable (grc-partitioner ${SOURCES_PARTITIONER})
target_link_libraries(grc-partitioner util core ${CMAKE_THREAD_LIBS_INIT})

add_executable (grc-tiler ${SOURCES_TILER})
target_link_libraries(grc-tiler util core ${CMAKE_THREAD_LIBS_INIT})

add_executable(grc-in-memory ${SOURCES_IN_MEMORY_CONVERTER})
target_link_libraries(grc-in-memory util core ${CMAKE_THREAD_LIBS_INIT})

# Only build the Xeon-Phi targeted generators if not in host-only mode.
IF(MOSAIC_HOST_ONLY)
ELSE()
  add_executable (grc-rmat-generator ${SOURCES_RMAT_GENERATOR})
  target_link_libraries(grc-rmat-generator util core pci_ring_buffer ${CMAKE_THREAD_LIBS_INIT})

  add_executable (grc-rmat-tiler ${SOURCES_RMAT_TILER})
  target_link_libraries(grc-rmat-tiler util core pci_ring_buffer ${CMAKE_THREAD_LIBS_INIT})
ENDIF()
