IF(MOSAIC_HOST_ONLY)
set(SOURCES
  ring_buffer.c
  ring_buffer_porting.c
)
ELSE()
set(SOURCES
  ring_buffer_scif.c
  ring_buffer_porting.c
)
ENDIF()

IF(MOSAIC_HOST_ONLY)
  set_source_files_properties(ring_buffer.c PROPERTIES LANGUAGE C)
ELSE()
set_source_files_properties(ring_buffer_scif.c PROPERTIES LANGUAGE C)
ENDIF()
set_source_files_properties(ring_buffer_porting.c PROPERTIES LANGUAGE C)
set(CMAKE_C_FLAGS_DEBUG "-DRING_BUFFER_TRACE_EVENT")

find_package(Threads)
add_library(pci_ring_buffer STATIC ${SOURCES})

IF(MOSAIC_HOST_ONLY)
  target_link_libraries(pci_ring_buffer rt)
ELSE()
  target_link_libraries(pci_ring_buffer rt scif)
ENDIF()
