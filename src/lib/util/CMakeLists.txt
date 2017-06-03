set(SOURCES
  util.cc
  runnable.cc
  hilbert.cc
  column_first.cc
  row_first.cc
  read-context.cc
  perf-event/perf-event-collector.cc
  perf-event/perf-event-manager.cc
  perf-event/perf-event-ringbuffer-sizes.cc
  perf-event/perf-event-scoped.cc
)

SET_SOURCE_FILES_PROPERTIES(ring_buffer.c PROPERTIES LANGUAGE C)

add_library(util STATIC ${SOURCES})

target_link_libraries (util rt pci_ring_buffer)
