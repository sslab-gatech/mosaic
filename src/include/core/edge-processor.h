#pragma once

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <util/runnable.h>
#include <util/atomic_counter.h>
#include <core/util.h>
#include <ring_buffer.h>
#include <core/datatypes.h>
#include <core/tile-reader.h>
#include <core/tile-processor.h>
#include <core/edge-perfmon.h>
#include <util/perf-event/perf-event-manager.h>

#if !defined(MOSAIC_HOST_ONLY)
#include <ring_buffer_scif.h>
#endif

namespace scalable_graphs {
namespace core {

  namespace pe = scalable_graphs::util::perf_event;

  template <class APP, typename TVertexType, bool is_weighted>
  class EdgeProcessor {
  public:
    EdgeProcessor(const config_edge_processor_t& config);
    ~EdgeProcessor();
    int init();
    void initActiveTiles();
    int start();
    void join();
    size_t updateActiveTiles();

    void shutdown();

    bool isShutdown();

    ringbuffer_config_t getRingbufferConfig();

  public:
    EdgePerfMonitor perfmon_;

  private:
    friend class TileReader<APP, TVertexType, is_weighted>;
    friend class TileProcessor<APP, TVertexType, is_weighted>;

    bool shutdown_;

    const config_edge_processor_t config_;
    std::vector<util::Runnable*> threads_;

    tile_stats_t* tile_stats_;
    ring_buffer_t* local_tiles_rb_;

    ring_buffer_type processed_rb_;
    ring_buffer_type tiles_rb_;

    pointer_offset_table_t<edge_block_t, tile_data_edge_engine_t>
        tiles_offset_table_;

    int tiles_fd_;
    size_t* tile_offsets_;

    /*<--- selective sched */
    pthread_barrier_t barrier_tile_readers_;

    // Used by the TileProcessors on shutdown to determine the last thread to
    // shutdown the VertexReducers.
    pthread_barrier_t barrier_tile_processors_;

    // for sharing the local tiles-active-array with the host
    ring_buffer_type active_tiles_rb_;

    // size of active tiles
    size_t size_tile_active_;

    // pointer to a local bool-array to tell which tile is active
    char* tile_active_;
    size_t n_active_tiles;
    size_t n_inactive_tiles;
    /*---> */

    scalable_graphs::util::AtomicCounter tile_reader_progress_;

    // In case of using the Fake or ConstantValue TileProcessor, the block ids
    // need to be distributed as an atomic counter.
    scalable_graphs::util::AtomicCounter fake_block_id_counter_;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "edge-processor.cc"
#endif
