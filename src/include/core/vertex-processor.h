#pragma once

#include <unordered_map>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <util/runnable.h>
#include <util/atomic_counter.h>
#include <core/util.h>
#include <ring_buffer.h>
#include <core/datatypes.h>
#include <core/vertex-reducer.h>
#include <core/vertex-fetcher.h>
#include <core/index-reader.h>
#include <util/perf-event/perf-event-ringbuffer-sizes.h>

#if !defined(MOSAIC_HOST_ONLY)
#include <ring_buffer_scif.h>
#endif

namespace scalable_graphs {
namespace core {

  namespace pe = scalable_graphs::util::perf_event;

  template <class APP, typename TVertexType, typename TVertexIdType>
  class VertexDomain;

  template <class APP, typename TVertexType, typename TVertexIdType>
  class VertexProcessor {
  public:
    VertexProcessor(VertexDomain<APP, TVertexType, TVertexIdType>& vp,
                    const config_vertex_domain_t& config, int mic_id,
                    int edge_engine_index);
    ~VertexProcessor();
    int init();
    int start();
    void join();
    void joinIndexReaders();
    void resetRound(size_t count_active_tiles);

    void sendActiveTiles(size_t count_active_tiles);

    void shutdown();
    bool isShutdown();

  public:
    VertexDomain<APP, TVertexType, TVertexIdType>& vd_;
    const config_vertex_domain_t config_;

    /* this for selective sched <---- */
    size_t size_tile_active_;
    // pointer to a local bool-array to tell which tile is active
    char* tile_active_current_;
    char* tile_active_next_;

    /*for tile fetched stat */
    static size_t count_tiles_fetched;

  private:
    friend class VertexReducer<APP, TVertexType, TVertexIdType>;
    friend class VertexFetcher<APP, TVertexType, TVertexIdType>;
    friend class IndexReader<APP, TVertexType, TVertexIdType>;

    void initRingBuffers();
    void allocate();

  private:
    int mic_id_;
    int edge_engine_index_;

    std::vector<util::Runnable*> threads_;
    std::vector<core::VertexFetcher<APP, TVertexType, TVertexIdType>*>
        vertex_fetchers_;
    std::vector<core::IndexReader<APP, TVertexType, TVertexIdType>*>
        index_readers_;

    pe::PerfEventRingbufferSizes* perf_event_ring_buffer_sizes_;

    tile_stats_t* tile_stats_;

    ring_buffer_type response_rb_;
    ring_buffer_type tiles_data_rb_;

    // for sharing the remote tiles-active-array with the host
    ring_buffer_type active_tiles_rb_;

    ring_buffer_t* index_rb_;

    int meta_fd_;
    size_t* tile_offsets_;

    scalable_graphs::util::AtomicCounter fetcher_progress_;

    scalable_graphs::util::AtomicCounter index_reader_progress_;

    pthread_barrier_t fetchers_barrier_;
    pthread_barrier_t barrier_readers_;

    // stores pointers to combined-edge-block-indices in the index_rb_
    // set by IndexReader, reset by VertexReducer
    pointer_offset_table_t<edge_block_index_t, tile_data_vertex_engine_t>
        index_offset_table_;

    const static size_t index_rb_size_ = 5ul * GB;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "vertex-processor.cc"
#endif
