#pragma once

#include <unordered_map>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

#include <vector>
#include <list>

#include <pthread.h>

#include <util/runnable.h>
#include <core/util.h>
#include <core/datatypes.h>
#include <core/vertex-applier.h>
#include <core/vertex-processor.h>
#include <core/vertex-reducer.h>
#include <core/vertex-fetcher.h>
#include <core/global-reducer.h>
#include <core/global-fetcher.h>
#include <core/vertex-perfmon.h>
#include <util/perf-event/perf-event-manager.h>

namespace scalable_graphs {
namespace core {

  namespace pe = scalable_graphs::util::perf_event;

  template <class APP, typename TVertexType, typename TVertexIdType>
  class VertexDomain {
  public:
    VertexDomain(const config_vertex_domain_t& config);
    ~VertexDomain();

    int init();
    int start();
    void join();
    void resetRound();
    /*this for selective scheduling */
    void initActiveTiles();

    void shutdown();

    bool isShutdown();

    void initAlgorithm();

    void initTimers();

    void calculateTileBreakPoint(const size_t& count_active_tiles);

  public:
    config_vertex_domain_t config_;
    VertexPerfMonitor perfmon_;

    pthread_mutex_t active_tiles_mutex_;

    // For the Locking LocalReducer strategy, provide the global locking table.
    vertex_lock_table_t vertex_lock_table;

  private:
    friend class VertexApplier<APP, TVertexType, TVertexIdType>;
    friend class VertexProcessor<APP, TVertexType, TVertexIdType>;
    friend class VertexReducer<APP, TVertexType, TVertexIdType>;
    friend class VertexFetcher<APP, TVertexType, TVertexIdType>;
    friend class GlobalReducer<APP, TVertexType, TVertexIdType>;
    friend class GlobalFetcher<APP, TVertexType, TVertexIdType>;
    friend class IndexReader<APP, TVertexType, TVertexIdType>;

    void initVertexArray();

    size_t countActiveTiles();

    size_t countActiveVertices();

  private:
    bool shutdown_;

    pthread_barrier_t end_reduce_barrier_;
    pthread_barrier_t end_apply_barrier_;
    pthread_barrier_t local_apply_barrier_;

    pthread_barrier_t init_algorithm_barrier_;
    pthread_barrier_t init_active_tiles_barrier_;
    pthread_barrier_t memory_init_global_reducer_barrier_;
    pthread_barrier_t memory_init_barrier_;

    pthread_barrier_t fault_tolerance_barrier_;

    GlobalReducer<APP, TVertexType, TVertexIdType>** global_reducers_;
    GlobalFetcher<APP, TVertexType, TVertexIdType>** global_fetchers_;
    std::vector<VertexProcessor<APP, TVertexType, TVertexIdType>*> vp_;
    std::vector<VertexApplier<APP, TVertexType, TVertexIdType>*>
        vertex_appliers_;

    std::vector<util::Runnable*> threads_;

    vertex_array_t<TVertexType>* vertices_;
    std::unordered_map<TVertexIdType, int64_t> global_to_orig_;

    // selective-scheduling-arrays
    size_t* vertex_to_tiles_offset_;
    uint32_t* vertex_to_tiles_count_;
    uint32_t* vertex_to_tiles_index_;

    size_t iteration_;

    size_t tile_break_point_;

    // for calculating the time spent in the current round
    struct timeval start_tv_round_;
    struct timeval init_tv_;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "vertex-domain.cc"
#endif
