#pragma once

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <cmath>
#include <pthread.h>
#include <sys/stat.h>
#include <util/runnable.h>
#include <util/arch.h>
#include <core/datatypes.h>
#include <core/util.h>

namespace scalable_graphs {
namespace core {
  struct processed_block_sizes_t {
    size_t size_active_vertex_src_block;
    size_t size_active_vertex_tgt_block;
    size_t size_target_vertex_block;
    size_t size_target_indices_block;
    size_t size_source_indices_block;
  };
  template <class APP, typename TVertexType, typename TVertexIdType>
  class VertexProcessor;

  template <class APP, typename TVertexType, typename TVertexIdType>
  class VertexReducer : public scalable_graphs::util::Runnable {
  public:
    VertexReducer(VertexProcessor<APP, TVertexType, TVertexIdType>& ctx,
                  vertex_array_t<TVertexType>* vertices,
                  const thread_index_t& thread_index);
    ~VertexReducer();

  private:
    virtual void run();
    bool put_edge_block_index(uint64_t tile_id);

    void preallocate();
    void initPreallocatedBlocks();
    void receive_response_block();
    void parse_arrays_from_response();
    void allocateGlobalReducerRingBufferSpace();
    processed_block_sizes_t calculateBlockSizeStruct(int index_global_reducer);
    size_t calculateBlockSize(int index_global_reducer);
    void setProcessedBlockHeader(processed_vertex_index_block_t& block,
                                 int index_global_reducer);
    void copyGlobalReducerFields(int index_global_reducer);
    void copyGlobalReducerBlocks(bool completed);

    void sendDummyBlock(bool completed);
    void reduceTargetVertices();

    void processSourceVertices();
    void processTargetVertices();

  private:
    VertexProcessor<APP, TVertexType, TVertexIdType>& ctx_;
    config_vertex_domain_t config_;

    vertex_array_t<TVertexType>* vertices_;
    thread_index_t thread_index_;

    processed_vertex_block_t* response_block_;

    char* active_vertices_src_next_;
    char* active_vertices_tgt_next_;
    TVertexType* tgt_vertices_;

    size_t max_size_global_reducer_block_;

    processed_vertex_index_block_t** global_reducer_blocks_local_;
    processed_vertex_index_block_t** global_reducer_blocks_remote_;

    edge_block_index_t* edge_block_index_;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "vertex-reducer.cc"
#endif
