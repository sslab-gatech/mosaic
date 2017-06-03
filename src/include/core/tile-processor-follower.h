#pragma once

#include <unordered_map>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <util/runnable.h>
#include <core/datatypes.h>
#include <core/util.h>

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, bool is_weighted>
  class TileProcessor;

  template <class APP, typename TVertexType, bool is_weighted>
  class TileProcessorFollower : public scalable_graphs::util::Runnable {
  public:
    TileProcessorFollower(TileProcessor<APP, TVertexType, is_weighted>* tp,
                          const thread_index_t& thread_index,
                          pthread_barrier_t* tile_processor_barrier);
    ~TileProcessorFollower();

  public:
    bool shutdown_;

    char* active_vertices_src_next_;
    char* active_vertices_tgt_next_;
    TVertexType* tgt_vertices_;

    uint32_t nedges_;

  private:
    virtual void run();
    void getTileProcessorData();
    void initData();
    uint32_t process_edges();
    void process_edges_range(uint32_t start, uint32_t end);
    void process_edges_range_list(uint32_t start, uint32_t end);
    void process_edges_range_rle(uint32_t start, uint32_t end);
    uint32_t get_rle_offset(uint32_t start, uint32_t& tgt_count);

    void advance_rle_offset(uint32_t advance, uint32_t* tgt_count,
                            uint32_t* rle_offset,
                            const vertex_count_t* tgt_block_rle);

  private:
    thread_index_t thread_index_;
    TileProcessor<APP, TVertexType, is_weighted>* tp_;
    config_edge_processor_t config_;

    pthread_barrier_t* tile_processor_barrier_;

    processed_vertex_block_t* response_block_;

    uint32_t tile_partition_id_;
    vertex_edge_tiles_block_t* vertex_edge_block_;
    tile_stats_t tile_stats_;
    char* active_vertices_src_;
    char* active_vertices_tgt_;
    vertex_degree_t* src_degrees_;
    vertex_degree_t* tgt_degrees_;
    TVertexType* src_vertices_;
    void* extension_fields_;
    edge_block_t* edge_block_;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "tile-processor-follower.cc"
#endif
