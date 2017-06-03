#pragma once

#include <unordered_map>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <util/runnable.h>
#include <core/datatypes.h>
#include <core/util.h>
#include <core/tile-processor-follower.h>

#ifndef TARGET_ARCH_K1OM
#include "gtest/gtest_prod.h"
#endif

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, bool is_weighted>
  class EdgeProcessor;

  template <class APP, typename TVertexType, bool is_weighted>
  class TileProcessor : public scalable_graphs::util::Runnable {
  public:
    TileProcessor(EdgeProcessor<APP, TVertexType, is_weighted>& ctx,
                  const thread_index_t& thread_index);
    ~TileProcessor();

  private:
#ifndef TARGET_ARCH_K1OM
    // enable test to see private members
    FRIEND_TEST(TileProcessorTest, GetRleOffset);
    FRIEND_TEST(TileProcessorTest, ProcessEdgesRangeList);
    FRIEND_TEST(TileProcessorTest, ProcessEdgesRangeRle);
#endif

    virtual void run();
    bool get_tile_block();
    void get_vertex_edge_block();
    void prepare_response();
    void get_tile_data();
    uint32_t process_edges();
    void process_edges_range(uint32_t start, uint32_t end);
    void process_edges_range_list(uint32_t start, uint32_t end);
    void process_edges_range_rle(uint32_t start, uint32_t end);
    uint32_t get_rle_offset(uint32_t start, uint32_t& tgt_count);
    void wrap_up(uint32_t nedges);

    void calc_start_end_current_tile(uint32_t* start, uint32_t* end);
    uint32_t count_edges_current_tile();

    uint32_t gather_follower_output();

    void shutdown();

  private:
    friend class TileProcessorFollower<APP, TVertexType, is_weighted>;

  private:
    thread_index_t thread_index_;
    EdgeProcessor<APP, TVertexType, is_weighted>& ctx_;
    // XXX: clean up [[[
    processed_vertex_block_t* response_block_;
    char* active_vertices_src_next_;
    char* active_vertices_tgt_next_;
    TVertexType* tgt_vertices_;

    pthread_barrier_t tile_processor_barrier_;

    vertex_edge_tiles_block_t* vertex_edge_block_;
    uint64_t block_id_;
    uint32_t tile_partition_id_;
    tile_stats_t tile_stats_;
    size_t size_active_vertex_src_block_;
    size_t size_active_vertex_tgt_block_;
    char* active_vertices_src_;
    char* active_vertices_tgt_;
    vertex_degree_t* src_degrees_;
    vertex_degree_t* tgt_degrees_;
    TVertexType* src_vertices_;
    void* extension_fields_;

    volatile void* bundle_raw_;
    volatile size_t* bundle_refcnt_;
    edge_block_t* edge_block_;
    pointer_offset_t<edge_block_t, tile_data_edge_engine_t>* tile_info_;

    config_edge_processor_t config_;

    vertex_edge_tiles_block_t* fake_vertex_edge_block_;

    TileProcessorFollower<APP, TVertexType, is_weighted>** followers_;

#if PROC_TIME_PROF
    struct timeval init_start, init_end, init_result;
    double init_lat = 0.0;
    struct timeval put_start, put_end, put_result;
    double put_lat = 0.0;
    struct timeval get_tile_start, get_tile_end, get_tile_result;
    double get_tile_lat = 0.0;
    struct timeval process_start, process_end, process_result;
    double process_lat = 0.0;
    int tile_count = 0;
#endif
    // XXX: clean up ]]]
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "tile-processor.cc"
#endif
