#pragma once

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <cmath>
#include <pthread.h>
#include <util/runnable.h>
#include <util/arch.h>
#include <core/datatypes.h>
#include <core/util.h>

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, typename TVertexIdType>
  class VertexProcessor;

  template <class APP, typename TVertexType, typename TVertexIdType>
  class VertexFetcher : public scalable_graphs::util::Runnable {
  public:
    VertexFetcher(VertexProcessor<APP, TVertexType, TVertexIdType>& ctx,
                  vertex_array_t<TVertexType>* vertices,
                  tile_stats_t* tile_stats, const thread_index_t& thread_index);
    ~VertexFetcher();

    void shutdown();

  public:
    ring_buffer_t* response_rb_;

  private:
    virtual void run();
    size_t grab_a_tile(size_t& iteration);
    void init();
    void fetch_vertices_for_tile(int tile_id);
    edge_block_index_t* get_edge_block_index(int tile_id);
    size_t fill_tile_block_header(int tile_id);
    void fill_source_fields(edge_block_index_t* edge_block_index);
    void fill_target_fields(edge_block_index_t* edge_block_index);
    void fetch_indices(edge_block_index_t* edge_block_index, int current_tile);
    void fetch_source_vertex_info(int tile_id);
    void send_tile_block(int tile_id, size_t len);

    void updateTileBreakPoint();

    bool sample_current_tile();

    uint32_t calc_num_percessors_per_tile(int tile_id);

  private:
    VertexProcessor<APP, TVertexType, TVertexIdType>& ctx_;
    vertex_array_t<TVertexType>* vertices_;
    tile_stats_t* tile_stats_;
    vertex_edge_tiles_block_t* tile_block_;
    thread_index_t thread_index_;
    size_t count_tiles_for_current_mic_;

    uint16_t** offset_indices_;
    fetch_vertices_request_t** fetch_requests_;
    TVertexIdType** fetch_requests_vertices_;
    TVertexType* src_vertices_aggregate_block_;

    config_vertex_domain_t config_;
    size_t tile_break_point_;
    const static size_t response_rb_size_ = 1ul * GB;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "vertex-fetcher.cc"
#endif
