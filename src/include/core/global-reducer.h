#pragma once

#include <ring_buffer.h>
#include <util/runnable.h>
#include <core/datatypes.h>

#define SIZE_RESPONSE_RB 65536

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, typename TVertexIdType>
  class VertexDomain;

  template <class APP, typename TVertexType, typename TVertexIdType>
  class GlobalReducer : public scalable_graphs::util::Runnable {
  public:
    GlobalReducer(VertexDomain<APP, TVertexType, TVertexIdType>& ctx,
                  vertex_array_t<TVertexType>* vertices,
                  const thread_index_t& thread_index);

    ~GlobalReducer();

  public:
    ring_buffer_t* response_rb_;

    // The average processing rate of edges per second, is public to allow the
    // VertexDomain to collect the value.
    double average_processing_rate_;

  private:
    virtual void run();

    void init_memory();

    void receive_reduce_block();

    void process_source_vertices();

    void process_target_vertices();

    void parse_arrays_from_response();

    void aggregateProcessingTime();

  private:
    config_vertex_domain_t config_;
    VertexDomain<APP, TVertexType, TVertexIdType>& ctx_;
    vertex_array_t<TVertexType>* vertices_;
    thread_index_t thread_index_;

    processed_vertex_index_block_t* reduce_block_;
    char* active_vertices_src_next_;
    char* active_vertices_tgt_next_;
    TVertexType* tgt_vertices_;
    TVertexIdType* tgt_index_;
    TVertexIdType* src_index_;

    uint32_t count_processing_times_;

    const static size_t processed_rb_size_ = 1ul * GB;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "global-reducer.cc"
#endif
