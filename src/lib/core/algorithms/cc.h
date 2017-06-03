#pragma once

#include <cmath>
#include <limits.h>
#include <string.h>

#include <core/util.h>

#include "algorithm-common.h"

namespace scalable_graphs {
namespace core {
  class CC {
  public:
    typedef uint32_t VertexType;

    const static bool need_active_block = false;
    const static bool need_active_source_block = false;
    const static bool need_active_source_input = true;
    const static bool need_active_target_block = false;
    const static bool need_degrees_source_block = false;
    const static bool need_degrees_target_block = false;
    const static bool need_vertex_block_extension_fields = false;

    const static size_t max_size_extension_fields_vertex_block = 0;

    const static VertexType neutral_element = UINT32_MAX;

    CC() = delete;
    ~CC() = delete;

    static inline size_t
    sizeExtensionFieldsVertexBlock(const tile_stats_t& tile_stats) {
      // not needed
      return 0;
    }

    static inline void fillExtensionFieldsVertexBlock(
        void* extension_fields,
        const volatile edge_block_index_t* edge_block_index,
        const uint32_t* src_index, const uint32_t* tgt_index,
        const vertex_array_t<VertexType>* vertex_array) {
      // not applicable
    }

    static inline void gather(const VertexType& u, VertexType& v, uint16_t id,
                              void* extension_fields) {
      v = std::min(u, v);
    }

    static inline void
    pullGather(const VertexType& u, VertexType& v, uint16_t id_src,
               uint16_t id_tgt, const vertex_degree_t* src_degree,
               const vertex_degree_t* tgt_degree, char* active_array_src,
               char* active_array_tgt, const config_edge_processor_t& config,
               void* extension_fields) {
      if (u != UINT32_MAX) {
        v = std::min(v, u);
      }
    }

    static inline void pullGatherWeighted(
        const VertexType& u, VertexType& v, const float weight, uint16_t id_src,
        uint16_t id_tgt, const vertex_degree_t* src_degree,
        const vertex_degree_t* tgt_degree, char* active_array_src,
        char* active_array_tgt, const config_edge_processor_t& config,
        void* extension_fields) {
      // not applicable
    }

    static inline void apply(vertex_array_t<VertexType>* vertices,
                             const uint64_t id,
                             const config_vertex_domain_t& config,
                             const uint32_t iteration) {
      // pass, nothing to be done here
      // if (vertices->next[id] != vertices->current[id]) {
      //   vertices->current[id] = vertices->next[id];
      //   set_active(vertices->active_next, id);
      // }
    }

    static inline void
    reduceVertex(VertexType& out, const VertexType& lhs, const VertexType& rhs,
                 const uint64_t& id_tgt, const vertex_degree_t& degree,
                 char* active_array, const config_vertex_domain_t& config) {
      if (lhs < out) {
        out = lhs;
        set_active(active_array, id_tgt);
      }
    }

    static void init_vertices(vertex_array_t<VertexType>* vertices,
                              void* args) {
      sg_print("Init vertices\n");
      // also need to init array for next round, otherwise everything will be 0
      for (int i = 0; i < vertices->count; ++i) {
        vertices->current[i] = i;
        vertices->next[i] = i;
      }
      // all vertices active in the beginning, for the first round.
      memset(vertices->active_current, (unsigned char)255,
             vertices->size_active * sizeof(char));
    }

    // reset current-array for next round
    static void reset_vertices(vertex_array_t<VertexType>* vertices,
                               bool* switchCurrentNext) {
      memset(vertices->active_current, 0x00,
             vertices->size_active * sizeof(char));
    }

    static void pre_processing_per_round(vertex_array_t<VertexType>* vertices,
                                         const config_vertex_domain_t& config,
                                         const uint32_t iteration) {}

    static inline void
    reset_vertices_tile_processor(VertexType* tgt_vertices,
                                  const size_t response_vertices) {
      for (int i = 0; i < response_vertices; ++i) {
        tgt_vertices[i] = UINT32_MAX;
      }
    }
  };
}
}
