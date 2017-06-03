#pragma once

#include <string.h>
#include <core/util.h>
#include <core/datatypes.h>

#include "algorithm-common.h"

#define ALPHA 0.85

#define EPSILON 0.01

namespace scalable_graphs {
namespace core {
  class PageRank {
  public:
    typedef float VertexType;

    const static bool need_active_block = false;
    const static bool need_active_source_block = false;
    const static bool need_active_source_input = false;
    const static bool need_active_target_block = false;
    const static bool need_degrees_source_block = true;
    const static bool need_degrees_target_block = false;
    const static bool need_vertex_block_extension_fields = false;

    const static size_t max_size_extension_fields_vertex_block = 0;

    constexpr static VertexType neutral_element = 0.;

    PageRank() = delete;
    ~PageRank() = delete;

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
      v = u + v;
    }

    static inline void
    pullGather(const VertexType& u, VertexType& v, uint16_t id_src,
               uint16_t id_tgt, const vertex_degree_t* src_degree,
               const vertex_degree_t* tgt_degree, char* active_array_src,
               char* active_array_tgt, const config_edge_processor_t& config,
               void* extension_fields) {
      v = v + (u / src_degree->out_degree);
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
      // Only apply to vertices receiving updates in this round, copy over old
      // value for inactive ones.
      // if (eval_bool_array(vertices_->changed, id)) {
      vertices->next[id] = (1 - ALPHA) + ALPHA * vertices->next[id];
      if (std::abs(vertices->current[id] - vertices->next[id]) > EPSILON) {
        set_bool_array(vertices->active_next, id, true);
      }
      // } else {
      //   vertices->next[id] = vertices->current[id];
      // }
    }

    static inline void
    reduceVertex(VertexType& out, const VertexType& lhs, const VertexType& rhs,
                 const uint64_t& id_tgt, const vertex_degree_t& degree,
                 char* active_array, const config_vertex_domain_t& config) {
      out = lhs + rhs;
    }

    static void init_vertices(vertex_array_t<VertexType>* vertices,
                              void* args) {
      sg_print("Init vertices\n");
      float initial_value = 1. / vertices->count;
      for (int i = 0; i < vertices->count; ++i) {
        vertices->current[i] = 1.;
      }
      // all vertices active in the beginning
      memset(vertices->next, 0, sizeof(VertexType) * vertices->count);
      memset(vertices->active_current, (unsigned char)255,
             vertices->size_active * sizeof(char));
      memset(vertices->active_next, (unsigned char)255,
             vertices->size_active * sizeof(char));
    }

    // reset current-array for next round
    static void reset_vertices(vertex_array_t<VertexType>* vertices,
                               bool* switchCurrentNext) {
      sg_print("Resetting vertices for next round\n");
      // activate all vertices for next round
      // reset all values of current-array, for next round
      memset(vertices->current, 0, sizeof(VertexType) * vertices->count);
      memset(vertices->active_current, 0x00,
             vertices->size_active * sizeof(char));
    }

    static void pre_processing_per_round(vertex_array_t<VertexType>* vertices,
                                         const config_vertex_domain_t& config,
                                         const uint32_t iteration) {}

    static inline void
    reset_vertices_tile_processor(VertexType* tgt_vertices,
                                  const size_t response_vertices) {
      memset(tgt_vertices, PageRank::neutral_element,
             sizeof(VertexType) * response_vertices);
    }
  };
}
}
