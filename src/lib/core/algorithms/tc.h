#pragma once

#include <cmath>
#include <limits.h>
#include <string.h>
#include <core/util.h>
#include <core/datatypes.h>

#include "algorithm-common.h"

namespace scalable_graphs {
namespace core {
  class TC {
  public:
    struct VertexType {
      int min_vertex;
      int random_vertex;
      float sum;
      float triangle_count;

      VertexType& operator=(const int& from) { return *this; }

      bool operator==(const VertexType& other) { return false; }
      bool operator!=(const VertexType& other) { return true; }
      operator int() { return 1; }

      friend std::ostream& operator<<(std::ostream& stream,
                                      const VertexType& v);
    };

    struct global_information_t {
      int phase;
      int current_iteration;
    };

    enum Phase { PHASE_COMPUTE, PHASE_COMPARE, PHASE_AGGREGATE };

  public:
    const static bool need_active_block = false;
    const static bool need_active_source_block = false;
    const static bool need_active_source_input = false;
    const static bool need_active_target_block = false;
    const static bool need_degrees_source_block = true;
    const static bool need_degrees_target_block = true;
    const static bool need_vertex_block_extension_fields = true;

    const static size_t max_size_extension_fields_vertex_block =
        sizeof(global_information_t);

#ifndef TARGET_ARCH_K1OM
    constexpr const static VertexType neutral_element = {INT_MAX, 0, 0., 0.};
#endif

    static global_information_t global_info;
    static unsigned int seed;

    TC() = delete;
    ~TC() = delete;

    static inline size_t
    sizeExtensionFieldsVertexBlock(const tile_stats_t& tile_stats) {
      return sizeof(global_information_t);
    }

    static inline void fillExtensionFieldsVertexBlock(
        void* extension_fields,
        const volatile edge_block_index_t* edge_block_index,
        const uint32_t* src_index, const uint32_t* tgt_index,
        const vertex_array_t<VertexType>* vertex_array) {
      global_information_t* g = (global_information_t*)extension_fields;
      g->phase = TC::global_info.phase;
      g->current_iteration = TC::global_info.current_iteration;
    }

    static inline void gather(const VertexType& u, VertexType& v, uint16_t id,
                              void* extension_fields) {
      global_information_t* g = (global_information_t*)extension_fields;
      int phase = g->phase;
      switch (phase) {
      case PHASE_COMPUTE:
        v.min_vertex = std::min(v.min_vertex, u.min_vertex);
        break;
      case PHASE_COMPARE:
        if (v.min_vertex == u.min_vertex)
          v.sum++;
        break;
      case PHASE_AGGREGATE:
        v.triangle_count += u.triangle_count;
        break;
      }
    }

    static inline void
    pullGather(const VertexType& u, VertexType& v, uint16_t id_src,
               uint16_t id_tgt, const vertex_degree_t* src_degree,
               const vertex_degree_t* tgt_degree, char* active_array_src,
               char* active_array_tgt, const config_edge_processor_t& config,
               void* extension_fields) {
      global_information_t* g = (global_information_t*)extension_fields;
      int phase = g->phase;
      int current_iteration = g->current_iteration;

      switch (phase) {
      case PHASE_COMPUTE:
        v.min_vertex = std::min(v.min_vertex, u.random_vertex);
        break;
      case PHASE_COMPARE:
        if (v.min_vertex == u.min_vertex)
          v.sum++;
        break;
      case PHASE_AGGREGATE:
        v.triangle_count += (v.sum / (v.sum + current_iteration)) *
                            (src_degree->out_degree + tgt_degree->in_degree);
        break;
      }
    }

    static inline void pullGatherWeighted(
        const VertexType& u, VertexType& v, const float weight, uint16_t id_src,
        uint16_t id_tgt, const vertex_degree_t* src_degree,
        const vertex_degree_t* tgt_degree, char* active_array_src,
        char* active_array_tgt, const config_edge_processor_t& config,
        void* extension_fields) {
      // not required in this case
    }

    static inline void apply(vertex_array_t<VertexType>* vertices,
                             const uint64_t id,
                             const config_vertex_domain_t& config,
                             const uint32_t iteration) {
      VertexType& v = vertices->next[id];
      int phase = TC::global_info.phase;
      int current_iteration = TC::global_info.current_iteration;
      switch (phase) {
      case PHASE_COMPARE:
        v.min_vertex = INT_MAX;
        v.random_vertex = rand32(&(TC::seed));
        break;
      case PHASE_AGGREGATE:
        v.triangle_count /= 2;
        break;
      default:
        break;
      }
    }

    static inline void
    reduceVertex(VertexType& out, const VertexType& lhs, const VertexType& rhs,
                 const uint64_t& id_tgt, const vertex_degree_t& degree,
                 char* active_array, const config_vertex_domain_t& config) {
      int phase = TC::global_info.phase;
      int current_iteration = TC::global_info.current_iteration;

      switch (phase) {
      case PHASE_COMPUTE:
        out.min_vertex = std::min(lhs.min_vertex, rhs.min_vertex);
        break;
      case PHASE_COMPARE:
        out.sum = lhs.sum + rhs.sum;
        break;
      case PHASE_AGGREGATE:
        out.triangle_count = lhs.triangle_count + rhs.triangle_count;
        break;
      }
    }

    static void init_vertices(vertex_array_t<VertexType>* vertices,
                              void* args) {
      sg_print("Init vertices\n");
      TC::seed = rand32_seedless();
      for (int i = 0; i < vertices->count; ++i) {
        vertices->current[i].min_vertex = INT_MAX;
        vertices->current[i].random_vertex = rand32(&(TC::seed));
        vertices->current[i].sum = 0.;
        vertices->current[i].triangle_count = 0.;
        vertices->next[i].min_vertex = INT_MAX;
        vertices->next[i].sum = 0.;
        vertices->next[i].triangle_count = 0.;
      }
      // all vertices active in the beginning
      memset(vertices->active_current, (unsigned char)255,
             vertices->size_active * sizeof(char));
    }

    // reset current-array for next round
    static void reset_vertices(vertex_array_t<VertexType>* vertices,
                               bool* switchCurrentNext) {
      sg_print("Resetting vertices for next round\n");
      // activate all vertices for next round
      int phase = TC::global_info.phase;

      memset(vertices->active_next, (unsigned char)255,
             vertices->size_active * sizeof(char));
      // reset all values of current-array, for next round
      if (phase == PHASE_COMPUTE || phase == PHASE_AGGREGATE) {
        for (int i = 0; i < vertices->count; ++i) {
          vertices->current[i].min_vertex = INT_MAX;
          vertices->current[i].sum = 0;
          vertices->current[i].triangle_count = 0;
        }
      } else {
        *switchCurrentNext = false;
      }
    }

    static void pre_processing_per_round(vertex_array_t<VertexType>* vertices,
                                         const config_vertex_domain_t& config,
                                         const uint32_t iteration) {
      int phase = TC::global_info.phase;
      int current_iteration = iteration;
      if (current_iteration + 1 == config.max_iterations)
        phase = PHASE_AGGREGATE;
      else {
        switch (phase) {
        case PHASE_COMPUTE:
          phase = PHASE_COMPARE;
          break;
        case PHASE_COMPARE:
          phase = PHASE_COMPUTE;
          break;
        }
      }
      TC::global_info.phase = phase;
      TC::global_info.current_iteration = current_iteration;
    }

    static inline void
    reset_vertices_tile_processor(VertexType* tgt_vertices,
                                  const size_t response_vertices) {
      for (int i = 0; i < response_vertices; ++i) {
        tgt_vertices[i].min_vertex = INT_MAX;
        tgt_vertices[i].sum = 0;
        tgt_vertices[i].triangle_count = 0;
      }
    }
  };

  TC::global_information_t TC::global_info;
  unsigned int TC::seed;
#ifndef TARGET_ARCH_K1OM
  constexpr const TC::VertexType TC::neutral_element;
#endif

  std::ostream& operator<<(std::ostream& stream, const TC::VertexType& v) {
    return stream;
  }
};
}
