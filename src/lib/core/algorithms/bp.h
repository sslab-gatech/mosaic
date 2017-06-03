#pragma once

#include <cmath>
#include <limits.h>
#include <string.h>
#include <core/util.h>
#include <core/datatypes.h>

#include "algorithm-common.h"

#define BP_STATES 2

namespace scalable_graphs {
namespace core {
  class BP {
  public:
    struct VertexType {
      float belief[BP_STATES];
      float pre_msg[BP_STATES];
      float msg[BP_STATES];
      float prior[BP_STATES];

      VertexType& operator=(const int& from) { return *this; }

      bool operator==(const VertexType& other) { return false; }
      bool operator!=(const VertexType& other) { return true; }
      operator int() { return 1; }

      friend std::ostream& operator<<(std::ostream& stream,
                                      const VertexType& v);
    };

    struct global_information_t {
      int phase;
    };

    enum Phase { PHASE_EMIT, PHASE_ABSORB, PHASE_CONVERGED };

  public:
    const static bool need_active_block = false;
    const static bool need_active_source_block = false;
    const static bool need_active_source_input = false;
    const static bool need_active_target_block = false;
    const static bool need_degrees_source_block = false;
    const static bool need_degrees_target_block = false;
    const static bool need_vertex_block_extension_fields = true;

    const static size_t max_size_extension_fields_vertex_block =
        sizeof(global_information_t);

#ifndef TARGET_ARCH_K1OM
    constexpr const static VertexType neutral_element = {
        {1., 1.}, {1., 1.}, {0., 0.}, {0., 0.}};
#endif

    static global_information_t global_info;

    BP() = delete;
    ~BP() = delete;

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
      g->phase = BP::global_info.phase;
    }

    static inline void gather(const VertexType& u, VertexType& v, uint16_t id,
                              void* extension_fields) {
      global_information_t* g = (global_information_t*)extension_fields;
      int phase = g->phase;
      for (int i = 0; i < BP_STATES; ++i) {
        switch (phase) {
        case PHASE_EMIT:
          v.pre_msg[i] *= u.pre_msg[i];
          break;
        case PHASE_ABSORB:
          v.msg[i] *= u.msg[i];
          break;
        case PHASE_CONVERGED:
          v.belief[i] *= u.msg[i];
          break;
        }
      }
    }

    static inline void
    pullGather(const VertexType& u, VertexType& v, uint16_t id_src,
               uint16_t id_tgt, const vertex_degree_t* src_degree,
               const vertex_degree_t* tgt_degree, char* active_array_src,
               char* active_array_tgt, const config_edge_processor_t& config,
               void* extension_fields) {}

    static inline void pullGatherWeighted(
        const VertexType& u, VertexType& v, const float weight, uint16_t id_src,
        uint16_t id_tgt, const vertex_degree_t* src_degree,
        const vertex_degree_t* tgt_degree, char* active_array_src,
        char* active_array_tgt, const config_edge_processor_t& config,
        void* extension_fields) {
      // not required in this case
      global_information_t* g = (global_information_t*)extension_fields;
      int phase = g->phase;
      for (int i = 0; i < BP_STATES; ++i) {
        switch (phase) {
        case PHASE_EMIT:
          v.pre_msg[i] *= u.pre_msg[i];
          break;
        case PHASE_ABSORB:
          v.msg[i] = weight * v.prior[i] * u.pre_msg[i] / v.pre_msg[i];
          break;
        case PHASE_CONVERGED:
          v.belief[i] *= u.msg[i];
          break;
        }
      }
    }

    static inline void apply(vertex_array_t<VertexType>* vertices,
                             const uint64_t id,
                             const config_vertex_domain_t& config,
                             const uint32_t iteration) {
      VertexType& v = vertices->next[id];
      int phase = BP::global_info.phase;
      if (phase == PHASE_CONVERGED) {
        float normalize_constant = 0.;
        for (int i = 0; i < BP_STATES; ++i) {
          v.belief[i] = v.belief[i] * v.prior[i];
          normalize_constant += v.belief[i];
        }
        if (normalize_constant > 0.0) {
          for (int i = 0; i < BP_STATES; ++i)
            v.belief[i] /= normalize_constant;
        }
      }
    }

    static inline void
    reduceVertex(VertexType& out, const VertexType& lhs, const VertexType& rhs,
                 const uint64_t& id_tgt, const vertex_degree_t& degree,
                 char* active_array, const config_vertex_domain_t& config) {
      int phase = BP::global_info.phase;

      for (int i = 0; i < BP_STATES; ++i) {
        switch (phase) {
        case PHASE_EMIT:
          out.pre_msg[i] = lhs.pre_msg[i] * rhs.pre_msg[i];
          break;
        case PHASE_ABSORB:
          out.msg[i] = lhs.msg[i] + rhs.msg[i];
          break;
        case PHASE_CONVERGED:
          out.belief[i] = lhs.belief[i] + rhs.belief[i];
          break;
        }
      }
    }

    static void init_vertices(vertex_array_t<VertexType>* vertices,
                              void* args) {
      sg_print("Init vertices\n");
      for (int i = 0; i < vertices->count; ++i) {
        for (int j = 0; j < BP_STATES; ++j) {
          vertices->current[i].belief[j] = 1.;
          vertices->current[i].pre_msg[j] = 1.;
          vertices->current[i].msg[j] = 0.;
          vertices->current[i].prior[j] = 1.0 / BP_STATES;
          vertices->next[i].belief[j] = 1.;
          vertices->next[i].pre_msg[j] = 1.;
          vertices->next[i].msg[j] = 0.;
        }
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
      int phase = BP::global_info.phase;

      memset(vertices->active_next, (unsigned char)255,
             vertices->size_active * sizeof(char));
      // reset all values of current-array, for next round
      if (phase == PHASE_EMIT || phase == PHASE_CONVERGED) {
        for (int i = 0; i < vertices->count; ++i) {
          for (int j = 0; j < BP_STATES; ++j) {
            vertices->current[i].belief[j] = 1.;
            vertices->current[i].pre_msg[j] = 1.;
            vertices->current[i].msg[j] = 0.;
          }
        }
      } else {
        *switchCurrentNext = false;
      }
    }

    static void pre_processing_per_round(vertex_array_t<VertexType>* vertices,
                                         const config_vertex_domain_t& config,
                                         const uint32_t iteration) {
      int phase = BP::global_info.phase;
      if (iteration + 1 == config.max_iterations)
        phase = PHASE_CONVERGED;
      else {
        switch (phase) {
        case PHASE_EMIT:
          phase = PHASE_ABSORB;
          break;
        case PHASE_ABSORB:
          phase = PHASE_EMIT;
          break;
        }
      }
      BP::global_info.phase = phase;
    }

    static inline void
    reset_vertices_tile_processor(VertexType* tgt_vertices,
                                  const size_t response_vertices) {
      for (int i = 0; i < response_vertices; ++i) {
        for (int j = 0; j < BP_STATES; ++j) {
          tgt_vertices[i].belief[j] = 1.;
          tgt_vertices[i].pre_msg[j] = 1.;
          tgt_vertices[i].msg[j] = 0.;
        }
      }
    }
  };

  BP::global_information_t BP::global_info;
#ifndef TARGET_ARCH_K1OM
  constexpr const BP::VertexType BP::neutral_element;
#endif

  std::ostream& operator<<(std::ostream& stream, const BP::VertexType& v) {
    return stream;
  }
}
}
