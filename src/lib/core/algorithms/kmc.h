#pragma once

#include <cmath>
#include <limits.h>
#include <string.h>
#include <core/util.h>
#include <core/datatypes.h>

#include "algorithm-common.h"

#define NUM_CLUSTERS 4

namespace scalable_graphs {
namespace core {
  class KMC {

    struct VertexType {
      float x_cord;
      int designated_cluster;
    };

    struct cluster_information_t {
      float cord_cluster[NUM_CLUSTERS];
    };

  public:
    const static bool need_active_block = false;
    const static bool need_active_source_block = false;
    const static bool need_active_source_input = false;
    const static bool need_active_target_block = false;
    const static bool need_degrees_source_block = false;
    const static bool need_degrees_target_block = false;
    const static bool need_vertex_block_extension_fields = false;

    const static size_t max_size_extension_fields_vertex_block = 0;

    static cluster_information_t cluster_info;

#ifndef TARGET_ARCH_K1OM
    constexpr const static VertexType neutral_element = {0., 0};
#endif

    KMC() = delete;
    ~KMC() = delete;

    static inline size_t
    sizeExtensionFieldsVertexBlock(const tile_stats_t& tile_stats) {
      // not required in this case
    }

    static inline void fillExtensionFieldsVertexBlock(
        void* extension_fields,
        const volatile edge_block_index_t* edge_block_index,
        const uint32_t* src_index, const uint32_t* tgt_index,
        const vertex_array_t<VertexType>* vertex_array) {
      // not required in this case
    }

    static inline void gather(const VertexType& u, VertexType& v, uint16_t id,
                              void* extension_fields) {
      // not required in this case
    }

    static inline void
    pullGather(const VertexType& u, VertexType& v, uint16_t id_src,
               uint16_t id_tgt, const vertex_degree_t* src_degree,
               const vertex_degree_t* tgt_degree, char* active_array_src,
               char* active_array_tgt, const config_edge_processor_t& config,
               void* extension_fields) {
      // not required in this case
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
      float min_distance = FLT_MAX;
      int cid = 0;
      for (int i = 0; i < NUM_CLUSTERS; ++i) {
        float distance =
            get_distance(v.x_cord, KMC::cluster_info.cord_cluster[i]);
        if (distance < min_distance) {
          min_distance = distance;
          cid = i;
        }
      }
      v.designated_cluster = cid;
    }

    static inline void
    reduceVertex(VertexType& out, const VertexType& lhs, const VertexType& rhs,
                 const uint64_t& id_tgt, const vertex_degree_t& degree,
                 char* active_array, const config_vertex_domain_t& config) {
      // not required in this case
    }

    static void init_vertices(vertex_array_t<VertexType>* vertices,
                              void* args) {
      sg_print("Init vertices\n");

      unsigned int seed = rand32_seedless();
      float* clusters = KMC::cluster_info.cord_cluster;
      for (int i = 0; i < NUM_CLUSTERS; ++i) {
        clusters[i] = (float)rand32(&seed);
      }

      for (int i = 0; i < vertices->count; ++i) {
        vertices->current[i].x_cord = (float)rand32(&seed);
        vertices->current[i].designated_cluster = -1;
        vertices->next[i].x_cord = FLT_MAX;
        vertices->next[i].designated_cluster = -1;
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

      memset(vertices->active_next, (unsigned char)255,
             vertices->size_active * sizeof(char));
      // reset all values of current-array, for next round
      *switchCurrentNext = false;
    }

    static void pre_processing_per_round(vertex_array_t<VertexType>* vertices,
                                         const config_vertex_domain_t& config,
                                         const uint32_t iteration) {
      if (iteration == 0)
        return;

      int elems_per_cluster[NUM_CLUSTERS] = {0};
      float total_distance[NUM_CLUSTERS] = {0.};
      VertexType* v = vertices->current;
      for (int i = 0; i < vertices->count; ++i) {
        int cid = v[i].designated_cluster;
        ++elems_per_cluster[cid];
        total_distance[cid] += v[i].x_cord;
      }
      for (int i = 0; i < NUM_CLUSTERS; ++i)
        cluster_info.cord_cluster[i] = total_distance[i] / elems_per_cluster[i];
    }

    static inline void
    reset_vertices_tile_processor(VertexType* tgt_vertices,
                                  const size_t response_vertices) {
      // not required in this case
    }

    static inline float get_distance(float x, float y) {
      return std::sqrt((y - x) * (y - x));
    }
  };
}
}
