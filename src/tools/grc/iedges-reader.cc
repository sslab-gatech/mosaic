#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "iedges-reader.h"
#endif

#include <cstdio>
#include <fstream>
#include <sstream>
#include <string.h>

#include <util/util.h>
#include <core/util.h>

namespace scalable_graphs {
namespace graph_load {

  template <typename TEdgeType, typename TVertexIdType>
  IEdgesReader<TEdgeType, TVertexIdType>::IEdgesReader(
      const config_partitioner_t& config, PartitionManager** partition_managers)
      : config_(config), vertex_id_base_(0),
        partition_managers_(partition_managers) {
    vertex_degrees_ = (vertex_degree_t*)calloc(1, sizeof(vertex_degree_t) *
                                                      config_.count_vertices);
    pthread_spin_init(&id_lock, PTHREAD_PROCESS_PRIVATE);
    pthread_spin_init(&gv_lock, PTHREAD_PROCESS_PRIVATE);
  }

  template <typename TEdgeType, typename TVertexIdType>
  void IEdgesReader<TEdgeType, TVertexIdType>::addEdge(
      IEdgesReaderContext<TVertexIdType>& ctx,
      const int64_t src,
      const int64_t tgt) {
    edge_t edge = getEdge(ctx, src, tgt);

    sg_assert(edge.src < this->config_.count_vertices, "src < count-vertices");
    sg_assert(edge.tgt < this->config_.count_vertices, "tgt < count-vertices");

    ctx.addEdge(edge);
  }

  template <typename TEdgeType, typename TVertexIdType>
  edge_t IEdgesReader<TEdgeType, TVertexIdType>::getEdge(
      IEdgesReaderContext<TVertexIdType>& ctx, const int64_t src, int64_t tgt) {
    edge_t edge;
    if (config_.use_original_ids) {
      edge.src = src;
      edge.tgt = tgt;
    } else {
      edge.src = getOrCreateIdFast(ctx, src);
      edge.tgt = getOrCreateIdFast(ctx, tgt);
    }

    addInDegFast(ctx, edge.tgt);
    addOutDegFast(ctx, edge.src);

    return edge;
  }

  template <typename TEdgeType, typename TVertexIdType>
  TVertexIdType IEdgesReader<TEdgeType, TVertexIdType>::getOrCreateIdFast(
      IEdgesReaderContext<TVertexIdType>& ctx, const int64_t orig_id) {
    // first, try to look up an Id at my local map
    auto it = ctx.vertex_id_original_to_global_.find(orig_id);
    if (it != ctx.vertex_id_original_to_global_.end()) {
      return it->second;
    }

    // at the cachemiss, go to slow path and then cache it for future access
    TVertexIdType new_id = getOrCreateIdSlow(orig_id);
    ctx.vertex_id_original_to_global_[orig_id] = new_id;
    return new_id;
  }

  template <typename TEdgeType, typename TVertexIdType>
  TVertexIdType IEdgesReader<TEdgeType, TVertexIdType>::getOrCreateIdSlow(
      const int64_t orig_id) {
    TVertexIdType id;
    pthread_spin_lock(&id_lock);
    {
      auto it = vertex_id_original_to_global_.find(orig_id);
      if (it != vertex_id_original_to_global_.end()) {
        id = it->second;
      } else {
        id = vertex_id_base_++;
        vertex_id_original_to_global_[orig_id] = id;
        vertex_id_global_to_original_[id] = orig_id;
      }
    }
    pthread_spin_unlock(&id_lock);

    return id;
  }

  template <typename TEdgeType, typename TVertexIdType>
  void IEdgesReader<TEdgeType, TVertexIdType>::addInDegFast(
      IEdgesReaderContext<TVertexIdType>& ctx, const TVertexIdType id) {
    ++ctx.vertex_degrees_[id].in_degree;
  }

  template <typename TEdgeType, typename TVertexIdType>
  void IEdgesReader<TEdgeType, TVertexIdType>::addOutDegFast(
      IEdgesReaderContext<TVertexIdType>& ctx, const TVertexIdType id) {
    ++ctx.vertex_degrees_[id].out_degree;
  }

  template <typename TEdgeType, typename TVertexIdType>
  void IEdgesReader<TEdgeType, TVertexIdType>::writeGlobalFiles() {
    // write the translation from orginal to global ids:

    int count_vertices = vertex_id_base_;
    // if using the orignal ids of the file, this map has to be created now:
    if (config_.use_original_ids) {
      count_vertices = config_.count_vertices;
      for (uint64_t i = 0; i < config_.count_vertices; ++i) {
        vertex_id_global_to_original_[i] = i;
      }
    }
    std::string vertex_translation_global_to_orig_file_name =
        core::getGlobalToOrigIDFileName(config_);

    core::writeMapToFile<TVertexIdType, int64_t>(
        vertex_translation_global_to_orig_file_name,
        vertex_id_global_to_original_);

    // write graph-statistics as well for tiler to know the exact count over
    // vertices
    std::string global_stats_file_name = core::getGlobalStatFileName(config_);

    scenario_stats_t stat;
    stat.count_vertices = count_vertices;

    util::writeDataToFile(global_stats_file_name,
                          reinterpret_cast<const void*>(&stat), sizeof(stat));

    // write the vertex-degree-file:
    std::string vertex_degree_file_name =
        core::getVertexDegreeFileName(config_);

    size_t size_vertex_degree_file =
        sizeof(vertex_degree_t) * config_.count_vertices;

    util::writeDataToFile(vertex_degree_file_name,
                          reinterpret_cast<const void*>(vertex_degrees_),
                          size_vertex_degree_file);
  }

  template <typename TEdgeType, typename TVertexIdType>
  void IEdgesReader<TEdgeType, TVertexIdType>::reduceVertexDegrees(
      const IEdgesReaderContext<TVertexIdType>& ctx) {
    pthread_spin_lock(&gv_lock);
    for (uint64_t id = 0; id < config_.count_vertices; ++id) {
      vertex_degrees_[id].in_degree += ctx.vertex_degrees_[id].in_degree;
      vertex_degrees_[id].out_degree += ctx.vertex_degrees_[id].out_degree;
    }
    pthread_spin_unlock(&gv_lock);
  }

  template <typename TEdgeType, typename TVertexIdType>
  IEdgesReader<TEdgeType, TVertexIdType>::~IEdgesReader() {
    free(vertex_degrees_);
    pthread_spin_destroy(&id_lock);
    pthread_spin_destroy(&gv_lock);
  }
}
}
