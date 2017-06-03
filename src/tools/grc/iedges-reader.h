#pragma once

#include <string>
#include <vector>

#include <core/datatypes.h>

#include "partition-manager.h"
#include "iedges-reader-context.h"

namespace scalable_graphs {
namespace graph_load {

  template <typename TEdgeType, typename TVertexIdType>
  class IEdgesReader {
  public:
    IEdgesReader(const config_partitioner_t& config,
                 PartitionManager** partition_managers);

    virtual int readEdges(int max_thread) = 0;

    virtual ~IEdgesReader();

  public:
  protected:
    void addEdge(IEdgesReaderContext<TVertexIdType>& ctx,
                 const int64_t src,
                 const int64_t tgt);

    inline edge_t getEdge(IEdgesReaderContext<TVertexIdType>& ctx,
                          const int64_t src,
                          const int64_t target);

    // ID-creation and retrieval with fast/slow-path
    TVertexIdType getOrCreateIdFast(IEdgesReaderContext<TVertexIdType>& ctx,
                                    const int64_t orig_id);

    TVertexIdType getOrCreateIdSlow(const int64_t orig_id);

    // degree-increment, both for in- and out-degree on a local copy
    void addInDegFast(IEdgesReaderContext<TVertexIdType>& ctx,
                      const TVertexIdType id);

    void addOutDegFast(IEdgesReaderContext<TVertexIdType>& ctx,
                       const TVertexIdType id);

    void writeGlobalFiles();

    void reduceVertexDegrees(const IEdgesReaderContext<TVertexIdType>& ctx);

  protected:
    pthread_spinlock_t id_lock;
    std::unordered_map<int64_t, TVertexIdType> vertex_id_original_to_global_;

    // is being written to file and can be used to de-translate the internal
    // array to global ids
    std::unordered_map<TVertexIdType, int64_t> vertex_id_global_to_original_;
    int64_t vertex_id_base_;

    pthread_spinlock_t gv_lock;
    vertex_degree_t* vertex_degrees_;

    PartitionManager** partition_managers_;

    config_partitioner_t config_;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "iedges-reader.cc"
#endif
