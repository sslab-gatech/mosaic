#pragma once

#include <unordered_map>

#include <core/datatypes.h>

#include "partition-manager.h"

namespace scalable_graphs {
  namespace graph_load {

    template<typename TVertexIdType>
    class IEdgesReaderContext {
    public:
      std::unordered_map<TVertexIdType, TVertexIdType>
          vertex_id_original_to_global_;
      vertex_degree_t* vertex_degrees_;

    public:
      IEdgesReaderContext(const uint64_t count_vertices,
                          const config_partitioner_t& config,
                          PartitionManager** partition_managers);

      void addEdge(const edge_t& edge);

      void sendEdgesToPartitionManager(const int manager);

      void sendEdgesToAllPartitionManagers();

      ~IEdgesReaderContext();

    private:
      partition_edge_list_t** edges_for_partition_managers_;

      PartitionManager** partition_managers_;

      config_partitioner_t config_;
    };
  }
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "iedges-reader-context.cc"
#endif
