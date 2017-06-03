#pragma once

#include <core/datatypes.h>

#include "iedges-reader.h"
#include "partition-manager.h"

namespace scalable_graphs {
namespace graph_load {

  template <typename TEdgeType, typename TVertexIdType>
  class RMATEdgeGenerator : public IEdgesReader<TEdgeType, TVertexIdType> {
  public:
    RMATEdgeGenerator(const config_partitioner_t& config,
                      PartitionManager** partition_managers);

    virtual int readEdges(int max_thread);

    virtual ~RMATEdgeGenerator();

  protected:
    double a_;
    double b_;
    double c_;

    uint64_t seed_;

    uint64_t count_edges_;
    uint64_t count_vertices_;

  private:
    static void* threadMain(void* arg);

    int readEdgesInRange(int tid, int64_t start, int64_t end);
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "rmat-edge-generator.cc"
#endif
