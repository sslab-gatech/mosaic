#pragma once

#include <pthread.h>
#include <core/datatypes.h>

#include "iedges-reader.h"
#include "partition-manager.h"

namespace scalable_graphs {
  namespace graph_load {

    template<typename TEdgeType, typename TVertexIdType>
    class BinaryEdgeReader : public IEdgesReader<TEdgeType, TVertexIdType> {
    public:
      BinaryEdgeReader(const config_partitioner_t& config,
                       PartitionManager** partition_managers);

      virtual int readEdges(int max_thread);

      virtual ~BinaryEdgeReader();

    private:
      int readEdgesInRange(uint64_t start,
                           uint64_t end,
                           TEdgeType* edges);

      int calcProperNumThreads(uint64_t file_size,
                               uint32_t max_thread);

      static void* threadMain(void* arg);

    private:
      const static uint64_t min_chunk_ = (16 * 1024 * 1024);
    };
  }
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "binary-edge-reader.cc"
#endif
