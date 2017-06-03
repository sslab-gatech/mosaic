#pragma once

#include <pthread.h>
#include <core/datatypes.h>

#include "iedges-reader.h"
#include "partition-manager.h"

namespace scalable_graphs {
namespace graph_load {

  template <typename TEdgeType, typename TVertexIdType>
  class DelimEdgesReader : public IEdgesReader<TEdgeType, TVertexIdType> {
  public:
    DelimEdgesReader(const config_partitioner_t& config,
                     PartitionManager** partition_managers);

    virtual int readEdges(int max_thread);

    virtual ~DelimEdgesReader();

  private:
    int readEdgesInRange(const std::string& out_dir, int64_t start,
                         int64_t end);

    int calcProperNumThreads(uint64_t file_size, int max_thread);

    static void* threadMain(void* arg);

  private:
    char delim_;
    const static int64_t read_ahead_ = 8192;
    const static int64_t min_chunk_ = (16 * 1024 * 1024);
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "delim-edges-reader.cc"
#endif
