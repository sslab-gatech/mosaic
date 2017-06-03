#pragma once

#include <stddef.h>
#include <ring_buffer.h>
#include <core/datatypes.h>

namespace scalable_graphs {
namespace graph_load {

  class InMemoryPartitionStore {
  public:
    InMemoryPartitionStore(const meta_partition_meta_t& local_partition_info,
                           const meta_partition_meta_t& global_partition_info,
                           const config_rmat_tiler_t& config);

    void initCounting();
    void initTiling();

    void cleanUpCounting();
    void cleanUpTiling();

    void addEdge(const edge_t& edge);
    void addEdgeCount(const edge_t& edge);

    partition_edge_t* getPartition(const partition_t& partition);
    size_t getSize(const partition_t& partition);

    ~InMemoryPartitionStore();

  public:
    size_t count_edges_;

  private:
    const config_rmat_tiler_t& config_;

    meta_partition_meta_t local_partition_info_;
    meta_partition_meta_t global_partition_info_;

    meta_partition_file_info_t meta_partition_info_;

    partition_edge_compact_t* partitions_;

    local_edge_t* edges_;

    size_t* partition_offsets_;
  };
}
}
