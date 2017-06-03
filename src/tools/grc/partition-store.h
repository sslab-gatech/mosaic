#pragma once

#include <stddef.h>
#include <ring_buffer.h>
#include <core/datatypes.h>
#include "ipartition-store.h"

namespace scalable_graphs {
namespace graph_load {

  class PartitionStore : public IPartitionStore {
  public:
    PartitionStore(const meta_partition_meta_t& local_partition_info,
                   const meta_partition_meta_t& global_partition_info,
                   ring_buffer_t* write_request_rb, const config_grc_t& config);

    virtual void cleanupWrite();

    virtual void initWrite();

    virtual void initRead();

    virtual void addEdge(const edge_t& edge);

    virtual partition_edge_t* getPartition(const partition_t& partition);

    virtual size_t getSize(const partition_t& partition);

    ~PartitionStore();

  private:
    meta_partition_file_info_t meta_partition_file_;

    int file_;
    ring_buffer_t* write_request_rb_;
    partition_edge_compact_t* partitions_;
  };
}
}
