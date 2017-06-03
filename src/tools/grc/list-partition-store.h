#pragma once

#include <vector>
#include <stddef.h>
#include <core/datatypes.h>
#include "ipartition-store.h"

namespace scalable_graphs {
  namespace graph_load {

    class ListPartitionStore : public IPartitionStore {
    public:
      ListPartitionStore(const meta_partition_meta_t& local_partition_info,
                         const meta_partition_meta_t& global_partition_info,
                         const config_grc_t& config);

      virtual void cleanupWrite();

      virtual void initWrite();

      virtual void initRead();

      virtual void addEdge(const edge_t& edge);

      virtual partition_edge_t* getPartition(const partition_t& partition);

      virtual size_t getSize(const partition_t& partition);

      ~ListPartitionStore();

    private:
      std::vector<edge_t>*** partitions_;
    };
  }
}
