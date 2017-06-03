#pragma once

#include <stddef.h>
#include <core/datatypes.h>

namespace scalable_graphs {
  namespace graph_load {

    class IPartitionStore {
    public:
      IPartitionStore(const meta_partition_meta_t& local_partition_info,
                      const meta_partition_meta_t& global_partition_info,
                      const config_grc_t& config);

      virtual void cleanupWrite() = 0;

      virtual void initWrite() = 0;

      virtual void initRead() = 0;

      virtual void addEdge(const edge_t& edge) = 0;

      virtual partition_edge_t* getPartition(const partition_t& partition) = 0;

      virtual size_t getSize(const partition_t& partition) = 0;

      virtual ~IPartitionStore();

    protected:
      meta_partition_meta_t local_partition_info_;
      meta_partition_meta_t global_partition_info_;
      config_grc_t config_;
    };
  }
}
