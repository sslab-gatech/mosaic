#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <pthread.h>
#include <ring_buffer.h>

#include <core/datatypes.h>
#include <util/runnable.h>

#include "ipartition-store.h"
#include "abstract-partition-manager.h"

namespace scalable_graphs {
namespace graph_load {

  class PartitionManager : public AbstractPartitionManager {
  public:
    PartitionManager(const config_grc_t& config,
                     const partition_manager_arguments_t& arguments);

    void cleanupWrite();
    void initWrite();

    virtual void initRead();

    virtual void run();

    virtual partition_edge_t* getPartition(const partition_t& partition);

    virtual size_t getSize(const partition_t& partition);

    virtual ~PartitionManager();

  public:
    ring_buffer_t* edges_rb_;

  private:
    static size_t count_processed_edges_;

    IPartitionStore** partition_stores_;

    size_t count_partition_stores_;

    const static size_t edges_rb_size_ = 1ul * GB;
  };
}
}
