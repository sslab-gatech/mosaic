#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <pthread.h>
#include <ring_buffer.h>

#include <core/datatypes.h>

#include <util/runnable.h>

namespace scalable_graphs {
namespace graph_load {

  class AbstractPartitionManager : public util::Runnable {
  public:
    AbstractPartitionManager(const config_grc_t& config,
                             const partition_manager_arguments_t& arguments);

    virtual void initRead() = 0;

    virtual partition_edge_t* getPartition(const partition_t& partition) = 0;

    virtual size_t getSize(const partition_t& partition) = 0;

    virtual ~AbstractPartitionManager();

  public:
    partition_manager_arguments_t arguments_;

  protected:
    config_grc_t config_;
  };
}
}
