#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <pthread.h>
#include <ring_buffer.h>

#include <core/datatypes.h>
#include <util/runnable.h>

#include "in-memory-partition-store.h"
#include "abstract-partition-manager.h"

namespace scalable_graphs {
namespace graph_load {

  class InMemoryPartitionManager : public AbstractPartitionManager {
  public:
    InMemoryPartitionManager(const config_rmat_tiler_t& config,
                             const partition_manager_arguments_t& arguments);
    virtual void initRead();

    virtual void run();

    void initCounting();
    void initTiling();

    virtual partition_edge_t* getPartition(const partition_t& partition);

    virtual size_t getSize(const partition_t& partition);

    void addEdge(const edge_t& edge);
    void addEdgeCount(const edge_t& edge);

    void cleanUpCouting();
    void cleanUpTiling();

    virtual ~InMemoryPartitionManager();

  public:
    ring_buffer_t* edges_rb_;

    size_t count_edges_;

    bool active_this_round_;

  private:
    config_rmat_tiler_t config_tiler_;
    const static size_t edges_rb_size_ = 1ul * GB;

    InMemoryPartitionStore** partition_stores_;

    size_t count_partition_stores_;
  };
}
}
