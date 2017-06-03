#pragma once

#include <pthread.h>

#include <core/datatypes.h>
#include <util/runnable.h>
#include <ring_buffer_scif.h>

#include "in-memory-partition-manager.h"

namespace util = scalable_graphs::util;

namespace scalable_graphs {
namespace graph_load {

  class RMATEdgeReceiver : public util::Runnable {
  public:
    RMATEdgeReceiver(const config_rmat_tiler_t& config,
                     InMemoryPartitionManager** partition_managers,
                     int node_id);

    virtual void run();

    void reduceVertexDegrees(vertex_degree_t* global_vertex_degrees);

    virtual ~RMATEdgeReceiver();

  public:
    pthread_barrier_t receiver_barrier_;

  private:
    static size_t global_received_edges;
    config_rmat_tiler_t config_;

    InMemoryPartitionManager** partition_managers_;
    ring_buffer_scif_t receive_rb_;
    ring_buffer_scif_t control_rb_;
    int node_id_;

    vertex_degree_t* vertex_degrees_;
  };
}
}
