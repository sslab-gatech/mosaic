#pragma once

#include <pthread.h>
#include <core/datatypes.h>
#include <ring_buffer_scif.h>

namespace scalable_graphs {
namespace graph_load {

  class RemoteRMATGenerator {
  public:
    RemoteRMATGenerator(const config_remote_rmat_generator_t& config);

    void init();

    int generateEdges();

    ~RemoteRMATGenerator();

  protected:
    double a_;
    double b_;
    double c_;

    uint64_t seed_;

    uint64_t count_vertices_;

    config_remote_rmat_generator_t config_;
    config_grc_t config_grc_;

    ring_buffer_scif_t rb_;
    ring_buffer_scif_t control_rb_;

  private:
    static void* threadMain(void* args);

    int generateEdgesInRange(int tid, int64_t start, int64_t end);

    void sendEdgeBlock(const remote_partition_edge_t* edge_block);

  private:
    bool* active_partition_managers_;

    pthread_barrier_t barrier_;

    const static size_t size_rb_ = 1ul * GB;
    const static size_t size_control_rb_ = 10ul * MB;
  };
}
}
