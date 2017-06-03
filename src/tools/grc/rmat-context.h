#pragma once

#include <core/datatypes.h>

#include "iedges-reader.h"
#include "partition-manager.h"

namespace scalable_graphs {
namespace graph_load {

  class RMATContext {
  public:
    RMATContext(double a, double b, double c, uint64_t count_vertices,
                uint64_t seed);

    ~RMATContext();

    edge_t rMat(uint64_t n, uint64_t rand_start, uint64_t rand_stride);
    edge_t rMatIterative(uint64_t n, uint64_t rand_start, uint64_t rand_stride);

    edge_t getEdge(uint64_t index);

  private:
    double a_;
    double ab_;
    double abc_;

    uint64_t count_vertices_;
    uint64_t hash_;
  };
}
}
