#pragma once

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <util/arch.h>
#include <util/runnable.h>
#include <core/util.h>

namespace scalable_graphs {
namespace core {
  class VertexPerfMonitor : public scalable_graphs::util::Runnable {
  public:
    VertexPerfMonitor(useconds_t tick = 1000000);
    ~VertexPerfMonitor();
    void stop();

  private:
    virtual void run();

  private:
    useconds_t tick_;
    bool forced_to_stop_;

  public:
    uint64_t count_XXX_ __attribute__((aligned(64)));
    uint64_t count_tiles_fetched_ __attribute__((aligned(64)));
    uint64_t count_tile_partitions_sent_ __attribute__((aligned(64)));
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "vertex-perfmon.cc"
#endif
