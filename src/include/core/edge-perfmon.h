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
  class EdgePerfMonitor : public scalable_graphs::util::Runnable {
  public:
    EdgePerfMonitor(useconds_t tick = 1000000);
    ~EdgePerfMonitor();
    void stop();

    /*Selective Scheduling Tile Stat*/
    void resetTileStat(void);
    void updateTileStat(size_t active_tiles, size_t inactive_tiles);
    void printTileStat(void);

  private:
    virtual void run();

  private:
    useconds_t tick_;
    bool forced_to_stop_;

  public:
    uint64_t count_edges_read_ __attribute__((aligned(64)));
    uint64_t count_edges_processed_ __attribute__((aligned(64)));
    uint64_t count_bytes_read_ __attribute__((aligned(64)));
    uint64_t count_tiles_read_ __attribute__((aligned(64)));
    uint64_t count_tiles_processed_ __attribute__((aligned(64)));

    uint64_t count_active_tiles_ __attribute__((aligned(64)));
    uint64_t count_inactive_tiles_ __attribute__((aligned(64)));
    // __cacheline_aligned;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "edge-perfmon.cc"
#endif
