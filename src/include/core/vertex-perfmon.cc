#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "vertex-perfmon.h"
#endif
#pragma once

namespace scalable_graphs {
namespace core {
  VertexPerfMonitor::VertexPerfMonitor(useconds_t tick)
      : tick_(tick), forced_to_stop_(false), count_tiles_fetched_(0),
        count_tile_partitions_sent_(0) {
    // do nothing
  }

  VertexPerfMonitor::~VertexPerfMonitor() { stop(); }

  void VertexPerfMonitor::stop() { forced_to_stop_ = true; }

  void VertexPerfMonitor::run() {
    uint64_t sec = 0;
    while (!forced_to_stop_) {
      ::usleep(tick_);
      sg_mon("Second %lu, tiles-fetched : %lu, tile-partitions-sent: %lu\n",
             sec, count_tiles_fetched_, count_tile_partitions_sent_);
      ++sec;
    }
  }
}
}
