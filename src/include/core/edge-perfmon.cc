#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "edge-perfmon.h"
#endif
#pragma once

namespace scalable_graphs {
namespace core {
  EdgePerfMonitor::EdgePerfMonitor(useconds_t tick)
      : tick_(tick), forced_to_stop_(false), count_edges_read_(0),
        count_edges_processed_(0), count_bytes_read_(0), count_tiles_read_(0),
        count_tiles_processed_(0), count_active_tiles_(0),
        count_inactive_tiles_(0) {
    // do nothing
  }

  EdgePerfMonitor::~EdgePerfMonitor() { stop(); }

  void EdgePerfMonitor::resetTileStat() {
    count_active_tiles_ = 0;
    count_inactive_tiles_ = 0;
  }

  void EdgePerfMonitor::updateTileStat(size_t active_tiles,
                                       size_t inactive_tiles) {
    smp_faa(&count_active_tiles_, active_tiles);
    smp_faa(&count_inactive_tiles_, inactive_tiles);
  }

  void EdgePerfMonitor::printTileStat() {
    sg_mon("active-tiles %lu, inactive-tiles %lu\n", count_active_tiles_,
           count_inactive_tiles_);
  }

  void EdgePerfMonitor::stop() { forced_to_stop_ = true; }

  void EdgePerfMonitor::run() {
    uint64_t sec = 0;
    while (!forced_to_stop_) {
      ::usleep(tick_);
      sg_mon(
          "Second %lu, edges-read: %lu, edges-processed %lu bytes-read %lu\n",
          sec, count_edges_read_, count_edges_processed_, count_bytes_read_);
      sg_mon("Second %lu, tiles-read: %lu, tiles-processed %lu \n", sec,
             count_tiles_read_, count_tiles_processed_);
      ++sec;
    }
  }
}
}
