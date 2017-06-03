#pragma once

#include <util/runnable.h>
#include <util/util.h>
#include <core/datatypes.h>

#include <ring_buffer.h>

namespace scalable_graphs {
namespace util {
  namespace perf_event {

    class PerfEventCollector : public Runnable {
    public:
      PerfEventCollector(ring_buffer_t* new_event_rb, const config_t& config,
                         PerfEventMode mode, int id);

      ~PerfEventCollector();

      virtual void run();

    private:
      ring_buffer_t* new_event_rb_;

      const config_t config_;

      PerfEventMode mode_;

      int id_;
    };
  }
}
}
