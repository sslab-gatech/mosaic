#pragma once

#include <vector>

#include <util/runnable.h>
#include <util/util.h>
#include <core/datatypes.h>
#include <util/perf-event/perf-event-collector.h>

#include <ring_buffer.h>

namespace scalable_graphs {
namespace util {
  namespace perf_event {

    class PerfEventManager {
    public:
      PerfEventManager(const config_vertex_domain_t& config);

      PerfEventManager(const config_edge_processor_t& config);

      ~PerfEventManager();

      void start();

      void stop();

      ring_buffer_t* getRingBuffer();

    public:
      static PerfEventManager*
      getInstance(const config_vertex_domain_t& config);

      static PerfEventManager*
      getInstance(const config_edge_processor_t& config);

    private:
      // Instance for the singleton, is initialized to nullptr.
      static PerfEventManager* instance;

      ring_buffer_t* new_event_rb_;

      const config_vertex_domain_t config_host_;
      const config_edge_processor_t config_client_;

      PerfEventMode mode_;

      std::vector<PerfEventCollector*> threads_;

      static const int SIZE_EVENT_RB = 1 * GB;
    };
  }
}
}
