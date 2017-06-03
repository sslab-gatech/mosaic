#include <util/perf-event/perf-event-collector.h>

namespace scalable_graphs {
namespace util {
  namespace perf_event {

    PerfEventCollector::PerfEventCollector(ring_buffer_t* new_event_rb,
                                           const config_t& config,
                                           PerfEventMode mode, int id)
        : new_event_rb_(new_event_rb), config_(config), mode_(mode), id_(id) {}

    PerfEventCollector::~PerfEventCollector() {}

    void PerfEventCollector::run() {
      FILE* file = initFileProfilingData(config_, mode_, id_);

      while (true) {
        ring_buffer_req_t req_event;
        ring_buffer_get_req_init(&req_event, BLOCKING);
        ring_buffer_get(new_event_rb_, &req_event);
        sg_rb_check(&req_event);

        profiling_transport_t* transport =
            (profiling_transport_t*)req_event.data;

        // Break from the loop if shutting down.
        if (transport->shutdown) {
          break;
        }

        if (transport->data.type == ProfilingType::PT_Duration) {
          writeProfilingDuration(transport->data, file);
        } else if (transport->data.type == ProfilingType::PT_RingbufferSizes) {
          writeRingBufferSizes(transport->data, file);
        }

        ring_buffer_elm_set_done(new_event_rb_, req_event.data);
      }

      sg_dbg("Shutting down EventCollector %d\n", id_);
      fclose(file);
    }
  }
}
}
