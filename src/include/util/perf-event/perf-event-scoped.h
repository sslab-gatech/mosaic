#pragma once

#include <core/datatypes.h>

#include <ring_buffer.h>

namespace scalable_graphs {
namespace util {
  namespace perf_event {

    class PerfEventScoped {
    public:
      PerfEventScoped(ring_buffer_t* new_event_rb, const std::string& name,
                      bool active);

      PerfEventScoped(ring_buffer_t* new_event_rb, const std::string& name,
                      ComponentType component, int64_t global_id, bool active);

      PerfEventScoped(ring_buffer_t* new_event_rb, const std::string& name,
                      ComponentType component, int64_t local_id,
                      int64_t global_id, int tile_id, bool active);

      PerfEventScoped(ring_buffer_t* new_event_rb, const std::string& name,
                      ComponentType component, int64_t local_id,
                      int64_t global_id, uint64_t metadata, int tile_id,
                      bool active);

      ~PerfEventScoped();

    private:
      bool active_;

      ring_buffer_t* new_event_rb_;

      profiling_data_t data_;

      std::string name_;
    };
  }
}
}
