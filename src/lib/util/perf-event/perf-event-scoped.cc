#include <util/perf-event/perf-event-scoped.h>

#include <pthread.h>

#include <util/util.h>

namespace scalable_graphs {
namespace util {
  namespace perf_event {

    PerfEventScoped::PerfEventScoped(ring_buffer_t* new_event_rb,
                                     const std::string& name, bool active)
        : active_(active), new_event_rb_(new_event_rb) {
      if (active) {
        data_.type = ProfilingType::PT_Duration;
        data_.pid = 0;
        data_.duration.time_start = get_time_nsec();
        data_.component = ComponentType::CT_None;
        data_.local_id = 0;
        data_.global_id = (int64_t)pthread_self();
        data_.duration.metadata = 0;

        name_ = name;
      }
    }

    PerfEventScoped::PerfEventScoped(ring_buffer_t* new_event_rb,
                                     const std::string& name,
                                     ComponentType component, int64_t global_id,
                                     bool active)
        : active_(active), new_event_rb_(new_event_rb) {
      if (active) {
        data_.type = ProfilingType::PT_Duration;
        data_.pid = 0;
        data_.duration.time_start = get_time_nsec();
        data_.component = component;
        data_.local_id = 0;
        data_.global_id = global_id;
        data_.duration.metadata = 0;

        name_ = name;
      }
    }

    PerfEventScoped::PerfEventScoped(ring_buffer_t* new_event_rb,
                                     const std::string& name,
                                     ComponentType component, int64_t local_id,
                                     int64_t global_id, int tile_id,
                                     bool active)
        : active_(active), new_event_rb_(new_event_rb) {
      if (active) {
        data_.type = ProfilingType::PT_Duration;
        data_.pid = 0;
        data_.duration.time_start = get_time_nsec();
        data_.component = component;
        data_.local_id = local_id;
        data_.global_id = global_id;
        data_.duration.metadata = 0;

        name_ = name + "_" + std::to_string(tile_id);
      }
    }

    PerfEventScoped::PerfEventScoped(ring_buffer_t* new_event_rb,
                                     const std::string& name,
                                     ComponentType component, int64_t local_id,
                                     int64_t global_id, uint64_t metadata,
                                     int tile_id, bool active)
        : active_(active), new_event_rb_(new_event_rb) {
      if (active) {
        data_.type = ProfilingType::PT_Duration;
        data_.pid = 0;
        data_.duration.time_start = get_time_nsec();
        data_.component = component;
        data_.local_id = local_id;
        data_.global_id = global_id;
        data_.duration.metadata = metadata;

        name_ = name + "_" + std::to_string(tile_id);
      }
    }

    PerfEventScoped::~PerfEventScoped() {
      if (active_) {
        data_.duration.time_end = get_time_nsec();

        // Size of name_ + 1 to account for \0.
        size_t size_name = name_.size() + 1;
        size_t size_profiling_data =
            sizeof(profiling_transport_t) + sizeof(char) * size_name;

        ring_buffer_req_t req_event;
        ring_buffer_put_req_init(&req_event, BLOCKING, size_profiling_data);
        ring_buffer_put(new_event_rb_, &req_event);
        sg_rb_check(&req_event);

        profiling_transport_t* transport =
            (profiling_transport_t*)req_event.data;
        transport->shutdown = false;
        transport->data = data_;

        // Copy the name.
        copy_to_ring_buffer(new_event_rb_, transport->data.name, name_.c_str(),
                            size_name);

        ring_buffer_elm_set_ready(new_event_rb_, req_event.data);
      }
    }
  }
}
}
