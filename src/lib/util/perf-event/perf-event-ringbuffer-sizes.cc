#include <util/perf-event/perf-event-ringbuffer-sizes.h>

#include <unistd.h>

#include <util/util.h>

namespace scalable_graphs {
namespace util {
  namespace perf_event {

    PerfEventRingbufferSizes::PerfEventRingbufferSizes(
        ring_buffer_t* new_event_rb, ring_buffer_t* index_rb,
        ring_buffer_type* tiles_rb,
        ring_buffer_type* response_rb,
        int64_t global_id, pthread_barrier_t* memory_init_barrier)
        : shutdown_(false), new_event_rb_(new_event_rb), index_rb_(index_rb),
          tiles_rb_(tiles_rb), response_rb_(response_rb),
          memory_init_barrier_(memory_init_barrier) {
      data_.global_id = global_id;
      data_.component = ComponentType::CT_RingBufferSizes;
      data_.type = ProfilingType::PT_RingbufferSizes;
      data_.pid = 0;
    }

    PerfEventRingbufferSizes::~PerfEventRingbufferSizes() {}

    void PerfEventRingbufferSizes::shutdown() { shutdown_ = true; }

    void PerfEventRingbufferSizes::run() {
      // First wait for all ringbuffers to be initialized before starting the
      // collection.
      pthread_barrier_wait(memory_init_barrier_);

      while (!shutdown_) {
        // Sleep for given time, then push ringbuffer sizes to the profiler.
        usleep(sample_timeout_us);

        data_.ringbuffer_sizes.size_index_rb =
            ring_buffer_free_space(index_rb_);
        data_.ringbuffer_sizes.size_tiles_rb =
#if defined(MOSAIC_HOST_ONLY)
            ring_buffer_free_space(*tiles_rb_);
#else
            ring_buffer_scif_free_space(tiles_rb_);
#endif

        data_.ringbuffer_sizes.size_response_rb =
#if defined(MOSAIC_HOST_ONLY)
            ring_buffer_free_space(*response_rb_);
#else
            ring_buffer_scif_free_space(response_rb_);
#endif

        data_.ringbuffer_sizes.time = get_time_nsec();

        size_t size_profiling_data = sizeof(profiling_transport_t);

        ring_buffer_req_t req_event;
        ring_buffer_put_req_init(&req_event, BLOCKING, size_profiling_data);
        ring_buffer_put(new_event_rb_, &req_event);
        sg_rb_check(&req_event);

        profiling_transport_t* transport =
            (profiling_transport_t*)req_event.data;
        transport->shutdown = false;
        transport->data = data_;

        ring_buffer_elm_set_ready(new_event_rb_, req_event.data);
      }
    }
  }
}
}
