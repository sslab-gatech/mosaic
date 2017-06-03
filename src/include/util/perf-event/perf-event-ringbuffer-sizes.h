#pragma once

#include <pthread.h>

#include <util/runnable.h>
#include <core/datatypes.h>

#include <ring_buffer.h>

#if !defined(MOSAIC_HOST_ONLY)
#include <ring_buffer_scif.h>
#endif

namespace scalable_graphs {
namespace util {
  namespace perf_event {

    class PerfEventRingbufferSizes : public Runnable {
    public:
      PerfEventRingbufferSizes(ring_buffer_t* new_event_rb,
                               ring_buffer_t* index_rb,
                               ring_buffer_type* tiles_rb,
                               ring_buffer_type* response_rb,
                               int64_t global_id,
                               pthread_barrier_t* memory_init_barrier);

      ~PerfEventRingbufferSizes();

      virtual void run();

      void shutdown();

    private:
      const static uint64_t sample_timeout_us = 1000;

      bool shutdown_;

      ring_buffer_t* new_event_rb_;
      ring_buffer_t* index_rb_;
      ring_buffer_type* tiles_rb_;
      ring_buffer_type* response_rb_;
      profiling_data_t data_;

      pthread_barrier_t* memory_init_barrier_;
    };
  }
}
}
