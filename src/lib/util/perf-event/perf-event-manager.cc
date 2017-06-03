#include <util/perf-event/perf-event-manager.h>

namespace scalable_graphs {
namespace util {
  namespace perf_event {

    PerfEventManager* PerfEventManager::instance = nullptr;

    PerfEventManager::PerfEventManager(const config_vertex_domain_t& config)
        : config_host_(config), config_client_(),
          mode_(PerfEventMode::PEM_Host) {
      int rc =
          ring_buffer_create(SIZE_EVENT_RB, L1D_CACHELINE_SIZE,
                             RING_BUFFER_BLOCKING, NULL, NULL, &new_event_rb_);
      if (rc) {
        sg_log("Fail to initialize ringbuffer for events: %d\n", rc);
        scalable_graphs::util::die(1);
      }
    }

    PerfEventManager::PerfEventManager(const config_edge_processor_t& config)
        : config_host_(), config_client_(config),
          mode_(PerfEventMode::PEM_Client) {
      int rc =
          ring_buffer_create(SIZE_EVENT_RB, L1D_CACHELINE_SIZE,
                             RING_BUFFER_BLOCKING, NULL, NULL, &new_event_rb_);
      if (rc) {
        sg_log("Fail to initialize ringbuffer for events: %d\n", rc);
        scalable_graphs::util::die(1);
      }
    }

    PerfEventManager::~PerfEventManager() {
      ring_buffer_destroy(new_event_rb_);
      for (auto t : threads_) {
        delete t;
      }
    }

    void PerfEventManager::start() {
      config_t config;
      int id;

      switch (mode_) {
      case PerfEventMode::PEM_Host:
        config = config_host_;
        id = 0;
        break;
      case PerfEventMode::PEM_Client:
        config = config_client_;
        id = config_client_.mic_index;
        break;
      default:
        break;
      }

      auto* t = new PerfEventCollector(new_event_rb_, config, mode_, id);
      t->start();
      t->setName("PECollector");
      threads_.push_back(t);
    }

    void PerfEventManager::stop() {
      // Push an element with a set shutdown flag to the collector to force it
      // to shutdown.
      // Just set the shutdown flag, everything else will be ignored by the
      // receiving collector.
      profiling_transport_t data;
      data.shutdown = true;

      size_t size_profiling_data = sizeof(profiling_transport_t);

      ring_buffer_req_t req_event;
      ring_buffer_put_req_init(&req_event, BLOCKING, size_profiling_data);
      ring_buffer_put(new_event_rb_, &req_event);
      sg_rb_check(&req_event);

      copy_to_ring_buffer(new_event_rb_, req_event.data, &data,
                          size_profiling_data);
      ring_buffer_elm_set_ready(new_event_rb_, req_event.data);

      for (auto& t : threads_) {
        t->join();
      }
    }

    ring_buffer_t* PerfEventManager::getRingBuffer() { return new_event_rb_; }

    PerfEventManager*
    PerfEventManager::getInstance(const config_vertex_domain_t& config) {
      if (PerfEventManager::instance == nullptr) {
        PerfEventManager::instance = new PerfEventManager(config);
      }
      return PerfEventManager::instance;
    }

    PerfEventManager*
    PerfEventManager::getInstance(const config_edge_processor_t& config) {
      if (PerfEventManager::instance == nullptr) {
        PerfEventManager::instance = new PerfEventManager(config);
      }
      return PerfEventManager::instance;
    }
  }
}
}
