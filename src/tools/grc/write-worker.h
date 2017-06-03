#pragma once

#include <stddef.h>
#include <ring_buffer.h>
#include <core/datatypes.h>
#include <util/runnable.h>

namespace scalable_graphs {
namespace graph_load {

  class WriteWorker : public util::Runnable {
  public:
    WriteWorker(ring_buffer_t* task_rb);

    virtual void run();

    ~WriteWorker();

  public:
    ring_buffer_t* task_rb_;
  };
}
}
