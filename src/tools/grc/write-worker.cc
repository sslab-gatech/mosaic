#include "write-worker.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <util/util.h>
#include <util/arch.h>
#include <core/util.h>

namespace scalable_graphs {
namespace graph_load {

  WriteWorker::WriteWorker(ring_buffer_t* task_rb) : task_rb_(task_rb) {}

  void WriteWorker::run() {
    ring_buffer_req_t get_req;
    while (true) {
      ring_buffer_get_req_init(&get_req, BLOCKING);
      ring_buffer_get(task_rb_, &get_req);
      sg_rb_check(&get_req);

      // first check shutdown-indicator which is located just after the
      // edge-write-request-struct
      uint64_t shutdown_indicator = ((
          uint64_t*)((uint8_t*)get_req.data + sizeof(edge_write_request_t)))[0];
      if (shutdown_indicator != 0) {
        break;
      }

      // only one request in the data but this still needs to be cast from the
      // one-element-array:
      edge_write_request_t write_request =
          ((edge_write_request_t*)get_req.data)[0];

      util::writeFileOffset(write_request.fd, &write_request.edge,
                            sizeof(local_edge_t), write_request.offset);

      // clean up edge, done
      ring_buffer_elm_set_done(task_rb_, get_req.data);
    }

    sg_log2("Shutdown WriteWorker\n");
  }

  WriteWorker::~WriteWorker() {}
}
}
