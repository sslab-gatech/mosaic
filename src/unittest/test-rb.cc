#include <assert.h>
#include <stdlib.h>

#include <util/ring_buffer.h>
#include <core/util.h>

#include "test.h"

static size_t calc_rb_size() { return 1024 * 1024 * 1024; }

static int init_ring_buffer(ring_buffer_t** rb) {
  int size = calc_rb_size();
  int rc, i;

  int blocking = RING_BUFFER_REQ_BLOCKING;

  rc = ring_buffer_create(size,               // XXX
                          L1D_CACHELINE_SIZE, // XXX: DISK_BLOCK_SIZE
                          blocking, rb);
  if (rc) {
    sg_dbg("Init ringbuffer failed with %d\n", rc);
    goto out;
  }
  sg_dbg("rb-size_init %lu\n", (*rb)->size);

out:
  return rc;
}

int main(int argc, char** argv) {

  int count_elements = 100;
  ring_buffer_t* rb = new ring_buffer_t;
  init_ring_buffer(&rb);

  size_t size = sizeof(int) * count_elements;

  ring_buffer_req_t* req = new ring_buffer_req_t;
  do {
    ring_buffer_put_req_init(req, BLOCKING, size);
    ring_buffer_put(rb, req);
  } while (req->rc == -EAGAIN);

  sg_rb_check(req);

  int* a1 = (int*)req->data;
  for (int i = 0; i < count_elements; ++i) {
    a1[i] = i;
  }
  ring_buffer_elm_set_ready(rb, a1);

  do {
    ring_buffer_put_req_init(req, BLOCKING, size);
    ring_buffer_put(rb, req);
  } while (req->rc == -EAGAIN);

  sg_rb_check(req);

  int* a2 = (int*)req->data;
  for (int i = 0; i < count_elements; ++i) {
    a2[i] = i + 1;
  }
  ring_buffer_elm_set_ready(rb, a2);

  do {
    ring_buffer_put_req_init(req, BLOCKING, size);
    ring_buffer_put(rb, req);
  } while (req->rc == -EAGAIN);

  sg_rb_check(req);

  int* a3 = (int*)req->data;
  for (int i = 0; i < count_elements; ++i) {
    a3[i] = i + 2;
  }
  ring_buffer_elm_set_ready(rb, a3);

  // assert content
  for (int i = 0; i < count_elements; ++i) {
    sg_test(a1[i] == i, "a1");
    sg_test(a2[i] == i + 1, "a2");
    sg_test(a3[i] == i + 2, "a3");
  }
  ring_buffer_elm_set_done(rb, a1);
  ring_buffer_elm_set_done(rb, a2);
  ring_buffer_elm_set_done(rb, a3);
  return 0;
}
