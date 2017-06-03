#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <limits.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <getopt.h>
#include <malloc.h>

#include <ring_buffer.h>
#include <ring_buffer_i.h>

int unit_test()
{
	struct ring_buffer_t *rb;
	struct ring_buffer_req_t req_put;
	struct ring_buffer_req_t req_get;
	char data[1024];
	int i, rc;

	rc = ring_buffer_create(4 * 1024, 64, RING_BUFFER_NON_BLOCKING, NULL, NULL, &rb);
	rb_test(rc == 0, "ring_buffer_create");


	ring_buffer_get_req_init(&req_get, BLOCKING);
	rc = ring_buffer_get(rb, &req_get);
	rb_test(rc == -EAGAIN, "ring_buffer_get");

	ring_buffer_put_req_init(&req_put, BLOCKING, sizeof(data));
	rc = ring_buffer_put(rb, &req_put);
	rb_test(rc == 0, "ring_buffer_put");

	ring_buffer_get_req_init(&req_get, BLOCKING);
	rc = ring_buffer_get(rb, &req_get);
	rb_test(rc == -EAGAIN, "ring_buffer_get");

	ring_buffer_elm_set_ready(rb, req_put.data);

	ring_buffer_get_req_init(&req_get, BLOCKING);
	rc = ring_buffer_get(rb, &req_get);
	rb_test(rc == 0, "ring_buffer_get: rc check");
	rb_test(req_get.size == sizeof(data), "ring_buffer_get: size check");
	ring_buffer_assert_fingerprint(req_get.data);
	ring_buffer_elm_set_done(rb, req_get.data);

	ring_buffer_get_req_init(&req_get, BLOCKING);
	rc = ring_buffer_get(rb, &req_get);
	rb_test(rc == -EAGAIN, "ring_buffer_get");

	for (i = 0; i < 3; ++i) {
		ring_buffer_put_req_init(&req_put, BLOCKING, sizeof(data));
		rc = ring_buffer_put(rb, &req_put);
		rb_test(rc == 0,
			"ring_buffer_put; "
			"in no double mmap, it could be different.");
		rb_dump(rb);
		rb_req_dump(&req_put);
		ring_buffer_elm_set_ready(rb, req_put.data);
	}
	ring_buffer_put_req_init(&req_put, BLOCKING, sizeof(data));
	rc = ring_buffer_put(rb, &req_put);
	rb_test(rc == -EAGAIN, "ring_buffer_put");

	ring_buffer_destroy(rb);
	return 0;
}

int main(int argc, char *argv[])
{
	return unit_test();
}
