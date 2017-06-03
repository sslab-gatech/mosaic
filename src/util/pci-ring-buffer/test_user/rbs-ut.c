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
#include <ring_buffer_scif.h>
#include <ring_buffer_i.h>

char data_buf[1024 * 1024];

struct cmd_opt {
	int master;
	int master_node;
	int local_port;
	int remote_port;
};

static int parse_option(int argc,
                        char *argv[],
                        struct cmd_opt *opt)
{
	static struct option options[] = {
		{"master",      required_argument, 0, 'm'},
		{"master_node", required_argument, 0, 'n'},
		{"lport",       required_argument, 0, 'l'},
		{"rport",       required_argument, 0, 'r'},

		{0,             0,                 0, 0},
	};

	int arg_cnt;
	int c;
	int idx;

	memset(opt, 0, sizeof(struct cmd_opt));
	for (arg_cnt = 0; 1; ++arg_cnt) {
		c = getopt_long(argc, argv,
				"n:n:l:r:", options, &idx);
		if (c == -1)
			break;
		switch(c) {
		case 'm':
			opt->master = atoi(optarg);
			break;
		case 'n':
			opt->master_node = atoi(optarg);
			break;
		case 'l':
			opt->local_port = atoi(optarg);
			break;
		case 'r':
			opt->remote_port = atoi(optarg);
			break;
		default:
			return -EINVAL;
		}
	}
	return arg_cnt;
}

static void usage(const char *bname)
{
	fprintf(stderr, "Usage: %s\n", bname);
	fprintf(stderr, " --master      = master / shadow [1, 0]\n");
	fprintf(stderr, " --master_node = host / mic [host -> 0, mic -> 1]\n");
	fprintf(stderr, " --lport       = local port (default: 8080)\n");
	fprintf(stderr, " --rport       = remote port (both should be same)\n");
}

int unit_test_master(int local_port)
{
	struct ring_buffer_scif_t rbs;
	struct ring_buffer_req_t req;
	int i, rc;

	/* create */
	rc = ring_buffer_scif_create_master(1024 * 1024 * 4,
					    64,
					    RING_BUFFER_NON_BLOCKING,
					    RING_BUFFER_SCIF_PRODUCER,
					    NULL, NULL,
					    &rbs);
	rb_test(rc == 0, "ring_buffer_scif_create_master");

	/* put four elements first */
	for (i = 0; i < 4; ++i) {
		ring_buffer_put_req_init(&req, BLOCKING, 128 * (i + 1));
		rc = ring_buffer_scif_put(&rbs, &req);
		rb_test(rc == 0, "ring_buffer_put");

		memset(data_buf, '0' + i, sizeof(data_buf));
		rc = copy_to_ring_buffer_scif(&rbs, req.data, data_buf, 128 * (i + 1));
		rb_test(rc == 0, "copy_to_ring_buffer_scif");

		ring_buffer_scif_elm_set_ready(&rbs, req.data);
		printf("  ==> put data: %c\n", data_buf[0]);
	}

	/* wait for client connection */
	printf("wait for shadow...[%d, %d]\n", local_port, 1);
	rc = ring_buffer_scif_wait_for_shadow(&rbs, local_port, 1);
	rb_test(rc == 0, "ring_buffer_scif_wait_for_shadow");

	/* take a nap while the shadow take the enqueued items */
	sleep(5);

	/* destroy */
	ring_buffer_scif_destroy_master(&rbs);
	rb_test(1, "ring_buffer_scif_destroy_master");
	return 0;
}

int unit_test_shadow(int local_port, int master_node, int remote_port)
{
	struct ring_buffer_scif_t rbs;
	struct ring_buffer_req_t req;
	int i, rc;

	/* create */
	rc = ring_buffer_scif_create_shadow(local_port,
					    master_node, remote_port,
					    NULL, NULL,
					    &rbs);
	rb_test(rc == 0, "ring_buffer_scif_create_shadow");

	/* get four elements first */
	for (i = 0; i < 4; ++i) {
		ring_buffer_get_req_init(&req, BLOCKING);
		rc = ring_buffer_scif_get(&rbs, &req);
		rb_test(rc == 0, "ring_buffer_scif_get");

		rc = copy_from_ring_buffer_scif(&rbs, data_buf, req.data, req.size);
		rb_test(rc == 0, "copy_from_ring_buffer_scif");

		ring_buffer_scif_elm_set_done(&rbs, req.data);
		printf("  ==> get data: %c\n", data_buf[0]);
		rb_test(data_buf[0] == (char)('0' + i),
			"check ring buffer data");
	}

	/* destory */
	ring_buffer_scif_destroy_shadow(&rbs);
	rb_test(1, "ring_buffer_scif_destroy_shadow");
	return 0;
}

int main(int argc, char *argv[])
{
	struct cmd_opt opt = {0, 0, 0, 0};

	int err = 0;

	if (parse_option(argc, argv, &opt) < 4) {
		usage(argv[0]);
		err = 1;
		goto main_out;
	}

	if (opt.master)
		unit_test_master(opt.local_port);
	else
		unit_test_shadow(opt.local_port,
				 opt.master_node, opt.remote_port);
	rb_test(1, "====== completed ======\n");
main_out:
	return err;
}
