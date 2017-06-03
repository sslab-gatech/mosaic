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
#include <signal.h>
#include <pthread.h>

#include <ring_buffer.h>
#include <ring_buffer_scif.h>
#include <ring_buffer_i.h>

/* #define NO_COPY */

struct cmd_opt {
	/* command line options */
	int master;
	int master_node;
	int local_port;
	int remote_port;
	int blocking;
	int time;
	int nthreads;
	int mic_nthreads;
	int elm_cnt;
	int elm_size;
	char elm_size_str[256];

	/* benchmark control */
	volatile int      go;
	volatile uint64_t count;

	/* ring buffers */
	struct ring_buffer_scif_t rbs;
};

struct cmd_opt _opt;

static int parse_option(int argc,
                        char *argv[],
                        struct cmd_opt *opt)
{
	static struct option options[] = {
		{"time",        required_argument, 0, 't'},
		{"nthreads",    required_argument, 0, 'n'},
		{"mic_nthreads",required_argument, 0, 'N'},
		{"elm_cnt",     required_argument, 0, 'c'},
		{"elm_size",    required_argument, 0, 's'},
		{"master",      required_argument, 0, 'm'},
		{"master_node", required_argument, 0, 'a'},
		{"lport",       required_argument, 0, 'l'},
		{"rport",       required_argument, 0, 'r'},
		{"blocking",    required_argument, 0, 'b'},

		{0,             0,                 0, 0},
	};

	int arg_cnt, c, idx, unit, len;

	memset(opt, 0, sizeof(struct cmd_opt));
	for (arg_cnt = 0; 1; ++arg_cnt) {
		c = getopt_long(argc, argv,
				"t:n:N:c:s:m:a:l:r:b:", options, &idx);
		if (c == -1)
			break;
		switch(c) {
		case 't':
			opt->time = atoi(optarg);
			break;
		case 'n':
			opt->nthreads = atoi(optarg);
			break;
		case 'N':
			opt->mic_nthreads = atoi(optarg);
			break;
		case 'c':
			opt->elm_cnt = atoi(optarg);
			break;
		case 's':
			strcpy(opt->elm_size_str, optarg);
			len = strlen(optarg);
			unit = 1;
			if (optarg[len - 1] == 'K') {
				unit = 1024;
				optarg[len - 1] = '\0';
			}
			else if (optarg[len - 1] == 'M') {
				unit = 1024 * 1024;
				optarg[len - 1] = '\0';
			}
			else if (optarg[len - 1] == 'G') {
				unit = 1024 * 1024 * 1024;
				optarg[len - 1] = '\0';
			}
			opt->elm_size = atoi(optarg) * unit;
			break;
		case 'm':
			opt->master = atoi(optarg);
			break;
		case 'a':
			opt->master_node = atoi(optarg);
			break;
		case 'l':
			opt->local_port = atoi(optarg);
			break;
		case 'r':
			opt->remote_port = atoi(optarg);
			break;
		case 'b':
			opt->blocking = atoi(optarg);
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
	fprintf(stderr, " --time     = benchmark time in seconds\n");
	fprintf(stderr, " --nthreads = number of threads\n");
	fprintf(stderr, " --mic_nthreads = number of threads running on mic\n");
	fprintf(stderr, " --elm_cnt  = initial number of elements\n");
	fprintf(stderr, " --elm_size = element size in bytes\n");
	fprintf(stderr, " --master      = master / shadow [1, 0]\n");
	fprintf(stderr, " --master_node = host / mic [host -> 0, mic -> 1]\n");
	fprintf(stderr, " --lport       = local port (default: 8080)\n");
	fprintf(stderr, " --rport       = remote port (both should be same)\n");
	fprintf(stderr, " --blocking    = blocking or non-blocking ring buffer\n");
}

static void sighandler(int x)
{
	_opt.go = 2;
	smp_mb();
}

static size_t calc_rb_size(struct cmd_opt *opt)
{
	return 1024*1024*1024;
}

static void do_random_work(unsigned int *seed)
{
	volatile unsigned int i;
	unsigned int rnum = rand32_range(seed, 1, 64);
	for(i = 0; i < rnum; ++i) ;
}

static void *put_worker(void *x)
{
	struct cmd_opt *opt = &_opt;
        unsigned int id = (long)x;
	unsigned int seed = id;
	uint64_t count = 0;
	struct ring_buffer_req_t req;
	char *data;
	int rc;

	data = calloc(1, opt->elm_size);
        if (id == 0) {
                if (signal(SIGALRM, sighandler) == SIG_ERR) {
			fprintf(stderr, "[%s:%d] signal failed\n",
				__func__, __LINE__);
			exit(1);
		}
                alarm(opt->time);
                opt->go = 1;
        } else {
                while (opt->go == 0) ;
        }

        for(; opt->go == 1; ++count) {
		do {
			ring_buffer_put_req_init(&req,
						 BLOCKING, opt->elm_size);
			rc = ring_buffer_scif_put(&opt->rbs, &req);
			if (rc && rc != -EAGAIN )
				exit(-1);
		} while(rc == -EAGAIN && opt->go == 1);
		if (rc)
			break;
#ifndef NO_COPY
		copy_to_ring_buffer_scif(&opt->rbs, req.data, data, opt->elm_size);
#endif
		ring_buffer_scif_elm_set_ready(&opt->rbs, req.data);
		do_random_work(&seed);
        }

	sleep(1);
	smp_faa(&opt->count, count);
        return NULL;
}


static void *get_worker(void *x)
{
	struct cmd_opt *opt = &_opt;
        unsigned int id = (long)x;
	unsigned int seed = id;
	uint64_t count = 0;
	struct ring_buffer_req_t req;
	char *data;
	int rc;

	data = calloc(1, opt->elm_size);
        if (id == 0) {
                if (signal(SIGALRM, sighandler) == SIG_ERR) {
			fprintf(stderr, "[%s:%d] signal failed\n",
				__func__, __LINE__);
			exit(1);
		}
                alarm(opt->time);
                opt->go = 1;
        } else {
                while (opt->go == 0) ;
        }

        for(; opt->go == 1; ++count) {
		do {
			ring_buffer_get_req_init(&req,
						 BLOCKING);
			rc = ring_buffer_scif_get(&opt->rbs, &req);
			if (rc && rc != -EAGAIN )
				exit(-1);
		} while(rc == -EAGAIN && opt->go == 1);
		if (rc)
			break;
#ifndef NO_COPY
		copy_from_ring_buffer_scif(&opt->rbs, data, req.data, req.size);
#endif
		ring_buffer_scif_elm_set_done(&opt->rbs, req.data);
		do_random_work(&seed);
        }

	sleep(1);
	smp_faa(&opt->count, count);
        return NULL;
}


int test_master(struct cmd_opt *opt)
{
	int size = calc_rb_size(opt);
	struct ring_buffer_req_t req;
	pthread_t *th = NULL;
	int i, rc;

	/* create */
	rc = ring_buffer_scif_create_master(size,
					    64,
					    opt->blocking,
					    RING_BUFFER_SCIF_PRODUCER,
					    NULL, NULL,
					    &opt->rbs);
	if (rc)
		goto out;

	/* put initial elements */
	for (i = 0; i < opt->elm_cnt; ++i) {
		ring_buffer_put_req_init(&req, BLOCKING, opt->elm_size);
		rc = ring_buffer_scif_put(&opt->rbs, &req);
		if (rc == -EAGAIN)
			break;
		if (rc) {
			fprintf(stderr,
				"[%d] [init] fail to put: %d rb_size: %d elm_size = %d\n",
				i, rc, (int)opt->rbs.rb->size, opt->elm_size);
			goto out;
		}
		ring_buffer_scif_elm_set_ready(&opt->rbs, req.data);
	}

	/* create threads */
	th = (pthread_t *)malloc(sizeof(*th) * opt->nthreads);
	for (i = 1; i < opt->nthreads; ++i) {
		rc = pthread_create(&th[i], NULL, put_worker,
				    (void*)(long)i);
		if (rc)
			goto out;
	}

	/* wait for client connection */
	rc = ring_buffer_scif_wait_for_shadow(&opt->rbs,
					      opt->local_port,
					      1);
	if (rc)
		goto out;

	/* run */
	put_worker(0);

	/* give a shadow time to wrap up */
	sleep(1);

	/* destroy */
	ring_buffer_scif_destroy_master(&opt->rbs);
out:
	return rc;
}

int test_shadow(struct cmd_opt *opt)
{
	pthread_t *th = NULL;
	int i, rc;

	/* create */
	rc = ring_buffer_scif_create_shadow(opt->local_port,
					    opt->master_node,
					    opt->remote_port,
					    NULL, NULL,
					    &opt->rbs);
	if (rc)
		goto out;

	/* create threads */
	th = (pthread_t *)malloc(sizeof(*th) * opt->nthreads);
	for (i = 1; i < opt->nthreads; ++i) {
		rc = pthread_create(&th[i], NULL, get_worker,
				    (void*)(long)i);
		if (rc)
			goto out;
	}
	if (rc)
		goto out;

	/* run */
	get_worker(0);

	/* take a breath */
	sleep(1);

	/* destroy */
	ring_buffer_scif_destroy_shadow(&opt->rbs);
out:
	return rc;
}

static void print_result(char *workload, struct cmd_opt *opt)
{
	unsigned long long io = (unsigned long long)opt->elm_size *
		(unsigned long long)opt->count;
	int runt_msec = opt->time * 1000;
	double iops = (double)opt->count / (double)opt->time;
	double bw = (double)io / (double)opt->time;
	double lat_usec = (double)(runt_msec * 1000) / (double)opt->count;

	printf("## %s:%s:%d\n",
	       workload, opt->elm_size_str, opt->mic_nthreads);
	printf("# io numjobs workload rs iops bw lat_usec runt_msec\n");
	printf("%llu %d %s %d %f %f %f %d\n\n",
	       io, opt->mic_nthreads, workload,
	       opt->elm_size, iops, bw, lat_usec, runt_msec);
}

int main(int argc, char *argv[])
{
	int err = 0;

	if (parse_option(argc, argv, &_opt) < 10) {
		usage(argv[0]);
		err = 1;
		goto main_out;
	}

	if (_opt.master)
		test_master(&_opt);
	else
		test_shadow(&_opt);
	sleep(1);
	print_result("rbs-flow", &_opt);
main_out:
	return err;
}
