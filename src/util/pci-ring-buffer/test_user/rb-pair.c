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
#include <ring_buffer_i.h>


enum {
	false,
	true
};

struct cmd_opt {
	/* command line options */
	int time;
	int nthreads;
	int elm_cnt;
	int elm_size;
	char elm_size_str[256];
	int blocking;

	/* benchmark control */
	volatile int      go;
	volatile uint64_t count;

	/* ring buffers */
	struct ring_buffer_t *rb;
};

struct cmd_opt opt;


static int parse_option(int argc,
                        char *argv[],
                        struct cmd_opt *opt)
{
	static struct option options[] = {
		{"time",        required_argument, 0, 't'},
		{"nthreads",    required_argument, 0, 'n'},
		{"elm_cnt",     required_argument, 0, 'c'},
		{"elm_size",    required_argument, 0, 's'},
		{"blocking",    required_argument, 0, 'b'},
		{0,             0,                 0, 0},
	};

	int arg_cnt;
	int c, idx, unit, len;

	memset(opt, 0, sizeof(struct cmd_opt));
	for (arg_cnt = 0; true; ++arg_cnt) {
		c = getopt_long(argc, argv,
				"t:n:c:s:", options, &idx);
		if (c == -1)
			break;
		switch(c) {
		case 't':
			opt->time = atoi(optarg);
			break;
		case 'n':
			opt->nthreads = atoi(optarg);
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
	fprintf(stderr, " --elm_cnt  = initial number of elements\n");
	fprintf(stderr, " --elm_size = element size in bytes\n");
	fprintf(stderr, " --blocking = blocking or non-blocking ring buffer\n");
}

static void sighandler(int x)
{
	opt.go = 2;
	smp_mb();
}

static size_t calc_rb_size(struct cmd_opt *opt)
{
	return 1024*1024*1024;
}

static int init_ring_buffer(struct cmd_opt *opt)
{
	int size = calc_rb_size(opt), rc, i;
	struct ring_buffer_req_t req;

	rc = ring_buffer_create(size, 64, opt->blocking, NULL, NULL, &opt->rb);
	if (rc)
		goto out;

	for (i = 0; i < opt->elm_cnt; ++i) {
		ring_buffer_put_req_init(&req, BLOCKING, opt->elm_size);
		rc = ring_buffer_put(opt->rb, &req);
		if (rc) {
			fprintf(stderr, "[%d] [init] fail to put: %d\n",
				i, rc);
			goto out;
		}
		ring_buffer_elm_set_ready(opt->rb, req.data);
	}
out:
	return rc;
}

static void deinit_ring_buffer(struct cmd_opt *opt)
{
	ring_buffer_destroy(opt->rb);
}

static void *worker(void *x)
{
        unsigned int id = (long)x;
	uint64_t count = 0;
	struct ring_buffer_req_t put_req, get_req;
	char *data;
	int rc;

	data = calloc(1, opt.elm_size);
        if (id == 0) {
                if (signal(SIGALRM, sighandler) == SIG_ERR) {
			fprintf(stderr, "[%s:%d] signal failed\n",
				__func__, __LINE__);
			exit(1);
		}
                alarm(opt.time);
                opt.go = 1;
        } else {
                while (opt.go == 0) ;
        }

        for(; opt.go == 1; ++count) {
		/* enqueue */
		ring_buffer_put_req_init(&put_req, BLOCKING, opt.elm_size);
		rc = ring_buffer_put(opt.rb, &put_req);
		if ( unlikely(rc) ) {
			fprintf(stderr, "[%llu] fail to put: %d, "
				"tail: %d, tail2: %d, head: %d\n",
				(long long unsigned int)count, rc,
				(int)opt.rb->tail,
				(int)opt.rb->tail2,
				(int)opt.rb->head);
			exit(-1);
		}
		copy_to_ring_buffer(opt.rb, put_req.data, data, put_req.size);
		ring_buffer_elm_set_ready(opt.rb, put_req.data);

		/* dequeue */
		do {
			ring_buffer_get_req_init(&get_req,
						 BLOCKING);
			rc = ring_buffer_get(opt.rb, &get_req);
		} while(rc == -EAGAIN && opt.go == 1);
		if ( rc && rc != -EAGAIN ) {
			fprintf(stderr, "[%llu] fail to get: %d, "
				"tail: %d, tail2: %d, head: %d\n",
				(long long unsigned int)count, rc,
				(int)opt.rb->tail,
				(int)opt.rb->tail2,
				(int)opt.rb->head);
			exit(-1);
		}
		copy_from_ring_buffer(opt.rb, data, get_req.data, get_req.size);
		ring_buffer_elm_set_done(opt.rb, get_req.data);
        }

	sleep(1);
	smp_faa(&opt.count, count);
        return NULL;
}

static void print_result(char *workload, struct cmd_opt *opt)
{
	unsigned long long io = (unsigned long long)opt->elm_size *
		(unsigned long long)opt->count * 2;
	int runt_msec = opt->time * 1000;
	double iops = (double)opt->count / (double)opt->time;
	double bw = (double)io / (double)opt->time;
	double lat_usec = (double)(runt_msec * 1000) / (double)opt->count;

	printf("## %s:%s:%d\n",
	       workload, opt->elm_size_str, opt->nthreads);
	printf("# io numjobs workload rs iops bw lat_usec runt_msec\n");
	printf("%llu %d %s %d %f %f %f %d\n\n",
	       io, opt->nthreads, workload,
	       opt->elm_size, iops, bw, lat_usec, runt_msec);
}

int main(int argc, char *argv[])
{
        pthread_t *th = NULL;
	int err = 0;
	int i;

	memset(&opt, 0, sizeof(opt));
	if (parse_option(argc, argv, &opt) != 5) {
		usage(argv[0]);
		err = 1;
		goto main_out;
	}

	err = init_ring_buffer(&opt);
	if (err)
		goto main_out;
	th = (pthread_t *)malloc(sizeof(*th) * opt.nthreads);
	for (i = 1; i < opt.nthreads; ++i) {
                err = pthread_create(&th[i], NULL, worker, (void*)(long)i);
                if (err)
                        goto main_out;
	}

	if (i > 1)
		sleep(1);
	worker(0);
	sleep(1);
	print_result("enq-deq-pair", &opt);
main_out:
	deinit_ring_buffer(&opt);
	free(th);
	return err;
}
