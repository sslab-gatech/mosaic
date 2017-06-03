#ifndef _RING_BUFFER_H_
#define _RING_BUFFER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <linux/limits.h>
#include <arch.h>
#include <ring_buffer_common.h>

/*
 * configurations for lock-based version
 */
/* #define RING_BUFFER_TWO_LOCK */
/* #define RING_BUFFER_TWO_LOCK_TICKETLOCK */
/* #define RING_BUFFER_TWO_LOCK_MCSLOCK */

#ifdef  RING_BUFFER_TWO_LOCK
# if defined(RING_BUFFER_TWO_LOCK_TICKETLOCK)
typedef struct ticketlock_t spinlock_t;
# elif defined(RING_BUFFER_TWO_LOCK_MCSLOCK)
typedef struct mcslock_t spinlock_t;
# endif
#endif /* RING_BUFFER_TWO_LOCK */

/*
 * ring buffer object
 */
#define RING_BUFFER_NAME_MAX        128

struct ring_buffer_t;
struct ring_buffer_elm_t;
struct ring_buffer_req_t;

typedef int  (*ring_buffer_is_healthy_t)(struct ring_buffer_t *);

struct ring_buffer_nap_info_t {
	volatile int                       is_nap_time;
	int                                monitoring_status;
	volatile struct ring_buffer_elm_t *monitoring_elm;
#ifndef RING_BUFFER_CONF_KERNEL
	pthread_mutex_t                    mutex;
	pthread_cond_t                     wait;
#else
	struct mutex                       mutex;
	wait_queue_head_t                  wait;
#endif /* RING_BUFFER_CONF_KERNEL */
};

struct ring_buffer_t {
	size_t size ____cacheline_aligned2;         /* in bytes */
	size_t align_mask;                          /* data alignment mask */
	void *buff;                                 /* start of ring buffer */
	int  is_blocking;                           /* blocking or non-blocking */
	ring_buffer_reap_cb_t reap_cb;              /* user-defined reap callback */
	void* reap_cb_arg;                          /* user-defined reap callback argument */
	ring_buffer_is_healthy_t is_healthy;        /* health check function */
	void *private_value;                        /* any value by its user */

	volatile size_t head  ____cacheline_aligned2; /* byte offset */
	volatile size_t tail2;                      /* byte offset */

	volatile size_t tail  ____cacheline_aligned2; /* byte offset */

#ifndef RING_BUFFER_TWO_LOCK
	volatile struct ring_buffer_req_t *put_req ____cacheline_aligned2;

	volatile struct ring_buffer_req_t *get_req ____cacheline_aligned2;
#else
	spinlock_t put_lock  ____cacheline_aligned2;
	spinlock_t get_lock  ____cacheline_aligned2;
#endif  /* RING_BUFFER_TWO_LOCK */

	struct ring_buffer_nap_info_t      put_nap ____cacheline_aligned2;
	struct ring_buffer_nap_info_t      get_nap ____cacheline_aligned2;

	char name[RING_BUFFER_NAME_MAX];            /* name for debugging */
} ____cacheline_aligned2;

/*
 * ring buffer API
 */
int  __ring_buffer_create(const char *where, unsigned int line, const char *var,
			  size_t size_hint, size_t align, int is_blocking,
			  ring_buffer_reap_cb_t reap_cb, void* reap_cb_arg,
			  struct ring_buffer_t **prb);

#define ring_buffer_create(size_hint, align, is_blocking, reap_cb, reap_cb_arg, prb) \
	__ring_buffer_create(__func__, __LINE__, #prb,			\
			     size_hint, align, is_blocking,		\
			     reap_cb, reap_cb_arg, prb)
void ring_buffer_init(struct ring_buffer_t* rb);
void ring_buffer_destroy(struct ring_buffer_t *rb);

int  ring_buffer_put(struct ring_buffer_t *rb, struct ring_buffer_req_t *req);
int  ring_buffer_get(struct ring_buffer_t *rb, struct ring_buffer_req_t *req);
int  ring_buffer_put_nolock(struct ring_buffer_t *rb, struct ring_buffer_req_t *req);
int  ring_buffer_get_nolock(struct ring_buffer_t *rb, struct ring_buffer_req_t *req);

void ring_buffer_elm_set_ready(struct ring_buffer_t *rb, void *data);
void ring_buffer_elm_set_done(struct ring_buffer_t *rb, void *data);

int  copy_from_ring_buffer(struct ring_buffer_t *rb,
			   void *dest_mem, const void *src_rb, size_t n);
int  copy_to_ring_buffer(struct ring_buffer_t *rb,
			 void *dest_rb, const void *src_mem, size_t n);
unsigned int ring_buffer_get_compat_vector(void);
int    ring_buffer_is_empty(struct ring_buffer_t *rb);
int    ring_buffer_is_full(struct ring_buffer_t *rb);
size_t ring_buffer_free_space(struct ring_buffer_t *rb);
#ifdef __cplusplus
}
#endif
#endif /* _RING_BUFFER_H_ */
