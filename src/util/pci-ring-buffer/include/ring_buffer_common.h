#ifndef _RING_BUFFER_COMMON_H_
#define _RING_BUFFER_COMMON_H_

#ifdef __cplusplus
extern "C" {
#endif
#include <arch.h>

#ifndef RING_BUFFER_CONF_KERNEL
# include <pthread.h>
# include <errno.h>
#else  /* RING_BUFFER_CONF_KERNEL */
# include <linux/mutex.h>
# include <linux/wait.h>
# include <linux/sched.h>
# include <linux/errno.h>
#endif /* RING_BUFFER_CONF_KERNEL */

/*
 * ring buffer static configuration
 */

/** static configuration */
/* #define RING_BUFFER_CONF_KERNEL */
/* #define RING_BUFFER_CONF_NO_MMAP */
/* #define RING_BUFFER_CONF_NO_DOUBLE_MMAP */

/* check validity of configuration */
#if defined(RING_BUFFER_CONF_KERNEL) && !defined(RING_BUFFER_CONF_NO_MMAP)
# error RING_BUFFER_CONF_KERNEL does not support mmap().
#endif

#if defined(RING_BUFFER_CONF_NO_MMAP) && !defined(RING_BUFFER_CONF_NO_DOUBLE_MMAP)
# error RING_BUFFER_CONF_NO_DOUBLE_MMAP cannot be supported without mmap().
#endif


/** debugging and profiling */
/* #define RING_BUFFER_DEBUG */
/* #define RING_BUFFER_TRACE_FINGERPRINT */
/* #define RING_BUFFER_TRACE_EVENT */


/*
 * ring buffer configuration
 */
#define RING_BUFFER_NON_BLOCKING    0x0
#define RING_BUFFER_BLOCKING        0x1


/*
 * ring buffer scif configuration
 */
enum {
	RING_BUFFER_SCIF_NODE_HOST    = 0,
	RING_BUFFER_SCIF_NODE_MIC0    = 1,
	RING_BUFFER_SCIF_NODE_MIC1    = 2,
	RING_BUFFER_SCIF_NODE_MIC2    = 3,
	RING_BUFFER_SCIF_NODE_MIC3    = 4,
	RING_BUFFER_SCIF_NODE_MIC4    = 5,
	RING_BUFFER_SCIF_NODE_MIC5    = 6,
	RING_BUFFER_SCIF_NODE_MIC6    = 7,

	RING_BUFFER_SCIF_NODE_MIC_MAX = RING_BUFFER_SCIF_NODE_MIC6,
};

#define RING_BUFFER_SCIF_PRODUCER     0x0
#define RING_BUFFER_SCIF_CONSUMER     0x1

#define RING_BUFFER_SCIF_NUM_DMA_CHANNEL 8
#define RING_BUFFER_SCIF_NUM_PORTS       RING_BUFFER_SCIF_NUM_DMA_CHANNEL

/*
 * ring buffer request for put() and get() operation
 */
struct ring_buffer_req_t {
	size_t       __size; /* [internal] cache aligned size */
	size_t       size;   /* [in] size */
	volatile struct ring_buffer_req_t *__next; /* [internal] next waiter */
	void *data;          /* [out] pointer to the buffer element */
	volatile int rc;     /* [out] return code */
	volatile int flag;   /* [in, out] operation mode */
} ____cacheline_aligned;     /* aligned to a cacheline
			      * for a combiner to reduce cache access */

#define RING_BUFFER_REQ_BLOCKING       0x0001
#define RING_BUFFER_REQ_NON_BLOCKING   0x0002
#define RING_BUFFER_REQ_COMBINER       0x4000
#define RING_BUFFER_REQ_DONE           0x8000

#define ring_buffer_get_req_init(__req, __md) do {		\
		(__req)->__next = NULL;				\
		(__req)->rc = -EINPROGRESS;			\
		(__req)->flag   = RING_BUFFER_REQ_##__md;	\
	} while(0)
#define ring_buffer_put_req_init(__req, __md, __sz) do {	\
		ring_buffer_get_req_init(__req, __md);		\
		(__req)->size = __sz;		                \
	} while(0)
#define ring_buffer_poll_request(__req) (		  \
		(__req)->flag & RING_BUFFER_REQ_DONE)

void ring_buffer_req_barrier(struct ring_buffer_req_t *req);
void ring_buffer_assert_fingerprint(void *data);

/*
 * user-defined callback functions
 */
typedef void (*ring_buffer_reap_cb_t)(void*, void *);
#ifdef __cplusplus
}
#endif
#endif  /* _RING_BUFFER_COMMON_H_ */
