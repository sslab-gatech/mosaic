#include <ring_buffer.h>
#include "ring_buffer_porting.h"
#include "ring_buffer_i.h"

rb_static_assert(sizeof(struct ring_buffer_t) <= PAGE_SIZE,
		 "struct ring_buffer_t is too large");
rb_static_assert(sizeof(struct ring_buffer_elm_t) == sizeof(void *),
		 "hey, your change will make things slower.");

#if defined(RING_BUFFER_TWO_LOCK_TICKETLOCK)
#define spinlock_init(__lock)   ticketlock_init(__lock)
#define spinlock_lock(__lock)   ticketlock_lock(__lock)
#define spinlock_unlock(__lock) ticketlock_unlock(__lock)

#elif defined(RING_BUFFER_TWO_LOCK_MCSLOCK)
#define spinlock_init(__lock)   mcslock_init(__lock)
#define spinlock_lock(__lock)   do {		\
	struct mcsqnode_t ___q_n_o_d_e___;	\
	mcslock_lock(__lock, &___q_n_o_d_e___);
#define spinlock_unlock(__lock)				\
	mcslock_unlock(__lock, &___q_n_o_d_e___);	\
	} while(0)
#endif

static void _rb_trace_event_blocked(struct ring_buffer_t *rb, int op_type);
static void _rb_trace_event_unblocked(struct ring_buffer_t *rb, int op_type);

int _ring_buffer_init_nap_time(struct ring_buffer_t *rb)
{
	struct ring_buffer_nap_info_t *nap[2] = {
		&rb->put_nap, &rb->get_nap};
	int i, rc;

	/* first, check if poking is eanbled */
	if (!rb->is_blocking)
		return 0;

	/* init the nap lock and condtion variable */
	for (i = 0; i < 2; ++i) {
		rc = __rb_mutex_init(&(nap[i])->mutex);
		if (rc)
			goto out;
		rc = __rb_wait_init(&(nap[i])->wait);
		if (rc)
			goto out;
	}
out:
	return rc;
}

void _ring_buffer_deinit_nap_time(struct ring_buffer_t *rb)
{
	struct ring_buffer_nap_info_t *nap[2] = {
		&rb->put_nap, &rb->get_nap};
	int i;

	if (rb->is_blocking) {
		for (i = 0; i < 2; ++i) {
			__rb_wait_destroy(&(nap[i])->wait);
			__rb_mutex_destroy(&(nap[i])->mutex);
		}
	}
}

static inline
struct ring_buffer_nap_info_t *
_rb_get_nap_info(struct ring_buffer_t *rb, int op_type)
{
	return (op_type == RING_BUFFER_OP_GET) ?
		&rb->get_nap : &rb->put_nap;
}

static
void _nap_wake_up_all_waiters(struct ring_buffer_t *rb, int op_type,
			      struct ring_buffer_nap_info_t *nap)
{
	/* opportunistically wake up all waiters
	 * to reduce wake-up latency */
	if (nap->is_nap_time) {
		/* trace event */
		_rb_trace_event_unblocked(rb, op_type);

		/* then, wake up waiters */
		nap->is_nap_time = 0;
		smp_wmb();
		__rb_mutex_lock(&nap->mutex); {
			__rb_wait_wake_up_all(&nap->wait);
		} __rb_mutex_unlock(&nap->mutex);
	}
	nap->monitoring_elm = NULL;
}

static
void _nap_wake_up_all_waiters_nolock(struct ring_buffer_t *rb, int op_type,
				     struct ring_buffer_nap_info_t *nap)
{
	if (nap->is_nap_time) {
		/* trace event */
		_rb_trace_event_unblocked(rb, op_type);

		/* then, wake up waiters */
		nap->is_nap_time = 0;
		smp_wmb();
	}
	nap->monitoring_elm = NULL;
}

static
int  _nap_peek(struct ring_buffer_t *rb, int op_type,
	       struct ring_buffer_nap_info_t *nap)
{
	int rc = 0;

	/* check if this ring buffer is healthy or not */
	if (rb->is_healthy) {
		rc = rb->is_healthy(rb);
		if (rc < 0)
			goto wake_up_out;
	}

	/* announce that it is nap time
	 * for all waiters to take a nap */
	if (!nap->is_nap_time) {
		/* trace event */
		_rb_trace_event_blocked(rb, op_type);

		/* then, wake up waiters */
		nap->is_nap_time = 1;
		/* we don't need smp_wmb() here,
		 * since nap time is not necessary
		 * to be know immeditely. */
	}
	smp_rmb();

	/* case 1
	 * - if there is no hint to poll,
	 * idle polling by retrying the operation */
	if (!nap->monitoring_elm) {
		__rb_yield();
		goto no_wake_up_out;
	}

	/* case 2
	 * - idle-polling for the change of status of a monitoring element */
	while ( !(nap->monitoring_elm->status & nap->monitoring_status) ) {
		__rb_yield();
		smp_rmb();
	}

wake_up_out:
	/* the nap time is over so that wake up all waiters
	 * regardless of errors */
	_nap_wake_up_all_waiters(rb, op_type, nap);
no_wake_up_out:
	return rc;
}

static inline
int _nap_need_doze(struct ring_buffer_nap_info_t *nap,
		   struct ring_buffer_req_t *req)
{
	/* We should check if the request is already processed or not,
	 * since nap->is_nap_time can be ABA-ed. */
	return req->rc == -EINPROGRESS && nap->is_nap_time;
}

static
void _nap_doze(struct ring_buffer_nap_info_t *nap,
	       struct ring_buffer_req_t *req)
{
	/* triple-check locking optimization */
	/* - save rmb() here */
	if ( _nap_need_doze(nap, req)) {
		/* - save mutex() here */
		smp_rmb();
		if ( _nap_need_doze(nap, req)) {
			/* - in fact, we need mutex() here */
			__rb_mutex_lock(&nap->mutex); {
				/* take a nap until a combiner wakes me up */
				/* - if possible, avoid pthread_cond_wait() */
				smp_rmb();

				while ( _nap_need_doze(nap, req)) {
					__rb_wait_sleep_on(&nap->wait,
							   &nap->mutex);
					smp_rmb();
				}
			} __rb_mutex_unlock(&nap->mutex);
		}
	}
}

static
size_t start_offset_coloring(size_t align)
{
	size_t offset, color = 1;

#ifdef RING_BUFFER_START_OFFSET_COLORING
	int num_color = PAGE_SIZE / align;

	if (num_color > 1) {
	    unsigned int rand = rand32_seedless();
	    color  = 1 + (rand % (num_color - 1));
	}
#endif
	offset = (color * align) - sizeof(struct ring_buffer_elm_t);
	rb_assert(offset <= (PAGE_SIZE - sizeof(struct ring_buffer_elm_t)),
		  "start from the first page");
	return offset;
}

#ifdef RING_BUFFER_CONF_NO_MMAP
static
void * __rb_alloc_memory(size_t rb_size, int *rc)
{
	size_t mmap_size;
	void  *rb_addr;

	/* page-aligned memory allocation */
	mmap_size = __rb_size_to_mmap_size(rb_size);
#ifndef RING_BUFFER_CONF_KERNEL
	if ( posix_memalign(&rb_addr, PAGE_SIZE, mmap_size) )
		rb_addr = NULL;
#else
	rb_addr = vmalloc(mmap_size);
#endif /* RING_BUFFER_CONF_KERNEL */
	if (!rb_addr)
		*rc = ENOMEM;
	else
		*rc = 0;
	return rb_addr;
}
#else /* RING_BUFFER_CONF_NO_MMAP */
static
char *generate_unique_name(char *buff, const char *prefix, int size)
{
	uint64_t when = rdtsc();
	snprintf(buff, size, "/%s-%d-%p-%" PRIu64 ".map",
		 prefix, getpid(), &when, when);
	return buff;
}

static
void * __rb_alloc_memory(size_t rb_size, int *rc)
{
	char shm_name[NAME_MAX] = {0};
	size_t mmap_size;
	void *rb_addr, *buff_addr0, *buff_addr1, *addr;
	int fd  = -1;

	/* make compiler happy under NO_DOUBLE_MMAP build */
	buff_addr0 = buff_addr1 = addr = NULL;

	/* create a backing file for mmap */
	generate_unique_name(shm_name, "___rb", NAME_MAX);
	fd = shm_open(shm_name,
		      O_CREAT | O_RDWR | O_TRUNC,
		      S_IRUSR | S_IWUSR);
	if (fd == -1)
		goto err_out;

	/* unlink the shared memory so it will be deleted
	 * as soon as the shared memory region is unmapped. */
	shm_unlink(shm_name);
	shm_name[0] = '\0';

	/* enlarge shm */
	if (ftruncate(fd, __rb_size_to_shm_size(rb_size)) == -1)
		goto err_out;

	/* allocate ring buffer structure and data region */
	mmap_size = __rb_size_to_mmap_size(rb_size);
	rb_addr = mmap(NULL, mmap_size,
		       PROT_READ | PROT_WRITE,
#ifdef TARGET_ARCH_K1OM
		       /* Since MIC is a UMA architecture,
			* it is always safe to populate pages
			* without worrying first-touch NUMA issues.
			* More importantly, populating pages
			* in a single thread can help to reduce
			* physical memory fragmentation. */
		       MAP_POPULATE |
#endif
		       MAP_SHARED,
		       fd, 0);
	if (rb_addr == MAP_FAILED)
		goto err_out;
	buff_addr0 = __map_addr_to_rb_buff(rb_addr);

	/* overriding the buffer region
	 * if HUGE_PAGE mapping is possible */
	mmap(buff_addr0, rb_size,
	     PROT_READ | PROT_WRITE,
	     MAP_SHARED | MAP_FIXED | MAP_HUGETLB,
	     fd, buff_addr0 - rb_addr);

#ifndef RING_BUFFER_CONF_NO_DOUBLE_MMAP
	/* double mmap to make data region circular */
	buff_addr1 = buff_addr0 + rb_size;
	addr = mmap(buff_addr1, rb_size,
		    PROT_READ | PROT_WRITE,
		    MAP_SHARED | MAP_FIXED,
		    fd, buff_addr0 - rb_addr);
	if (addr == MAP_FAILED)
		goto err_out;
#endif /* RING_BUFFER_CONF_NO_DOUBLE_MMAP */

	/* close fd */
	close(fd);
	fd = -1;
	*rc = 0;
	return rb_addr;
err_out:
	*rc = errno;
	if (fd != -1)
		close(fd);
	if (shm_name[0] != '\0')
		shm_unlink(shm_name);
	return NULL;
}
#endif /* RING_BUFFER_CONF_NO_MMAP */

int  __ring_buffer_create(const char *where, unsigned int line,
			  const char *var,
			  size_t size_hint, size_t align, int is_blocking,
			  ring_buffer_reap_cb_t reap_cb, void* reap_cb_arg,
			  struct ring_buffer_t **prb)
{
	struct ring_buffer_t *rb = NULL;
	size_t start_offset;
	size_t rb_size, min_size;
	void  *rb_addr, *buff_addr0;
	int rc;

	/*               == memory map of ring buffer ==
	 *
	 * +-------------+------------------------------------------------+
	 * | 4K page     | data                                           |
	 * +-------------+------------------------------------------------+
	 *  \\\\          \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
	 *   +--+          +-----------------------------------------------+
	 *   ring buffer   double mmap-ed data region
	 *   structure
	 */

	/*                == four control variables ==
	 *
	 * +-----------+======================================+-----------+
	 * |:::::::::::||||| A C T I V E  E L E M E N T S |||||...........|
	 * +-----------+======================================+-----------+
	 *  \           \                                    /           /
	 *   tail2       tail                          {head2}       head
	 *
	 * [tail2, tail): elements are dequeued but still in use
	 * [tail, head2): elements are enqueued and ready to dequeue
	 * [head2, head): elements are enqueued but not ready to use
	 *
	 * o put/enqueue operation
	 *  - updates head
	 * o get/dequeue operation
	 *  - update tail
	 * o reap elements
	 *  - updates tail2 to reclaim 'done' elements
	 *    when more free spaces are needed at put operations
	 */

	/* sanith check
	 * - alignment should be power of 2 */
	if (align & (align - 1))
		return -EINVAL;

	/* decide ring buffer size
	 * : ring buffer structure + padding for double mmap + data */
	min_size = align + sizeof(struct ring_buffer_elm_t);
	if (size_hint > (HUGE_PAGE_SIZE * 3))
		rb_size = (size_hint + min_size + ~HUGE_PAGE_MASK)
			& HUGE_PAGE_MASK;
	else
		rb_size = (size_hint + min_size + ~PAGE_MASK)
			& PAGE_MASK;

	/* alloc memory */
	rb_addr = __rb_alloc_memory(rb_size, &rc);
	if (!rb_addr)
		goto err_out;
	buff_addr0 = __map_addr_to_rb_buff(rb_addr);

	/* init rb */
	rb               = (struct ring_buffer_t *)rb_addr;
	memset(rb, 0, sizeof(*rb));

	rb->buff         = buff_addr0;
	rb->size         = rb_size;
	rb->align_mask   = align - 1;
	rb->reap_cb      = reap_cb;
	rb->reap_cb_arg  = reap_cb_arg;
#ifndef RING_BUFFER_TWO_LOCK
	rb->put_req      = NULL;
	rb->get_req      = NULL;
#else
	spinlock_init(&rb->put_lock);
	spinlock_init(&rb->get_lock);
#endif  /* RING_BUFFER_TWO_LOCK */

	/*
	 * make data in elements aligned
	 * and assign random cacheline color
	 *
	 *   +--------+--------------------+
	 *   | header | data               |
	 *   +--------+--------------------+
	 *   header    \
	 *              cacheline aligned
	 */
	start_offset = start_offset_coloring(align);
	rb->head     = start_offset;
	rb->tail     = start_offset;
	rb->tail2    = start_offset;

	/* init nap time */
	rb->is_blocking = is_blocking;
	rc = _ring_buffer_init_nap_time(rb);
	if (rc)
		goto err_out;

	/* record my name */
	snprintf(rb->name, RING_BUFFER_NAME_MAX, "%s@%s:%d", var, where, line);
	rb->name[RING_BUFFER_NAME_MAX - 1] = '\0';

	/* pass out the created ring buffer */
	smp_mb();
	*prb = rb;

#ifndef RING_BUFFER_CONF_NO_DOUBLE_MMAP
	/* check whether a ring buffer is a really ring */
	int *p, *q;
	p = (int *)rb->buff;
	q = (int *)(rb->buff + rb->size);
	if (p != q) {
		rb_assert(*p == *q,
			  "ring buffer is not a ring");
		rb_assert(*p = 0xdeadbeef,
			  "write to the first of ring buffer");
		rb_assert(*p == *q,
			  "ring buffer is not a ring");
	}
#endif /* RING_BUFFER_CONF_NO_DOUBLE_MMAP */
	return 0;
err_out:
	/* clean up  */
	rb_dbg("fail to create a ring buffer: %d\n", rc);
	ring_buffer_destroy(rb);
	*prb = NULL;
	return -rc;
}
EXPORT_SYMBOL(__ring_buffer_create);

#ifdef RING_BUFFER_CONF_NO_MMAP
static
void __rb_free_memory(struct ring_buffer_t *rb)
{
#ifndef RING_BUFFER_CONF_KERNEL
	free(rb);
#else
	vfree(rb);
#endif /* RING_BUFFER_CONF_KERNEL */
}
#else /* RING_BUFFER_CONF_NO_MMAP */
static
void __rb_free_memory(struct ring_buffer_t *rb)
{
	void *addr;

	/* unmap memory regions
	 * - if the data buffer region is mapped
	 *   with huge pages, we need this.
	 * - See the kernel document in below:
	 *   https://www.kernel.org/doc/Documentation/vm/hugetlbpage.txt
	 * - Here are three regions to munmap():
	 *   [  ][    ][    ]
	 *   ^   ^     ^
	 *   |   |     +- buff + rb->size: valid when double-mmap()-ed
	 *   |   +- buff: buffer space
	 *   +- buff - PAGE_SIZE: rb, which can be safely got by __rb_orb()
	 */
#ifndef RING_BUFFER_CONF_NO_DOUBLE_MMAP
	addr = rb->buff + rb->size;
	if (munmap(addr, rb->size) == -1) {
		rb_err("[rb: %p] fail to munmap(%p, %d)\n",
		       rb, addr, (unsigned int)rb->size);
		/* ignore error */
	}
#endif /* RING_BUFFER_CONF_NO_DOUBLE_MMAP */

	addr = rb->buff;
	if (munmap(addr, rb->size) == -1) {
		rb_err("[rb: %p] fail to munmap(%p, %d)\n",
		       rb, addr, (unsigned int)rb->size);
		/* ignore error */
	}

	addr = __rb_org(rb);
	if (munmap(addr, PAGE_SIZE) == -1) {
		rb_err("[rb: %p] fail to munmap(%p, %d)\n",
		       rb, addr, PAGE_SIZE);
		/* ignore error */
	}
}
#endif /* RING_BUFFER_CONF_NO_MMAP */

void ring_buffer_destroy(struct ring_buffer_t *rb)
{
	/* is this valid rb? */
	if (!rb)
		return;

	/* deinit the nap time related resources */
	_ring_buffer_deinit_nap_time(rb);

	/* do we have memory regions to munmap? */
	if (rb->size <= 0)
		return;

	/* free ring buffer memory */
	__rb_free_memory(rb);
}
EXPORT_SYMBOL(ring_buffer_destroy);

void ring_buffer_init(struct ring_buffer_t* rb){
#ifndef RING_BUFFER_CONF_NO_DOUBLE_MMAP
  memset(rb->buff + rb->size, 0, rb->size);
#endif /* RING_BUFFER_CONF_NO_DOUBLE_MMAP */
  memset(rb->buff, 0, rb->size);
}
EXPORT_SYMBOL(ring_buffer_init);

static inline
void rb_rmb(struct ring_buffer_t *rb, int op_type)
{
	struct ring_buffer_t *rb_org;

	if ( !__rb_is_shadow(rb) )
		return;

	rb_org = __rb_org(rb);
	if (op_type == RING_BUFFER_OP_GET)
		rb->head  = rb_org->head;
	else
		rb->tail  = rb_org->tail;
}

static inline
void rb_wmb(struct ring_buffer_t *rb, int op_type)
{
	struct ring_buffer_t *rb_org;

	if ( !__rb_is_shadow(rb) )
		return;

	/* flush out store buffer to enforce elm change first
	 * before updating head or tail */
	smp_wmb();
	rb_org = __rb_org(rb);
	if (op_type == RING_BUFFER_OP_GET)
		rb_org->tail  = rb->tail;
	else {
		volatile size_t __dummy;
		/* enforce previous on-the-fly write completed */
		__dummy = rb_org->head;
		smp_cmb();
		rb_org->head  = rb->head;
	}
}

static
void reap_ring_buffer_elms(struct ring_buffer_t *rb)
{
	/*
	 * control variables should not be prefetched by a compiler
	 */
	smp_cmb(); {

	/*
	 * reap elements consumed by get()
	 */

	struct ring_buffer_elm_t *elm_addr, elm;
	size_t rb_tail = rb->tail, rb_tail2 = rb->tail2;
	int changed = 0;

	/* update tail2 */
	elm_addr = (struct ring_buffer_elm_t *)(rb->buff + rb_tail2);
	elm = *elm_addr;
	while(rb_tail2 != rb_tail) {
		/* if an element is not 'DONE' */
		if ( !(elm.status & RING_BUFFER_ELM_STATUS_DONE) ) {
			/* if an user provides a reap_cb, call the call back */
			if (rb->reap_cb) {
				rb->reap_cb(rb->reap_cb_arg, (void *)elm_addr + sizeof(*elm_addr));
				/* then, re-check the status */
				if ( !(elm.status & RING_BUFFER_ELM_STATUS_DONE) )
					break;
			}
			/* otherwise, stop to reap elements */
			else
				break;
		}
		/* here, the element is 'DONE' so that it can be reaped. */

		rb_tail2 = (rb_tail2 + elm.__size) % rb->size;
		elm_addr = (struct ring_buffer_elm_t *)(rb->buff + rb_tail2);
		elm = *elm_addr;
		changed = 1;
	}

	/* well, branch is cheaper than
	 * unnecessary shared cacheline update */
	if (changed)
		rb->tail2 = rb_tail2;
	} /* smp_cmb(); */
}

static
void reap_ring_buffer_elms_aggressively(struct ring_buffer_t *rb)
{
	/*
	 * control variables should not be prefetched by a compiler
	 */
	smp_cmb(); {
	/*
	 * reap elements consumed by set_done()
	 * - this is rather expensive since it accesses rb->head
	 */

	struct ring_buffer_elm_t *elm_addr, elm;
	size_t rb_tail = rb->tail, rb_tail2 = rb->tail2, rb_head = rb->head;
	int changed_tail = 0, changed_tail2 = 0;

	/* update tail2 */
	elm_addr = (struct ring_buffer_elm_t *)(rb->buff + rb_tail2);
	elm = *elm_addr;

	/* we assume that element can be consumbed by simply calling
	 * ring_buffer_elm_set_done() without ring_buffer_get().
	 * so tail2 can pass tail but cannot head. */
	while(rb_tail2 != rb_head) {
		/* set change flag */
		changed_tail2 = 1;
		if (rb_tail == rb_tail2)
			changed_tail = 1;

		/* if an element is not 'DONE' */
		if ( !(elm.status & RING_BUFFER_ELM_STATUS_DONE) ) {
			/* if an user provides a reap_cb, call the call back */
			if (rb->reap_cb) {
				rb->reap_cb(rb->reap_cb_arg, (void *)elm_addr + sizeof(*elm_addr));
				/* then, re-check the status */
				if ( !(elm.status & RING_BUFFER_ELM_STATUS_DONE) )
					break;
			}
			/* otherwise, stop reaping elements */
			else
				break;
		}
		/* here, the element is 'DONE' so that it can be reaped. */

		/* advance tail2 and element address */
		rb_tail2 = (rb_tail2 + elm.__size) % rb->size;
		elm_addr = (struct ring_buffer_elm_t *)(rb->buff + rb_tail2);
		elm = *elm_addr;
	}

	/* well, branch is cheaper than
	 * unnecessary shared cacheline update */
	if (changed_tail2) {
		rb->tail2 = rb_tail2;
		/* we need to update tail to catch up tail2 */
		if (changed_tail) {
			/* were there any other concurrent putters
			 * updating tail? */

			/* first do check optimistically
			 * to avoid costly smp_cas. */
			if (rb->tail == rb_tail) {
				/* ok, there has been no contender so far.
				 * let's update tail precisely using smp_cas. */
				smp_cas(&rb->tail, rb_tail, rb_tail2);
			}
		}
	}
	} /* smp_cmb(); */
}

int ring_buffer_is_full(struct ring_buffer_t *rb)
{
	size_t head_next = (rb->head + 1) % rb->size;
	return rb->tail2 == head_next;
}
EXPORT_SYMBOL(ring_buffer_is_full);

static
size_t free_space_size(struct ring_buffer_t *rb)
{
	size_t free_space;
	size_t head_next = (rb->head + 1) % rb->size;

	/* is it full? */
	if (rb->tail2 == head_next) {
		free_space = 0;
		goto out;
	}

	/* calc free space size */
	if (rb->tail2 < rb->head) {
		free_space = rb->size - (rb->head - rb->tail2);
	}
	else if (rb->tail2 > rb->head) {
		free_space = rb->tail2 - rb->head;
	}
	else {
		free_space = rb->size;
	}

	free_space = free_space - sizeof(struct ring_buffer_elm_t) - 1;
out:
	return free_space;
}

size_t ring_buffer_free_space(struct ring_buffer_t *rb)
{
	return free_space_size(rb);
}
EXPORT_SYMBOL(ring_buffer_free_space);

static
size_t _secure_free_space(struct ring_buffer_t *rb, size_t n)
{
	size_t free_size = free_space_size(rb);

	if (free_size < n) {
		/* if there is no enough free space,
		 * reap done elements then re-check */
		rb_rmb(rb, RING_BUFFER_OP_PUT);
		reap_ring_buffer_elms(rb);
		free_size = free_space_size(rb);

		if (free_size < n) {
			/* one more try
			 * let's reap elements aggressively */
			reap_ring_buffer_elms_aggressively(rb);
			free_size = free_space_size(rb);
		}
	}

	return free_size;
}

static
void _leave_fingerprint(void *data)
{
#ifdef RING_BUFFER_TRACE_FINGERPRINT
	struct ring_buffer_elm_t *elm_addr;
	char *data_end;
	int data_padding_len, i;

	elm_addr = data - sizeof(*elm_addr);
	data_padding_len = elm_addr->padding - sizeof(*elm_addr);
	data_end = (char *)elm_addr + elm_addr->__size - data_padding_len;

	/* typical fingerprint should be as follows:
	 * <BCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvw */
	if (data_padding_len == 0)
		return;
	data_end[0] = '<';
	for (i = 1; i < (data_padding_len - 1); ++i)
		data_end[i] = (char)('A' + i);
	if (data_padding_len > 1)
		data_end[data_padding_len - 1] = '\0';
#else
	data = data; /* to make compiler happy */
#endif /* RING_BUFFER_TRACE_FINGERPRINT */
}

static
void _check_fingerprint(void *data)
{
#ifdef RING_BUFFER_TRACE_FINGERPRINT
	struct ring_buffer_elm_t *elm_addr;
	char *data_end;
	int data_padding_len, i;

	elm_addr = data - sizeof(*elm_addr);
	data_padding_len = elm_addr->padding - sizeof(*elm_addr);
	data_end = (char *)elm_addr + elm_addr->__size - data_padding_len;

	/* typical fingerprint should be as follows:
	 * <BCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvw */
	if (data_padding_len == 0)
		return;
	if (data_end[0] != '<')
		goto assert_fail;
	for (i = 1; i < (data_padding_len - 1); ++i) {
		if (data_end[i] != (char)('A' + i))
			goto assert_fail;
	}
	if (data_padding_len > 1 && data_end[data_padding_len - 1] != '\0')
		goto assert_fail;
	return;
assert_fail:
	rb_err("trailing padding was corrupted: %p: %s\n",
	       data_end, data_end);
	rb_assert(0, "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
	return;
#else
	data = data; /* to make compiler happy */
#endif /* RING_BUFFER_TRACE_FINGERPRINT */
}

void ring_buffer_assert_fingerprint(void *data)
{
	_check_fingerprint(data);
}
EXPORT_SYMBOL(ring_buffer_assert_fingerprint);

static
void __ring_buffer_put(struct ring_buffer_t *rb, struct ring_buffer_req_t *req)
{
	struct ring_buffer_elm_t *elm_addr, elm;
	unsigned short elm_status;
	unsigned short elm_init_status = RING_BUFFER_ELM_STATUS_INIT;
	size_t rb_head;
	int rc = 0;

	goto start; /* to make compiler happy */
start:
	/* check whether there is enough free space */
	elm_status = elm_init_status;
	if ( _secure_free_space(rb, req->__size) < req->__size ) {
		rb->put_nap.monitoring_elm = rb->buff + rb->tail2;
		rb->put_nap.monitoring_status = RING_BUFFER_ELM_STATUS_DONE;
		rc = -EAGAIN;
		goto out;
	}

	/* locate elm */
	elm_addr    = (struct ring_buffer_elm_t *)(rb->buff + rb->head);

	/* locate new head and prefetch the next elm */
	rb_head     = (rb->head + req->__size) % rb->size;
	smp_prefetchw(rb->buff + rb_head);

#ifdef RING_BUFFER_CONF_NO_DOUBLE_MMAP
	/* check if data is wrapped */
	if (unlikely(rb_head < rb->head)) {
		elm_status       = RING_BUFFER_ELM_STATUS_TOMBSTONE |
			           RING_BUFFER_ELM_STATUS_READY;
		elm_init_status |= RING_BUFFER_ELM_STATUS_TOMBSTONE_OWNER;
	}
#endif /* RING_BUFFER_CONF_NO_DOUBLE_MMAP */

	/* update the new elm */
	elm.__size  = req->__size;
	elm.padding = req->__size - req->size;
	elm.status  = elm_status;
	*elm_addr   = elm;

#ifdef RING_BUFFER_CONF_NO_DOUBLE_MMAP
	/* retry if it is a tombstone */
	if (unlikely(elm_status & RING_BUFFER_ELM_STATUS_TOMBSTONE)) {
		/* Publish new head
		 * Make sure that the elm is published before the head, otherwise the head
		 * might move on before the element is filled. */
		smp_cmb();
		rb->head = rb_head;
		rb_wmb(rb, RING_BUFFER_OP_PUT);
		goto start;
	}
#endif /* RING_BUFFER_CONF_NO_DOUBLE_MMAP */

	/* fill request info.  */
	req->data = &elm_addr[1];
	rb_assert( !((size_t)req->data & rb->align_mask),
		   "oops, data is misaligned");

	/* leave fingerprint */
	_leave_fingerprint(req->data);

	/* Publish new head
	 * Make sure that the elm is published before the head, otherwise the head
	 * might move on before the element is filled. */
	smp_cmb();
	rb->head = rb_head;
out:
	req->rc = rc;
}

int ring_buffer_is_empty(struct ring_buffer_t *rb)
{
	if (rb->head == rb->tail) {
		rb_rmb(rb, RING_BUFFER_OP_GET);
		if (rb->head == rb->tail) {
			return 1;
		}
	}
	return 0;
}
EXPORT_SYMBOL(ring_buffer_is_empty);

static
void __ring_buffer_get(struct ring_buffer_t *rb, struct ring_buffer_req_t *req)
{
	struct ring_buffer_elm_t *elm_addr, elm;
	size_t rb_tail;
	int rc = 0;

	goto start; /* to make compiler happy */
start:
	/* check whether it is empty */
	if (ring_buffer_is_empty(rb)) {
		rc = -EAGAIN;
		goto out;
	}

	/* locate elm */
	elm_addr = (struct ring_buffer_elm_t *)(rb->buff + rb->tail);
	elm = *elm_addr;

	/* check whether a tail element is ready */
	if ( !(elm.status & RING_BUFFER_ELM_STATUS_READY) ) {
		rb->get_nap.monitoring_elm = elm_addr;
		rb->get_nap.monitoring_status = RING_BUFFER_ELM_STATUS_READY;
		rc = -EAGAIN;
		goto out;
	}

	/* locate new tail and prefetch the next elm */
	rb_tail = (rb->tail + elm.__size) % rb->size;
	smp_prefetchw(rb->buff + rb_tail);

#ifdef RING_BUFFER_CONF_NO_DOUBLE_MMAP
	/* retry if it is a tombstone */
	if (unlikely(elm.status &
		     RING_BUFFER_ELM_STATUS_TOMBSTONE)) {
		/* make the tombstone reclaimable */
		elm_addr->status = RING_BUFFER_ELM_STATUS_TOMBSTONE |
			           RING_BUFFER_ELM_STATUS_READY |
			           RING_BUFFER_ELM_STATUS_DONE;

		/* publish new tail */
		rb->tail = rb_tail;
		rb_wmb(rb, RING_BUFFER_OP_GET);
		goto start;
	}
	/* if a tombstone is already reclaimed,
	 * myself is not a tombstone onwner any more. */
	else if (unlikely(elm.status &
			  RING_BUFFER_ELM_STATUS_TOMBSTONE_OWNER)) {
		elm_addr->status &= ~RING_BUFFER_ELM_STATUS_TOMBSTONE_OWNER;
		smp_wmb_tso();
	}
#endif /* RING_BUFFER_CONF_NO_DOUBLE_MMAP */

	/* fill request info.  */
	req->size = elm.__size - elm.padding;
	req->data = (void *)elm_addr + sizeof(*elm_addr);

	/* check fingerprint */
	_check_fingerprint(req->data);

	/* publish new tail */
	rb->tail = rb_tail;
out:
	req->rc = rc;
}

static
int ask_request(volatile struct ring_buffer_req_t **tail,
		struct ring_buffer_req_t *req)
{
	struct ring_buffer_req_t *old_tail;

	old_tail = (struct ring_buffer_req_t *)smp_swap(tail, req);
	if (old_tail) {
		old_tail->__next = req;
		return 0;
	}

	return 1;
}

static
int wait_until_done(struct ring_buffer_t *rb,
		    int op_type,
		    struct ring_buffer_req_t *req)
{
	struct ring_buffer_nap_info_t *nap;
	nap =_rb_get_nap_info(rb, op_type);

	smp_mb();
	do {
		/* done? */
		if (ring_buffer_poll_request(req))
			return 1;

		/* am I a combiner? */
		if (req->flag & RING_BUFFER_REQ_COMBINER)
			return 0;

		/* if I know how to doze, take a nap */
		if (rb->is_blocking)
			_nap_doze(nap, req);
		smp_rmb();
	} while(1);
}

void ring_buffer_req_barrier(struct ring_buffer_req_t *req)
{
	/* fast path without memory barrier */
	if (ring_buffer_poll_request(req))
		return;

	/* slow path with memory barrier */
	smp_mb();
	do {
		if (ring_buffer_poll_request(req))
			break;
		smp_rmb();
	} while(1);
}
EXPORT_SYMBOL(ring_buffer_req_barrier);

static inline
void exec_op(struct ring_buffer_t *rb, int op_type,
	     struct ring_buffer_req_t *req)
{
	if (op_type == RING_BUFFER_OP_PUT)
		__ring_buffer_put(rb, req);
	else
		__ring_buffer_get(rb, req);
}

static inline
int need_peek(struct ring_buffer_req_t *r)
{
	return r->rc == -EAGAIN && r->flag & RING_BUFFER_REQ_BLOCKING;
}

#ifndef RING_BUFFER_TWO_LOCK
static
void ring_buffer_op(struct ring_buffer_t *rb,
		    volatile struct ring_buffer_req_t **req_tail,
		    int op_type,
		    struct ring_buffer_req_t *req)
{
	struct ring_buffer_req_t *r, *r_next;
	struct ring_buffer_nap_info_t *nap;
	int max_combining, i;

	/* insert a request to the wait list */
	if ( !ask_request(req_tail, req) ) {
		/* non-blocking mode */
		if (req->flag & RING_BUFFER_REQ_NON_BLOCKING)
			return;
		/* blocking mode */
		if (wait_until_done(rb, op_type, req))
			return;
	}

	/* yes, I am a combiner.
	 * load fresh control variables
	 * and perform prefn() */
	r = req;
	max_combining = RING_BUFFER_MAX_COMBINING;
	nap =_rb_get_nap_info(rb, op_type);
combine_more:
	for (i = max_combining - 1; i >= 0; r = r_next, --i) {
	retry_exec_op:
		/* perform operation for a waiter */
		exec_op(rb, op_type, r);

		/* for blocking ring buffer */
		if (rb->is_blocking) {
			/* need peek? */
			if ( need_peek(r) ) {
				/* if mine is already served,
				 * hand over the combiner role to the next. */
				if (r != req)
					goto hand_over;

				/* if peek is successful, retry exec_op */
				if ( _nap_peek(rb, op_type, nap) >= 0)
					goto retry_exec_op;
			}
			/* need to wait up pending waiters? */
			else if (r == req)
				_nap_wake_up_all_waiters(rb, op_type, nap);
		}

		/* we need wmb() here to wake up the waiting thread
		 * only after all values are properly set up. */
		smp_wmb_tso();

		/* is it the last one? */
		if ( unlikely(!(r_next =
				(struct ring_buffer_req_t *)r->__next)) ) {
			/* reflect my updates */
			rb_wmb(rb, op_type);

			/* r is the last one in the waiting list. */
			if ( unlikely(smp_cas(req_tail, r, NULL)) ) {
				/* let the waiter go */
				r->flag |= RING_BUFFER_REQ_DONE;
				return;
			}

			/* if another request is partially inserted
			 * -- tail is smp_swap()-ed
			 * but oldtail->__next is not updated,
			 * wait until it is fully inserted. */
			smp_mb();
			while(!(r_next =
				(struct ring_buffer_req_t *)r->__next)) {
				smp_rmb();
			}
		}

		/* let the waiter go */
		r->flag |= RING_BUFFER_REQ_DONE;
	}

	/* a non-blocking waiter cannot be a comibner. */
	if (r->flag & RING_BUFFER_REQ_NON_BLOCKING) {
		max_combining = (max_combining >> 1) + 1;
		goto combine_more;
	}

hand_over:
	/* reflect my updates */
	rb_wmb(rb, op_type);

	/* ok, a blocking waiter is eligible to be a combiner. */
	r->flag |= RING_BUFFER_REQ_COMBINER;
}
#endif /* RING_BUFFER_TWO_LOCK */

static
int __ring_buffer_put_align_size(struct ring_buffer_t *rb,
				 struct ring_buffer_req_t *req)
{
	/* cacheline alignment for faster DMA
	 * and to completely remove false sharing
	 * among elements. */
	req->__size  = (req->size +
			sizeof(struct ring_buffer_elm_t) +
			rb->align_mask) & ~rb->align_mask;
	if ( unlikely(rb->size < req->__size) ) {
		req->rc = -EINVAL;
		req->flag |= RING_BUFFER_REQ_DONE;
		return req->rc;
	}

	return 0;
}

int ring_buffer_put(struct ring_buffer_t *rb, struct ring_buffer_req_t *req)
{
	/* align request size */
	if (__ring_buffer_put_align_size(rb, req))
		goto out;

	/* perform a put operation */
#ifndef RING_BUFFER_TWO_LOCK
	ring_buffer_op(rb, &rb->put_req, RING_BUFFER_OP_PUT, req);
#else
	spinlock_lock(&rb->put_lock); {
		__ring_buffer_put(rb, req);
	} spinlock_unlock(&rb->put_lock);
#endif
out:
	return req->rc;
}
EXPORT_SYMBOL(ring_buffer_put);

static inline
void __ring_buffer_do(struct ring_buffer_t *rb,
		      int op_type,
		      struct ring_buffer_req_t *req)
{
        if (op_type == RING_BUFFER_OP_GET)
		__ring_buffer_get(rb, req);
	else {
		if (!__ring_buffer_put_align_size(rb, req))
			__ring_buffer_put(rb, req);
	}
}

static
int ring_buffer_op_nolock(struct ring_buffer_t *rb, int op_type,
			  struct ring_buffer_req_t *req)
{
	__ring_buffer_do(rb, op_type, req);

	/* for blocking ring buffer */
	if (rb->is_blocking) {
		int blocked = 0;
		struct ring_buffer_nap_info_t *nap;
		nap =_rb_get_nap_info(rb, op_type);

		/* need peek? */
		while ( need_peek(req) ) {
			/* if something goes wrong, break */
			if ( _nap_peek(rb, op_type, nap) < 0)
				break;

			/* retry operation */
			__ring_buffer_do(rb, op_type, req);

			blocked = 1;
		}

		/* wake up path */
		if (blocked)
			_nap_wake_up_all_waiters_nolock(rb, op_type, nap);
	}

	/* reflect my updates */
	rb_wmb(rb, op_type);
	return req->rc;
}

int ring_buffer_put_nolock(struct ring_buffer_t *rb, struct ring_buffer_req_t *req)
{
	return ring_buffer_op_nolock(rb, RING_BUFFER_OP_PUT, req);
}
EXPORT_SYMBOL(ring_buffer_put_nolock);

int ring_buffer_get(struct ring_buffer_t *rb, struct ring_buffer_req_t *req)
{
	/* perform a get operation */
#ifndef RING_BUFFER_TWO_LOCK
	ring_buffer_op(rb, &rb->get_req, RING_BUFFER_OP_GET, req);
#else
	spinlock_lock(&rb->get_lock); {
		__ring_buffer_get(rb, req);
	} spinlock_unlock(&rb->get_lock);
#endif
	return req->rc;
}
EXPORT_SYMBOL(ring_buffer_get);

int ring_buffer_get_nolock(struct ring_buffer_t *rb, struct ring_buffer_req_t *req)
{
	return ring_buffer_op_nolock(rb, RING_BUFFER_OP_GET, req);
}
EXPORT_SYMBOL(ring_buffer_get_nolock);

void ring_buffer_elm_set_ready(struct ring_buffer_t *rb, void *data)
{
	struct ring_buffer_elm_t *elm;

	rb = rb; /* to make compiler happy */

	/* check whether you pass the right data pointer */
	_check_fingerprint(data);

	/* set ready */
	elm = (struct ring_buffer_elm_t *)(data - sizeof(*elm));
	smp_wmb_tso(); {
		rb_assert(elm->status & RING_BUFFER_ELM_STATUS_INIT,
			  "invalid status");
		rb_assert(!(elm->status & RING_BUFFER_ELM_STATUS_TOMBSTONE),
			  "a tombstone cannot be set ready");
#ifdef RING_BUFFER_CONF_NO_DOUBLE_MMAP
		/* if elm is over PCIe bus,
		 * read-modify-write is 3x slower than write. */
		elm->status |= RING_BUFFER_ELM_STATUS_READY;
#else
		elm->status  = RING_BUFFER_ELM_STATUS_READY;
#endif /* RING_BUFFER_CONF_NO_DOUBLE_MMAP */
	} smp_wmb_tso();
}
EXPORT_SYMBOL(ring_buffer_elm_set_ready);

void ring_buffer_elm_set_done(struct ring_buffer_t *rb, void *data)
{
	struct ring_buffer_elm_t *elm;

	rb = rb; /* to make compiler happy */

	/* check whether you pass the right data pointer */
	_check_fingerprint(data);

	/* set done */
	elm = (struct ring_buffer_elm_t *)(data - sizeof(*elm));
	smp_wmb_tso(); {
#ifdef RING_BUFFER_CONF_NO_DOUBLE_MMAP
		if (unlikely(elm->status &
			     RING_BUFFER_ELM_STATUS_TOMBSTONE_OWNER)) {
			void *tail = rb->buff + rb->size;
			int offset = rb->buff - ((void *)elm - elm->__size);
			struct ring_buffer_elm_t *tombstone = tail - offset;

			rb_assert(tombstone->status & RING_BUFFER_ELM_STATUS_TOMBSTONE,
				  "the previous element of a tombstone owner "
				  "should be a tombstone.");
			tombstone->status = RING_BUFFER_ELM_STATUS_TOMBSTONE |
				            RING_BUFFER_ELM_STATUS_READY |
				            RING_BUFFER_ELM_STATUS_DONE;
		}
#endif /* RING_BUFFER_CONF_NO_DOUBLE_MMAP */
		rb_assert(elm->status & RING_BUFFER_ELM_STATUS_READY,
			  "invalid status");
		rb_assert(!(elm->status & RING_BUFFER_ELM_STATUS_TOMBSTONE),
			  "a tombstone cannot be set done");
		elm->status = RING_BUFFER_ELM_STATUS_DONE;
	} smp_wmb_tso();
}
EXPORT_SYMBOL(ring_buffer_elm_set_done);

int copy_from_ring_buffer(struct ring_buffer_t *rb,
			  void *dest_mem, const void *src_rb, size_t n)
{
	rb = rb; /* to make compiler happy */
	rb_assert(rb->size > n, "copy size is too large");
	__rb_memcpy(dest_mem, src_rb, n);
	return 0;
}
EXPORT_SYMBOL(copy_from_ring_buffer);

int copy_to_ring_buffer(struct ring_buffer_t *rb,
			void *dest_rb, const void *src_mem, size_t n)
{
	rb = rb; /* to make compiler happy */
	rb_assert(rb->size > n, "copy size is too large");
	__rb_memcpy(dest_rb, src_mem, n);
	return 0;
}
EXPORT_SYMBOL(copy_to_ring_buffer);

void rb_print_stack_trace(void) {
#ifndef RING_BUFFER_CONF_KERNEL
	/*
	 * quick and dirty backtrace implementation
	 * - http://stackoverflow.com/questions/4636456/how-to-get-a-stack-trace-for-c-using-gcc-with-line-number-information
	 */
	char pid_buf[30];
	char name_buf[512];
	int child_pid;

	sprintf(pid_buf, "%d", getpid());
	name_buf[readlink("/proc/self/exe", name_buf, 511)] = 0;
	child_pid = fork();

	if (!child_pid) {
		dup2(2, 1); /* redirect output to stderr */
		fprintf(stdout, "stack trace for %s pid=%s\n",
			name_buf, pid_buf);
		execlp("gdb", "gdb", "--batch", "-n", "-ex", "thread",
		       "-ex", "bt", name_buf, pid_buf, NULL);
		fprintf(stdout, "gdb is not installed. ");
		fprintf(stdout, "Please, install gdb to see stack trace.");
		abort(); /* If gdb failed to start */
	} else
		waitpid(child_pid, NULL, 0);
#else  /* RING_BUFFER_CONF_KERNEL */
	dump_stack();
#endif /* RING_BUFFER_CONF_KERNEL */
}
EXPORT_SYMBOL(rb_print_stack_trace);

#ifdef RING_BUFFER_TRACE_EVENT
static
char *_get_current_time_string(char *buff, int len)
{
# ifndef RING_BUFFER_CONF_KERNEL
	time_t t = time(NULL);
	struct tm *tmp = localtime(&t);

	if (tmp)
		strftime(buff, len, "%I:%M:%S %p", tmp);
	else
		snprintf(buff, len, "time error!");
	buff[len - 1] = '\0';

	return buff;
# else  /* RING_BUFFER_CONF_KERNEL */
	return "";
# endif /* RING_BUFFER_CONF_KERNEL */
}
#endif /* RING_BUFFER_TRACE_EVENT */

static
void _rb_trace_event_blocked(struct ring_buffer_t *rb, int op_type)
{
#ifdef RING_BUFFER_TRACE_EVENT
	char buff[256];

	rb_trace("%s is blocked for %s @ %s\n",
		 rb->name,
		 op_type == RING_BUFFER_OP_GET ? "get()" : "put()",
		 _get_current_time_string(buff, sizeof(buff)));
#else
	rb = rb, op_type = op_type; /* to make compiler happy */
#endif /* RING_BUFFER_TRACE_EVENT */
}

static
void _rb_trace_event_unblocked(struct ring_buffer_t *rb, int op_type)
{
#ifdef RING_BUFFER_TRACE_EVENT
	char buff[256];

	rb_trace("%s is woken up for %s @ %s\n",
		 rb->name,
		 op_type == RING_BUFFER_OP_GET ? "get()" : "put()",
		 _get_current_time_string(buff, sizeof(buff)));
#else
	rb = rb, op_type = op_type; /* to make compiler happy */
#endif /* RING_BUFFER_TRACE_EVENT */
}

unsigned int ring_buffer_get_compat_vector(void)
{
#ifndef RING_BUFFER_CONF_NO_DOUBLE_MMAP
	return 0;
#else
	return 1;
#endif
}
EXPORT_SYMBOL(ring_buffer_get_compat_vector);


#ifdef RING_BUFFER_CONF_KERNEL
MODULE_AUTHOR("Changwoo Min <changwoo@gatech.edu>");
MODULE_DESCRIPTION("PCI Ring Buffer (PRB)");
MODULE_LICENSE("GPL");
#endif /* RING_BUFFER_CONF_KERNEL */
