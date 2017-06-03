#include <ring_buffer_scif.h>
#include <ring_buffer.h>
#include "ring_buffer_porting.h"
#include "ring_buffer.c" /* Yes, it is amalganated. */


#ifndef RING_BUFFER_CONF_NO_DOUBLE_MMAP
rb_static_assert(RING_BUFFER_SCIF_NUM_DMA_CHANNEL >= 2,
		 "it should be two or more for double mapping");
#endif /* RING_BUFFER_CONF_NO_DOUBLE_MMAP */

rb_static_assert(RING_BUFFER_SCIF_NUM_DMA_CHANNEL ==
		 NUM_DMA_ENGINE,
		 "not optimized number of DMA channels");

static unsigned int _random_salt;

static
void _rbs_init_random_salt(void)
{
	if (!_random_salt)
		_random_salt = rand32_seedless();
}

static
int _rbs_is_healthy(struct ring_buffer_t *rb)
{
	struct ring_buffer_scif_t *rbs = rb->private_value;
	char byte;
	int rc;

	/* if a remote end is not connected,
	 * it is healthy. */
	if (rbs->wait_status == -EAGAIN)
		return 0;

	/* scif does not provide an API to check
	 * if a connection is still alive. */
	rc = scif_recv(rbs->scif[0].client.epd, &byte,
		       sizeof(byte), 0);
	rc = __rbs_conv_scif_ret(rc);

	return rc;
}

int __ring_buffer_scif_create_master(
	const char *where, unsigned int line, const char *var,
	size_t size_hint, size_t align,
	int is_blocking, int type,
	ring_buffer_reap_cb_t reap_cb, void* reap_cb_arg,
	struct ring_buffer_scif_t *rbs)
{
	struct ring_buff_scif_info_t *scif;
	int i, rc;

	/* sanity check */
	if ( (type != RING_BUFFER_SCIF_PRODUCER) &&
	     (type != RING_BUFFER_SCIF_CONSUMER) )
		return -EINVAL;

	/* init random salt */
	_rbs_init_random_salt();

	/* init rbs */
	memset(rbs, 0, sizeof(*rbs));
	rbs->wait_status = -EAGAIN;
	rbs->is_master   = 1;

	/* allocate a ring buffer */
	rc = __ring_buffer_create(where, line, var,
				  size_hint, align, is_blocking,
				  reap_cb, reap_cb_arg, &rbs->rb);
	if (rc)
		goto err_out;
	rbs->rb->private_value = rbs;

	/* init scif-related stuff */
	for (i = 0; i < RING_BUFFER_SCIF_NUM_DMA_CHANNEL; ++i) {
		scif = &rbs->scif[i];
		scif->size = __rb_map_size(rbs->rb);
		scif->type = type;
	}

	/* install health checker */
	rbs->rb->is_healthy = _rbs_is_healthy;
	return 0;
err_out:
	/* clean up a partially created ring buffer */
	ring_buffer_scif_destroy_master(rbs);
	return rc;
}
EXPORT_SYMBOL(__ring_buffer_scif_create_master);

void ring_buffer_scif_destroy_master(struct ring_buffer_scif_t *rbs)
{
	struct ring_buff_scif_info_t *scif;
	int i;

	if (!rbs)
		return;

	/* close connections first for the other end to get an error
	 * closing an endpoint does not affect mappings to remote memory.*/
	for (i = 0; i < RING_BUFFER_SCIF_NUM_DMA_CHANNEL; ++i) {
		scif = &rbs->scif[i];
		if (scif->client.epd &&
		    scif->client.epd != SCIF_OPEN_FAILED) {
			if (scif->offset) {
				scif_unregister(
					scif->client.epd,
					scif->offset,
					scif->size);
			}
			scif_close(scif->client.epd);
		}
	}
	if (rbs->server.epd && rbs->server.epd != SCIF_OPEN_FAILED) {
		scif_close(rbs->server.epd);
	}

	/* then release memories */
	ring_buffer_destroy(rbs->rb);
}
EXPORT_SYMBOL(ring_buffer_scif_destroy_master);

static
void *__wait_for_shadow(void *p)
{
	struct ring_buffer_scif_t *rbs = (struct ring_buffer_scif_t *)p;
	struct ring_buff_scif_info_t *scif;
	struct ring_buff_scif_info_t shadow_scif;
	struct scif_portID peer;
	int shadow_type;
	int i, rc;

	/* the shadow should have an opposite type */
	shadow_type = (rbs->scif[0].type == RING_BUFFER_SCIF_PRODUCER) ?
		RING_BUFFER_SCIF_CONSUMER :
		RING_BUFFER_SCIF_PRODUCER;

	/* establish multiple connections for a single client
	 * for concurrent DMA operations */
	for (i = 0; i < RING_BUFFER_SCIF_NUM_DMA_CHANNEL; ++i) {
		scif = &rbs->scif[i];

		/* blocking wait for a client connection */
		rc = scif_accept(rbs->server.epd, &peer,
				 &scif->client.epd, SCIF_ACCEPT_SYNC);
		rc = __rbs_conv_scif_ret(rc);
		if (rc < 0)
			goto err_out;

		/* register ring buffer for shadow */
		scif->offset = scif_register(
			scif->client.epd,
			(void *)rbs->rb,
			scif->size,
			0,
			SCIF_PROT_READ | SCIF_PROT_WRITE,
#ifndef RING_BUFFER_CONF_KERNEL
			0
#else
			SCIF_MAP_KERNEL
#endif
			);
		rc = __rbs_conv_scif_ret(scif->offset);
		if (rc < 0)
			goto err_out;

		/* set shadow type and client.epd */
		shadow_scif            = *scif;
		shadow_scif.type       = shadow_type;
		shadow_scif.compat     = ring_buffer_get_compat_vector();
		shadow_scif.client.epd = (scif_epd_t)0xdeadbeef;

		/* let the peer know the size of scif_mmap */
		rc = scif_send(scif->client.epd, &shadow_scif,
			       sizeof(shadow_scif), SCIF_SEND_BLOCK);
		rc = __rbs_conv_scif_ret(rc);
		if (rc < 0)
			goto err_out;
	}

	rbs->wait_status = 0;
	return NULL;
err_out:
	rbs->wait_status = rc;
	return NULL;
}

#ifdef RING_BUFFER_CONF_KERNEL
static
int __wait_for_shadow_kernel(void *p)
{
	__wait_for_shadow(p);
	complete_and_exit(NULL, 0);
	return 0;
}
#endif /* RING_BUFFER_CONF_KERNEL */

static
int wait_for_shadow(struct ring_buffer_scif_t *rbs, int blocking)
{
	/* client connection can be made only once */
	if (rbs->wait_status != -EAGAIN)
		goto err_out;

	/* blocking wait */
	if (blocking) {
		__wait_for_shadow(rbs);
		return rbs->wait_status;
	}

	/* non-blocking wait */
#ifndef RING_BUFFER_CONF_KERNEL
	{
		pthread_t wait_thread;
		int rc;
		rc = pthread_create(&wait_thread,
				    NULL,
				    __wait_for_shadow,
				    rbs);
		if (rc) {
			rbs->wait_status = rc;
			goto err_out;
		}
	}
#else
	{
		struct task_struct *wait_thread;
		wait_thread = kthread_run(__wait_for_shadow_kernel,
					  rbs,
					  "rbs-wait-thread");
		if (IS_ERR(wait_thread)) {
			rbs->wait_status = PTR_ERR(wait_thread);
			goto err_out;
		}

	}
#endif /* RING_BUFFER_CONF_KERNEL */
	return 0;
err_out:
	return rbs->wait_status;
}

int ring_buffer_scif_wait_for_shadow(struct ring_buffer_scif_t *rbs,
				     int local_port, int blocking)
{
	int rc;

	/* create a server socket */
	rbs->server.epd = scif_open();
#ifndef RING_BUFFER_CONF_KERNEL
	if (rbs->server.epd == SCIF_OPEN_FAILED) {
		rc = -errno;
		goto err_out;
	}
#else
	if (rbs->server.epd == NULL) {
		rc = -ENOMEM;
		goto err_out;
	}
#endif /* RING_BUFFER_CONF_KERNEL */

	rc = scif_bind(rbs->server.epd, local_port);
	rc = __rbs_conv_scif_ret(rc);
	if (rc < 0)
		goto err_out;

	/* multiple connections for concurrent DMA operations */
	rc = scif_listen(rbs->server.epd,
			 RING_BUFFER_SCIF_NUM_DMA_CHANNEL);
	rc = __rbs_conv_scif_ret(rc);
	if (rc < 0)
		goto err_out;

	/* wait for shadow */
	rc = wait_for_shadow(rbs, blocking);
	return rc;
err_out:
	rb_dbg("fail to wait for a shadow: %d\n", rc);
	return rc;
}
EXPORT_SYMBOL(ring_buffer_scif_wait_for_shadow);

static
void *_rbs_mmap_no_lock(struct ring_buffer_scif_t *rbs)
{
	struct ring_buff_scif_info_t *scif;
	void  *scif_mmap_addr0,  *scif_mmap_addr1;
	void  *mmap_addr, *mmap_addr0, *mmap_addr1;
	size_t mmap_size;
	int retry_flag, retry_count = 0;

	/*
	 *               SCIF mmap() layout
	 *               ==================
	 *
	 *         +- scif->size
	 *        /   = __rb_map_size(rbs->rb) = rb->size + PAGE_SIZE
	 *       /
	 *      /             +- scif->size - PAGE_SIZE
	 *     /             /    = __map_size_to_rb_size(scif->size)
	 *    /             /
	 *   <------------><-------->
	 *   [rb ][rb->size][rb->size]
	 *   ^    ^         ^
	 *   |    |         |
	 *   |    |         +-- scif_mmap_addr1
	 *   |    |
	 *   |    +-- rbs->rb->buff
	 +   |         = __map_addr_to_rb_buff(scif_mmap_addr0)
	 +   |         = scif_mmap_addr0 + PAGE_SIZE
	 +   |
	 *   +-- scif_mmap_addr0
	 *
	 *
	 *               rbs->rb shadowing
	 *               =================
	 *
	 * # master ring buffer
	 *   [rb ][rb->size][rb->size]: master's local memory
	 *    \
	 *     +-- rbs->rb in a master ring buffer
	 *
	 * # shadow ring buffer
	 *   [rb ][rb->size][rb->size]:  master's memory across PCIe bus
	 *   ||||| \
	 *   |||||  \
	 *   |PCI|   \
	 *   |||||    +-- rbs->rb->buff
	 *   |||||
	 *   [rb']: shadow copy of master's rb in shadow's local memory
	 *    \
	 *     +-- rbs->rb in a shadow ring buffer
	 *
	 *   * rb': shadow copy of master's rb allocated by calloc()
	 *     - lazily synchronized by rb_rmb() and rb_wmb()
	 *       to minimize the number of PCIe transactions
	 *       in accessing control variables.
	 *
	 *   * rb: original rb in master ring buffer
	 *     - accessable from the shadow ring buffer
	 *       using  __rb_org(rbs->rb),
	 *       which is rbs->rb->buff - PAGE_SIZE
	 */

	/* get double mmap size */
	scif = &rbs->scif[0];
	mmap_size = __scif_size_to_mmap_size(scif->size);

start_mmap:
	/* init variables */
	retry_count++;
	retry_flag       = 0;
	mmap_addr        = mmap_addr0 = mmap_addr1 = NULL;
	scif_mmap_addr0  = NULL;
	scif_mmap_addr1  = NULL;

	if (retry_count >= RING_BUFFER_DOUBLE_MMAP_RETRY)
		goto err_out;

	/* first, find large enough virtual address space */
#ifndef RING_BUFFER_CONF_KERNEL
	mmap_addr = mmap(NULL,
			 mmap_size,
			 PROT_READ | PROT_WRITE,
			 MAP_ANONYMOUS | MAP_PRIVATE,
			 -1,
			 0);
	if (mmap_addr == MAP_FAILED)
		goto err_out;
	munmap(mmap_addr, mmap_size);
#else
	mmap_addr = NULL;
#endif /*  RING_BUFFER_CONF_KERNEL */

	mmap_addr0 = mmap_addr;
	mmap_addr1 = mmap_addr + scif->size;
	mmap_addr = NULL;

	/* second, scif_mmap() the first half with the first DMA channel */
	scif = &rbs->scif[0];
	scif_mmap_addr0 = __rbs_scif_mmap(mmap_addr0,
					  scif->size,
					  SCIF_PROT_READ | SCIF_PROT_WRITE,
					  0,
					  scif->client.epd,
					  scif->offset,
					  &rbs->mmap_info);
	if (scif_mmap_addr0 == SCIF_MMAP_FAILED)
		goto err_out;
	if (mmap_addr0 && scif_mmap_addr0 != mmap_addr0)
		goto retry_mmap;

#ifndef RING_BUFFER_CONF_NO_DOUBLE_MMAP
	/* third, scif_mmap() the second half with the second DMA channel */
	scif = &rbs->scif[1];
	scif_mmap_addr1 = __rbs_scif_mmap(mmap_addr1,
					  __map_size_to_rb_size(
						  scif->size),
					  SCIF_PROT_READ | SCIF_PROT_WRITE,
					  0,
					  scif->client.epd,
					  __map_offset_to_rb_buff_offset(
					    scif->offset),
					  &rbs->mmap_info);
	if (scif_mmap_addr1 == SCIF_MMAP_FAILED)
		goto err_out;
	if (mmap_addr1 && scif_mmap_addr1 != mmap_addr1)
		goto retry_mmap;
#endif /* RING_BUFFER_CONF_NO_DOUBLE_MMAP */

	/* now, it succeeded in double-scif_mmap() */
out:
	/* here is the only place to exit this function */
	return scif_mmap_addr0;

retry_mmap:
	/* there is a chance of race condition
	 * between mmap()/munmap() and two consecutive scif_mmap() calls.
	 * if such race condition actually happens,
	 * retry until it succeeds. */
	retry_flag = 1;

err_out:
	/* munmap() mmap-ed regions */
	if (scif_mmap_addr1) {
		scif = &rbs->scif[1];
		__rbs_scif_munmap(scif_mmap_addr1,
				  __map_size_to_rb_size(scif->size),
				  &rbs->mmap_info);
	}
	if (scif_mmap_addr0) {
		scif = &rbs->scif[0];
		__rbs_scif_munmap(scif_mmap_addr0, scif->size,
				  &rbs->mmap_info);
	}
#ifndef RING_BUFFER_CONF_KERNEL
	if (mmap_addr) {
		munmap(mmap_addr, mmap_size);
	}
#endif /* RING_BUFFER_CONF_KERNEL */

	/* retry */
	if (retry_flag)
		goto start_mmap;

	/* error return */
	scif_mmap_addr0 = NULL;
	goto out;
}

static
void *_rbs_mmap(struct ring_buffer_scif_t *rbs)
{
#ifndef RING_BUFFER_CONF_KERNEL
	static pthread_mutex_t LOCK = PTHREAD_MUTEX_INITIALIZER;
	void *addr;

	pthread_mutex_lock(&LOCK); {
		addr = _rbs_mmap_no_lock(rbs);
	} pthread_mutex_unlock(&LOCK);
	return addr;
#else
	return _rbs_mmap_no_lock(rbs);
#endif /* RING_BUFFER_CONF_KERNEL */
}

static
void   _rbs_munmap(struct ring_buffer_scif_t *rbs)
{
	void  *mmap_addr = __rb_org(rbs->rb);
	struct ring_buff_scif_info_t *scif = &rbs->scif[0];

#ifndef RING_BUFFER_CONF_NO_DOUBLE_MMAP
	/* first, scif_munmap the second half of a double-mmaped region */
	__rbs_scif_munmap(mmap_addr + scif->size,
			  __map_size_to_rb_size(scif->size),
			  &rbs->mmap_info);
#endif /* RING_BUFFER_CONF_NO_DOUBLE_MMAP */

	/* second, scif_munmap the first half of a double-mmaped region */
	__rbs_scif_munmap(mmap_addr, scif->size, &rbs->mmap_info);
}

int __ring_buffer_scif_create_shadow(
	const char *where, unsigned int line, const char *var,
	int local_port, int remote_node, int remote_port,
	ring_buffer_reap_cb_t reap_cb, void* reap_cb_arg,
	struct ring_buffer_scif_t *rbs)
{
	struct scif_portID remote_addr = {.node = remote_node,
					  .port = remote_port};
	struct ring_buff_scif_info_t master_scif, *scif;
	struct ring_buffer_t *remote_rb;
	void *map_addr;
	int i, rc = 0;

	/* init random salt */
	_rbs_init_random_salt();

	/* init rbs */
	memset(rbs, 0, sizeof(*rbs));
	rbs->rb = __rb_calloc(1, sizeof(struct ring_buffer_t));
	if (!rbs->rb) {
		rc = -ENOMEM;
		goto err_out;
	}

	/* establish multiple connections for concurrent DMA operations */
	for (i = 0; i < RING_BUFFER_SCIF_NUM_DMA_CHANNEL; ++i) {
		scif = &rbs->scif[i];

		/* connect to server */
		scif->client.epd = scif_open();
#ifndef RING_BUFFER_CONF_KERNEL
		if (scif->client.epd == SCIF_OPEN_FAILED) {
			rc = -errno;
			goto err_out;
		}
#else
		if (scif->client.epd == NULL) {
			rc = -ENOMEM;
			goto err_out;
		}
#endif /* RING_BUFFER_CONF_KERNEL */

		rc = scif_bind(scif->client.epd, local_port + i);
		rc = __rbs_conv_scif_ret(rc);
		if (rc < 0)
			goto err_out;

		rc = scif_connect(scif->client.epd, &remote_addr);
		rc = __rbs_conv_scif_ret(rc);
		if (rc < 0)
			goto err_out;

		/* init scif related stuff */
		rc = scif_recv(scif->client.epd, &master_scif,
			       sizeof(master_scif), SCIF_RECV_BLOCK);
		rc = __rbs_conv_scif_ret(rc);
		if (rc < 0)
			goto err_out;
		master_scif.client.epd = scif->client.epd;
		*scif = master_scif;

		/* check if a master and shadow are compatible */
		if (scif->compat != ring_buffer_get_compat_vector()) {
			rc = -ENOPROTOOPT;
			rb_err("Master and shadow ring buffers are NOT compatible: "
			       " master: 0x%x vs. shadow: 0x%x\n",
			       scif->compat, ring_buffer_get_compat_vector());
			goto err_out;
		}
	}

	/* scif_mmap the remote region */
	map_addr = _rbs_mmap(rbs);
	if (map_addr == SCIF_MMAP_FAILED)
		goto err_out;

	/* init shadow ring buffer */
	remote_rb              = map_addr;
	rbs->rb->size          = remote_rb->size;
	rbs->rb->buff          = __map_addr_to_rb_buff(map_addr);
	rbs->rb->align_mask    = remote_rb->align_mask;
	rbs->rb->head          = remote_rb->head;
	rbs->rb->tail          = remote_rb->tail;
	rbs->rb->tail2         = remote_rb->tail2;
	rbs->rb->is_blocking   = remote_rb->is_blocking;
	rbs->rb->private_value = rbs;

	/* init nap time */
	rc = _ring_buffer_init_nap_time(rbs->rb);
	if (rc)
		goto err_out;

	/* instal an user-defined reap callback */
	rbs->rb->reap_cb     = reap_cb;
	rbs->rb->reap_cb_arg = reap_cb_arg;

	/* install health checker */
	rbs->rb->is_healthy = _rbs_is_healthy;

	/* record my name */
	snprintf(rbs->rb->name, RING_BUFFER_NAME_MAX,
		 "%s@%s:%d", var, where, line);
	rbs->rb->name[RING_BUFFER_NAME_MAX - 1] = '\0';

#ifndef RING_BUFFER_CONF_NO_DOUBLE_MMAP
	/* check whether a ring buffer is a really ring */
	int *p, *q;
	p = (int *)rbs->rb->buff;
	q = (int *)(rbs->rb->buff + rbs->rb->size);
	if (p != q) {
		rb_assert(*p == *q,
			  "ring buffer is not a ring");
	}
#endif
	return 0;
err_out:
	/* clean up a partially created ring buffer */
	rb_dbg("fail to create a shadow: %d\n", rc);
	ring_buffer_scif_destroy_shadow(rbs);
	return rc;
}
EXPORT_SYMBOL(__ring_buffer_scif_create_shadow);

void ring_buffer_scif_destroy_shadow(struct ring_buffer_scif_t *rbs)
{
	struct ring_buff_scif_info_t *scif;
	int i;

	if (!rbs)
		return;

	/* close connections first for the other end to get an error
	 * closing an endpoint does not affect mappings to remote memory.*/
	for (i = 0; i < RING_BUFFER_SCIF_NUM_DMA_CHANNEL; ++i) {
		scif = &rbs->scif[i];
		if (scif->client.epd &&
		    scif->client.epd == SCIF_OPEN_FAILED) {
			scif_close(scif->client.epd);
		}
	}

	/* then release resources */
	if (rbs->rb) {
		scif = &rbs->scif[0];
		if (rbs->rb->buff != SCIF_MMAP_FAILED && rbs->rb->buff)
			_rbs_munmap(rbs);
		_ring_buffer_deinit_nap_time(rbs->rb);
		__rb_free(rbs->rb);
	}
	memset(rbs, 0, sizeof(*rbs));
}
EXPORT_SYMBOL(ring_buffer_scif_destroy_shadow);

int  ring_buffer_scif_put(struct ring_buffer_scif_t *rbs,
			  struct ring_buffer_req_t *req)
{
	/* sanity check */
	if ( unlikely(rbs->scif[0].type != RING_BUFFER_SCIF_PRODUCER) )
		return -EOPNOTSUPP;

	/* ring buffer operation */
	return ring_buffer_put(rbs->rb, req);
}
EXPORT_SYMBOL(ring_buffer_scif_put);

int  ring_buffer_scif_put_nolock(struct ring_buffer_scif_t *rbs,
				 struct ring_buffer_req_t *req)
{
	/* sanity check */
	if ( unlikely(rbs->scif[0].type != RING_BUFFER_SCIF_PRODUCER) )
		return -EOPNOTSUPP;

	/* ring buffer operation */
	return ring_buffer_put_nolock(rbs->rb, req);
}
EXPORT_SYMBOL(ring_buffer_scif_put_nolock);

int  ring_buffer_scif_get(struct ring_buffer_scif_t *rbs,
			  struct ring_buffer_req_t *req)
{
	/* sanity check */
	if ( unlikely(rbs->scif[0].type != RING_BUFFER_SCIF_CONSUMER) )
		return -EOPNOTSUPP;

	/* ring buffer operation */
	return ring_buffer_get(rbs->rb, req);
}
EXPORT_SYMBOL(ring_buffer_scif_get);

int  ring_buffer_scif_get_nolock(struct ring_buffer_scif_t *rbs,
				 struct ring_buffer_req_t *req)
{
	/* sanity check */
	if ( unlikely(rbs->scif[0].type != RING_BUFFER_SCIF_CONSUMER) )
		return -EOPNOTSUPP;

	/* ring buffer operation */
	return ring_buffer_get_nolock(rbs->rb, req);
}
EXPORT_SYMBOL(ring_buffer_scif_get_nolock);

void ring_buffer_scif_elm_set_ready(struct ring_buffer_scif_t *rbs,
				    void *data)
{
	/* sanity check: siliently ignore an error */
	if ( unlikely(rbs->scif[0].type != RING_BUFFER_SCIF_PRODUCER) ) {
		rb_assert(0, "set-ready is not allowed");
		return;
	}

	/* set ready */
	ring_buffer_elm_set_ready(rbs->rb, data);
}
EXPORT_SYMBOL(ring_buffer_scif_elm_set_ready);

void ring_buffer_scif_elm_set_done(struct ring_buffer_scif_t *rbs,
				   void *data)
{
	/* sanity check: siliently ignore an error */
	if ( unlikely(rbs->scif[0].type != RING_BUFFER_SCIF_CONSUMER) ) {
		rb_assert(0, "set-done is not allowed");
		return;
	}

	/* set done */
	ring_buffer_elm_set_done(rbs->rb, data);
}
EXPORT_SYMBOL(ring_buffer_scif_elm_set_done);

static inline
int is_DMA_better(struct ring_buffer_scif_t *rbs, size_t n)
{
	/* XXX: a master cannot perform DMA */
	if (rbs->is_master)
		return 0;

	/* if request is too small, memcpy is faster */
	if (n < RING_BUFFER_DMA_OP_THRESHOLD)
		return 0;

	/* otherwise, DMA is faster */
	return 1;
}

static inline
struct ring_buff_scif_info_t *
choose_DMA_channel(struct ring_buffer_scif_t *rbs, unsigned int start_offset)
{
	unsigned int seed, rand_int, channel_id;

	/* >> 6 to neuturalize cacheline alignment */
	seed       = (start_offset >> 6) ^ _random_salt;
	rand_int   = rand32(&seed);
	channel_id = rand_int % RING_BUFFER_SCIF_NUM_DMA_CHANNEL;
	return &rbs->scif[channel_id];
}

int copy_from_ring_buffer_scif(struct ring_buffer_scif_t *rbs,
			       void *dest_mem, const void *src_rbs, size_t n)
{
#if !defined(RING_BUFFER_CONF_KERNEL) || defined(MICSCIF_KERNEL_EXT)
	struct ring_buff_scif_info_t *scif;
	off_t start_offset, end_offset;
	size_t n1, n2;
	int rc;

	rb_assert(rbs->rb->size > n, "copy size is too large");

	/* is DMA better? */
	if ( !is_DMA_better(rbs, n) ) {
		return copy_from_ring_buffer(rbs->rb, dest_mem, src_rbs, n);
	}

	/* it is large enough to initiate DMA operations */
	scif           = &rbs->scif[0];
	start_offset   = src_rbs - (void *)__rb_org(rbs->rb);
	end_offset     = start_offset + n;

	/* if start_offset is already overlapped,
	 * then adjust both start_ and end_offset */
	if ( unlikely((size_t)start_offset >= scif->size) ) {
		start_offset -= rbs->rb->size;
		end_offset   -= rbs->rb->size;
	}
	rb_assert(start_offset < scif->size,
		  "probarbly incorrect address?");

	/* if the range is overlapped, chop it down */
	if ( unlikely((size_t)end_offset > scif->size) ) {
		n2 = end_offset - scif->size;
		n1 = n - n2;
		rc = copy_from_ring_buffer_scif(
			rbs, dest_mem, src_rbs, n1);
		if (rc)
			goto out;
		rc = copy_from_ring_buffer_scif(
			rbs, dest_mem + n1, rbs->rb->buff, n2);
	}
	else {
		/* select a DMA channel */
		scif = choose_DMA_channel(rbs, start_offset);

		/* issue a synchronous DMA operation */
		rc = __rbs_scif_vreadfrom(scif->client.epd,
					  dest_mem, n,
					  scif->offset + start_offset,
					  SCIF_RMA_SYNC);
	}
out:
	return rc;
#else
	return copy_from_ring_buffer(rbs->rb, dest_mem, src_rbs, n);
#endif
}
EXPORT_SYMBOL(copy_from_ring_buffer_scif);

int copy_to_ring_buffer_scif(struct ring_buffer_scif_t *rbs,
			     void *dest_rbs, const void *src_mem, size_t n)
{
#if !defined(RING_BUFFER_CONF_KERNEL) || defined(MICSCIF_KERNEL_EXT)
	struct ring_buff_scif_info_t *scif;
	off_t start_offset, end_offset;
	size_t n1, n2;
	int rc;

	rb_assert(rbs->rb->size > n, "copy size is too large");

	/* is DMA better? */
	if ( !is_DMA_better(rbs, n) ) {
		return copy_to_ring_buffer(rbs->rb, dest_rbs, src_mem, n);
	}

	/* it is large enough to initiate DMA operations */
	scif           = &rbs->scif[0];
	start_offset   = dest_rbs - (void *)__rb_org(rbs->rb);
	end_offset     = start_offset + n;

	/* if start_offset is already overlapped,
	 * then adjust both start_ and end_offset */
	if ( unlikely((size_t)start_offset >= scif->size) ) {
		start_offset -= rbs->rb->size;
		end_offset   -= rbs->rb->size;
	}
	rb_assert(start_offset < scif->size,
		  "probarbly incorrect address?");

	/* if the range is overlapped, chop it down */
	if ( unlikely((size_t)end_offset > scif->size) ) {
		n2 = end_offset - scif->size;
		n1 = n - n2;
		rc = copy_to_ring_buffer_scif(
			rbs, dest_rbs, src_mem, n1);
		if (rc)
			goto out;
		rc = copy_to_ring_buffer_scif(
			rbs, rbs->rb->buff, src_mem + n1, n2);
	}
	else {
		/* select a DMA channel */
		scif = choose_DMA_channel(rbs, start_offset);

		/* issue a synchronous DMA operation */
		rc = __rbs_scif_vwriteto(scif->client.epd,
					 (void *)src_mem, n,
					 scif->offset + start_offset,
					 SCIF_RMA_SYNC);
	}
out:
	return rc;
#else
	return copy_to_ring_buffer(rbs->rb, dest_rbs, src_mem, n);
#endif
}
EXPORT_SYMBOL(copy_to_ring_buffer_scif);

int ring_buffer_scif_is_empty(struct ring_buffer_scif_t *rbs)
{
	return ring_buffer_is_empty(rbs->rb);
}
EXPORT_SYMBOL(ring_buffer_scif_is_empty);

int  ring_buffer_scif_is_full(struct ring_buffer_scif_t *rbs)
{
	return ring_buffer_is_full(rbs->rb);
}
EXPORT_SYMBOL(ring_buffer_scif_is_full);

size_t ring_buffer_scif_free_space(struct ring_buffer_scif_t *rbs)
{
	return ring_buffer_free_space(rbs->rb);
}
EXPORT_SYMBOL(ring_buffer_scif_free_space);
