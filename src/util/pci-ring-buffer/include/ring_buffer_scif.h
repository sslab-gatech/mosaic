#ifndef _RING_BUFFER_SCIF_H_
#define _RING_BUFFER_SCIF_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <scif.h>
#include <ring_buffer_common.h>

/*
 * ring buffer across scif
 */
struct ring_buffer_t;

struct conn_endp_t {
	union  {
		scif_epd_t epd;     /* endpoint */
		void *     __dummy; /* hack to circumvent scif_epd_t size difference
				     * at kernel: scif_epd_t = void *
				     * at user:   scif_epd_t = int
				     */
	};
};

struct scif_range;
struct mmap_info_t {
	struct scif_range *range;
};

struct ring_buff_scif_info_t {
	off_t                offset; /* scif-registered offset */
	size_t               size;   /* size of scif-registered window
				      * i.e., scif->size
				      *       = __rb_map_size(rbs->rb)
				      *       = rb->size + PAGE_SIZE */
	int                  type;   /* producer or consumer */
	unsigned int         compat; /* compatibility vector */
	struct conn_endp_t client;   /* remote endpoint */
} __attribute__((__packed__));

struct ring_buffer_scif_t {
	struct ring_buffer_t *rb;    /* ring buffer object */
	struct ring_buff_scif_info_t scif[RING_BUFFER_SCIF_NUM_DMA_CHANNEL];
	struct conn_endp_t server;   /* server endpoint for listening */
	int        wait_status;      /* return code of waiting for shadow */
	int        is_master;        /* master or slave */
	struct mmap_info_t mmap_info; /* mmap info */
} ____cacheline_aligned;


/*
 * ring buffer scif API
 */
int __ring_buffer_scif_create_master(
	const char *where, unsigned int line, const char *var,
	size_t size_hint, size_t align,
	int is_blocking, int type,
	ring_buffer_reap_cb_t reap_cb, void* reap_cb_arg,
	struct ring_buffer_scif_t *rbs);

#define ring_buffer_scif_create_master(size_hint, align, is_blocking, type, \
				       reap_cb, reap_cb_arg, rbs)	\
	__ring_buffer_scif_create_master(__func__, __LINE__, #rbs, \
					 size_hint, align, is_blocking, \
					 type, reap_cb, reap_cb_arg, rbs)
void ring_buffer_scif_destroy_master(struct ring_buffer_scif_t *rbs);
int  ring_buffer_scif_wait_for_shadow(struct ring_buffer_scif_t *rbs,
				      int local_port, int blocking);

int __ring_buffer_scif_create_shadow(
	const char *where, unsigned int line, const char *var,
	int local_port, int remote_node, int remote_port,
	ring_buffer_reap_cb_t reap_cb, void* reap_cb_arg,
	struct ring_buffer_scif_t *rbs);
#define ring_buffer_scif_create_shadow(local_port, remote_node, remote_port, \
				       reap_cb, reap_cb_arg, rbs)	     \
	__ring_buffer_scif_create_shadow(__func__, __LINE__, #rbs, \
					 local_port, remote_node, remote_port, \
					 reap_cb, reap_cb_arg, rbs)
void ring_buffer_scif_destroy_shadow(struct ring_buffer_scif_t *rbs);

int  ring_buffer_scif_put(struct ring_buffer_scif_t *rbs,
			  struct ring_buffer_req_t *req);
int  ring_buffer_scif_get(struct ring_buffer_scif_t *rbs,
			  struct ring_buffer_req_t *req);
int  ring_buffer_scif_put_nolock(struct ring_buffer_scif_t *rbs,
				 struct ring_buffer_req_t *req);
int  ring_buffer_scif_get_nolock(struct ring_buffer_scif_t *rbs,
				 struct ring_buffer_req_t *req);

void ring_buffer_scif_elm_set_ready(struct ring_buffer_scif_t *rbs,
				    void *data);
void ring_buffer_scif_elm_set_done(struct ring_buffer_scif_t *rbs,
				   void *data);

int  copy_from_ring_buffer_scif(struct ring_buffer_scif_t *rbs,
				void *dest_mem, const void *src_rbs, size_t n);
int  copy_to_ring_buffer_scif(struct ring_buffer_scif_t *rbs,
			      void *dest_rbs, const void *src_mem, size_t n);

int    ring_buffer_scif_is_empty(struct ring_buffer_scif_t *rbs);
int    ring_buffer_scif_is_full(struct ring_buffer_scif_t *rbs);
size_t ring_buffer_scif_free_space(struct ring_buffer_scif_t *rbs);
#ifdef __cplusplus
}
#endif
#endif /* _RING_BUFFER_SCIF_H_ */
