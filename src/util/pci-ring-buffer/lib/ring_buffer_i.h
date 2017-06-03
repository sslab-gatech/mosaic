#ifndef _RING_BUFFER_I_H_
#define _RING_BUFFER_I_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "ring_buffer_porting.h"

/*
 * configurable parameters
 */
#define RING_BUFFER_START_OFFSET_COLORING
#define RING_BUFFER_MAX_COMBINING     128
#define RING_BUFFER_DOUBLE_MMAP_RETRY  16

#ifdef TARGET_ARCH_K1OM
#define RING_BUFFER_DMA_OP_THRESHOLD  (32 * 1024) /* xeon phi */
#else
#define RING_BUFFER_DMA_OP_THRESHOLD  1024        /* xeon */
#endif /* TARGET_ARCH_K1OM */

/*
 * internal macros
 */
#define __map_addr_to_rb_buff(__addr)    ( ((void *)(__addr)) + PAGE_SIZE )
#define __map_offset_to_rb_buff_offset(__offset)  \
                                         ( (__offset) + PAGE_SIZE )
#define __map_size_to_rb_size(__size)    ( (__size) - PAGE_SIZE )
#define __rb_org(__rb)                   ( (__rb)->buff - PAGE_SIZE )
#define __rb_map_size(__rb)              ( (__rb)->size + PAGE_SIZE )
#define __rb_is_shadow(__rb)             ( (void *)(__rb) != __rb_org(__rb) )
#define __rb_size_to_shm_size(__size)    ( (__size) + PAGE_SIZE )

#ifndef RING_BUFFER_CONF_NO_DOUBLE_MMAP
# define __rb_size_to_mmap_size(__size)   ( ((__size) * 2) + PAGE_SIZE )
# define __scif_size_to_mmap_size(__size) ( ((__size) * 2) - PAGE_SIZE )
#else  /* RING_BUFFER_CONF_NO_DOUBLE_MMAP */
# define __rb_size_to_mmap_size(__size)   ( (__size) + PAGE_SIZE )
# define __scif_size_to_mmap_size(__size) (__size)
#endif /* RING_BUFFER_CONF_NO_DOUBLE_MMAP */

#ifndef RING_BUFFER_CONF_KERNEL
#define EXPORT_SYMBOL(sym)
#endif /* RING_BUFFER_CONF_KERNEL */


/*
 * element in a ring buffer
 */
struct ring_buffer_t;
struct ring_buffer_elm_t {
	unsigned int __size;            /* internal aligned size */
	unsigned short padding;         /* padding size for alignment */
	volatile unsigned short status; /* status flags of an element */
} __attribute__((__packed__));

#define RING_BUFFER_ELM_STATUS_INIT            0x0be0
#define RING_BUFFER_ELM_STATUS_TOMBSTONE       0x1000
#define RING_BUFFER_ELM_STATUS_TOMBSTONE_OWNER 0x2000
#define RING_BUFFER_ELM_STATUS_READY           0x0001
#define RING_BUFFER_ELM_STATUS_DONE            0x0002

/*
 * op code
 */
#define RING_BUFFER_OP_GET    0x0
#define RING_BUFFER_OP_PUT    0x1

/*
 * nap-related intrnal API
 */
int  _ring_buffer_init_nap_time(struct ring_buffer_t *rb);
void _ring_buffer_deinit_nap_time(struct ring_buffer_t *rb);

/*
 * logging and debug macros
 */
void rb_print_stack_trace(void);

#define rb_static_assert(__c, __m) typedef		\
	int ___rb_static_assert___##__LINE__[(__c) ? 1 : -1]

#ifndef RING_BUFFER_CONF_KERNEL
# define print_out(...) fprintf(stdout, __VA_ARGS__)
# define print_err(...) fprintf(stderr, __VA_ARGS__)
#else  /* RING_BUFFER_CONF_KERNEL */
# define print_out(...) printk(KERN_INFO __VA_ARGS__)
# define print_err(...) printk(KERN_ERR  __VA_ARGS__)
#endif /* RING_BUFFER_CONF_KERNEL */

#ifndef RING_BUFFER_ASSERT
# define rb_assert(__cond, __msg)
#else /* RING_BUFFER_ASSERT */
# define rb_assert(__cond, __msg) if (!(__cond)) {	\
		int *__p = NULL;			\
		print_err("\033[91m");			\
		print_err(				\
			"[RB-ASSERT:%s:%d] %s\n",	\
			__func__, __LINE__,		\
			__msg);				\
                print_err("\033[92m");			\
		rb_print_stack_trace();			\
                print_err("\033[0m");			\
		*__p = 0;				\
	}
#endif /* RING_BUFFER_ASSERT */

#ifndef RING_BUFFER_DEBUG
# define rb_dbg(fmt, ...)
# define rb_here()
#else
# define rb_dbg(__fmt, ...)				\
	print_err(					\
		"[RB-DBG:%s:%d] " __fmt,		\
		__func__, __LINE__, __VA_ARGS__)
# define rb_here()				\
	print_err(				\
		"[RB-HERE:%s:%d] <== \n",	\
		__func__, __LINE__)
#endif /* RING_BUFFER_DEBUG */

#define rb_log(__fmt, ...)				\
	print_out(					\
		"[RB-LOG:%s:%d] " __fmt,		\
		__func__, __LINE__, __VA_ARGS__)

#define rb_err(__fmt, ...)				\
	print_err(					\
		"[RB-ERR:%s:%d] " __fmt,		\
		__func__, __LINE__, __VA_ARGS__)

#define rb_test(__cond, __msg) do {				\
		print_out(					\
			"%s[RB-TEST:%s:%d] [%s] %s%s\n",	\
			(__cond) ? "\033[92m" : "\033[91m",	\
			__func__, __LINE__,			\
			(__cond) ? "PASS" : "FAIL",		\
			__msg,					\
			"\033[0m");				\
	} while(0)

#define rb_trace(__fmt, ...) do {				\
		print_err("\033[93m");				\
		print_err(					\
			"[RB-TRACE] " __fmt, __VA_ARGS__);	\
		print_err("\033[0m\n");				\
	} while(0)

#define rb_dump(__rb) do {						\
		rb_log("  rb: name(%s)    size(%d)    buff(%p)    head(%d, %p)    tail(%d, %p)    tail2(%d, %p)\n", \
		       (__rb)->name,					\
		       (int)(__rb)->size, (__rb)->buff,			\
		       (int)(__rb)->head, (__rb)->buff + (__rb)->head,	\
		       (int)(__rb)->tail, (__rb)->buff + (__rb)->tail,	\
		       (int)(__rb)->tail2,(__rb)->buff + (__rb)->tail2); \
	} while(0)

#define rb_get_req_dump(__req, __buff) do {					\
		rb_log("  req: flag: 0x%x    size: %d    data: (%d, %p)    rc: %d\n", \
		       (__req)->flag, (int)(__req)->size,		\
		       (int)((__req)->data - (__buff)), (__req)->data,	\
		       (__req)->rc);					\
        } while(0)

#define rb_req_dump(__req) do {					\
		rb_log("  req: flag: 0x%x    size: %d    data: %p    rc: %d\n", \
		       (__req)->flag, (int)(__req)->size,		\
		       (__req)->data,					\
		       (__req)->rc);					\
        } while(0)

static inline
void dump_proc_maps(void)
{
#ifndef RING_BUFFER_CONF_KERNEL
	pid_t pid = getpid();
	char cat_maps[128];
	sprintf(cat_maps, "cat /proc/%d/maps", pid);
	if ( system(cat_maps) ) {}
#endif /* RING_BUFFER_CONF_KERNEL */
}

#ifdef __cplusplus
}
#endif
#endif /* _RING_BUFFER_I_H_ */
