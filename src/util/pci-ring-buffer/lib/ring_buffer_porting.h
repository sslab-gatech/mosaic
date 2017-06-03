#ifndef _RING_BUFFER_PORTING_H_
#define _RING_BUFFER_PORTING_H_

#ifndef RING_BUFFER_CONF_KERNEL
# include <sys/types.h>
# include <sys/stat.h>
# include <sys/mman.h>
# include <sys/wait.h>
# include <fcntl.h>
# include <unistd.h>
# include <stdio.h>
# include <stdlib.h>
# include <time.h>
# include <string.h>
# include <assert.h>
# define __STDC_FORMAT_MACROS
# include <inttypes.h>
# include <sys/mman.h>
# include <sched.h>
# include <pthread.h>
# include <errno.h>
#else
# include <linux/kernel.h>
# include <linux/kthread.h>
# include <linux/string.h>
# include <linux/vmalloc.h>
# include <linux/slab.h>
# include <linux/module.h>
# include <asm/i387.h>
# include <asm/io.h>
# include <asm/cacheflush.h>
# include <asm/pgtable.h>
# include <asm/tlbflush.h>
# include <asm/pgalloc.h>
# include <asm/pat.h>
#endif /* RING_BUFFER_CONF_KERNEL */
#if !defined(MOSAIC_HOST_ONLY)
#include <scif.h> /* XXX: need to separate out scif-related ones */
#endif

#ifdef __cplusplus
extern "C" {
#endif

struct mmap_info_t;

#ifndef RING_BUFFER_CONF_KERNEL
static inline
void *__rb_memcpy(void *dest, const void *src, size_t n)
{
	return memcpy(dest, src, n);
}

static inline
void *__rb_malloc(size_t size)
{
        return malloc(size);
}

static inline
void __rb_free(void *ptr)
{
        free(ptr);
}

static inline
void *__rb_calloc(size_t nmemb, size_t size)
{
        return calloc(nmemb, size);
}

static inline
int __rb_mutex_init(pthread_mutex_t *mutex)
{
	return pthread_mutex_init(mutex, NULL);
}

static inline
int __rb_mutex_destroy(pthread_mutex_t *mutex)
{
	return pthread_mutex_destroy(mutex);
}

static inline
int __rb_mutex_lock(pthread_mutex_t *mutex)
{
	return pthread_mutex_lock(mutex);
}

static inline
int __rb_mutex_unlock(pthread_mutex_t *mutex)
{
	return pthread_mutex_unlock(mutex);
}

static inline
int __rb_wait_init(pthread_cond_t *wait)
{
	return pthread_cond_init(wait, NULL);
}

static inline
int __rb_wait_destroy(pthread_cond_t *wait)
{
	return pthread_cond_destroy(wait);
}

static inline
int __rb_wait_wake_up(pthread_cond_t *wait)
{
	return pthread_cond_signal(wait);
}

static inline
int __rb_wait_wake_up_all(pthread_cond_t *wait)
{
	return pthread_cond_broadcast(wait);
}

static inline
int __rb_wait_sleep_on(pthread_cond_t *wait, pthread_mutex_t *mutex)
{
	return pthread_cond_wait(wait, mutex);
}

static inline
void __rb_yield(void)
{
	sched_yield();
}

#if !defined(MOSAIC_HOST_ONLY)
static inline
int __rbs_conv_scif_ret(int rc)
{
	return (rc >= 0) ? rc : -errno;
}

static inline
void *__rbs_scif_mmap(void *addr, size_t len, int prot_flags,
		      int map_flags, scif_epd_t epd, off_t offset,
		      struct mmap_info_t *mmap_info)
{
	return scif_mmap(addr, len, prot_flags, map_flags, epd, offset);
}


static inline
int __rbs_scif_munmap(void *addr, size_t len, struct mmap_info_t *mmap_info)
{
	return scif_munmap(addr, len);
}

static inline
int __rbs_scif_vwriteto(scif_epd_t epd, void *addr, size_t len, off_t offset,
			int rma_flags)
{
	return scif_vwriteto(epd, addr, len, offset, rma_flags);
}

static inline
int __rbs_scif_vreadfrom(scif_epd_t epd, void *addr, size_t len, off_t offset,
			 int rma_flags)
{
	return scif_vreadfrom(epd, addr, len, offset, rma_flags);
}
#endif

#else /* RING_BUFFER_CONF_KERNEL */
static inline
void *__rb_memcpy(void *dest, const void *src, size_t n)
{
	return memcpy(dest, src, n);
}

static inline
void *__rb_malloc(size_t size)
{
        return kmalloc(size, GFP_KERNEL);
}

static inline
void __rb_free(void *ptr)
{
        kfree(ptr);
}

static inline
void *__rb_calloc(size_t nmemb, size_t size)
{
        return kzalloc(nmemb * size, GFP_KERNEL);
}

static inline
int __rb_mutex_init(struct mutex *mutex)
{
	mutex_init(mutex);
	return 0;
}

static inline
int __rb_mutex_destroy(struct mutex *mutex)
{
	mutex_destroy(mutex);
	return 0;
}

static inline
int __rb_mutex_lock(struct mutex *mutex)
{
	mutex_lock(mutex);
	return 0;
}

static inline
int __rb_mutex_unlock(struct mutex *mutex)
{
	mutex_unlock(mutex);
	return 0;
}

static inline
int __rb_wait_init(wait_queue_head_t *wait)
{
	init_waitqueue_head(wait);
	return 0;
}

static inline
int __rb_wait_destroy(wait_queue_head_t *wait)
{
	return 0;
}

static inline
int __rb_wait_wake_up(wait_queue_head_t *wait)
{
	wake_up(wait);
	return 0;
}

static inline
int __rb_wait_wake_up_all(wait_queue_head_t *wait)
{
	wake_up_all(wait);
	return 0;
}

static inline
int __rb_wait_sleep_on(wait_queue_head_t *wait, struct mutex *mutex)
{
	interruptible_sleep_on(wait);
	return 0;
}

static inline
void __rb_yield(void)
{
	schedule();
}

static inline
int __rbs_conv_scif_ret(int rc)
{
	return rc;
}

void *__rbs_scif_mmap(void *addr, size_t len, int prot_flags,
		      int map_flags, scif_epd_t epd, off_t offset,
		      struct mmap_info_t *mmap_info);
int __rbs_scif_munmap(void *addr, size_t len,
		      struct mmap_info_t *mmap_info);
int __rbs_scif_vwriteto(scif_epd_t epd, void *addr, size_t len,
			off_t roffset, int rma_flags);
int __rbs_scif_vreadfrom(scif_epd_t epd, void *addr, size_t len,
			 off_t roffset, int rma_flags);
#endif /* RING_BUFFER_CONF_KERNEL */

#ifdef __cplusplus
}
#endif
#endif /* _RING_BUFFER_PORTING_H_ */
