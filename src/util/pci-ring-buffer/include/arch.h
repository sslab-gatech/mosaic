#ifndef _ARCH_H_
#define _ARCH_H_

#ifndef RING_BUFFER_CONF_KERNEL
# include <stdint.h>
#include <stddef.h>
#else  /* RING_BUFFER_CONF_KERNEL */
# include <linux/types.h>
# include <linux/cache.h>
# include <linux/compiler.h>
#endif /* RING_BUFFER_CONF_KERNEL */

/*
 * machine-, architecture-specific information
 */
#ifndef RING_BUFFER_CONF_KERNEL
# define L1D_CACHELINE_SIZE   64
# define ____cacheline_aligned  __attribute__ (	\
		(aligned (L1D_CACHELINE_SIZE)))
#else
# define L1D_CACHELINE_SIZE   L1_CACHE_BYTES
#endif /* RING_BUFFER_CONF_KERNEL */

#define ____cacheline_aligned2 __attribute__ (		\
		(aligned (2 * L1D_CACHELINE_SIZE)))

#ifndef RING_BUFFER_CONF_KERNEL
# define PAGE_SIZE            4096
# define PAGE_MASK            ~(PAGE_SIZE - 1)
#endif /* RING_BUFFER_CONF_KERNEL */

#define HUGE_PAGE_SIZE       (2 * 1024 * 1024)
#define HUGE_PAGE_MASK       ~(HUGE_PAGE_SIZE - 1)

#define DISK_BLOCK_SIZE      512

#define NUM_DMA_ENGINE       8

/*
 * compiler hints
 */
#ifndef RING_BUFFER_CONF_KERNEL
# define likely(x)       __builtin_expect((long int)(x),1)
# define unlikely(x)     __builtin_expect((long int)(x),0)
#endif /* RING_BUFFER_CONF_KERNEL */

/*
 * memory barrier
 */
#ifndef ACCESS_ONCE
# define ACCESS_ONCE(x) (*(volatile typeof(x) *)&(x))
#endif /* ACCESS_ONCE */

#ifndef RING_BUFFER_CONF_KERNEL
static inline void smp_cmb(void)
{
	__asm__ __volatile__("":::"memory");
}

static inline void smp_wmb_tso(void)
{
	/* in total-store-order architectures,
	 * a write memory barrier is simply
	 * a compiler memory barrier. */
	smp_cmb();
}

static inline void smp_mb(void)
{
#ifdef TARGET_ARCH_K1OM
	smp_cmb();
#else
	__asm__ __volatile__("mfence":::"memory");
#endif
}

static inline void smp_rmb(void)
{
#ifdef TARGET_ARCH_K1OM
	smp_cmb();
#else
	__asm__ __volatile__("lfence":::"memory");
#endif
}

static inline void smp_wmb(void)
{
#ifdef TARGET_ARCH_K1OM
	smp_cmb();
#else
	__asm__ __volatile__("sfence":::"memory");
#endif
}
#else  /* RING_BUFFER_CONF_KERNEL */
# define smp_cmb()     barrier()
# define smp_wmb_tso() smp_cmb()
#endif /* RING_BUFFER_CONF_KERNEL */


/*
 * atomic opeartions
 */
#define smp_cas(__ptr, __old_val, __new_val)	\
	__sync_bool_compare_and_swap(__ptr, __old_val, __new_val)

#define smp_swap(__ptr, __val)			\
	__sync_lock_test_and_set(__ptr, __val)

#define smp_faa(__ptr, __val)			\
	__sync_fetch_and_add(__ptr, __val)

#define smp_prefetchr(__ptr)			\
	__builtin_prefetch((void*)__ptr, 0, 3)

#define smp_prefetchw(__ptr)			\
	__builtin_prefetch((void*)__ptr, 1, 3)

/*
 * rdtsc
 */
static inline uint64_t __attribute__((__always_inline__))
rdtsc(void)
{
	uint32_t a, d;
	__asm __volatile("rdtsc" : "=a" (a), "=d" (d));
	return ((uint64_t) a) | (((uint64_t) d) << 32);
}

static inline uint64_t __attribute__((__always_inline__))
rdtsc_begin(void)
{
	/* Don't let anything float into or out of the TSC region.
	 * (The memory clobber on this is actually okay as long as GCC
	 * knows that no one ever took the address of things it has in
	 * registers.) */
	smp_cmb(); {
	/* See the "Improved Benchmarking Method" in Intel's "How to
	 * Benchmark Code Execution Times on Intel® IA-32 and IA-64
	 * Instruction Set Architectures" */
	uint64_t tsc;
#if defined(__x86_64__)
	/* This generates tighter code than the __i386__ version */
	__asm __volatile("cpuid; rdtsc; shl $32, %%rdx; or %%rdx, %%rax"
			 : "=a" (tsc)
			 : : "%rbx", "%rcx", "%rdx");
#elif defined(__i386__)
	uint32_t a, d;
	__asm __volatile("cpuid; rdtsc; mov %%eax, %0; mov %%edx, %1"
			 : "=r" (a), "=r" (d)
			 : : "%rax", "%rbx", "%rcx", "%rdx");
	tsc = ((uint64_t) a) | (((uint64_t) d) << 32);
#endif
	return tsc;
	} smp_cmb();
}

static inline uint64_t __attribute__((__always_inline__))
rdtsc_end(void)
{
	smp_cmb(); {
	uint32_t a, d;
#ifndef TARGET_ARCH_K1OM
	__asm __volatile("rdtscp; mov %%eax, %0; mov %%edx, %1; cpuid"
			 : "=r" (a), "=r" (d)
			 : : "%rax", "%rbx", "%rcx", "%rdx");
#else
	/* Unfortunately, Xeon Phi does not support rdtscp.
	 * But, fortunately, Xeon Phi does not reorder instructions. */
	__asm __volatile("rdtsc; mov %%eax, %0; mov %%edx, %1; cpuid"
			 : "=r" (a), "=r" (d)
			 : : "%rax", "%rbx", "%rcx", "%rdx");
#endif
	return ((uint64_t) a) | (((uint64_t) d) << 32);
	} smp_cmb();
}


/*
 * spinlock
 */

/* ticket lock */
struct ticketlock_t {
	volatile int now_serving;
	volatile int next_ticket;
};

static inline
void ticketlock_init(struct ticketlock_t *lock)
{
	lock->now_serving = lock->next_ticket = 0;
}

static inline
void ticketlock_lock(struct ticketlock_t *lock)
{
	int my_ticket = smp_faa(&lock->next_ticket, 1);
	while(lock->now_serving != my_ticket) ;
}

static inline
void ticketlock_unlock(struct ticketlock_t *lock)
{
	lock->now_serving++;
}

/* MCS queue lock */
struct mcsqnode_t;

struct mcslock_t {
	volatile struct mcsqnode_t *qnode;
};

struct mcsqnode_t {
	volatile int locked;
	struct mcsqnode_t *next;
};


static inline
void mcslock_init(struct mcslock_t *lock)
{
	lock->qnode = NULL;
	smp_wmb_tso();
}

static inline
void mcslock_lock(struct mcslock_t *lock, struct mcsqnode_t *qnode)
{
	struct mcsqnode_t *prev;

	qnode->locked = 1;
	qnode->next = NULL;
	smp_wmb();

	prev = (struct mcsqnode_t *)smp_swap(&lock->qnode, qnode);
	if (prev) {
		prev->next = qnode;
		smp_wmb();
		while(qnode->locked) ;
	}
}

static inline
void mcslock_unlock(struct mcslock_t *lock, struct mcsqnode_t *qnode)
{
	if (!qnode->next) {
		if (smp_cas(&lock->qnode, qnode, NULL))
			return;
		while (!qnode->next) smp_rmb();
	}
	qnode->next->locked = 0;
	smp_wmb();
}

/*
 * random number generator
 * - https://en.wikipedia.org/wiki/Linear_congruential_generator
 */

static inline
unsigned int rand32(unsigned int *seed)
{
	*seed = *seed * 1103515245 + 12345;
	return *seed & 0x7fffffff;
}

static inline
unsigned int rand32_range(unsigned int *seed,
			  unsigned int low,
			  unsigned int high)
{
	return low + (rand32(seed) % (high - low + 1));
}

static inline
unsigned int rand32_seedless(void)
{
	volatile uint64_t salt = rdtsc() ^ (((uint64_t)&salt) >> 3);
	unsigned int seed = (unsigned int)salt;

	return rand32(&seed);
}
#endif /* _ARCH_H_ */
