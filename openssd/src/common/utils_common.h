#ifndef __UTILS_COMMON_H
#define __UTILS_COMMON_H

#include <stdint.h>
#include <stddef.h>
#include "xtime_l.h"
#include "xil_cache.h"
#include "config.h"

#define GET_ARM_V8_CYCLE_COUNT(var) do { asm volatile("mrs %0, PMCCNTR_EL0" : "=r" (var) : : "memory"); } while (0)

#define ALIGN_FLOOR(n, align) ((n) / (align) * (align))
#define ALIGN_CEILING(n, align) (((n) + (align) - 1) / (align) * (align))

#define NS2CYCLE(ns) (uint64_t)((double)(ns) * COUNTS_PER_SECOND / 1e9)

#define COMPILER_BARRIER() do { __asm__ volatile ("" : : : "memory"); } while (0)
#define MEMORY_BARRIER() do { __asm__ volatile ("dmb sy" : : : "memory"); } while (0)
#define MEMORY_BARRIER_STRONG() do { __asm__ volatile ("dsb sy" : : : "memory"); } while (0)

#define FLUSH_CACHE(addr, size) \
    do { \
        MEMORY_BARRIER_STRONG(); \
        Xil_DCacheFlushRange((UINTPTR)(addr), (UINTPTR)(size)); \
    } while (0)

void wait_cpu0_up();

void linear_malloc_init(uintptr_t start, uintptr_t end);
void *linear_malloc(size_t size, size_t align);
void linear_malloc_reset();
void linear_malloc_set_base();

struct qpair {
    int nr_entries;
    int sqe_size;
    int cqe_size;
    int sq_head;
    int sq_tail;
    int cq_head;
    int cq_tail;
    void *sq_payload;
    void *cq_payload;
};

void qpair_init(struct qpair *qp, int nr_entries, int sqe_size, int cqe_size);
void *qpair_alloc_sqe(volatile struct qpair *qp);
void qpair_submit_sqe(volatile struct qpair *qp, void *sqe);
void *qpair_peek_sqe(volatile struct qpair *qp);
void qpair_consume_sqe(volatile struct qpair *qp, void *sqe);
void *qpair_alloc_cqe(volatile struct qpair *qp);
void qpair_submit_cqe(volatile struct qpair *qp, void *cqe);
void *qpair_peek_cqe(volatile struct qpair *qp);
void qpair_consume_cqe(volatile struct qpair *qp, void *cqe);

static inline uint64_t get_time_cycle()
{
    XTime time;

    XTime_GetTime(&time);

    return time;
}

static inline uint64_t get_time_ns()
{
    XTime time;

    XTime_GetTime(&time);

    return (uint64_t)time * 1000000000 / COUNTS_PER_SECOND;
}

static inline void nsleep(uint64_t ns)
{
    uint64_t start = get_time_ns();

    while (get_time_ns() - start < ns);
}

#endif
