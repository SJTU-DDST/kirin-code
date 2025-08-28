#include <string.h>
#include <assert.h>
#include "utils_common.h"
#include "debug.h"
#include "xil_cache.h"
#include "memory_map.h"
#include "shared_mem.h"

static uintptr_t malloc_base;
static uintptr_t curr_base;
static uintptr_t malloc_end;
static size_t default_align = 64;

void wait_cpu0_up()
{
	volatile struct shared_mem *m = (struct shared_mem *)SHARED_MEM_BASE_ADDR;

    while (m->cpu0_magic != CPU0_MAGIC_NUM);
}

void linear_malloc_init(uintptr_t start, uintptr_t end)
{
    malloc_base = curr_base = ALIGN_CEILING(start, default_align);
    malloc_end = ALIGN_FLOOR(end, default_align);
}

void *linear_malloc(size_t size, size_t align)
{
    void *ret;

    if (align > 0)
        curr_base = ALIGN_CEILING(curr_base, align);
    ret = (void *)curr_base;

    if (curr_base + size > malloc_end) {
        ASSERT(0);
        return NULL;
    }

    curr_base = ALIGN_CEILING(curr_base + size, default_align);

    return ret;
}

void linear_malloc_reset()
{
    curr_base = malloc_base;
}

void linear_malloc_set_base()
{
    malloc_base = curr_base;
}

void linear_malloc_set_default_align(size_t align)
{
    default_align = align;
}

void qpair_init(struct qpair *qp, int nr_entries, int sqe_size, int cqe_size)
{
    memset(qp, 0, sizeof(struct qpair));

    qp->nr_entries = nr_entries;
    qp->sqe_size = sqe_size;
    qp->cqe_size = cqe_size;

    qp->sq_payload = linear_malloc(nr_entries * sqe_size, 0);
    qp->cq_payload = linear_malloc(nr_entries * cqe_size, 0);

    MEMORY_BARRIER();
}

void *qpair_alloc_sqe(volatile struct qpair *qp)
{
    int nr_entries = qp->nr_entries;
    int sq_head, sq_tail;

    while (1) {
        sq_head = qp->sq_head;
        sq_tail = qp->sq_tail;

        if ((sq_tail + 1) % nr_entries != sq_head)
            break;
    }

    return qp->sq_payload + sq_tail * qp->sqe_size;
}

void qpair_submit_sqe(volatile struct qpair *qp, void *sqe)
{
    ASSERT(sqe == qp->sq_payload + qp->sq_tail * qp->sqe_size);

    MEMORY_BARRIER();
    qp->sq_tail = (qp->sq_tail + 1) % qp->nr_entries;
}

void *qpair_peek_sqe(volatile struct qpair *qp)
{
    return qp->sq_head != qp->sq_tail ?
           qp->sq_payload + qp->sq_head * qp->sqe_size :
           NULL;
}

void qpair_consume_sqe(volatile struct qpair *qp, void *sqe)
{
    ASSERT(sqe == qp->sq_payload + qp->sq_head * qp->sqe_size);

    MEMORY_BARRIER();
    qp->sq_head = (qp->sq_head + 1) % qp->nr_entries;
}

void *qpair_alloc_cqe(volatile struct qpair *qp)
{
    int nr_entries = qp->nr_entries;
    int cq_head, cq_tail;

    while (1) {
        cq_head = qp->cq_head;
        cq_tail = qp->cq_tail;

        if ((cq_tail + 1) % nr_entries != cq_head)
            break;
    }

    return qp->cq_payload + cq_tail * qp->cqe_size;
}

void qpair_submit_cqe(volatile struct qpair *qp, void *cqe)
{
    ASSERT(cqe == qp->cq_payload + qp->cq_tail * qp->cqe_size);

    MEMORY_BARRIER();
    qp->cq_tail = (qp->cq_tail + 1) % qp->nr_entries;
}

void *qpair_peek_cqe(volatile struct qpair *qp)
{
    return qp->cq_head != qp->cq_tail ?
           qp->cq_payload + qp->cq_head * qp->cqe_size :
           NULL;
}

void qpair_consume_cqe(volatile struct qpair *qp, void *cqe)
{
    ASSERT(cqe == qp->cq_payload + qp->cq_head * qp->cqe_size);

    MEMORY_BARRIER();
    qp->cq_head = (qp->cq_head + 1) % qp->nr_entries;
}