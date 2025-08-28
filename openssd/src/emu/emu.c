#include "utils_common.h"
#include "shared_mem.h"
#include "ftl.h"
#include "queue.h"

struct pq_entry {
    struct host_io_req *req;
    uint64_t end_time;
    size_t pos;
    QTAILQ_ENTRY(pq_entry) qent;
};

static volatile struct shared_mem *m = (struct shared_mem *)SHARED_MEM_BASE_ADDR;
static struct ssd *ssd;
static struct pqueue_t *req_pqueue_0, *req_pqueue_1, *req_pqueue_2;
static struct pq_entry *pq_entries;
static QTAILQ_HEAD(free_pq_entries, pq_entry) free_pq_entries;

static int req_cmp_pri(pqueue_pri_t next, pqueue_pri_t curr)
{
    return next > curr;
}

static pqueue_pri_t req_get_pri(void *a)
{
    return ((struct pq_entry *)a)->end_time;
}

static void req_set_pri(void *a, pqueue_pri_t pri)
{
    ((struct pq_entry *)a)->end_time = pri;
}

static size_t req_get_pos(void *a)
{
    return ((struct pq_entry *)a)->pos;
}

static void req_set_pos(void *a, size_t pos)
{
    ((struct pq_entry *)a)->pos = pos;
}

static void process_emu_req_sq(struct qpair* qp, struct pqueue_t* req_pqueue)
{
    struct emu_req_sqe *sqe;

    while ((sqe = qpair_peek_sqe(qp)) != NULL) {
        struct host_io_req *req = sqe->host_io_req;
        struct pq_entry *entry;
        uint64_t now = get_time_cycle();
        uint64_t lat;

        if (req->is_read)
            lat = ssd_read(ssd, req, now);
        else
            lat = ssd_write(ssd, req, now);

        ASSERT(!QTAILQ_EMPTY(&free_pq_entries));
        entry = QTAILQ_FIRST(&free_pq_entries);
        QTAILQ_REMOVE(&free_pq_entries, entry, qent);

        entry->req = req;
        entry->end_time = now + lat;
        pqueue_insert(req_pqueue, entry);

        qpair_consume_sqe(qp, sqe);
    }
}

static void send_emu_req_cqes(struct qpair* qp, struct pqueue_t* req_pqueue)
{
    while (1) {
        uint64_t now = get_time_cycle();
        struct pq_entry *entry = pqueue_peek(req_pqueue);
        struct emu_req_cqe *cqe;

        if (entry == NULL || now < entry->end_time)
            break;

        pqueue_pop(req_pqueue);

        cqe = qpair_alloc_cqe(qp);
        cqe->host_io_req = entry->req;
        qpair_submit_cqe(qp, cqe);

        QTAILQ_INSERT_HEAD(&free_pq_entries, entry, qent);
    }
}

static void process_emu_req_sq_passthru(struct qpair* qp)
{
    struct emu_req_sqe *sqe;
    struct emu_req_cqe *cqe;

    while ((sqe = qpair_peek_sqe(qp)) != NULL) {
        cqe = qpair_alloc_cqe(qp);
        cqe->host_io_req = sqe->host_io_req;
        qpair_submit_cqe(qp, cqe);

        qpair_consume_sqe(qp, sqe);
    }
}

void emu_main()
{
    linear_malloc_set_default_align(4);

    ssd = linear_malloc(sizeof(struct ssd), 0);
    ssd_init(ssd);

    req_pqueue_0 = pqueue_init(EMU_REQ_QP_ENTRIES + 1, req_cmp_pri, req_get_pri, req_set_pri,
                            req_get_pos, req_set_pos);
    req_pqueue_1 = pqueue_init(COMPACTION_FILE_REQ_QP_ENTRIES + 1, req_cmp_pri, req_get_pri, req_set_pri,
                            req_get_pos, req_set_pos);
    req_pqueue_2 = pqueue_init(SEARCH_FILE_REQ_QP_ENTRIES + 1, req_cmp_pri, req_get_pri, req_set_pri,
                            req_get_pos, req_set_pos);
    
    pq_entries = linear_malloc(EMU_REQ_QP_ENTRIES * sizeof(struct pq_entry), 0);
    QTAILQ_INIT(&free_pq_entries);
    for (int i = 0; i < EMU_REQ_QP_ENTRIES; i++)
        QTAILQ_INSERT_TAIL(&free_pq_entries, &pq_entries[i], qent);

    while (1) {
        if (CONFIG_ENABLE_NAND_EMULATION) {
            process_emu_req_sq(&m->emu_req_qp, req_pqueue_0);
            send_emu_req_cqes(&m->emu_req_qp, req_pqueue_0);

            process_emu_req_sq(&m->compaction_file_req_qp, req_pqueue_1);
            send_emu_req_cqes(&m->compaction_file_req_qp, req_pqueue_1);

            process_emu_req_sq(&m->search_file_req_qp, req_pqueue_2);
            send_emu_req_cqes(&m->search_file_req_qp, req_pqueue_2);
        } else {
            process_emu_req_sq_passthru(&m->emu_req_qp);
            process_emu_req_sq_passthru(&m->compaction_file_req_qp);
            process_emu_req_sq_passthru(&m->search_file_req_qp);
        }
    }
}
