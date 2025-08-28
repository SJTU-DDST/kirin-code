#include <assert.h>
#include "cs_io.h"
#include "ext4_cs.h"
#include "utils_common.h"
#include "memory_map.h"
#include "cdma.h"
#include "nvme/debug.h"

static struct cs_io_req *cs_io_reqs;
static struct cs_io_req *current_cs_io_req; /* this is a dangling node */
static QTAILQ_HEAD(free_cs_io_reqs, cs_io_req) free_cs_io_reqs;
static QTAILQ_HEAD(pending_cs_io_reqs, cs_io_req) pending_cs_io_reqs;
static uint64_t cs_io_seq;

void do_low_level_tasks();
void signal_cs_io_req_done(struct cs_file_req *file_req);

static void sync_free_cs_io_reqs()
{
    while (QTAILQ_EMPTY(&free_cs_io_reqs))
        do_low_level_tasks();
}

/* returns true if a new req can be submitted */
static bool check_cs_io_req_done()
{
    bool busy = cdma_is_busy();

    if (current_cs_io_req != NULL &&
            cdma_transfer_done(current_cs_io_req->cdma_seq)) {
        assert(current_cs_io_req->unfinished);
        current_cs_io_req->unfinished = 0;
        if (current_cs_io_req->file_req != NULL)
            signal_cs_io_req_done(current_cs_io_req->file_req);
        QTAILQ_INSERT_HEAD(&free_cs_io_reqs, current_cs_io_req, qent);
        current_cs_io_req = NULL;
    }

    return !busy;
}

static void run_cs_io_req()
{
    struct cs_io_req *req;
    void *storage_addr;

    assert(current_cs_io_req == NULL);

    if (QTAILQ_EMPTY(&pending_cs_io_reqs))
        return;

    req = QTAILQ_FIRST(&pending_cs_io_reqs);
    QTAILQ_REMOVE(&pending_cs_io_reqs, req, qent);

    assert(req->unfinished);
    storage_addr = (void *)(DDR4_BUFFER_BASE_ADDR + req->offset);
    if (req->is_read) {
        req->cdma_seq = cdma_transfer(req->buf, storage_addr,
                                      req->length, 1, 1, 1, 0);
    } else {
        req->cdma_seq = cdma_transfer(storage_addr, req->buf,
                                      req->length, 1, 1, 1, 0);
    }
    assert(req->cdma_seq != 0);

    current_cs_io_req = req;
}

static struct cs_io_req *alloc_cs_io_req()
{
    struct cs_io_req *req;

    sync_free_cs_io_reqs();

    req = QTAILQ_FIRST(&free_cs_io_reqs);
    QTAILQ_REMOVE(&free_cs_io_reqs, req, qent);

    return req;
}

static void do_storage_io(volatile void *buf, uint64_t offset, uint64_t length, struct cs_file_req *file_req,
        int is_read, struct cs_io_handle *handle)
{
    struct cs_io_req *req;

    // assert(offset % 512 == 0 && length % 512 == 0);

    req = alloc_cs_io_req();
    assert(req->unfinished == 0);

    req->seq = ++cs_io_seq;
    req->unfinished = 1;
    req->is_read = is_read;
    req->buf = buf;
    req->offset = offset;
    req->length = length;
    req->file_req = file_req;

    handle->seq = req->seq;
    handle->req = req;

    QTAILQ_INSERT_TAIL(&pending_cs_io_reqs, req, qent);

    schedule_cs_io_reqs();
}

void schedule_cs_io_reqs()
{
    if (check_cs_io_req_done())
        run_cs_io_req();
}

void do_sync_cs_io_req(struct cs_io_handle *handle)
{
    while (handle->seq == handle->req->seq && handle->req->unfinished)
        do_low_level_tasks();
}

struct cs_io_handle read_from_storage(volatile void *buf, uint64_t offset, uint64_t length, struct cs_file_req *file_req)
{
    struct cs_io_handle handle;

    do_storage_io(buf, offset, length, file_req, 1, &handle);

    return handle;
}

struct cs_io_handle write_to_storage(volatile void *buf, uint64_t offset, uint64_t length, struct cs_file_req *file_req)
{
    struct cs_io_handle handle;

    do_storage_io(buf, offset, length, file_req, 0, &handle);

    return handle;
}

void init_cs_io_reqs()
{
    cs_io_reqs = linear_malloc(CONFIG_NR_CS_IO_REQS * sizeof(struct cs_io_req), 0);
    assert(cs_io_reqs != NULL);
    current_cs_io_req = NULL;
    QTAILQ_INIT(&free_cs_io_reqs);
    QTAILQ_INIT(&pending_cs_io_reqs);
    cs_io_seq = 0;

    for (int i = 0; i < CONFIG_NR_CS_IO_REQS; i++) {
        memset(&cs_io_reqs[i], 0, sizeof(*cs_io_reqs));
        QTAILQ_INSERT_TAIL(&free_cs_io_reqs, &cs_io_reqs[i], qent);
    }
}
