#include <stdlib.h>
#include <string.h>
#include "cs_file.h"
#include "memory_map.h"
#include "shared_mem.h"
#include "debug.h"
#include "utils_common.h"

static struct cs_file *cs_files;
static QTAILQ_HEAD(free_cs_files, cs_file) free_cs_files;
static QTAILQ_HEAD(opened_cs_files, cs_file) opened_cs_files;
static QTAILQ_HEAD(closing_cs_files, cs_file) closing_cs_files;
static volatile struct shared_mem *m = (struct shared_mem *)SHARED_MEM_BASE_ADDR;

static struct cs_file *alloc_cs_file()
{
    struct cs_file *file;

    ASSERT(!QTAILQ_EMPTY(&free_cs_files));

    file = QTAILQ_FIRST(&free_cs_files);
    ASSERT(file->status == CS_FILE_STATUS_FREE);
    QTAILQ_REMOVE(&free_cs_files, file, qent);

    return file;
}

static void process_cq()
{
    struct file_req_cqe *cqe;

    while ((cqe = qpair_peek_cqe(&m->file_req_qp)) != NULL) {
        /* cqe for file open should have been processed in sync_file_open() */
        ASSERT(cqe->req_type != FILE_REQ_OPEN && cqe->req_type != FILE_REQ_CLOSE);
        ASSERT(cs_files[cqe->file_idx].pending_ios > 0);
        ASSERT(cs_files[cqe->file_idx].status != CS_FILE_STATUS_FREE);
        cs_files[cqe->file_idx].pending_ios--;
        if (cs_files[cqe->file_idx].pending_ios == 0 && cs_files[cqe->file_idx].status == CS_FILE_STATUS_CLOSING) {
            cs_files[cqe->file_idx].status = CS_FILE_STATUS_FREE;
            QTAILQ_REMOVE(&closing_cs_files, &cs_files[cqe->file_idx], qent);
            QTAILQ_INSERT_HEAD(&free_cs_files, &cs_files[cqe->file_idx], qent);
        }
        qpair_consume_cqe(&m->file_req_qp, cqe);
    }
}

static void sync_file_open(struct cs_file *file)
{
    while (1) {
        struct file_req_cqe *cqe;

        while ((cqe = qpair_peek_cqe(&m->file_req_qp)) != NULL) {
            if (cqe->req_type == FILE_REQ_OPEN) {
                ASSERT(cqe->file_idx == file - cs_files);
                file->size = cqe->size;
                file->disk_offset = cqe->disk_offset;
                qpair_consume_cqe(&m->file_req_qp, cqe);
                return;
            }

            ASSERT(cqe->req_type != FILE_REQ_OPEN && cqe->req_type != FILE_REQ_CLOSE);
            ASSERT(cs_files[cqe->file_idx].pending_ios > 0);
            cs_files[cqe->file_idx].pending_ios--;
            if (cs_files[cqe->file_idx].pending_ios == 0 && cs_files[cqe->file_idx].status == CS_FILE_STATUS_CLOSING) {
                cs_files[cqe->file_idx].status = CS_FILE_STATUS_FREE;
                QTAILQ_REMOVE(&closing_cs_files, &cs_files[cqe->file_idx], qent);
                QTAILQ_INSERT_HEAD(&free_cs_files, &cs_files[cqe->file_idx], qent);
            }
            qpair_consume_cqe(&m->file_req_qp, cqe);
        }
    }
}

struct cs_file *cs_file_open(uint64_t id)
{
    struct cs_file *ret;
    struct file_req_sqe *sqe;

    if (!m->fs_ready)
        return NULL;

    ret = alloc_cs_file();

    ret->id = id;
    ret->size = 0;
    ret->pointer = 0;
    ret->pending_ios = 0;
    ret->status = CS_FILE_STATUS_OPENED;
    QTAILQ_INSERT_TAIL(&opened_cs_files, ret, qent);

    sqe = qpair_alloc_sqe(&m->file_req_qp);
    sqe->file_idx = ret - cs_files;
    sqe->req_type = FILE_REQ_OPEN;
    sqe->file_id = id;
    qpair_submit_sqe(&m->file_req_qp, sqe);

    sync_file_open(ret);

    return ret;
}

void cs_file_close(struct cs_file *file)
{
    struct file_req_sqe *sqe;

    sqe = qpair_alloc_sqe(&m->file_req_qp);
    sqe->file_idx = file - cs_files;
    sqe->req_type = FILE_REQ_CLOSE;
    qpair_submit_sqe(&m->file_req_qp, sqe);

    ASSERT(file->status == CS_FILE_STATUS_OPENED);
    QTAILQ_REMOVE(&opened_cs_files, file, qent);
    if (file->pending_ios == 0) {
        file->status = CS_FILE_STATUS_FREE;
        QTAILQ_INSERT_HEAD(&free_cs_files, file, qent);
    } else {
        file->status = CS_FILE_STATUS_CLOSING;
        QTAILQ_INSERT_TAIL(&closing_cs_files, file, qent);
    }
}

uint64_t cs_file_io(struct cs_file *file, void *buf, uint64_t len, uint64_t offset, int is_read)
{
    struct file_req_sqe *sqe;

    process_cq();

    if (len == 0 || offset >= file->size)
        return 0;

    if (offset + len > file->size)
        len = file->size - offset;

    file->pending_ios++;

    sqe = qpair_alloc_sqe(&m->file_req_qp);
    sqe->file_idx = file - cs_files;
    sqe->req_type = is_read ? FILE_REQ_READ : FILE_REQ_WRITE;
    sqe->file_id = file->id;
    sqe->offset = offset;
    sqe->length = len;
    sqe->buf = buf;
    qpair_submit_sqe(&m->file_req_qp, sqe);

    return len;
}

uint64_t cs_file_pread(struct cs_file *file, void *buf, uint64_t len, uint64_t offset)
{
    return cs_file_io(file, buf, len, offset, 1);
}

uint64_t cs_file_pwrite(struct cs_file *file, void *buf, uint64_t len, uint64_t offset)
{
    return cs_file_io(file, buf, len, offset, 0);
}

uint64_t cs_file_read(struct cs_file *file, void *buf, uint64_t len)
{
    uint64_t ret = cs_file_io(file, buf, len, file->pointer, 1);

    file->pointer += ret;

    return ret;
}

uint64_t cs_file_write(struct cs_file *file, void *buf, uint64_t len)
{
    uint64_t ret = cs_file_io(file, buf, len, file->pointer, 0);

    file->pointer += ret;

    return ret;
}

int cs_file_seek(struct cs_file *file, uint64_t offset)
{
    if (offset > file->size)
        return -1;

    file->pointer = offset;

    return 0;
}

void sync_file_pending_ios(struct cs_file *file)
{
    while (file->pending_ios > 0)
        process_cq();
}

void sync_closing_files()
{
    while (!QTAILQ_EMPTY(&closing_cs_files))
        process_cq();
}

void init_cs_files()
{
    cs_files = linear_malloc(CONFIG_NR_CS_FILES * sizeof(*cs_files), 0);
    ASSERT(cs_files != NULL);
    linear_malloc_set_base();

    QTAILQ_INIT(&free_cs_files);
    QTAILQ_INIT(&opened_cs_files);
    QTAILQ_INIT(&closing_cs_files);

    memset(cs_files, 0, CONFIG_NR_CS_FILES * sizeof(struct cs_file));
    for (int i = 0; i < CONFIG_NR_CS_FILES; i++) {
        cs_files[i].status = CS_FILE_STATUS_FREE;
        QTAILQ_INSERT_TAIL(&free_cs_files, &cs_files[i], qent);
    }
}
