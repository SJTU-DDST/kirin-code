#ifndef SHARED_MEM_H_
#define SHARED_MEM_H_

#include <stdint.h>
#include "memory_map.h"
#include "utils_common.h"
#include <stdbool.h>
#include "queue.h"

#ifndef BYTES_PER_NVME_BLOCK
#define BYTES_PER_NVME_BLOCK 4096
#endif

#ifndef NVME_BLOCK_SIZE
#define NVME_BLOCK_SIZE 4096
#endif

#define UNCACHED_BUFFER_SIZE (2 * 1024 * 1024)
#define compaction_args_buf ((uint8_t *)DMA_START_ADDR)

#define FILE_REQ_QP_ENTRIES 1024
#define COMPACTION_FILE_REQ_QP_ENTRIES 1024
#define SEARCH_FILE_REQ_QP_ENTRIES 1024
#define EMU_REQ_QP_ENTRIES 65536

#define CPU0_MAGIC_NUM 0x15121512
#define CPU1_MAGIC_NUM 0x02203321
#define CPU2_MAGIC_NUM 0x11903391
#define CPU3_MAGIC_NUM 0x04342349

enum {
    CS_STATUS_IDLE = 0,
    CS_STATUS_ARGS_RX,
    CS_STATUS_RUNNING,
    CS_STATUS_DONE,
    CS_STATUS_ARGS_TX,
};

enum {
    FILE_REQ_READ = 0,
    FILE_REQ_WRITE,
    FILE_REQ_OPEN,
    FILE_REQ_CLOSE,
};

struct file_req_sqe {
    int file_idx;
    int req_type;
    uint64_t req_seq;
    uint32_t file_id;
    uint32_t offset;
    uint32_t length;
    void *buf;
};

struct file_req_cqe {
    int file_idx;
    int req_type;
    uint64_t req_seq;
    uint32_t size; /* used only by file open requests */
    uint64_t disk_offset;
};

struct cs_file_req;

struct host_io_req {
    bool is_read;
    bool is_cs;
    struct cs_file_req *file_req;
    uint32_t slba;
    uint32_t nlb;
    uint32_t cmd_slot_tag;
    uint32_t qid, cid;
    uint32_t dma_tail, dma_overflow_cnt;
    QTAILQ_ENTRY(host_io_req) qent;
};

struct emu_req_sqe {
    struct host_io_req *host_io_req;
};

struct emu_req_cqe {
    struct host_io_req *host_io_req;
};

struct shared_mem {
    int cs_status[3];
    int fs_ready;
    
    int batch_id;
    int batch_size;
    int nr_search[2];
    int nr_result[2];
    uint64_t args_file_disk_offset;
    uint64_t result_file_disk_offset;
    uint64_t* file_disk_offsets;
    
    int cpu0_magic;
    int cpu1_magic;
    int cpu2_magic;
    int cpu3_magic;

    struct qpair file_req_qp;
    struct qpair compaction_file_req_qp;
    struct qpair search_file_req_qp;
    struct qpair emu_req_qp;
};

#endif
