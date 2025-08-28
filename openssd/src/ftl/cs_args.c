#include <stdio.h>
#include <assert.h>
#include "memory_map.h"
#include "shared_mem.h"
#include "cs_args.h"
#include "queue.h"
#include "utils_common.h"
#include "nvme/host_lld.h"
#include "nvme/nvme.h"
#include "cdma.h"

static QTAILQ_HEAD(free_reqs, cs_args_req) free_reqs;
static QTAILQ_HEAD(pending_reqs, cs_args_req) pending_reqs;
static struct cs_args_req *cs_args_reqs;
static struct cs_args_req *cs_args_pending_reqs;
static volatile struct shared_mem *m;
uint64_t time1, time2;
static struct queued_transfer_req {
    unsigned int cmd_slot_tag;
    unsigned int qid;
    unsigned int cid;
    unsigned int nlb_0;
    int type;
} queued_req[3];

static int has_queued[3] = { 0 };
static int transfer_busy = 0;

void reset_cs_status()
{
    for(int i = 0; i < 3; i++)
    {
        m->cs_status[i] = CS_STATUS_IDLE;
        has_queued[i] = 0;
    }
}

void init_cs_args()
{
    QTAILQ_INIT(&free_reqs);
    QTAILQ_INIT(&pending_reqs);
    m = (struct shared_mem *)SHARED_MEM_BASE_ADDR;

    cs_args_reqs = linear_malloc(CONFIG_NR_CS_ARGS_REQS * sizeof(struct cs_args_req), 0);
    assert(cs_args_reqs != NULL);
    // cs_args_pending_reqs = linear_malloc(CONFIG_NR_CS_ARGS_REQS * sizeof(struct cs_args_req), 0);
    // assert(cs_args_pending_reqs != NULL);

    for (int i = 0; i < CONFIG_NR_CS_ARGS_REQS; i++) {
        cs_args_reqs[i].type = CS_ARGS_FREE;
        QTAILQ_INSERT_TAIL(&free_reqs, &cs_args_reqs[i], qent);
        // QTAILQ_INSERT_TAIL(&pending_reqs, &cs_args_reqs[i], qent);
    }
}

void queue_cs_args_req(unsigned int cmd_slot_tag, unsigned int qid, unsigned int cid,
                       unsigned int nlb_0, int type)
{
    int index;
    if(type == COMPACTION_ARGS_TX)
        index = 0;
    else if (type == SEARCH_ARGS_TX0 || type == SEARCH_ARGS_TX1)
        index = 1;
    queued_req[index].cmd_slot_tag = cmd_slot_tag;
    queued_req[index].qid = qid;
    queued_req[index].cid = cid;
    queued_req[index].nlb_0 = nlb_0;
    queued_req[index].type = type;

    has_queued[index] = 1;
}

void transfer_cs_args(unsigned int cmd_slot_tag, unsigned int qid, unsigned int cid,
                      unsigned int nlb_0, int type, int nr_search, int batch_size)
{
    struct cs_args_req *req;

    /* now only support 1 concurrent request */
    /* need a memory allocator for higher concurrency */
    while(transfer_busy)
    {
        check_done_cs_args_reqs();
    }
    assert(QTAILQ_EMPTY(&pending_reqs));

    req = QTAILQ_FIRST(&free_reqs);
    QTAILQ_REMOVE(&free_reqs, req, qent);

    int buf_size = 0;  
    if(type == SEARCH_ARGS_TX0)
    {
        buf_size = ((m->nr_result[0] + NVME_BLOCK_SIZE - 1) / NVME_BLOCK_SIZE) * NVME_BLOCK_SIZE;
        nlb_0 = buf_size / NVME_BLOCK_SIZE - 1;
    }
    else if(type == SEARCH_ARGS_TX1)
    {
        buf_size = ((m->nr_result[1] + NVME_BLOCK_SIZE - 1) / NVME_BLOCK_SIZE) * NVME_BLOCK_SIZE;
        nlb_0 = buf_size / NVME_BLOCK_SIZE - 1;
    }

    assert(req->type == CS_ARGS_FREE);
    req->type = type;
    req->cmd_slot_tag = cmd_slot_tag;
    req->qid = qid;
    req->cid = cid;
    req->nlb = nlb_0 + 1;

    transfer_busy = 1;
    uintptr_t addr;
    for (int i = 0; i < nlb_0 + 1; i++) {
        if(type == COMPACTION_ARGS_RX || type == COMPACTION_ARGS_TX)
        {
            addr = (uintptr_t)(compaction_args_buf + i * BYTES_PER_NVME_BLOCK);
        }
        else if(type == SEARCH_ARGS_RX0)
        {
            m->nr_search[0] = nr_search;
            m->batch_size = batch_size;
            addr = (uintptr_t)(DDR4_BUFFER_BASE_ADDR + m->args_file_disk_offset * 64 + i * BYTES_PER_NVME_BLOCK);
        }
        else if(type == SEARCH_ARGS_RX1)
        {
            m->nr_search[1] = nr_search;
            m->batch_size = batch_size;
            addr = (uintptr_t)(DDR4_BUFFER_BASE_ADDR + m->args_file_disk_offset * 64 + 32 * 1024 * 1024 + i * BYTES_PER_NVME_BLOCK);
        }
        else if(type == SEARCH_ARGS_TX0)
        {
            addr = (uintptr_t)(DDR4_BUFFER_BASE_ADDR + m->result_file_disk_offset * 64 + i * BYTES_PER_NVME_BLOCK);
        }
        else if(type == SEARCH_ARGS_TX1)
        {
            addr = (uintptr_t)(DDR4_BUFFER_BASE_ADDR + m->result_file_disk_offset * 64 + 16 * 1024 * 1024 + i * BYTES_PER_NVME_BLOCK);
        }

        unsigned int addr_hi = (addr >> 32);
        unsigned int addr_lo = (addr & 0xffffffff);

        if (type == COMPACTION_ARGS_RX || type == SEARCH_ARGS_RX0 || type == SEARCH_ARGS_RX1)
            set_auto_rx_dma(cmd_slot_tag, i, addr_hi, addr_lo, NVME_COMMAND_AUTO_COMPLETION_OFF);
        else if(type == COMPACTION_ARGS_TX || type == SEARCH_ARGS_TX0 || type == SEARCH_ARGS_TX1)
            set_auto_tx_dma(cmd_slot_tag, i, addr_hi, addr_lo, NVME_COMMAND_AUTO_COMPLETION_OFF);
    }

    if (type == COMPACTION_ARGS_RX || type == SEARCH_ARGS_RX0 || type == SEARCH_ARGS_RX1) 
    {
        req->dma_tail = g_hostDmaStatus.fifoTail.autoDmaRx;
        req->dma_overflow_cnt = g_hostDmaAssistStatus.autoDmaRxOverFlowCnt;
    } else if(type == COMPACTION_ARGS_TX || type == SEARCH_ARGS_TX0 || type == SEARCH_ARGS_TX1)
    {
        req->dma_tail = g_hostDmaStatus.fifoTail.autoDmaTx;
        req->dma_overflow_cnt = g_hostDmaAssistStatus.autoDmaTxOverFlowCnt;
    }

    QTAILQ_INSERT_TAIL(&pending_reqs, req, qent);

    MEMORY_BARRIER();

    // if (type == COMPACTION_ARGS_RX)
    //     m->cs_status[0] = CS_STATUS_ARGS_RX;
    // else if (type == COMPACTION_ARGS_TX)
    //     m->cs_status[0] = CS_STATUS_ARGS_TX;
    // else if (type == SEARCH_ARGS_RX)
    //     m->cs_status[1] = CS_STATUS_ARGS_RX;
    // else if (type == SEARCH_ARGS_TX)
    //     m->cs_status[1] = CS_STATUS_ARGS_TX;
}

void execute_queued_cs_args_reqs()
{
    for(int i = 0; i < 2; i++)
    {
        if (has_queued[i] && m->cs_status[i] == CS_STATUS_DONE) {
            has_queued[i] = 0;
            transfer_cs_args(queued_req[i].cmd_slot_tag, queued_req[i].qid, queued_req[i].cid,
                        queued_req[i].nlb_0, queued_req[i].type, 0, 0);
        }
    }
}

void check_done_cs_args_reqs()
{
    struct cs_args_req *req, *next_req;

    QTAILQ_FOREACH_SAFE(req, &pending_reqs, qent, next_req) {
        // if(req->type == COMPACTION_ARGS_RX || req->type == COMPACTION_ARGS_TX)
        {
            int done = (req->type == COMPACTION_ARGS_RX || req->type == SEARCH_ARGS_RX0 || req->type == SEARCH_ARGS_RX1) ?
                check_auto_rx_dma_partial_done(req->dma_tail, req->dma_overflow_cnt):
                check_auto_tx_dma_partial_done(req->dma_tail, req->dma_overflow_cnt);

            if (done) {
            // printf("%s transfer complete\n", req->type == COMPACTION_ARGS_RX ? "Rx" : "Tx");
            // (*((uint32_t *)NVME_DMA_BASE_ADDR))++;
            // set_nvme_cpl(req->qid, req->cid, 0, 0);
                set_auto_nvme_cpl(req->cmd_slot_tag, 0, 0);
                MEMORY_BARRIER();

                if (req->type == COMPACTION_ARGS_RX)
                    m->cs_status[0] = CS_STATUS_RUNNING;
                else if (req->type == COMPACTION_ARGS_TX)
                    m->cs_status[0] = CS_STATUS_IDLE;
                else if ((req->type == SEARCH_ARGS_RX0 || req->type == SEARCH_ARGS_RX1) && m->cs_status[1] != CS_STATUS_IDLE)
                {
                    m->batch_id = req->type == SEARCH_ARGS_RX0 ? 0 : 1;
                    has_queued[2] = 1;
                }
                else if ((req->type == SEARCH_ARGS_RX0 || req->type == SEARCH_ARGS_RX1))
                {
                    m->batch_id = req->type == SEARCH_ARGS_RX0 ? 0 : 1;
                    m->cs_status[1] = CS_STATUS_RUNNING;
                }
                else if (req->type == SEARCH_ARGS_TX0 || req->type == SEARCH_ARGS_TX1)
                {
                    m->cs_status[1] = CS_STATUS_IDLE;
                    if (has_queued[2])
                    {
                        has_queued[2] = 0;
                        m->cs_status[1] = CS_STATUS_RUNNING;
                    }
                }

                // if(req->type == COMPACTION_ARGS_TX)
                // {
                //     compaction_args* args = (compaction_args*)compaction_args_buf;
                //     uint64_t* output_num = (uint64_t*)&args->payload[args->output_file_nums_offset];
                //     uint64_t* output_size = (uint64_t*)&args->payload[args->output_sizes_offset];
                //     // if(output_size[0] > 10 * 64 * 1024 * 1024)
                //     {
                //         for(int i = 0; i < 5; i++)
                //             printf("%d, %ld, %ld\n",i, output_num[i], output_size[i]);
                //     }
                // }

                req->type = CS_ARGS_FREE;

                QTAILQ_REMOVE(&pending_reqs, req, qent);
                QTAILQ_INSERT_HEAD(&free_reqs, req, qent);

                transfer_busy = 0;

                
            }
        }
    }
}

int get_cs_status()
{
    return m->cs_status[0];
}
