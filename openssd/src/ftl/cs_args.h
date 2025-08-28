#include "config.h"
#include "queue.h"

enum {
    CS_ARGS_FREE = 0,
    COMPACTION_ARGS_RX,
    COMPACTION_ARGS_TX,
    SEARCH_ARGS_RX0,
    SEARCH_ARGS_RX1,
    SEARCH_ARGS_TX0,
    SEARCH_ARGS_TX1,
};

struct cs_args_req {
    int type;
    unsigned int cmd_slot_tag;
    unsigned int qid;
    unsigned int cid;
    unsigned int nlb; /* 1-based */
    unsigned int dma_tail;
    unsigned int dma_overflow_cnt;
    QTAILQ_ENTRY(cs_args_req) qent;
};

void init_cs_args();
void transfer_cs_args(unsigned int cmd_slot_tag, unsigned int qid, unsigned int cid,
                      unsigned int nlb_0, int type, int nr_search, int batch_size);
void queue_cs_args_req(unsigned int cmd_slot_tag, unsigned int qid, unsigned int cid,
                       unsigned int nlb_0, int type);
void execute_queued_cs_args_reqs();
void check_done_cs_args_reqs();
int get_cs_status();
void reset_cs_status();
