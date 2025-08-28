#ifndef COMPACTION_H
#define COMPACTION_H
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include "table.h"
#include "kv.h"
#include "bloom_filter.h"
#include <aio.h>
#include <vector>
#define MAX_INPUTS_PER_LEVEL 512
#define MAX_LEVELS_NUM 32
#define OUTPUT_BUFFER_SIZE (4*1024)
#define OUTPUT_BUFFER_THRES (OUTPUT_BUFFER_SIZE - KV_LENGTH)
#define OUTPUT_TABLE_SIZE_THRES (TABLE_SIZE_THRES)
#define OUTPUT_TABLE_MAX_SIZE (TABLE_MAX_SIZE)

struct _aiocb
{
    int fd;
    struct aiocb* cb;
};

typedef struct
{
    int value_length;
    int output_table_size_thres;
    int gamma;
    int best_len;
    int learned_index_file_id;
    int nr_inputs;
    int nr_outputs;
    int nr_levels;
    int input_file_ids_offset;      /* int[nr_inputs] */
    int input_file_nums_offset;     /* uint64_t[nr_inputs]*/
    int output_file_ids_offset;     /* int[nr_outputs] */
    int output_file_nums_offset;    /* uint32_t[nr_outputs] */
    int levels_offset;              /* int[nr_levels] */

    /* out */
    int output_sizes_offset;        /* uint64_t[nr_outputs] */
    int output_smallests_offset;    /* KeyType[nr_outputs] */
    int output_largests_offset;     /* KeyType[nr_outputs] */
    int output_segs_size_offset;    /* uint64_t[nr_outputs]*/

    uint8_t payload[0];
}compaction_args;

static inline size_t compaction_args_size(int nr_inputs, int nr_outputs, int nr_levels)
{
    return sizeof(compaction_args) +
           sizeof(uint64_t) * nr_inputs +          /* input_file_ids */
           sizeof(uint64_t) * nr_inputs +          /* input_file_nums*/
           sizeof(uint64_t) * nr_outputs +         /* output_file_ids */
           sizeof(uint64_t) * nr_outputs +         /* output_file_nums */
           sizeof(uint64_t) * nr_levels +          /* levels */
           sizeof(uint64_t) * nr_outputs +         /* output_sizes */
           sizeof(KeyType) * nr_outputs +          /* output_smallests */
           sizeof(KeyType) * nr_outputs +           /* output_largests */
           sizeof(uint64_t) * nr_outputs;          /* output_segs_sizes*/
}

typedef struct {
    uint64_t table_num;
    void *mapped;
    size_t offset;
    size_t size;
}table_file;

typedef struct {
    int nr_inputs;
    int cur;
    // int input_fds[LEVELDB_MAX_INPUTS_PER_LEVEL];
    table_file input[MAX_INPUTS_PER_LEVEL];
}level_iterator;

typedef struct {
    uint8_t* buffer;
    uint32_t buffer_cursor;
    int fd;
    uint64_t table_size;
    std::vector<KeyType> keys;
    segment* segs;
    int segs_offset;
    uint64_t segs_num;
    BloomFilter* filter;
}output_table;

typedef struct {
    __u8    opcode;
    __u8    flags;
    __u16   rsvd1;
    __u32   nsid;
    __u32   cdw2;
    __u32   cdw3;
    __u64   metadata;
    __u64   addr;
    __u32   metadata_len;
    __u32   data_len;
    __u32   cdw10;
    __u32   cdw11;
    __u32   cdw12;
    __u32   cdw13;
    __u32   cdw14;
    __u32   cdw15;
    __u32   timeout_ms;
    __u32   result;
}nvme_passthru_cmd;

enum nvme_opcode {
    nvme_cmd_flush          = 0x00,
    nvme_cmd_write          = 0x01,
    nvme_cmd_read           = 0x02,
    nvme_cmd_write_uncor    = 0x04,
    nvme_cmd_compare        = 0x05,
    nvme_cmd_write_zeroes   = 0x08,
    nvme_cmd_dsm            = 0x09,
    nvme_cmd_verify         = 0x0c,
    nvme_cmd_resv_register  = 0x0d,
    nvme_cmd_resv_report    = 0x0e,
    nvme_cmd_resv_acquire   = 0x11,
    nvme_cmd_resv_release   = 0x15,
    nvme_cmd_copy           = 0x19,
    nvme_zns_cmd_mgmt_send  = 0x79,
    nvme_zns_cmd_mgmt_recv  = 0x7a,
    nvme_zns_cmd_append     = 0x7d,

    nvme_admin_cs_status    = 0x94,
    nvme_admin_get_address  = 0x95,
    nvme_admin_reset_status = 0x96,
};

void compaction(compaction_args* args, segment* segs, std::vector<BloomFilter*>& filters);
#endif