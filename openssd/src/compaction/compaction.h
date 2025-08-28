#ifndef __COMPACTION_H
#define __COMPACTION_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "cs_file.h"
#include "kv.h"
#include "xcompaction.h"
#include "shared_mem.h"
#include "utils_common.h"

#define MAX_KEY_LENGTH 256
#define OUTPUT_TABLE_MAX_SIZE          (4 * 1024 * 1024)
#define OUTPUT_TABLE_SIZE_THRES        (2 * 1024 * 1024)
#define MAX_INPUTS_PER_LEVEL    256
#define DATA_PACK (512 / 8)

typedef struct table_file{
    void *mapped;
    uint64_t size;
    uint64_t offset;
}table_file;

typedef struct level_iterator{
    int nr_inputs;
    int cur;
    struct cs_file *input_files[MAX_INPUTS_PER_LEVEL];
    void *file_bufs[MAX_INPUTS_PER_LEVEL];
    struct table_file input;
}level_iterator;

typedef struct output_table{
    uint8_t *output_buf;
    struct cs_file *file;
    uint64_t table_size;
    uint64_t output_buf_cursor;
    uint64_t flushed_cursor;
}output_table;

typedef struct compaction_args{
    int value_length;
    int output_table_size_thres;
	int gamma;
	int best_len;
    int learned_index_file_id;
    /* in */
    int nr_inputs;
    int nr_outputs;
    int nr_levels;
    int input_file_ids_offset;      /* uint64_t[nr_inputs] */
    int input_file_nums_offset;     /* uint64_t[nr_inputs]*/
    int output_file_ids_offset;     /* uint64_t[nr_outputs] */
    int output_file_nums_offset;    /* uint64_t[nr_outputs] */
    int levels_offset;              /* uint64_t[nr_levels] */

    /* out */
    int output_sizes_offset;        /* uint64_t[nr_outputs] */
    int output_smallests_offset;    /* KeyType[nr_outputs] */
    int output_largests_offset;     /* KeyType[nr_outputs] */
    int output_segs_sizes_offset;   /* uint64_t[nr_outputs]*/

    uint8_t payload[0];
}compaction_args;

struct fpga_compaction_args
{
	int value_length;
    int output_table_size_thres;
    /* in */
    int nr_inputs;
    int nr_outputs;
    int nr_levels;
    int input_files_size_offset;     /* uint64_t[nr_inputs]*/
    int input_files_pos_offset;      /* uint64_t[nr_inputs] need to be divided by sizeof(data_pack) */  
    int output_files_pos_offset;     /* uint64_t[nr_outputs] need to be divided by sizeof(data_pack) */
    int levels_num[2];           

    /* out */
    int output_sizes_offset;        /* uint64_t[nr_outputs] */
    int output_smallests_offset;    /* KeyType[nr_outputs] */
    int output_largests_offset;     /* KeyType[nr_outputs] */

    int gamma;
    int best_len;
    int segs_offset;
};

struct segment {
	uint64_t x;
	double k;
	double b;
};


extern XCompaction compaction_fpga;
extern XCompaction_Config* compaction_cfg;
void init_compaction_io_reqs();
void compaction(uint8_t* uncached_buffer, uint64_t* disk_offsets);

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

#ifdef __cplusplus
}
#endif

#endif
