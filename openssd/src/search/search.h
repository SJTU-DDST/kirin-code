#ifndef SEARCH_H
#define SEARCH_H
#include "xtop_search.h"
#include <stdint.h>
#include <stdio.h>
#include "kv.h"
#include "memory_map.h"
#include "shared_mem.h"
#include "xtime_l.h"
#include "queue.h"
#include "debug.h"

typedef struct search_args
{
    int nr_search;
	int batch_id;
    int files_num_offset;
    int target_keys_offset;
    int kv_pos_offset;
    uint8_t payload[0];
}search_args;

typedef struct fpga_search_args
{
	int nr_search;
	uint64_t args_file_offset;
	uint64_t result_file_offset;
	uint64_t debug_offset;
	/*in*/
	int search_files_pos_offset;  // file_pos = physical_address / DATA_WIDTH
	int target_keys_offset;
	int kv_pos_offset;            // kv_pos = logical_address * KV_LENGTH
}fpga_search_args;
void init_search_io_reqs();
int search(int nr_search, int batch_size, int batch_id, uint64_t args_file_disk_offset, uint64_t result_file_disk_offset, uint64_t* disk_offsets);
extern XTop_search search_engine;
extern XTop_search_Config* search_engine_cfg;
#endif