#ifndef SEARCH_CSD_H
#define SEARCH_CSD_H
#include "table.h"
#include "compaction.h"
#include <cstdint>
#include <fcntl.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <errno.h>

#define NVME_BLOCK_SIZE 4096
#define MAX_BATCH_SIZE 2048
typedef struct search_args
{
    int nr_search;
    int batch_id;
    int files_num_offset;
    int target_keys_offset;
    int kv_pos_offset;
    uint8_t payload[0];
}search_args;
void resetStatus(int dev_id, int dev_fd);
void getFileAddress(uint64_t file_id, uint32_t file_num, int dev_id, int dev_fd, int is_args_file, int is_result_file);
void offloadSearch(uint64_t* args, int dev_id, int dev_fd, int batch_id, uint32_t nr_search, uint32_t batch_size);
void collectSearchResult(int nr_search, int dev_id, int dev_fd, int batch_id, uint8_t* buffer);
#endif