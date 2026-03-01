#ifndef LSM_H
#define LSM_H
#include "table.h"
#include "compaction.h"
#include "bst.h"
#include <cstdint>
#include <math.h>
#include <sys/ioctl.h>
#include <queue>
#include <semaphore>
#include <aio.h>
#define NUM_LEVELS 6
#define LEVEL0_MAX_TABLE_NUM 4
// #define TABLE_SIZE_THRES (64*1024*1024)
// #define TABLE_MAX_SIZE (68*1024*1024)
#define LEVEL1_MAX_SIZE (10 * TABLE_SIZE_THRES)
#define NVME_BLOCK_SIZE 4096
// #define min(a, b) (((a) < (b)) ? (a) : (b))
// #define max(a, b) (((a) > (b)) ? (a) : (b))

typedef struct 
{
    SSTable* SSTable_list;
    int num_table;
    uint64_t size;
    uint64_t level_max_size;
    SSTable* compaction_ptr;
    mutable shared_mutex level_mutex;
}Level;

typedef struct 
{
    MemTable* mt;
    ImmutableTable* immu;
    Level* levels;
    uint64_t seq_number;
    int levels_num;
    int average_length;
    mutable shared_mutex seq_mutex;
    mutable shared_mutex table_mutex;
    mutable shared_mutex levels_mutex;
    mutex compaction_mutex;
    atomic<bool> compaction;
    atomic<bool> stop;
    condition_variable compaction_cv;
    thread compaction_thread;
    int error_bound;
    int batch_size;

    int batch_id;
    mutable shared_mutex csd_mutex;
    mutable shared_mutex csd_result_mutex;
} LSM_tree;

void lsmInit();
void insert(KeyType key, uint8_t* value);
void update(KeyType key, uint8_t* value);
void del(KeyType key);
uint8_t* search(KeyType key, uint8_t* value, bool learned_index);
std::vector<std::pair<KeyType, uint8_t*>> rangeQuery(const KeyType& start_key, int len);
void compaction_worker();
void set_error_bound(int error_bound);
void set_batch_size(int batch_size);
void destroy_lsm();
void print();
void search_in_batch(std::vector<KeyType>& keys, int nr_read, int batch_size);
void search_in_batch_mutil_threads(std::vector<KeyType>& keys, int _keys_cursor, int nr_read, int batch_size);

extern bool csd_compaction;
extern double learned_index_search_time;
extern double mem_search_time;
extern double immu_search_time;
extern double L0_search_time;
extern double L1_search_time;
extern double Ln_search_time;
extern double bst_search_time;
extern int mem_num, immu_num, L0_num, L1_num, Ln_num;
extern uint64_t max_models_size;
extern uint64_t write_stall_time;
extern bool start_read;
#endif