#ifndef TABLE_H
#define TABLE_H
#include <cstdint>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/mman.h>
#include "skiplist.h"
#include "utils.h"
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <vector>
#include <math.h>
#include <sys/time.h>
#include "plr.h"
#include "bloom_filter.h"
#include "search_csd.h"
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>

#define TABLE_SIZE_THRES (64*1024*1024)
#define TABLE_MAX_SIZE (66*1024*1024)
extern int ERROR;
using std::shared_mutex, std::mutex;
using std::unique_lock, std::shared_lock;
using std::condition_variable, std::atomic;
using std::thread;
typedef struct
{
    SkipList* table;
    uint32_t size;
    uint32_t max_size;
    BloomFilter* filter;
    mutable shared_mutex table_mutex;
}MemTable;

typedef struct 
{
    SkipList* table;
    uint32_t size;
    uint8_t* kv_buffer;
    uint32_t kv_num;
    KeyType smallest;
    KeyType largest;
    BloomFilter* filter;
}ImmutableTable;

typedef struct SSTable
{
    uint64_t table_num;
    int fd;
    char table_name[50];
    uint64_t size;
    uint64_t max_size;
    uint64_t kv_num;
    uint64_t smallest_key;
    uint64_t largest_key;
    struct SSTable* next_table;         // used for level 0 and level n (left sub tree pointer)
    struct SSTable* next_table_right;   // used for level n
    segment* segs;
    int segs_size;
    bool is_selected;
    uint64_t timestamp;
    uint64_t search_times;
    BloomFilter* filter;
}SSTable;

MemTable* createMemTable();
ImmutableTable* createImmutableTable();
void insertMemTable(MemTable* mt, KeyValuePair kv);
void updateMemTable(MemTable* mt, KeyValuePair kv);
void deleteMemTable(MemTable* mt, KeyValuePair kv);
uint8_t* searchMemTable(MemTable* mt, KeyType key, uint8_t* value);
uint8_t* searchImmuTable(ImmutableTable* immu, KeyType key, uint8_t* value);
uint8_t* searchSSTable(SSTable* st, KeyType key);
uint8_t* searchSSTableLearnedIndex(SSTable* st, KeyType key, uint8_t* value, double error);
void rangeSearchMemTable(MemTable* mt, KeyType start_key, int len, std::vector<std::pair<KeyType, uint8_t*>>& result);
void rangeSearchImmuTable(ImmutableTable* immu, KeyType start_key, int len, std::vector<std::pair<KeyType, uint8_t*>>& result);
uint8_t* rangeSearchSSTableLearnedIndex(SSTable* st, KeyType start_key, int len, std::vector<std::pair<KeyType, uint8_t*>>& result, int& max_size, double error);
void memToImmu(MemTable* mt, ImmutableTable* immu);
SSTable* flushMemTable(ImmutableTable* immu, int& average_length);
std::pair<uint64_t, uint64_t> predict(segment* segs, int segs_size, const KeyType target, double error, uint64_t max_size);

extern uint64_t table_num;
extern double disk_read_time;
extern double calculate_time;
extern double search_time;
extern int nr_pread;
extern int max_size;
#endif