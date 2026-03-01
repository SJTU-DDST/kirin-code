#include "table.h"
#include "kv.h"
#include "search_csd.h"
#include "utils.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
uint64_t table_num = 0;
int ERROR = 32;
MemTable* createMemTable()
{
    MemTable* mt = (MemTable*)malloc(sizeof(MemTable));
    mt->table = createSkipList();
    mt->filter = create_bloom_filter(TABLE_SIZE_THRES / KV_LENGTH + 1);
    mt->max_size = TABLE_SIZE_THRES;
    mt->size = 0;
    return mt;
}

ImmutableTable* createImmutableTable()
{
    ImmutableTable* immu = (ImmutableTable*)malloc(sizeof(ImmutableTable));
    immu->filter = NULL;
    int ret = posix_memalign((void **)&(immu->kv_buffer), 4096, TABLE_MAX_SIZE);
    assert(ret == 0);
    return immu;
}

void insertMemTable(MemTable* mt, KeyValuePair kv)
{
    unique_lock<shared_mutex> lock(mt->table_mutex);
    int ret = insert_skiplist(mt->table, kv);
    if(ret > 0)
    {
        mt->size += KV_LENGTH;
        add_to_bloom_filter(mt->filter, kv.key);
    }
}

void updateMemTable(MemTable* mt, KeyValuePair kv)
{
    insertMemTable(mt, kv);
}

void deleteMemTable(MemTable* mt, KeyValuePair kv)
{
    assert((kv.seq & DELETE_SEQ) > 0);
    insertMemTable(mt, kv);
}

uint8_t* searchMemTable(MemTable* mt, KeyType key, uint8_t* value)
{
    Node* node = NULL;
    {
        // shared_lock<shared_mutex> lock(mt->table_mutex);
        if(query_bloom_filter(mt->filter, key))
            node = search_skiplist(mt->table, key);
        else
            return NULL;
    }
   
    if(node != NULL)
    {
        memcpy(value, node->kv.value, VALUE_LENGTH);
        return value;
    }
    return NULL;
}

void rangeSearchMemTable(MemTable* mt, KeyType start_key, int len, std::vector<std::pair<KeyType, uint8_t*>>& result)
{
    range_query_skiplist(mt->table, start_key, len, result);
}

uint8_t* searchImmuTable(ImmutableTable* immu, KeyType key, uint8_t* value)
{
    Node* node = NULL;
    {
        if(query_bloom_filter(immu->filter, key))
            node = search_skiplist(immu->table, key);
        else
            return NULL;
    }
    
    if(node != NULL)
    {
        memcpy(value, node->kv.value, VALUE_LENGTH);
        return value;
    }
    return NULL;
}

void rangeSearchImmuTable(ImmutableTable* immu, KeyType start_key, int len, std::vector<std::pair<KeyType, uint8_t*>>& result)
{
    range_query_skiplist(immu->table, start_key, len, result);
}

uint8_t* searchSSTable(SSTable* st, KeyType key)
{
    uint8_t* value = (uint8_t*)malloc(VALUE_LENGTH);
    int fd;
    if(st->fd >= 0)
        fd = st->fd;
    else
        fd = open(st->table_name, O_RDONLY);
    if(fd < 0)
    {
        perror("open error");
    }
    assert(fd >= 0);
    uint8_t* buffer = (uint8_t*)mmap(NULL, st->size, PROT_READ, MAP_PRIVATE, fd, 0);
    // uint64_t read_size = readStorage(st->fd, (void*)buffer, st->size);
    // assert(read_size == st->size);
    int mid, left = 0, right = st->kv_num - 1;
    uint8_t* mid_ptr;
    KeyType test_key;
    // printf("%ld\n", key);

    while(left <= right)
    {
        mid = (right + left) / 2;
        mid_ptr = buffer + mid * (KV_LENGTH);
        test_key = ((uint64_t*)mid_ptr)[0];
        if(key == test_key)
        {
            printf("%ld\n", st->table_num);
            printf("real pos = %d\n", mid);
            memcpy(value, mid_ptr + VALUE_OFFSET, VALUE_LENGTH);
            munmap(buffer, st->size);
            close(fd);
            st->fd = -1;
            return value;
        }
        else if(key > test_key)
        {
            left = mid + 1;
        }
        else
        {
            right = mid - 1;
        }
    }
    munmap(buffer, st->size);
    close(fd);
    return NULL;
}

std::pair<uint64_t, uint64_t> predict(segment* segs, int segs_size, const KeyType target, double error, uint64_t max_size)
{
    uint32_t left = 0, right = segs_size;
    while (left != right - 1) {
        uint32_t mid = (right + left) / 2;
        if (target < segs[mid].x)
            right = mid;
        else
            left = mid;
    }
    double result = segs[left].k * (double)target + segs[left].b;
    uint64_t lower = result - error > 0 ? (uint64_t)std::floor(result - error) : 0;
    uint64_t upper = (uint64_t)std::ceil(result + error);
    uint64_t size = (upper - lower + 1) * KV_LENGTH;
    // if(size > 4096)
    // {
    //     printf("%ld, %lf, %lf, %ld\n", segs[left].x, segs[left].k, segs[left].b, size);
    // }
    if (lower >= max_size) 
        return std::make_pair(max_size, max_size);
    upper = upper < max_size ? upper : max_size;
    return std::make_pair(lower, upper);
}

double disk_read_time = 0;
double calculate_time = 0;
double search_time = 0;
int nr_pread = 0;
int max_size = 0;
thread_local uint8_t* scan_buffer = NULL;
thread_local int buffer_size = 0;
uint8_t* searchSSTableLearnedIndex(SSTable* st, KeyType key, uint8_t* value, double error)
{
    st->search_times++;
    int fd;
    if(st->fd < 0)
    {
        fd = open(st->table_name, O_RDONLY | O_DIRECT);
        if(fd < 0)
            perror("error open");
        assert(fd >= 0);
        st->fd = fd;
    }
    else {
        fd = st->fd;
    }

    std::pair<uint64_t, uint64_t> pos = predict(st->segs, st->segs_size, key, error, st->size / KV_LENGTH);


    uint64_t offset = pos.first * KV_LENGTH;
    uint64_t size = (pos.second - pos.first + 1) * KV_LENGTH;

    uint64_t read_offset = offset / 4096 * 4096;
    uint64_t buffer_offset = offset - read_offset;

    uint64_t read_size = ((size + buffer_offset) / 4096 + 1) * 4096;
    if(scan_buffer == NULL || read_size > buffer_size)
    {
        posix_memalign((void **)&(scan_buffer), 4096, read_size);
        buffer_size = read_size;
    }
    
    int r_size = pread(fd, scan_buffer, read_size, read_offset);
    if(r_size < 0)
    {
        printf("pread failed: %s\n", strerror(errno));
    }
    assert(r_size > 0);

    KeyType test_key;
    for(int i = 0; i < size / KV_LENGTH; i++)
    {
        test_key = ((KeyType*)(scan_buffer + buffer_offset + i * KV_LENGTH))[0];
        if(test_key == key)
        {
            // printf("%ld, %ld, %ld, %ld, %d\n", st->table_num, key, pos.first, pos.second, i);
            memcpy(value, scan_buffer + buffer_offset + i * KV_LENGTH + VALUE_OFFSET, VALUE_LENGTH);
            // free(buffer);
            // close(fd);
            // gettimeofday(&end, NULL);
            // seconds = end.tv_sec  - start.tv_sec;
            // useconds = end.tv_usec - start.tv_usec;
            // elapsed = seconds * 1000000.0 + useconds;
            // search_time += elapsed;
            return value;
        }
    }
    // free(buffer);
    // close(fd);
    // gettimeofday(&end, NULL);
    // seconds  = end.tv_sec  - start.tv_sec;
    // useconds = end.tv_usec - start.tv_usec;
    // elapsed = seconds * 1000000.0 + useconds;
    // search_time += elapsed;
    return NULL;
}

uint8_t* rangeSearchSSTableLearnedIndex(SSTable* st, KeyType start_key, int len, std::vector<std::pair<KeyType, uint8_t*>>& result, int& max_size, double error)
{
    int fd;
    if(st->fd < 0)
    {
        fd = open(st->table_name, O_RDONLY);
        if(fd < 0)
            perror("error open");
        assert(fd >= 0);
        st->fd = fd;
    }
    else
    {
        fd = st->fd;
    }
    std::pair<uint64_t, uint64_t> pos = predict(st->segs, st->segs_size, start_key, error, st->size / KV_LENGTH);
    uint64_t offset = pos.first * KV_LENGTH;
    uint64_t size = (pos.second - pos.first + len) * KV_LENGTH;
    size = std::min(st->size - offset, size);
    uint8_t* buffer = (uint8_t*)malloc(size);
    max_size = size;
    pread(fd, buffer, size, offset);
    // uint8_t* buffer = (uint8_t*)mmap(NULL, 4096, PROT_READ, MAP_PRIVATE, fd, 0);
    return buffer;
}

void memToImmu(MemTable* mt, ImmutableTable* immu)
{
    immu->size = mt->size;
    immu->table = mt->table;
    immu->filter = mt->filter;
    // free(immu->kv_buffer);
    // int ret = posix_memalign((void **)&(immu->kv_buffer), 4096, TABLE_MAX_SIZE);
    // assert(ret == 0);
}

SSTable* flushMemTable(ImmutableTable* immu, int& average_length)
{
    SSTable* sst = (SSTable*)malloc(sizeof(SSTable));
    sst->timestamp = get_time_us();
    sst->search_times = 0;
    std::vector<KeyType> keys;
    PLR plr = PLR(ERROR);
    immu->kv_num = traverseSkipList(immu->table, immu->kv_buffer, &(immu->smallest), &(immu->largest), keys);

    thread train_thread(&PLR::train, &plr, std::ref(keys), std::ref(average_length));
    
    sprintf(sst->table_name, "/mnt/openssd/%ld.txt", table_num);
    sst->table_num = table_num;
    table_num++;
    int fd = open(sst->table_name, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if(fd < 0)
        perror("open error");
    assert(fd >= 0);
    posix_fallocate64(fd, 0, TABLE_MAX_SIZE);
    uint64_t written_size = writeStorage(fd, immu->kv_buffer, immu->size);
    int ret = ftruncate(fd, immu->size);
    assert(ret == 0);
    fsync(fd);
    close(fd);
    train_thread.join();
    keys.clear();
    sst->fd = open(sst->table_name, O_RDONLY);
    assert(sst->fd >= 0);
    sst->filter = immu->filter;
    sst->segs = (segment*)malloc(plr.segments.size() * sizeof(segment));
    sst->segs_size = plr.segments.size();
    std::copy(plr.segments.begin(), plr.segments.end(), sst->segs);
    sst->size = immu->size;
    sst->kv_num = immu->kv_num;
    sst->smallest_key = immu->smallest;
    sst->largest_key = immu->largest;
    sst->next_table = NULL;
    sst->is_selected = false;
    return sst;
}