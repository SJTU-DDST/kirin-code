#include "bloom_filter.h"
#define FALSE_POSITIVE_RATE 0.05
// 创建布隆过滤器
BloomFilter* create_bloom_filter(size_t element_count) {
    // 计算最优位数组大小和哈希函数数量
    size_t m = - (element_count * log(FALSE_POSITIVE_RATE)) / (log(2) * log(2));
    size_t k = (m / element_count) * log(2) + 1;
    BloomFilter *filter = (BloomFilter *)malloc(sizeof(BloomFilter));
    filter->bit_array = (uint8_t *)calloc((m + 7) / 8, sizeof(uint8_t)); // 确保分配足够的字节数
    filter->size = m;
    filter->hash_count = k;
    return filter;
}

// 哈希函数示例
uint64_t hash1(uint64_t key) {
    key = (~key) + (key << 21);
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8);
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4);
    key = key ^ (key >> 28);
    key = key + (key << 31);
    return key;
}

uint64_t hash2(uint64_t key) {
    key = (~key) + (key << 18);
    key = key ^ (key >> 31);
    key = (key + (key << 8)) + (key << 12);
    key = key ^ (key >> 22);
    key = (key + (key << 2)) + (key << 7);
    key = key ^ (key >> 24);
    key = key + (key << 15);
    return key;
}

// 向布隆过滤器中添加键
void add_to_bloom_filter(BloomFilter *filter, uint64_t key) {
    uint64_t hash1_val = hash1(key) % filter->size;
    uint64_t hash2_val = hash2(key) % filter->size;

    for (uint64_t i = 0; i < filter->hash_count; i++) {
        uint64_t combined_hash = (hash1_val + i * hash2_val) % filter->size;
        filter->bit_array[combined_hash / 8] |= (1 << (combined_hash % 8));
    }
}

// 查询布隆过滤器
bool query_bloom_filter(BloomFilter *filter, uint64_t key) {
    uint64_t hash1_val = hash1(key) % filter->size;
    uint64_t hash2_val = hash2(key) % filter->size;

    for (uint64_t i = 0; i < filter->hash_count; i++) {
        uint64_t combined_hash = (hash1_val + i * hash2_val) % filter->size;
        if (!(filter->bit_array[combined_hash / 8] & (1 << (combined_hash % 8)))) {
            return false;
        }
    }
    return true;
}

// 清理布隆过滤器
void free_bloom_filter(BloomFilter *filter) {
    free(filter->bit_array);
    free(filter);
}