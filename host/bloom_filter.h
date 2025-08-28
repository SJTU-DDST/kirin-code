#ifndef BLOOM_H
#define BLOOM_H
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include "kv.h"

// 布隆过滤器结构体
typedef struct {
    size_t size;
    size_t hash_count;
    uint8_t *bit_array;
} BloomFilter;

BloomFilter* create_bloom_filter(size_t element_count);
void add_to_bloom_filter(BloomFilter *filter, uint64_t key);
bool query_bloom_filter(BloomFilter *filter, uint64_t key);
void free_bloom_filter(BloomFilter *filter);
#endif