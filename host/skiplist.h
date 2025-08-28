#ifndef SKIPLIST_H
#define SKIPLIST_H
#include <stdlib.h>
#include <stdbool.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>
#include <vector>
#include <utility>
#include "kv.h"
// 节点结构体
#define P 0.5
#define MAX_LEVEL 64
typedef struct Node{
    KeyValuePair kv;
    struct Node **forward;
} Node;

// 跳跃表结构体
typedef struct {
    int level;
    Node *header;
    float p;
} SkipList;

int insert_skiplist(SkipList *skipList, KeyValuePair kv);
Node *search_skiplist(SkipList *skipList, KeyType key);
Node *createNode(int level, KeyValuePair kv);
SkipList *createSkipList();
uint32_t traverseSkipList(SkipList* skipList, uint8_t* buffer, KeyType* smallest, KeyType* largest, std::vector<KeyType>& keys);
void range_query_skiplist(SkipList* skipList, KeyType start_key, int len, std::vector<std::pair<KeyType, uint8_t*>>& result);
#endif