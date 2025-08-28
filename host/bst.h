#ifndef BST_H
#define BST_H
#include "table.h"
#include <sys/stat.h>

SSTable* findBstMin(SSTable* node);
SSTable* bstInsert(SSTable* root, SSTable* sst);
SSTable* bstSearch(SSTable* root, KeyType key);
SSTable* deleteBstNode(SSTable* root, uint64_t table_num, SSTable** deleted, bool& fl);
void bstRangeSearch(SSTable* root, KeyType low, KeyType high, uint64_t* input_ids_i1, uint64_t* input_nums_i1, int* level_i1_index);
SSTable* bstSearchHigherTable(SSTable* root, KeyType last_smallest_key);
void inorderTraversal(SSTable* root);
#endif