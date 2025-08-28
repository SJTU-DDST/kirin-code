#include "skiplist.h"
#include "kv.h"
#include <cassert>
#include <cstdint>
// 创建新节点
Node *createNode(int level, KeyValuePair kv) {
    Node *node = (Node *)malloc(sizeof(Node) + sizeof(Node *) * (level + 1));
    node->kv = kv;
    node->forward = (Node **)(node + 1);
    for(int i = 0; i < level + 1; i++)
    {
        node->forward[i] = NULL;
    }
    return node;
}

// 初始化跳跃表
SkipList *createSkipList() {
    SkipList *skipList = (SkipList *)malloc(sizeof(SkipList));
    KeyValuePair kv;
    kv.key = 0;
    kv.value = NULL;
    kv.seq = 0;
    skipList->level = 0;
    skipList->header = createNode(MAX_LEVEL, kv);
    skipList->p = P;
    for (int i = 0; i <= skipList->level; i++) {
        skipList->header->forward[i] = NULL;
    }
    return skipList;
}

// 生成一个随机级别
int randomLevel(SkipList *skipList) {
    int level = 0;
    while ((rand() / (float)RAND_MAX) < skipList->p && level < MAX_LEVEL - 1) {
        level++;
    }
    return level;
}

int insert_skiplist(SkipList *skipList, KeyValuePair kv) {
    Node *update[MAX_LEVEL];
    Node *current = skipList->header;
    
    // Find position to insert and update array
    for (int i = skipList->level; i >= 0; i--) {
        while (current->forward[i] != NULL && current->forward[i]->kv.key < kv.key) {
            current = current->forward[i];
        }
        update[i] = current;
    }
    
    current = current->forward[0]; 
    // Check if the key already exists
    if (current == NULL || current->kv.key != kv.key) {
        // Generate a new level
        int newLevel = randomLevel(skipList);

        // If new level is greater than the current level, adjust the level and update array
        if (newLevel > skipList->level) {
            for (int i = skipList->level + 1; i <= newLevel; i++) {
                update[i] = skipList->header;
            }
            skipList->level = newLevel;
        }
       
        // Create new node
        Node *newNode = createNode(newLevel, kv);
        for (int i = 0; i <= newLevel; i++) {
            newNode->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = newNode;
        }
    } 
    else {
        // If the key exists, update the value
        current->kv = kv;
        return 0; // Return 0 to indicate key exists and was updated
    }

    return 1; // Return 1 to indicate a new key was inserted
}

// 插入节点
// int insert_skiplist(SkipList *skipList, KeyValuePair kv) {
//     Node *update[MAX_LEVEL];
//     Node *current = skipList->header;
//     for (int i = skipList->level; i >= 0; i--) {
//         while (current->forward[i] != NULL && current->forward[i]->kv.key < kv.key) {
//             current = current->forward[i];
//         }
//         update[i] = current;
//     }
    
//     current = current->forward[0]; 
//     if (current == NULL || current->kv.key != kv.key) { //|| (kv.seq & DELETE_SEQ) > 0
//         int newLevel = randomLevel(skipList);
//         if (newLevel > skipList->level) {
//             for (int i = skipList->level + 1; i <= newLevel; i++) {
//                 update[i] = skipList->header;
//             }
//             skipList->level = newLevel;
//         }
       
//         Node *newNode = createNode(newLevel, kv);
//         for (int i = 0; i <= newLevel; i++) {
//             newNode->forward[i] = update[i]->forward[i];
//             update[i]->forward[i] = newNode;
//         }
//     } 
//     else
//     {
//         current->kv = kv;
//         return 0;
//     }
//     return 1;
// }

// 查找节点
Node *search_skiplist(SkipList *skipList, KeyType key) {
    Node *current = skipList->header;
    for (int i = skipList->level; i >= 0; i--) {
        while (current->forward[i] != NULL && current->forward[i]->kv.key < key) {
            current = current->forward[i];
        }
    }
    current = current->forward[0];
    if (current != NULL && current->kv.key == key) {
        return current;
    } else {
        return NULL;
    }
}

uint32_t traverseSkipList(SkipList* skipList, uint8_t* buffer, KeyType* smallest, KeyType* largest, std::vector<KeyType>& keys) {
    Node *current = skipList->header->forward[0];
    uint32_t buffer_cursor = 0;
    uint32_t kv_num = 0;
    *smallest = current->kv.key;
    while (current != NULL) 
    {
        keys.emplace_back(current->kv.key);
        memcpy(buffer + buffer_cursor, &(current->kv), VALUE_OFFSET);
        buffer_cursor += VALUE_OFFSET;
        memcpy(buffer + buffer_cursor, current->kv.value, VALUE_LENGTH);
        buffer_cursor += VALUE_LENGTH;
        *largest = current->kv.key;
        current = current->forward[0];
        kv_num++;
    }
    return kv_num;
}

void range_query_skiplist(SkipList* skipList, KeyType start_key, int len, std::vector<std::pair<KeyType, uint8_t*>>& result) 
{
    bool is_empty = result.size() == 0;
    Node* current = skipList->header;
    for (int i = skipList->level; i >= 0; i--) {
        while (current->forward[i] != NULL && current->forward[i]->kv.key < start_key) {
            current = current->forward[i];
        }
    }

    current = current->forward[0];
    if (current && current->kv.key >= start_key) {
        current = current->forward[0];
    }
    
    while (current && len > 0) {
        if (current->kv.key >= start_key) {
            if(is_empty || std::find_if(result.begin(), result.end(), [current](const auto& pair) { return pair.first == current->kv.key; }) == result.end()) {
                uint8_t* value = (uint8_t*)malloc(VALUE_LENGTH);
                memcpy(value, current->kv.value, VALUE_LENGTH);
                result.emplace_back(std::make_pair(current->kv.key, value));
            }
        }
        current = current->forward[0];
        len--;
    }
}

// int main()
// {
//     SkipList* list = createSkipList();
//     KeyValuePair kv;
//     uint8_t* buffer = (uint8_t*)malloc(4*1024*1024);
//     KeyType smallest, largest;
//     for(int i = 1; i <= 30000; i++)
//     {
//         kv.key = i;
//         kv.seq = i + 1;
//         kv.type = INSERT;
//         kv.value = (uint8_t*)malloc(VALUE_LENGTH);
//         insert_skiplist(list, kv);
//     }
//     printf("%ld\n", KV_LENGTH);
//     printf("%d\n", list->level);
//     traverseSkipList(list, buffer, &smallest, &largest);
//     return 0;
// }