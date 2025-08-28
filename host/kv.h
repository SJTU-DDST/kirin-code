#ifndef KV_H
#define KV_H
#include <stdint.h>
#include "utils.h"
typedef uint64_t KeyType;
typedef struct {
    KeyType key;
    uint64_t seq;
    uint8_t* value;
} KeyValuePair;

#define KEY_LENGTH sizeof(KeyType)
#define VALUE_LENGTH 64
#define DELETE_SEQ 0x8000000000000000
#define INSERT_SEQ 0x0
#define KV_LENGTH (KEY_LENGTH + VALUE_LENGTH + sizeof(uint64_t))
#define VALUE_OFFSET (KEY_LENGTH + sizeof(uint64_t))
#endif