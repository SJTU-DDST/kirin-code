#ifndef CS_FILE_H_
#define CS_FILE_H_

#include <stdint.h>
#include <stdlib.h>
#include "config.h"
#include "queue.h"

enum {
    CS_FILE_STATUS_FREE = 0,
    CS_FILE_STATUS_OPENED,
    CS_FILE_STATUS_CLOSING,
};

struct cs_file {
    uint32_t id;
    uint32_t size;
    uint64_t disk_offset;
    uint32_t pointer;
    uint32_t pending_ios;
    uint32_t status;
    QTAILQ_ENTRY(cs_file) qent;
};

struct cs_file *cs_file_open(uint64_t id);
void cs_file_close(struct cs_file *file);
uint64_t cs_file_io(struct cs_file *file, void *buf, uint64_t len, uint64_t offset, int is_read);
uint64_t cs_file_pread(struct cs_file *file, void *buf, uint64_t len, uint64_t offset);
uint64_t cs_file_pwrite(struct cs_file *file, void *buf, uint64_t len, uint64_t offset);
uint64_t cs_file_read(struct cs_file *file, void *buf, uint64_t len);
uint64_t cs_file_write(struct cs_file *file, void *buf, uint64_t len);
int cs_file_seek(struct cs_file *file, uint64_t offset);
void sync_file_pending_ios(struct cs_file *file);
void sync_closing_files();
void init_cs_files();

#endif
