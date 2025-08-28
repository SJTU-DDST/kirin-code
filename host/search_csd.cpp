#include "search_csd.h"
#include "kv.h"
#include <cassert>
#include <cstdint>
#include <sys/select.h>
uint8_t* search_ioctl_buf = new uint8_t [MAX_BATCH_SIZE * 3 * sizeof(uint64_t)];
uint8_t* search_result_buf = new uint8_t [VALUE_LENGTH * MAX_BATCH_SIZE];
void resetStatus(int dev_id, int dev_fd)
{
    nvme_passthru_cmd cmd;
    int ret;
    memset(&cmd, 0, sizeof(cmd));
    cmd.opcode = nvme_admin_reset_status;
    cmd.nsid = dev_id;
    cmd.metadata = 0;
    cmd.addr = 0;
    cmd.cdw2 = 0;
    cmd.cdw3 = 0;
    cmd.cdw10 = 0;
    cmd.cdw11 = 0;
    cmd.cdw12 = 0;
    cmd.cdw13 = 0;
    cmd.cdw14 = 0;
    cmd.cdw15 = 0;
    cmd.data_len = 0;
    cmd.metadata_len = 0;
    ret = ioctl(dev_fd, _IOWR('N', 0x41, nvme_passthru_cmd), &cmd);
    if (ret < 0) {
        perror("ioctl");
        return;
    }
}

void getFileAddress(uint64_t file_id, uint32_t file_num, int dev_id, int dev_fd, int is_args_file, int is_result_file)
{
    nvme_passthru_cmd cmd;
    int ret;
    memset(&cmd, 0, sizeof(cmd));
    cmd.opcode = nvme_admin_get_address;
    cmd.nsid = dev_id;
    cmd.metadata = 0;
    cmd.addr = 0;
    cmd.cdw2 = 0;
    cmd.cdw3 = 0;
    cmd.cdw10 = file_num;
    cmd.cdw11 = file_id >> 32;
    cmd.cdw12 = file_id;
    cmd.cdw13 = is_result_file;
    cmd.cdw14 = is_args_file;
    cmd.cdw15 = 0;
    cmd.data_len = 0;
    cmd.metadata_len = 0;
    ret = ioctl(dev_fd, _IOWR('N', 0x41, nvme_passthru_cmd), &cmd);
    if (ret < 0) {
        perror("ioctl");
        return;
    }
}

void collectSearchResult(int nr_search, int dev_id, int dev_fd, int batch_id, uint8_t* buffer)
{
    size_t transfer_size = VALUE_LENGTH * nr_search;
    size_t buf_size = ((transfer_size + NVME_BLOCK_SIZE - 1) / NVME_BLOCK_SIZE) * NVME_BLOCK_SIZE;
    nvme_passthru_cmd cmd;
    int ret;
    
    memset(&cmd, 0, sizeof(cmd));
    cmd.opcode = nvme_cmd_read;
    cmd.nsid = dev_id;
    cmd.metadata = 0;
    cmd.addr = (uint64_t)buffer;
    cmd.cdw2 = 0;
    cmd.cdw3 = 0;
    cmd.cdw10 = 0;
    cmd.cdw11 = 0;
    if(batch_id == 1)
        cmd.cdw12 = (0) | (1 << 16) | (1 << 18) | (1 << 19);
    else
        cmd.cdw12 = (0) | (1 << 16) | (1 << 18);
    cmd.cdw13 = 0;
    cmd.cdw14 = 0;
    cmd.cdw15 = 0;
    cmd.data_len = buf_size;
    cmd.metadata_len = 0;
    
    ret = ioctl(dev_fd, _IOWR('N', 0x43, nvme_passthru_cmd), &cmd);
    if (ret == -1) {
        fprintf(stderr, "ioctl failed: %s\n", strerror(errno));
        exit(0);
    }
    assert(ret == 0);
}

void offloadSearch(uint64_t* args, int dev_id, int dev_fd, int batch_id, uint32_t nr_search, uint32_t batch_size)
{
    size_t args_size = sizeof(uint64_t) * nr_search * 3;
    assert(nr_search <= batch_size);
    size_t buf_size = ((args_size + NVME_BLOCK_SIZE - 1) / NVME_BLOCK_SIZE) * NVME_BLOCK_SIZE;
    nvme_passthru_cmd cmd;
    int ret;
    memcpy(search_ioctl_buf, args, args_size);
    
    memset(&cmd, 0, sizeof(cmd));
    cmd.opcode = nvme_cmd_write;
    cmd.nsid = dev_id;
    cmd.metadata = 0;
    cmd.addr = (uint64_t)search_ioctl_buf;
    cmd.cdw2 = 0;
    cmd.cdw3 = 0;
    cmd.cdw10 = 0;
    cmd.cdw11 = 0;
    if(batch_id == 1)
        cmd.cdw12 = (buf_size / NVME_BLOCK_SIZE - 1) | (1 << 16) | (1 << 18) | (1 << 19);
    else
        cmd.cdw12 = (buf_size / NVME_BLOCK_SIZE - 1) | (1 << 16) | (1 << 18);
    cmd.cdw13 = nr_search;
    cmd.cdw14 = batch_size;
    cmd.cdw15 = 0;
    cmd.data_len = buf_size;
    cmd.metadata_len = 0;

    ret = ioctl(dev_fd, _IOWR('N', 0x43, nvme_passthru_cmd), &cmd);
    if (ret == -1) {
        fprintf(stderr, "ioctl failed: %s\n", strerror(errno));
        exit(0);
    }
    assert(ret == 0);
}