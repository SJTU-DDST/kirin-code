#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include "kv.h"
#include "table.h"
#include "compaction.h"
#include "lsm.h"
#include <errno.h>
typedef struct
{
    int nr_search;
    int batch_id;
    int files_num_offset;
    int target_keys_offset;
    int kv_pos_offset;
    uint8_t payload[0];
}search_args_test;
void getFileAddress_test(uint64_t file_id, uint32_t file_num, int dev_id, int dev_fd, int is_args_file, int is_result_file)
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
    cmd.cdw13 = is_args_file;
    cmd.cdw14 = is_result_file;
    cmd.cdw15 = 0;
    cmd.data_len = 0;
    cmd.metadata_len = 0;
    ret = ioctl(dev_fd, _IOWR('N', 0x41, nvme_passthru_cmd), &cmd);
    if (ret < 0) {
        perror("ioctl");
        return;
    }
}
uint8_t* ioctl_buf_test = new uint8_t[32 * 1024];
void collectSearchResult_test(int nr_search, int dev_id, int dev_fd, int batch_id)
{
    size_t transfer_size = VALUE_LENGTH * nr_search;
    size_t buf_size = ((transfer_size + NVME_BLOCK_SIZE - 1) / NVME_BLOCK_SIZE) * NVME_BLOCK_SIZE;
    nvme_passthru_cmd cmd;
    int ret;
    
    memset(&cmd, 0, sizeof(cmd));
    cmd.opcode = nvme_cmd_read;
    cmd.nsid = dev_id;
    cmd.metadata = 0;
    cmd.addr = (uint64_t)ioctl_buf_test;
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

void offloadSearch_test(uint64_t* args, int dev_id, int dev_fd, int batch_id, uint32_t nr_search)
{
    size_t args_size = sizeof(uint64_t) * nr_search * 3;
    size_t buf_size = ((args_size + NVME_BLOCK_SIZE - 1) / NVME_BLOCK_SIZE) * NVME_BLOCK_SIZE;
    nvme_passthru_cmd cmd;
    int ret;
    memcpy(ioctl_buf_test, args, args_size);
    
    memset(&cmd, 0, sizeof(cmd));
    cmd.opcode = nvme_cmd_write;
    cmd.nsid = dev_id;
    cmd.metadata = 0;
    cmd.addr = (uint64_t)ioctl_buf_test;
    cmd.cdw2 = 0;
    cmd.cdw3 = 0;
    cmd.cdw10 = 0;
    cmd.cdw11 = 0;
    if(batch_id == 1)
        cmd.cdw12 = (buf_size / NVME_BLOCK_SIZE - 1) | (1 << 16) | (1 << 18) | (1 << 19);
    else
        cmd.cdw12 = (buf_size / NVME_BLOCK_SIZE - 1) | (1 << 16) | (1 << 18);
    cmd.cdw13 = nr_search;
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


int lmain()
{
    printf("start test search engine\n");
    int dev_fd = open(openssd_dir, O_DIRECT | O_RDWR);
    assert(dev_fd >= 0);
    int dev_id = ::ioctl(dev_fd, _IO('N', 0x40));
    assert(dev_id >= 0);
    int files[1024];
    int nr_search = 32;
    char filename[50];
    uint8_t* kv_buffer = (uint8_t*)malloc(4096);
    uint64_t target_key = 1, kv_pos = 5, kv_pos_offset = 8;

    uint64_t* args = (uint64_t*)malloc(sizeof(uint64_t) * nr_search * 3);
    // posix_memalign((void **)&(args), 4096, 8*1024);
    uint64_t* files_num_ptr = args;
    uint64_t* target_keys_ptr = args + nr_search;
    uint64_t* kv_pos_ptr = args + 2 * nr_search;


    // 第一列
    // int file_num[] = {7, 6, 5, 6, 4, 5, 6, 6, 5, 7, 7, 4, 6, 6, 6, 7, 6, 7, 6, 6, 4, 7, 7, 4, 6, 4, 4, 5, 7, 4, 5, 5};
    int file_num[] = {0, 1, 2, 3, 4, 5, 6, 7, 5, 7, 7, 4, 6, 6, 6, 7, 6, 7, 6, 6, 4, 7, 7, 4, 6, 4, 4, 5, 7, 4, 5, 5};
    // 第二列
    // uint64_t keys[] = {
    //     35783064990, 28920862796, 11647824951, 25324755715, 5488962118,
    //     10000000000, 28958701992, 26277145387, 16909385856, 36755623396,
    //     32953865398, 1699561032, 28599770283, 25815758018, 20953016723,
    //     36778509273, 21269715324, 37077835040, 24066425077, 21154461614,
    //     80949482, 33920277645, 35392774897, 9273616358, 21014161778,
    //     8418713015, 4759582267, 13126045792, 31052273256, 9083164630,
    //     10042124366, 13288233375
    // };
    uint64_t keys[] = {
        1000000000000, 20000000000000, 30000000000000, 4000000000000, 500000000000000,
        10000000000000, 1111, 2627, 16909385856, 36755623396,
        32953865398, 1699561032, 28599770283, 25815758018, 20953016723,
        36778509273, 21269715324, 37077835040, 24066425077, 21154461614,
        80949482, 33920277645, 35392774897, 9273616358, 21014161778,
        8418713015, 4759582267, 13126045792, 31052273256, 9083164630,
        10042124366, 13288233375
    };
    // 第三列
    // uint64_t kv_pos_array[] = {
    //     578173, 822335, 174554, 513714, 485496, 596766, 825943, 595305,
    //     625013, 662622, 337468, 155367, 794751, 556580, 131271, 664710,
    //     159374, 691020, 404303, 149181, 7669, 418326, 545765, 811879,
    //     136617, 738858, 420870, 301177, 172628, 796132, 37413, 315322
    // };
    uint64_t kv_pos_array[] = {
        4, 4, 4, 4, 4, 4, 4, 595305,
        625013, 662622, 337468, 155367, 794751, 556580, 131271, 664710,
        159374, 691020, 404303, 149181, 7669, 418326, 545765, 811879,
        136617, 738858, 420870, 301177, 172628, 796132, 37413, 315322
    };
    // uint64_t kv_pos_array[] = {
    //     578173, 822335, 174554, 513714, 485496, 111111, 825943, 595305,
    //     625013, 662622, 337468, 155367, 794751, 556580, 131271, 664710,
    //     159374, 691020, 404303, 149181, 7669, 418326, 545765, 811879,
    //     136617, 738858, 420870, 301177, 172628, 796132, 37413, 315322
    // };
    // 第四列
    int col4[] = {
        578190, 822352, 174571, 513731, 485513, 596783, 825960, 595322,
        625030, 662639, 337485, 155384, 794768, 556597, 131288, 664727,
        159391, 691037, 404320, 149198, 7686, 418343, 545782, 811896,
        136634, 738875, 420887, 301194, 172645, 796149, 37430, 315339
    };
    
    // 第五列
    // uint64_t kv_offset_array[] = {
    //     8, 10, 13, 8, 11, 8, 12, 11, 13, 10, 4, 6, 6, 3, 11, 9, 10,
    //     11, 5, 10, 12, 7, 4, 5, 9, 12, 5, 13, 8, 4, 9
    // };
    uint64_t kv_offset_array[] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 4, 
        6, 6, 3, 11, 9, 10,
        11, 5, 10, 12, 7, 4, 5, 9, 
        12, 5, 13, 8, 4, 9, 10};
    for(int i = 0; i < 8; i++)
    {
        sprintf(filename, "/mnt/openssd/%d.txt", i);
        files[i] = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
        posix_fallocate64(files[i], 0, TABLE_SIZE_THRES);
        fsync(files[i]);
        struct stat64 st;
        int ret = stat64(filename, &st);
        assert(ret == 0);
        getFileAddress_test(st.st_ino, i, dev_id, dev_fd, 0, 0);
    }

    for(int i = 0; i < nr_search; i++)
    {
        // for(uint64_t j = 0; j < 18; j++)
        // {
        //     memcpy(kv_buffer + j * KV_LENGTH, &j, sizeof(uint64_t));
        // }
        memcpy(kv_buffer + kv_offset_array[i] * KV_LENGTH, keys + i, sizeof(uint64_t));
        memset(kv_buffer + kv_offset_array[i] * KV_LENGTH + VALUE_OFFSET, 'a', VALUE_LENGTH);
        pwrite(files[file_num[i]], kv_buffer, 4096, KV_LENGTH * kv_pos_array[i]);
        fsync(files[file_num[i]]);
        // close(files[i]);
        files_num_ptr[i] = file_num[i];
        target_keys_ptr[i] = keys[i];
        kv_pos_ptr[i] = kv_pos_array[i] * KV_LENGTH;
    }

    // for(int i = 0; i < nr_search; i++)
    // {
    //     sprintf(filename, "/mnt/openssd/%d.txt", i);
    //     files[i] = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
    //     posix_fallocate64(files[i], 0, TABLE_SIZE_THRES);
    //     memcpy(kv_buffer + kv_pos_offset * KV_LENGTH, &target_key, sizeof(uint64_t));
    //     memset(kv_buffer + kv_pos_offset * KV_LENGTH + VALUE_OFFSET, 'a', VALUE_LENGTH);
    //     pwrite(files[i], kv_buffer, 4096, KV_LENGTH * kv_pos);
    //     fsync(files[i]);
    //     struct stat64 st;
    //     int ret = stat64(filename, &st);
    //     assert(ret == 0);
    //     close(files[i]);
    //     getFileAddress_test(st.st_ino, i, dev_id, dev_fd, 0);
    //     files_num_ptr[i] = i;
    //     target_keys_ptr[i] = target_key;
    //     kv_pos_ptr[i] = kv_pos * KV_LENGTH;
    //     target_key += 2000000000;
    //     kv_pos += 10;
    // }

    uint8_t* buffer = (uint8_t*)malloc(TABLE_SIZE_THRES);
    memset(buffer, 222, TABLE_SIZE_THRES);
    int result_fd = open("/mnt/openssd/result_file.txt", O_RDWR | O_CREAT | O_TRUNC, 0644);
    posix_fallocate64(result_fd, 0, TABLE_SIZE_THRES);
    int ret = write(result_fd, buffer, TABLE_SIZE_THRES);
    printf("%d\n", ret);
    fsync(result_fd);
    struct stat64 st;
    ret = stat64("/mnt/openssd/result_file.txt", &st);
    uint8_t* result_buffer = (uint8_t*)malloc(1024*1024);
    getFileAddress_test(st.st_ino, 4095, dev_id, dev_fd, 0, 1);

    struct timeval start, end;

    int args_fd = open("/mnt/openssd/args_file.txt", O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0644);
    posix_fallocate64(args_fd, 0, TABLE_SIZE_THRES);
    fsync(args_fd);

    gettimeofday(&start, NULL);
    // ret = write(args_fd, args, 4096);
    // fsync(args_fd);
    gettimeofday(&end, NULL);
    long sec  = end.tv_sec  - start.tv_sec;
    long usec = end.tv_usec - start.tv_usec;
    double elaps = sec * 1000000 + usec;
    printf("write disk time = %.6f useconds\n", elaps);
    
    ret = stat64("/mnt/openssd/args_file.txt", &st);
    getFileAddress_test(st.st_ino, 4095, dev_id, dev_fd, 1, 0);


    
    gettimeofday(&start, NULL);
    offloadSearch_test(args, dev_id, dev_fd, 0, nr_search);
    gettimeofday(&end, NULL);
    long seconds  = end.tv_sec  - start.tv_sec;
    long useconds = end.tv_usec - start.tv_usec;
    double elapsed = seconds * 1000000 + useconds;
    printf("CSD search time: %.6f useconds\n", elapsed);

    gettimeofday(&start, NULL);
    offloadSearch_test(args, dev_id, dev_fd, 1, nr_search);
    gettimeofday(&end, NULL);
    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;
    elapsed = seconds * 1000000 + useconds;
    printf("CSD search time: %.6f useconds\n", elapsed);
    collectSearchResult_test(nr_search, dev_id, dev_fd, 0);
    

    // sleep(1);
    // gettimeofday(&start, NULL);
    // collectSearchResult(args, dev_id, dev_fd);
    // gettimeofday(&end, NULL);
    // seconds  = end.tv_sec  - start.tv_sec;
    // useconds = end.tv_usec - start.tv_usec;
    // elapsed = seconds * 1000000 + useconds;
    // printf("CSD collect time: %.6f useconds\n", elapsed);
    // ret = pread(result_fd, result_buffer, nr_search * VALUE_LENGTH, 0);
    // printf("%d\n", ret);
    // close(result_fd);
    printf("nr_search = %d\n", nr_search);
    for(int i = 0; i < nr_search / 8 + 1; i++)
    {
        // if(ioctl_buf_test[i*VALUE_LENGTH] != 'a')
        printf("%d, %d\n", i, ioctl_buf_test[i]);
    }
    // sleep(1);
    collectSearchResult_test(nr_search, dev_id, dev_fd, 1);
    // for(int i = 0; i < 4; i++)
    // {
    //     if(ioctl_buf_test[i] != 'a')
    //         printf("%d\n", ioctl_buf_test[i]);
    // }
    return 0;
}