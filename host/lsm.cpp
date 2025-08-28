#include "lsm.h"
#include "bloom_filter.h"
#include "compaction.h"
#include "kv.h"
#include "search_csd.h"
#include "table.h"
#include "utils.h"
#include <bits/types/struct_timeval.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/select.h>
#include <sys/types.h>
#include <unistd.h>
#define CS

static LSM_tree lsm_tree;
std::counting_semaphore<2> sem(2);
int dev_fd = -1;
int dev_id = -1;
size_t ioctl_buf_size_ = 0;
uint8_t* ioctl_buf_ = new uint8_t[1024 * 32];
segment* seg_buf = nullptr;
segment* seg_buf_host = nullptr;
std::vector<BloomFilter*> filters;
bool csd_compaction;
uint64_t write_stall_time = 0;
uint64_t max_models_size = 0;
void lsmInit()
{
    csd_compaction = true;

    dev_fd = open(openssd_dir, O_DIRECT | O_RDWR);
    assert(dev_fd >= 0);
    dev_id = ::ioctl(dev_fd, _IO('N', 0x40));
    assert(dev_id >= 0);

    lsm_tree.stop = false;
    lsm_tree.levels_num = 1;
    lsm_tree.seq_number = 0;    
    lsm_tree.average_length = -1;
    lsm_tree.mt = createMemTable();
    lsm_tree.immu = createImmutableTable();
    lsm_tree.levels = (Level*)malloc(sizeof(Level) * NUM_LEVELS);
    lsm_tree.batch_size = 0;
    lsm_tree.error_bound = -1;
    lsm_tree.batch_id = 0;

    Level* levels = lsm_tree.levels;
    levels[0].num_table = 0;
    levels[0].size = 0;
    levels[0].SSTable_list = NULL;
    levels[0].compaction_ptr = NULL;
    for(int i = 1; i < NUM_LEVELS; i++)
    {
        levels[i].num_table = 0;
        levels[i].size = 0;
        levels[i].level_max_size = (i > 1) ? levels[i - 1].level_max_size * 10 : LEVEL1_MAX_SIZE;
        levels[i].SSTable_list = NULL;
        levels[i].compaction_ptr = NULL;
    }
    int ret;
    int fd = open("/mnt/openssd/learned_index.txt", O_DIRECT | O_RDWR | O_CREAT | O_TRUNC, 0644);
    if(fd < 0)
        perror("open error");
    assert(fd >= 0);
    posix_fallocate64(fd, 0, 128 * 1024 * 1024);
    fsync(fd);
    close(fd);

    fd = open("/mnt/openssd/args_file.txt", O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0644);
    if(fd < 0)
        perror("open error");
    assert(fd >= 0);
    posix_fallocate64(fd, 0, 64 * 1024 * 1024);
    fsync(fd);
    close(fd);
    struct stat64 st;
    stat64("/mnt/openssd/args_file.txt", &st);
    getFileAddress(st.st_ino, 4095, dev_id, dev_fd, 1, 0);

    fd = open("/mnt/openssd/result_file.txt", O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0644);
    if(fd < 0)
        perror("open error");
    assert(fd >= 0);
    posix_fallocate64(fd, 0, 64 * 1024 * 1024);
    uint8_t* buffer;
    ret = posix_memalign((void **)&(buffer), 4096, 64 * 1024 * 1024);
    memset(buffer, 222, TABLE_SIZE_THRES);
    write(fd, buffer, TABLE_SIZE_THRES);
    
    fsync(fd);
    close(fd);
    free(buffer);
    stat64("/mnt/openssd/result_file.txt", &st);
    getFileAddress(st.st_ino, 4095, dev_id, dev_fd, 0, 1);

    resetStatus(dev_id, dev_fd);

    lsm_tree.compaction_thread = thread(compaction_worker);
    ret = posix_memalign((void **)&(seg_buf), 4096, 128 * 1024 * 1024);
    seg_buf_host = (segment*)malloc(128 * 1024 * 1024);
    assert(ret == 0);
}

void remove_SSTable(int level_i, int levels_tables_num, uint64_t* input_file_nums)
{
    Level* levels = lsm_tree.levels;
    SSTable* deleted_node;
    KeyType compaction_smallest_key = 0;
    if(levels_tables_num == 0)
        return;
    if(level_i  > 0)
    {
        for(int i = 0; i < levels_tables_num; i++)
        {
            bool fl = 0;
            levels[level_i].SSTable_list = deleteBstNode(levels[level_i].SSTable_list, input_file_nums[i], &deleted_node, fl);
            levels[level_i].size -= deleted_node->size;
            levels[level_i].num_table--;
            // if(compaction_smallest_key >= deleted_node->smallest_key)
            // {
            //     printf("%ld, %ld, %d, %ld\n", compaction_smallest_key, deleted_node->smallest_key, i, input_file_nums[i]);
            //     inorderTraversal(levels[level_i].SSTable_list);
            // }
            assert(compaction_smallest_key < deleted_node->smallest_key || compaction_smallest_key == 0);
            compaction_smallest_key = deleted_node->smallest_key;
            if(deleted_node->fd >= 0)
            {
                close(deleted_node->fd);
            }
            unlink(deleted_node->table_name);
            if(deleted_node->filter != NULL)
            {
                // printf("%d\n", i);
                free_bloom_filter(deleted_node->filter);
                deleted_node->filter = NULL;
            }
                
            free(deleted_node);
        }
        levels[level_i].compaction_ptr = bstSearchHigherTable(levels[level_i].SSTable_list, compaction_smallest_key);
        if(levels[level_i].compaction_ptr == NULL)
        {
            levels[level_i].compaction_ptr = findBstMin(levels[level_i].SSTable_list);   
        }
        assert(levels[level_i].size < levels[level_i].level_max_size);
    }
    else
    {
        SSTable* sst_iter = levels[0].SSTable_list;
        SSTable* last_sst = sst_iter;
        for(int i = 0; i < levels_tables_num; i++)
        {
            last_sst = sst_iter;
            while (sst_iter != NULL && sst_iter->table_num != input_file_nums[i])
            {
                last_sst = sst_iter;
                sst_iter = sst_iter->next_table;
            }
            assert(sst_iter != NULL);
            if(last_sst == levels[0].SSTable_list)
            {
                levels[0].SSTable_list = sst_iter->next_table;
            }
            else
            {
                last_sst->next_table = sst_iter->next_table;
            }
            
            levels[0].size -= sst_iter->size;
            levels[0].num_table--;
            deleted_node = sst_iter;
            sst_iter = sst_iter->next_table;
            if(deleted_node->fd >= 0)
            {
                close(deleted_node->fd);
            }
            free(deleted_node->segs);
            if(deleted_node->filter != lsm_tree.immu->filter)
                free_bloom_filter(deleted_node->filter);
            unlink(deleted_node->table_name);
            free(deleted_node);
        }
    }
}

compaction_args* prepare_compaction(int level_i, int nr_tables)
{
    Level* levels = lsm_tree.levels;
    SSTable* sst_iter;
    int fd;
    struct stat64 st;
    uint64_t input_ids_i[MAX_INPUTS_PER_LEVEL];
    uint64_t input_nums_i[MAX_INPUTS_PER_LEVEL];
    uint64_t input_ids_i1[MAX_INPUTS_PER_LEVEL];
    uint64_t input_nums_i1[MAX_INPUTS_PER_LEVEL];
    uint64_t levels_num[MAX_LEVELS_NUM];
    levels_num[0] = 0;
    int level_i_index = 0, level_i1_index = 0;
    int nr_inputs = 0, nr_outputs = 0, nr_levels = 1;
    uint64_t smallest, largest;

    sst_iter = levels[level_i].compaction_ptr;
    if(sst_iter == NULL || level_i == 0)
        sst_iter = levels[level_i].SSTable_list;
    smallest = sst_iter->smallest_key;
    largest = sst_iter->largest_key;
    if(level_i == 0)  // Level0使用单链表存储SSTable元数据
    {
        // while(sst_iter->next_table != NULL)
        //     sst_iter = sst_iter->next_table;
        // smallest = sst_iter->smallest_key;
        // largest = sst_iter->largest_key;
        while(sst_iter != NULL)
        {
            // if(sst_iter->smallest_key <= largest && sst_iter->largest_key >= smallest)
            {
                int ret = stat64(sst_iter->table_name, &st);
                assert(ret == 0);
                input_ids_i[level_i_index] = st.st_ino;
                input_nums_i[level_i_index] = sst_iter->table_num;
                level_i_index++;
                smallest = std::min(smallest, sst_iter->smallest_key);
                largest = std::max(largest, sst_iter->largest_key);
                levels_num[nr_levels] = level_i_index;
                nr_levels++;
                sst_iter->is_selected = true;
            }
            sst_iter = sst_iter->next_table;
        }
    }
    else
    {
        // Level i可能有多个SSTable参与compaction
        uint64_t level_size = levels[level_i].size;
        while(level_size >= levels[level_i].level_max_size || nr_tables > 0)
        {
            int ret = stat64(sst_iter->table_name, &st);
            input_ids_i[level_i_index] = st.st_ino;
            input_nums_i[level_i_index] = sst_iter->table_num;
            level_i_index++;
            smallest = std::min(smallest, sst_iter->smallest_key);
            largest = std::max(largest, sst_iter->largest_key);
            level_size -= sst_iter->size;
            sst_iter->is_selected = true;
            
            levels[level_i].compaction_ptr = bstSearchHigherTable(levels[level_i].SSTable_list, sst_iter->largest_key);
            if(levels[level_i].compaction_ptr == NULL)
                levels[level_i].compaction_ptr = findBstMin(levels[level_i].SSTable_list);
            sst_iter = levels[level_i].compaction_ptr;
            nr_tables--;
        }
        levels_num[nr_levels] = level_i_index;
        nr_levels++;
    }
    nr_inputs += level_i_index;
    sst_iter = levels[level_i + 1].SSTable_list;
    // printf("level_%d_index = %d\n", level_i, level_i_index);

    // Level n使用二叉查找树存储SSTable元数据
    bstRangeSearch(sst_iter, smallest, largest, input_ids_i1, input_nums_i1, &level_i1_index);
    
    // printf("level_%d_index = %d\n", level_i + 1, level_i1_index);
    // printf("\n");
    
    nr_inputs += level_i1_index;
    levels_num[nr_levels] = nr_inputs;
    nr_levels++;
    nr_outputs = nr_inputs + 1;
    uint32_t args_size = compaction_args_size(nr_inputs, nr_outputs, nr_levels);
    compaction_args* args = (compaction_args*)malloc(args_size);
    
    args->nr_inputs = nr_inputs;
    args->nr_outputs = nr_outputs;
    args->nr_levels = nr_levels - 1;

    args->input_file_ids_offset = 0;
    args->input_file_nums_offset = args->input_file_ids_offset + sizeof(uint64_t) * nr_inputs;
    args->output_file_ids_offset = args->input_file_nums_offset + sizeof(uint64_t) * nr_inputs;
    args->output_file_nums_offset = args->output_file_ids_offset + sizeof(uint64_t) * nr_outputs;
    args->levels_offset = args->output_file_nums_offset + sizeof(uint64_t) * nr_outputs;

    args->output_sizes_offset = args->levels_offset + sizeof(uint64_t) * nr_levels;
    args->output_smallests_offset = args->output_sizes_offset + sizeof(uint64_t) * nr_outputs;
    args->output_largests_offset = args->output_smallests_offset + sizeof(KeyType) * nr_outputs;
    args->output_segs_size_offset = args->output_largests_offset + sizeof(KeyType) * nr_outputs;

    uint64_t *input_file_ids = (uint64_t *)&args->payload[args->input_file_ids_offset];
    uint64_t *input_file_nums = (uint64_t *)&args->payload[args->input_file_nums_offset];
    uint64_t *output_file_ids = (uint64_t *)&args->payload[args->output_file_ids_offset];
    uint64_t *output_file_nums = (uint64_t *)&args->payload[args->output_file_nums_offset];
    uint64_t *levels_table_num = (uint64_t *)&args->payload[args->levels_offset];

    //copy input file id
    memcpy(input_file_ids, input_ids_i, level_i_index * sizeof(uint64_t));
    memcpy(input_file_ids + level_i_index, input_ids_i1, level_i1_index * sizeof(uint64_t));
    
    //copy input file num
    memcpy(input_file_nums, input_nums_i, level_i_index * sizeof(uint64_t));
    memcpy(input_file_nums + level_i_index, input_nums_i1, level_i1_index * sizeof(uint64_t));
    
    //copy levels num
    memcpy(levels_table_num, levels_num, nr_levels * sizeof(uint64_t));
    
    char filename[100];
    for(int i = 0; i < nr_outputs; i++)
    {
        sprintf(filename, "/mnt/openssd/%ld.txt", table_num);
        fd = open(filename, O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0644);
        assert(fd > 0);
        int ret = stat64(filename, &st);
        assert(ret == 0);
        posix_fallocate64(fd, 0, TABLE_MAX_SIZE);
        fsync(fd);
        close(fd);

        output_file_ids[i] = st.st_ino;
        output_file_nums[i] = table_num;
        table_num++;
        assert(output_file_ids[i] >= 0);
    }

    int ret = stat64("/mnt/openssd/learned_index.txt", &st);
    assert(ret == 0);
    
    args->value_length = 64;
    args->output_table_size_thres = TABLE_SIZE_THRES;
    args->learned_index_file_id = st.st_ino;
    args->gamma = ERROR; // lsm_tree.error_bound > 0 ? lsm_tree.error_bound : 8;
    args->best_len = lsm_tree.average_length; // lsm_tree.batch_size >= 0 ? lsm_tree.batch_size : lsm_tree.average_length;
    return args;
}

void offload_compaction(compaction_args* args)
{
    unique_lock<shared_mutex> csd_lock(lsm_tree.csd_mutex);
    size_t args_size = compaction_args_size(args->nr_inputs, args->nr_outputs, args->nr_levels + 1);
    size_t buf_size = ((args_size + NVME_BLOCK_SIZE - 1) / NVME_BLOCK_SIZE) * NVME_BLOCK_SIZE;
    nvme_passthru_cmd cmd;
    int ret;

    if (buf_size > ioctl_buf_size_) {
        if (ioctl_buf_ != nullptr)
            delete[] ioctl_buf_;
        ioctl_buf_ = new uint8_t[buf_size * 2];
        ioctl_buf_size_ = buf_size * 2;
    }

    memcpy(ioctl_buf_, args, args_size);

    memset(&cmd, 0, sizeof(cmd));
    cmd.opcode = nvme_cmd_write;
    cmd.nsid = dev_id;
    cmd.metadata = 0;
    cmd.addr = (uint64_t)ioctl_buf_;
    cmd.cdw2 = 0;
    cmd.cdw3 = 0;
    cmd.cdw10 = 0;
    cmd.cdw11 = 0;
    cmd.cdw12 = (buf_size / NVME_BLOCK_SIZE - 1) | (1 << 16) | (1 << 17);
    cmd.cdw13 = 0;
    cmd.cdw14 = 0;
    cmd.cdw15 = 0;
    cmd.data_len = buf_size;
    cmd.metadata_len = 0;

    // printf("nlb = %ld\n", buf_size / NVME_BLOCK_SIZE - 1);

    // struct timeval start, end;
    // gettimeofday(&start, NULL);
    ret = ioctl(dev_fd, _IOWR('N', 0x43, nvme_passthru_cmd), &cmd);
    // gettimeofday(&end, NULL);
    if (ret == -1) {
        fprintf(stderr, "ioctl failed: %s\n", strerror(errno));
        exit(0);
    }
    // long seconds = end.tv_sec  - start.tv_sec;
    // long useconds = end.tv_usec - start.tv_usec;
    // printf("nvme time = %ld, buf_size = %ld\n", useconds, buf_size);
    assert(ret == 0);
}

void collect_offload_compaction_results(compaction_args* args)
{
    unique_lock<shared_mutex> csd_result_lock(lsm_tree.csd_result_mutex);
    size_t args_size = compaction_args_size(args->nr_inputs, args->nr_outputs, args->nr_levels + 1);
    size_t buf_size = ((args_size + NVME_BLOCK_SIZE - 1) / NVME_BLOCK_SIZE) * NVME_BLOCK_SIZE;
    nvme_passthru_cmd cmd;
    int ret;
    // uint8_t ioctl_buf_[32*1024]; 
    // uint8_t* ioctl_buf_test;
    // posix_memalign((void**)&(ioctl_buf_test), 4096, 32*1024);
    // if (buf_size > ioctl_buf_size_) {
    //     if (ioctl_buf_ != nullptr)
    //         delete[] ioctl_buf_;
    //     ioctl_buf_ = new uint8_t[buf_size * 2];
    //     ioctl_buf_size_ = buf_size * 2;
    // }
    
    memset(&cmd, 0, sizeof(cmd));
    cmd.opcode = nvme_cmd_read;
    cmd.nsid = dev_id;
    cmd.metadata = 0;
    cmd.addr = (uint64_t)ioctl_buf_;
    cmd.cdw2 = 0;
    cmd.cdw3 = 0;
    cmd.cdw10 = 0;
    cmd.cdw11 = 0;
    cmd.cdw12 = (buf_size / NVME_BLOCK_SIZE - 1) | (1 << 16) | (1 << 17);
    cmd.cdw13 = 0;
    cmd.cdw14 = 0;
    cmd.cdw15 = 0;
    cmd.data_len = buf_size;
    cmd.metadata_len = 0;
    ret = ::ioctl(dev_fd, _IOWR('N', 0x43, nvme_passthru_cmd), &cmd);
    if (ret == -1) {
        fprintf(stderr, "ioctl failed: %s\n", strerror(errno));
    }
    assert(ret == 0);
    memcpy(args, ioctl_buf_, args_size);
    // printf("%ld\n", args_size);
    // int fd = open("/mnt/openssd/result_file.txt", O_RDWR | O_DIRECT);
    // ret = read(fd, ioctl_buf_test, 16*1024);
    // close(fd);

    compaction_args* test_args = (compaction_args*)ioctl_buf_;
    uint64_t *input_file_id = (uint64_t*)&test_args->payload[args->input_file_ids_offset];
    uint64_t *output_file_id = (uint64_t*)&test_args->payload[args->output_file_ids_offset];
    uint64_t *output_file_num = (uint64_t*)&test_args->payload[args->output_file_nums_offset];
    uint64_t *file_size = (uint64_t*)&test_args->payload[args->output_sizes_offset];
    uint64_t *smallest = (uint64_t*)&test_args->payload[args->output_smallests_offset];
    uint64_t *largest = (uint64_t*)&test_args->payload[args->output_largests_offset];
    uint64_t *segs_size = (uint64_t*)&test_args->payload[args->output_segs_size_offset];
    for(int i = 0; i < args->nr_outputs; i++)
    {
        if(output_file_num[i] == 0 || segs_size[i] > 120000)
        {
            printf("%d, %ld, %ld, %ld, %ld, %ld, %ld, %ld\n", i, input_file_id[i], output_file_id[i],output_file_num[i], file_size[i], smallest[i], largest[i], segs_size[i]);
        }
    }
    
}

void destroy_compaction_args(compaction_args* args)
{
    uint64_t *output_file_nums = (uint64_t *)&args->payload[args->output_file_nums_offset];
    uint64_t *output_sizes = (uint64_t *)&args->payload[args->output_sizes_offset];
    char filename[100];
    int empty_table = 0;
    for (int i = 0; i < args->nr_outputs; i++) 
    {
        sprintf(filename, "/mnt/openssd/%ld.txt", output_file_nums[i]);
        int fd = open(filename, O_RDWR);
        int ret;

        if (output_sizes[i] == 0)
        {
            ret = unlink(filename);
            empty_table++;
            if(ret != 0)
                std::cout << filename << "\n";
        }
        else
        {
            ret = ftruncate(fd, output_sizes[i]);
            fsync(fd);
            if(ret != 0)
                std::cout << output_sizes[i] << "\n";
        }   
        if(ret != 0)
        {
            perror("Error");
            printf("too large size: %ld\n", output_sizes[i]);
        }
        assert(ret == 0);
        close(fd);
    }
    args->nr_outputs -= empty_table;
}

void collect_segments(compaction_args* args)
{
    uint64_t segs_sizes_sum = 0;
    uint64_t* segs_sizes = (uint64_t*)&args->payload[args->output_segs_size_offset];
    for(int i = 0; i < args->nr_outputs; i++)
    {
        if(segs_sizes[i] > 0)
            segs_sizes_sum += segs_sizes[i];
    }
    // printf("%ld\n", segs_sizes_sum);
    size_t transfer_size = segs_sizes_sum * sizeof(segment);
    if(seg_buf == nullptr)
    {
        int ret = posix_memalign((void **)&(seg_buf), 4096, 128*1024*1024);
        assert(ret == 0);
    }
    int fd = open("/mnt/openssd/learned_index.txt", O_DIRECT | O_RDONLY);
    assert(fd >= 0);
    uint64_t size = readStorage(fd, (void*)seg_buf, transfer_size);
    if(size > max_models_size)
        max_models_size = size;
    // assert(size > 0);
    close(fd);
}

void collect_compaction_result(compaction_args* args, int level_i, bool is_cs)
{
    Level* levels = lsm_tree.levels;
    KeyType *output_smallests = (KeyType *)&args->payload[args->output_smallests_offset];
    KeyType *output_largests = (KeyType *)&args->payload[args->output_largests_offset];
    uint64_t *levels_tables_num = (uint64_t *)&args->payload[args->levels_offset];
    uint64_t *input_file_nums = (uint64_t*)&args->payload[args->input_file_nums_offset];
    uint64_t *output_sizes = (uint64_t *)&args->payload[args->output_sizes_offset];
    uint64_t *output_file_nums = (uint64_t *)&args->payload[args->output_file_nums_offset];
    uint64_t *segs_sizes = (uint64_t*)&args->payload[args->output_segs_size_offset];
    if(is_cs)
    {
        collect_segments(args);
    }

    lsm_tree.levels_num = std::max(lsm_tree.levels_num, level_i + 1);

    unique_lock<shared_mutex> levels_lock(lsm_tree.levels_mutex);

    unique_lock<shared_mutex> leveli_lock(lsm_tree.levels[level_i].level_mutex);
    unique_lock<shared_mutex> leveli1_lock(lsm_tree.levels[level_i + 1].level_mutex);
    // remove Level i SSTable

    int level_i_table_num = (level_i > 0) ? levels_tables_num[1] : args->nr_levels - 1;
    // printf("1: %d, %d, %d\n", level_i, level_i_table_num, args->nr_levels);
    remove_SSTable(level_i, level_i_table_num, input_file_nums);
    leveli_lock.unlock();
    
    // remove level i+1 SSTable
    int level_i1_table_num = (level_i > 0) ? levels_tables_num[2] - levels_tables_num[1] : 
                    levels_tables_num[args->nr_levels] - levels_tables_num[args->nr_levels - 1];
    // printf("2: %d, %d, %d\n", level_i + 1, level_i1_table_num, args->nr_levels);
    remove_SSTable(level_i + 1, level_i1_table_num, input_file_nums + level_i_table_num);
    
    // add level i+1 SSTable
    SSTable* sst;
    uint64_t segs_offset = 0;
    uint8_t* buffer = (uint8_t*)malloc(TABLE_MAX_SIZE);
    for(int i = 0; i < args->nr_outputs; i++)
    {
        sst = (SSTable*)malloc(sizeof(SSTable));
        sst->fd = -1;
        sst->timestamp = std::chrono::system_clock::now().time_since_epoch().count();
        sst->filter = NULL;
        sst->table_num = output_file_nums[i];
        sprintf(sst->table_name, "/mnt/openssd/%ld.txt", sst->table_num);
        sst->kv_num = output_sizes[i] / KV_LENGTH;
        sst->max_size = levels[level_i + 1].level_max_size;
        sst->next_table = sst->next_table_right = NULL;
        sst->size = output_sizes[i];
        sst->smallest_key = output_smallests[i];
        sst->largest_key = output_largests[i];
        sst->is_selected = false;
        if(level_i < 1 || i < args->nr_outputs / 1)
            sst->fd = open(sst->table_name, O_RDONLY);
        else
            sst->fd = open(sst->table_name, O_RDONLY | O_DIRECT);
        assert(sst->fd >= 0);
        struct stat64 st;
        stat64(sst->table_name, &st);
        getFileAddress(st.st_ino, sst->table_num, dev_id, dev_fd, 0, 0);
        // if(level_i > 0)
        //pread(sst->fd, buffer, TABLE_SIZE_THRES, 0);
        
        if(level_i == 0)
        {
            sst->filter = filters[i];
        }
        // if(is_cs)
        {
            sst->segs = (segment*)malloc(sizeof(segment) * segs_sizes[i]);
            sst->segs_size = segs_sizes[i];
            if(is_cs)
                memcpy(sst->segs, seg_buf + segs_offset, sizeof(segment) * segs_sizes[i]);
            else
                memcpy(sst->segs, seg_buf_host + segs_offset, sizeof(segment) * segs_sizes[i]);
            segs_offset += segs_sizes[i];
        }
        levels[level_i + 1].SSTable_list = bstInsert(levels[level_i + 1].SSTable_list, sst);
        levels[level_i + 1].size += sst->size;
        levels[level_i + 1].num_table++;
    }
    free(buffer);
    levels[level_i + 1].compaction_ptr = findBstMin(levels[level_i + 1].SSTable_list);
    leveli1_lock.unlock();

    levels_lock.unlock();
    free(args);
    // 级联compaction
    if(levels[level_i + 1].size > levels[level_i + 1].level_max_size)
    {
        compaction_args* new_args = prepare_compaction(level_i + 1, 0);
        if(csd_compaction)
        {
            offload_compaction(new_args);
            collect_offload_compaction_results(new_args);
            destroy_compaction_args(new_args);
            collect_compaction_result(new_args, level_i + 1, 1);
        }
        else {
            compaction(new_args, seg_buf_host, filters);
            destroy_compaction_args(new_args);
            collect_compaction_result(new_args, level_i + 1, 0);
        }
    }
}
uint64_t cpu_total_time = 0, fpga_total_time = 0;
void compaction_worker()
{
    while(true)
    {
        compaction_args* args, *args_parallel;
        thread parallel_compaction_thread;
        unique_lock<mutex> lock(lsm_tree.compaction_mutex);
        lsm_tree.compaction_cv.wait(lock, []{ return lsm_tree.compaction || lsm_tree.stop; });
        lsm_tree.compaction = false;
        bool is_parallel = false;
        if(lsm_tree.stop)
        {
            // printf("CPU compaction total time = %ld\n", cpu_total_time);
            // printf("FPGA compaction total time = %ld\n", fpga_total_time);
            break;
        }
            
        {
            // unique_lock<shared_mutex> level0_lock(lsm_tree.levels[0].level_mutex);
            
            if(csd_compaction && lsm_tree.levels[1].size >= lsm_tree.levels[1].level_max_size - TABLE_SIZE_THRES)  
            {
                args_parallel = prepare_compaction(1, lsm_tree.levels[1].num_table);
                is_parallel = true;
                
                parallel_compaction_thread = thread([&](){
                    offload_compaction(args_parallel);
                    // uint64_t start = get_time_us();
                    collect_offload_compaction_results(args_parallel);
                    // uint64_t end = get_time_us();
                    // printf("%d, %d\n", args_parallel->nr_inputs, args_parallel->nr_levels);
                    // printf("FPGA compaction time = %ld us\n", end - start);
                    // fpga_total_time += (end - start);
                    destroy_compaction_args(args_parallel);
                    collect_compaction_result(args_parallel, 1, 1);
                });
            }
            args = prepare_compaction(0, 0);
        }
        
        #ifdef CS
        {
            // uint64_t start = get_time_us();
            compaction(args, seg_buf_host, filters);
            // uint64_t end = get_time_us();
            // cpu_total_time += (end - start);
            // printf("%d, %d\n", args->nr_inputs, args->nr_levels);
            // printf("CPU compaction time = %ld us\n", end - start);
            destroy_compaction_args(args);
            if(is_parallel)
            {
                parallel_compaction_thread.join();
            }
            collect_compaction_result(args, 0, 0);
            filters.clear();
        }   
        #else
        compaction(args);
        destroy_compaction_args(args);
        collect_compaction_result(args, 0, 0);
        #endif
    }
}

void insert(KeyType key, uint8_t* value)
{
    KeyValuePair kv;
    kv.key = key;

    {
        unique_lock<shared_mutex> seq_lock(lsm_tree.seq_mutex);
        kv.seq = lsm_tree.seq_number;
        lsm_tree.seq_number++;
    }
    
    MemTable* mt = lsm_tree.mt;
    
    if(value != NULL)
    {
        kv.value = value;
    }
    else
    {
        kv.value = (uint8_t*)malloc(sizeof(uint8_t) * VALUE_LENGTH);
        kv.seq |= DELETE_SEQ;
    }
    insertMemTable(mt, kv);
    uint64_t s = get_time_us();
    unique_lock<mutex> compaction_lock(lsm_tree.compaction_mutex);  // ensure no compaction work running
    uint64_t e = get_time_us();
    write_stall_time += (e - s);

    
    struct timeval start, end;

    Level* levels = lsm_tree.levels;

    {
        if(mt->size >= mt->max_size)
        {
            ImmutableTable* immu = lsm_tree.immu;
            unique_lock<shared_mutex> mt_lock(lsm_tree.table_mutex);
            memToImmu(mt, immu);
            free(mt);
            lsm_tree.mt = createMemTable();
            mt_lock.unlock();
            int average_length;
            SSTable* sst = flushMemTable(immu, average_length);
            struct stat64 st;
            stat64(sst->table_name, &st);
            getFileAddress(st.st_ino, sst->table_num, dev_id, dev_fd, 0, 0);
            if(lsm_tree.average_length > 0)
                lsm_tree.average_length = average_length;
            else
                lsm_tree.average_length = (lsm_tree.average_length + average_length) / 2;
            
            unique_lock<shared_mutex> level0_lock(levels[0].level_mutex);
            unique_lock<shared_mutex> levels_lock(lsm_tree.levels_mutex);
            // mt_lock.unlock();
            levels[0].num_table++;
            levels[0].size += sst->size;
            sst->next_table = levels[0].SSTable_list;
            levels[0].SSTable_list = sst;
            if(levels[0].num_table >= LEVEL0_MAX_TABLE_NUM)
            {
                lsm_tree.compaction = true;
                lsm_tree.compaction_cv.notify_all();
            }
        }
    }
    
    
}

void update(KeyType key, uint8_t* value)
{
    insert(key, value);
}

void del(KeyType key)
{
    insert(key, NULL);
}

double learned_index_search_time = 0;
double mem_search_time = 0;
double immu_search_time = 0;
double L0_search_time = 0;
double L1_search_time = 0;
double Ln_search_time = 0;
double bst_search_time = 0;
int _false = 0;
int mem_num = 0, immu_num = 0, L0_num = 0, L1_num = 0, Ln_num = 0;
uint8_t* search(KeyType key, uint8_t* value, bool learned_index)
{
    MemTable* mt = lsm_tree.mt;
    ImmutableTable* immu = lsm_tree.immu;
    Level* levels = lsm_tree.levels;
    long seconds, useconds;
    double elapsed;
    struct timeval start, end;

    
    //search memtable
    // gettimeofday(&start, NULL);
    shared_lock<shared_mutex> mt_lock(lsm_tree.table_mutex);
    uint8_t* v = searchMemTable(mt, key, value);
    // mt_lock.unlock();
    // gettimeofday(&end, NULL);
    // seconds = end.tv_sec  - start.tv_sec;
    // useconds = end.tv_usec - start.tv_usec;
    // elapsed = seconds * 1000000.0 + useconds;
    // if(learned_index)
    // {
    //     mem_search_time += elapsed;
    // }
    if(v != NULL)
    {
        // mem_num++;
        return value;
    }
    
    //search immutable
    // gettimeofday(&start, NULL);
    {
        v = searchImmuTable(immu, key, value);
    }
    // gettimeofday(&end, NULL);
    mt_lock.unlock();
    // seconds = end.tv_sec  - start.tv_sec;
    // useconds = end.tv_usec - start.tv_usec;
    // elapsed = seconds * 1000000.0 + useconds;
    // if(learned_index)
    // {
    //     immu_search_time += elapsed;
    // }
    if(v != NULL)
    {
        // immu_num++;
        return value;
    }
    
    
    // gettimeofday(&start, NULL);
    SSTable* sst;
    // search at level 0
    {
        shared_lock<shared_mutex> lock(levels[0].level_mutex);
        sst = levels[0].SSTable_list;
        for(int i = 0; i < levels[0].num_table; i++)
        {
            if(key >= sst->smallest_key && key <= sst->largest_key && query_bloom_filter(sst->filter, key))
            {
                // if(learned_index)
                // gettimeofday(&start, NULL);
                v = searchSSTableLearnedIndex(sst, key, value, ERROR);
                // gettimeofday(&end, NULL);
                // seconds = end.tv_sec  - start.tv_sec;
                // useconds = end.tv_usec - start.tv_usec;
                // elapsed = seconds * 1000000.0 + useconds;
                // L0_search_time += elapsed;
                // else
                //     value = searchSSTable(sst, key);
                if(v != NULL)
                {
                    // if(i == 1)
                    //     L1_num++;
                    // else
                    //     Ln_num++;
                    break;
                }
            }
            sst = sst->next_table;
        }
    }
    // gettimeofday(&end, NULL);
    // seconds = end.tv_sec  - start.tv_sec;
    // useconds = end.tv_usec - start.tv_usec;
    // elapsed = seconds * 1000000.0 + useconds;
    // if(learned_index)
    //     L0_search_time += elapsed;
    if(v != NULL)
    {
        return value;
    }

    // struct timeval s, e;
    //search at level i
    // gettimeofday(&start, NULL);
    {
        // shared_lock<shared_mutex> lock(lsm_tree.levels_mutex);
        // lock.unlock();
        for(int i = 1; i < NUM_LEVELS; i++)
        {
            shared_lock<shared_mutex> level_lock(levels[i].level_mutex);
            // gettimeofday(&start, NULL);
            sst = bstSearch(levels[i].SSTable_list, key);
            // gettimeofday(&end, NULL);
            // seconds = end.tv_sec  - start.tv_sec;
            // useconds = end.tv_usec - start.tv_usec;
            // elapsed = seconds * 1000000.0 + useconds;
            // bst_search_time+=elapsed;

            if(sst != NULL && key >= sst->smallest_key && key <= sst->largest_key)
            {
                // if(learned_index)
                {
                    if(i == 1 && query_bloom_filter(sst->filter, key))
                    {
                        // gettimeofday(&start, NULL);
                        v = searchSSTableLearnedIndex(sst, key, value, ERROR);
                        // gettimeofday(&end, NULL);
                        // seconds = end.tv_sec  - start.tv_sec;
                        // useconds = end.tv_usec - start.tv_usec;
                        // elapsed = seconds * 1000000.0 + useconds;
                        // Ln_search_time += elapsed;
                    }   
                    else
                    {
                        // gettimeofday(&start, NULL);
                        v = searchSSTableLearnedIndex(sst, key, value, ERROR);
                        // gettimeofday(&end, NULL);
                        // seconds = end.tv_sec  - start.tv_sec;
                        // useconds = end.tv_usec - start.tv_usec;
                        // elapsed = seconds * 1000000.0 + useconds;
                        // Ln_search_time += elapsed;
                    }
                        
                    if(v != NULL)
                    {
                        level_lock.unlock();
                        break;
                    }
                }
                // else
                //     value = searchSSTable(sst, key);
            }
            level_lock.unlock();
            // levels[i].test.fetch_sub(1);
            
        }
    }
    // gettimeofday(&end, NULL);
    // seconds = end.tv_sec  - start.tv_sec;
    // useconds = end.tv_usec - start.tv_usec;
    // elapsed = seconds * 1000000.0 + useconds;
    // Ln_search_time += elapsed;
    if(v != NULL)
    {
        return value;
    }
    return NULL;
}

struct db_iter
{
    uint8_t* mapped;
    int offset;
    int max_size;
};

std::vector<std::pair<KeyType, uint8_t*>> rangeQuery(const KeyType& start_key, int len) {
    std::vector<db_iter> db_iters;
    std::vector<std::pair<KeyType, uint8_t*>> result, memtable_buffer;
    int mem_buffer_pos = 0;

    MemTable* mt = lsm_tree.mt;
    ImmutableTable* immu = lsm_tree.immu;
    Level* levels = lsm_tree.levels;

    shared_lock<shared_mutex> mt_lock(lsm_tree.table_mutex);
    rangeSearchMemTable(mt, start_key, len, memtable_buffer);
    rangeSearchImmuTable(immu, start_key, len, memtable_buffer);
    mt_lock.unlock();

    std::sort(memtable_buffer.begin(), memtable_buffer.end());
    if (memtable_buffer.size() > static_cast<size_t>(len)) {
        memtable_buffer.resize(len);
    }
    db_iter it;

    shared_lock<shared_mutex> l0_lock(levels[0].level_mutex);
    SSTable* sst = levels[0].SSTable_list;
    for(int i = 0; i < levels[0].num_table; i++)
    {
        if(start_key >= sst->smallest_key && start_key <= sst->largest_key)
        {
            it.mapped = rangeSearchSSTableLearnedIndex(sst, start_key, len, result, it.max_size, ERROR);
            it.offset = 0;
            while(((KeyType*)(it.mapped + it.offset))[0] <= start_key)
            {
                it.offset += KV_LENGTH;
            }
            db_iters.push_back(it);
        }
        sst = sst->next_table;
    }
    l0_lock.unlock();

    for(int i = 1; i < lsm_tree.levels_num; i++)
    {
        shared_lock<shared_mutex> level_lock(levels[i].level_mutex);
        sst = bstSearch(levels[i].SSTable_list, start_key);
        if(sst != NULL && start_key >= sst->smallest_key && start_key <= sst->largest_key)
        {
            it.mapped = rangeSearchSSTableLearnedIndex(sst, start_key, len, result, it.max_size, ERROR);
            it.offset = 0;
            while(it.offset < it.max_size && ((KeyType*)(it.mapped + it.offset))[0] <= start_key)
            {
                it.offset += KV_LENGTH;
            }
            db_iters.push_back(it);
        }
        level_lock.unlock();
    }

    int min_pos;
    KeyType min_key, last_key = UINT64_MAX;
    while(result.size() < len && (mem_buffer_pos < memtable_buffer.size() || min_pos >= 0))
    {
        if(memtable_buffer.size() > 0)
            min_key = memtable_buffer[mem_buffer_pos].first;
        else
            min_key = UINT64_MAX;
        min_pos = -1;
        for(int j = 0; j < db_iters.size(); j++)
        {
            if(db_iters[j].offset < db_iters[j].max_size && ((KeyType*)(db_iters[j].mapped + db_iters[j].offset))[0] < min_key)
            {
                min_key = ((KeyType*)(db_iters[j].mapped + db_iters[j].offset))[0];
                min_pos = j;
            }
        }
        if(min_pos < 0 && mem_buffer_pos < memtable_buffer.size())
        {
            if(last_key != min_key)
            {
                result.emplace_back(memtable_buffer[mem_buffer_pos]);
                last_key = min_key;
            }
            mem_buffer_pos++;
        }
        else if(min_pos >= 0)
        {
            if(last_key != min_key)
            {
                uint8_t* value = (uint8_t*)malloc(VALUE_LENGTH);
                memcpy(value, db_iters[min_pos].mapped + db_iters[min_pos].offset + VALUE_OFFSET, VALUE_LENGTH);
                result.emplace_back(std::make_pair(min_key, value));
                last_key = min_key;
            }
            db_iters[min_pos].offset += KV_LENGTH;
        }
    }
    // if(result.size() != len)
    //     printf("%ld, %d, %d, %d, %d, %d\n", start_key, len, result.size(), memtable_buffer.size(), db_iters.size(), mem_buffer_pos);
    for(int j = 0; j < db_iters.size(); j++)
    {
        free(db_iters[j].mapped);
    }
    
    return result;
}

void set_error_bound(int error_bound) {
    lsm_tree.error_bound = error_bound;
}

void set_batch_size(int batch_size) {
    lsm_tree.batch_size = batch_size;
}

void destroy_lsm()
{
    lsm_tree.stop = true;
    lsm_tree.compaction_cv.notify_all();
    lsm_tree.compaction_thread.join();
}

void print()
{
    // SSTable* sst = levels[0].SSTable_list;
    printf("levels num = %d\n", lsm_tree.levels_num);
    printf("table num = %ld\n", table_num);
    // printf("false num = %d\n", _false);
    // for(int i = 0; i < levels[0].num_table; i++)
    // {
    //     printf("%ld, %ld\n", sst->smallest_key, sst->largest_key);
    //     sst = sst->next_table;
    // }
    // for(int i = 0; i <= lsm_tree.levels_num; i++)
    // {
    //     printf("level %d: %d\n", i, lsm_tree.levels[i].num_table);
    // }
    // inorderTraversal(lsm_tree.levels[2].SSTable_list);
}

thread_local int search_num;

typedef struct
{
    KeyType key;
    int level;    // level number
    int table_id;
    int start_pos;
    int cur_L0_index;
}query_states;

void supply_queue(std::vector<KeyType>& keys, std::queue<query_states>& search_queue, int& cursor, int supply_size)
{
    KeyType key;
    MemTable* mt = lsm_tree.mt;
    ImmutableTable* immu = lsm_tree.immu;
    Level* levels = lsm_tree.levels;
    uint8_t* value = (uint8_t*)malloc(VALUE_LENGTH);
    for(int i = 0; i < supply_size; i++)
    {
        key = keys[cursor];
        shared_lock<shared_mutex> mt_lock(lsm_tree.table_mutex);
        if(searchMemTable(mt, key, value) != NULL || searchImmuTable(immu, key, value) != NULL)
        {
            search_num++;
            cursor++;
            mt_lock.unlock();
            continue;
        }
        mt_lock.unlock();
        
        query_states query_node;
        query_node.key = key;
        query_node.level = 0;
        query_node.cur_L0_index = 0;
        SSTable* sst = levels[0].SSTable_list;
        uint8_t* v = NULL;
        for(int j = 0; j < levels[0].num_table && sst != NULL; j++)
        {
            if(key >= sst->smallest_key && key <= sst->largest_key && query_bloom_filter(sst->filter, key))
            {
                // query_node.cur_L0_index = j;
                // query_node.table_id = sst->table_num;
                v = searchSSTableLearnedIndex(sst, key, value, ERROR);
                if(v != NULL)
                {
                    break;
                }
                // std::pair<uint64_t, uint64_t> pos = predict(sst->segs, sst->segs_size, key, ERROR, sst->size / KV_LENGTH);
                // printf("L0: %ld, %ld\n", sst->table_num, pos.first);
                // query_node.start_pos = pos.first * KV_LENGTH;
            }
            sst = sst->next_table;
        }
        // search_queue.push(query_node);
        if(v != NULL)
        {
            search_num++;
            cursor++;
            continue;
        }
        
        for(int i = 1; i < NUM_LEVELS; i++)
        {
            sst = bstSearch(levels[i].SSTable_list, key);
            if(sst != NULL && key >= sst->smallest_key && key <= sst->largest_key)
            {
                if((i == 1 && query_bloom_filter(sst->filter, key) || i > 1))
                {
                    query_node.level = i;
                    query_node.table_id = sst->table_num;
                    std::pair<uint64_t, uint64_t> pos = predict(sst->segs, sst->segs_size, key, ERROR, sst->size / KV_LENGTH);
                    query_node.start_pos = pos.first * KV_LENGTH;
                    break;
                }   
            }
        }
        search_queue.push(query_node);
        cursor++;
    }
}

void update_queue(std::queue<query_states>& search_queue, std::queue<query_states>& result_queue, uint8_t* result_buf, int nr_search)
{
    query_states query_node;
    Level* levels = lsm_tree.levels;
    int queue_size = result_queue.size();
    for(int i = 0; i < nr_search; i++)
    {
        query_node = result_queue.front();
        result_queue.pop();
        KeyType key = query_node.key;
        int bitmap_pos = i / 64;
        int bitmap_offset = i % 64;
        uint64_t is_found = (((uint64_t*)result_buf)[bitmap_pos]) & (1ULL << bitmap_offset);
        if(is_found == 0)
        {
            bool flag = false;
            SSTable* sst;
            if(query_node.level == 0)
            {
                sst = levels[0].SSTable_list;
                int i;
                for(i = 0; i < query_node.cur_L0_index && sst != NULL; i++)
                    sst = sst->next_table;
                for(; i < levels[0].num_table && sst != NULL; i++)
                {
                    query_node.cur_L0_index++;
                    if(key >= sst->smallest_key && key <= sst->largest_key && query_bloom_filter(sst->filter, key))
                    {
                        query_node.table_id = sst->table_num;
                        std::pair<uint64_t, uint64_t> pos = predict(sst->segs, sst->segs_size, key, ERROR, sst->size / KV_LENGTH);
                        // printf("L0: %ld, %ld\n", sst->table_num, pos.first);
                        query_node.start_pos = pos.first * KV_LENGTH;
                        search_queue.push(query_node);
                        flag = true;
                        break;
                    }
                    sst = sst->next_table;
                }
            }
            if(!flag || query_node.level > 0)
            {
                int next_level = query_node.level + 1;
                for(int j = next_level; j < NUM_LEVELS; j++)
                {
                    sst = bstSearch(levels[j].SSTable_list, key);
                    if(sst != NULL && key >= sst->smallest_key && key <= sst->largest_key)
                    {
                        if((j == 1 && query_bloom_filter(sst->filter, key)) || j > 1)
                        {
                            query_node.table_id = sst->table_num;
                            query_node.level = j;
                            std::pair<uint64_t, uint64_t> pos = predict(sst->segs, sst->segs_size, key, ERROR, sst->size / KV_LENGTH);
                            // printf("%ld, L%d: %ld, %ld\n", query_node.key, j, sst->table_num, pos.first);
                            query_node.start_pos = pos.first * KV_LENGTH;
                            search_queue.push(query_node);
                            break;
                        }   
                    }
                }
            }
        }
        else
            search_num++;
    }
}

int prepare_search_args(uint64_t* args, std::queue<query_states>& search_queue, std::queue<query_states>& result_queue, int batch_size)
{
    uint64_t* files_num_ptr = args;
    assert(search_queue.size() <= batch_size);
    uint64_t* target_keys_ptr = args + search_queue.size();
    uint64_t* kv_pos_ptr = args + 2 * search_queue.size();
    int i;
    for(i = 0; i < batch_size && !search_queue.empty(); i++)
    {
        query_states query_node = search_queue.front();
        search_queue.pop();
        result_queue.push(query_node);
        files_num_ptr[i] = query_node.table_id;
        target_keys_ptr[i] = query_node.key;
        kv_pos_ptr[i] = query_node.start_pos;
    }
    return i;
}

void search_in_batch(std::vector<KeyType>& keys, int nr_read, int batch_size)
{
    if(nr_read == 0)
        return;
    search_num = 0;
    int keys_cursor = 0, batch_id;
    int nr_search[2];
    std::queue<query_states> search_queue; 
    std::queue<query_states> result_queue;
    uint64_t* args = (uint64_t*)malloc(batch_size * sizeof(uint64_t) * 3);
    uint8_t* result_buf = new uint8_t [VALUE_LENGTH * MAX_BATCH_SIZE];
    struct timeval start, end;

    shared_lock<shared_mutex> levels_lock(lsm_tree.levels_mutex);
    // gettimeofday(&start, NULL);
    supply_queue(keys, search_queue, keys_cursor, batch_size);
    batch_id = 0;
    nr_search[batch_id] = prepare_search_args(args, search_queue, result_queue, batch_size);
    
    // gettimeofday(&start, NULL);
    offloadSearch(args, dev_id, dev_fd, batch_id, nr_search[batch_id], batch_size);
    // gettimeofday(&end, NULL);
    // long seconds  = end.tv_sec  - start.tv_sec;
    // long useconds = end.tv_usec - start.tv_usec;
    // double elapsed = seconds * 1000000 + useconds;
    // printf("Offload time: %.6f useconds, nr_search = %d\n", elapsed, args->nr_search);

    supply_queue(keys, search_queue, keys_cursor, batch_size - search_queue.size());
    batch_id = !batch_id;
    nr_search[batch_id] = prepare_search_args(args, search_queue, result_queue, batch_size);
    
    // gettimeofday(&start, NULL);
    offloadSearch(args, dev_id, dev_fd, batch_id, nr_search[batch_id], batch_size);
    // gettimeofday(&end, NULL);
    // seconds  = end.tv_sec  - start.tv_sec;
    // useconds = end.tv_usec - start.tv_usec;
    // elapsed = seconds * 1000000 + useconds;
    // printf("Offload time: %.6f useconds, nr_search = %d\n", elapsed, args->nr_search);
    int n1 = 0, n2 = 0;
    double collect_time = 0, update_time = 0, supply_time = 0, prepare_time = 0, offload_time = 0;
    while(keys_cursor < nr_read || !search_queue.empty() || !result_queue.empty())
    {
        levels_lock.unlock();
        // printf("%d, %d, %d, %d, %d, %d\n", batch_id, keys_cursor, search_queue.size(), result_queue.size(), nr_search[0], nr_search[1]);
        // gettimeofday(&start, NULL);
        if(nr_search[!batch_id] > 0)
        {
            unique_lock<shared_mutex> csd_lock(lsm_tree.csd_result_mutex);
            collectSearchResult(batch_size, dev_id, dev_fd, !batch_id, result_buf);
        }
        // gettimeofday(&end, NULL);
        // long seconds  = end.tv_sec  - start.tv_sec;
        // long useconds = end.tv_usec - start.tv_usec;
        // double elapsed = seconds * 1000000 + useconds;
        // collect_time += elapsed;
        // printf("Collect time: %.6f useconds, nr_search = %d\n", elapsed, nr_search);
        levels_lock.lock();

        // gettimeofday(&start, NULL);
        update_queue(search_queue, result_queue, result_buf, nr_search[!batch_id]);
        // gettimeofday(&end, NULL);
        // seconds  = end.tv_sec  - start.tv_sec;
        // useconds = end.tv_usec - start.tv_usec;
        // elapsed = seconds * 1000000 + useconds;
        // update_time += elapsed;
        // printf("Update time: %.6f useconds, nr_search = %d\n", elapsed, nr_search);

        // gettimeofday(&start, NULL);
        int supply_size = std::min(batch_size - (int)search_queue.size(), nr_read - keys_cursor);
        supply_queue(keys, search_queue, keys_cursor, supply_size);
        // gettimeofday(&end, NULL);
        // seconds  = end.tv_sec  - start.tv_sec;
        // useconds = end.tv_usec - start.tv_usec;
        // elapsed = seconds * 1000000 + useconds;
        // supply_time += elapsed;
        // printf("Supply time: %.6f useconds, nr_search = %d\n", elapsed, nr_search);

        // gettimeofday(&start, NULL);
        batch_id = !batch_id;
        nr_search[batch_id] = prepare_search_args(args, search_queue, result_queue, batch_size);
        // printf("after prepare %d, %d, %d\n", batch_id, nr_search[0], nr_search[1]);
        // gettimeofday(&end, NULL);
        // seconds  = end.tv_sec  - start.tv_sec;
        // useconds = end.tv_usec - start.tv_usec;
        // elapsed = seconds * 1000000 + useconds;
        // prepare_time += elapsed;
        // printf("Prepare time: %.6f useconds, nr_search = %d\n", elapsed, nr_search);
        // n1++;

        if(nr_search[batch_id] == 0)
        {
            // batch_id = !batch_id;
            continue;
        }
        // n2++;
        // gettimeofday(&start, NULL);
        {
            unique_lock<shared_mutex> csd_lock(lsm_tree.csd_mutex);
            offloadSearch(args, dev_id, dev_fd, batch_id, nr_search[batch_id], batch_size);
        }
        
        // gettimeofday(&end, NULL);
        // seconds  = end.tv_sec  - start.tv_sec;
        // useconds = end.tv_usec - start.tv_usec;
        // elapsed = seconds * 1000000 + useconds;
        // offload_time += elapsed;
        // printf("Offload time: %.6f useconds, nr_search = %d\n", elapsed, nr_search);
        // printf("\n");
    }
    // printf("%d keys are found, average search num = %lf\n", search_num, 1.0f * n1 * batch_size / nr_read);
    // printf("collect_time = %lf, update_time = %lf, supply_time = %lf, prepare_time = %lf, offload_time = %lf\n", collect_time / nr_read, update_time / nr_read, supply_time / nr_read, prepare_time / nr_read, offload_time / nr_read);
    // printf("total host time = %lf\n", (collect_time + update_time + supply_time + prepare_time + offload_time) / nr_read);

    // gettimeofday(&end, NULL);
    // long seconds  = end.tv_sec  - start.tv_sec;
    // long useconds = end.tv_usec - start.tv_usec;
    // double elapsed = seconds + useconds / 1000000.0;
    // printf("total time: %.6f seconds, nr_search = %d\n", elapsed, args->nr_search);
    // printf("%d\n", nnn);
}

void search_in_batch_mutil_threads(std::vector<KeyType>& keys, int _keys_cursor, int nr_read, int batch_size)
{
    if(nr_read == 0)
        return;
    search_num = 0;
    int keys_cursor = _keys_cursor;
    int keys_end = nr_read + _keys_cursor;
    int nr_search = 0;
    std::queue<query_states> search_queue; 
    std::queue<query_states> result_queue;
    uint64_t* args = (uint64_t*)malloc(batch_size * sizeof(uint64_t) * 3);

    struct timeval start, end;
    
    uint8_t* result_buf = new uint8_t [VALUE_LENGTH * MAX_BATCH_SIZE];
    while(keys_cursor < keys_end || !search_queue.empty() || !result_queue.empty())
    {
        if(!csd_compaction)
            std::this_thread::sleep_for(std::chrono::nanoseconds(1000000));
        shared_lock<shared_mutex> levels_lock(lsm_tree.levels_mutex);
        update_queue(search_queue, result_queue, result_buf, nr_search);
        int supply_size = std::min(batch_size - (int)search_queue.size(), keys_end - keys_cursor);
        supply_queue(keys, search_queue, keys_cursor, supply_size);
        nr_search = prepare_search_args(args, search_queue, result_queue, batch_size);
        if(nr_search == 0)
            break;
        sem.acquire();
        unique_lock<shared_mutex> csd_lock(lsm_tree.csd_mutex);
        offloadSearch(args, dev_id, dev_fd, lsm_tree.batch_id, nr_search, batch_size);
        
        unique_lock<shared_mutex> csd_result_lock(lsm_tree.csd_result_mutex);
        lsm_tree.batch_id = !(lsm_tree.batch_id);
        csd_lock.unlock();

        collectSearchResult(batch_size, dev_id, dev_fd, !(lsm_tree.batch_id), result_buf);

        levels_lock.unlock();
        sem.release();
        csd_result_lock.unlock();
    }
    // printf("%d\n", search_num);
}