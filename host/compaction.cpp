#include "compaction.h"
#include <aio.h>
#include <unistd.h>
int compare_keys(KeyValuePair* a, KeyValuePair* b)
{
    int ret;
    if(a->key > b->key)
        ret = 1;
    else if(a->key < b->key)
        ret = -1;
    else
        ret = 0;

    if(ret == 0)
    {
        assert(a->seq != b->seq);
        if (a->seq > b->seq)
            ret = -1;
        else
            ret = 1;
    }
    return ret;
}

int table_iterator_next(table_file* table_it, KeyValuePair* kv)
{
    if(table_it->offset >= table_it->size)
    {
        // *key = 0;
        kv->value = NULL;
        return -1;
    }
    memcpy((void*)kv, (uint8_t*)(table_it->mapped) + table_it->offset, VALUE_OFFSET);
    table_it->offset += VALUE_OFFSET;
    kv->value = &(((uint8_t*)(table_it->mapped) + table_it->offset)[0]);
    table_it->offset += VALUE_LENGTH;
    return 0;
}

int level_iterator_next(level_iterator *it, KeyValuePair* kv)
{
    if(it->nr_inputs == 0 || it->cur >= it->nr_inputs)
    {
        kv->value = NULL;
        return -1;
    }

    if(table_iterator_next(&it->input[it->cur], kv) != 0)
    {
        it->cur++;
        if(it->cur >= it->nr_inputs)
        {
            return -1;
        }
        if(table_iterator_next(&it->input[it->cur], kv) != 0)
            assert(0);
    }
    return 0;
}

void output_table_flush(output_table* output, std::vector<_aiocb>& aio_cbs)
{
    PLR plr = PLR(ERROR);
    int average_length;
    thread train_thread(&PLR::train, &plr, std::ref(output->keys), std::ref(average_length));
    writeStorage(output->fd, output->buffer, output->buffer_cursor);
    train_thread.join();
    output->segs_num = plr.segments.size();
    std::copy(plr.segments.begin(), plr.segments.end(), output->segs + output->segs_offset);
    output->segs_offset += output->segs_num;
    output->filter = plr.filter;
    output->buffer_cursor = 0;
    output->keys.clear();
    // fsync(output->fd);
    // close(output->fd);
    _aiocb cb_pack;
    cb_pack.cb = (struct aiocb*)malloc(sizeof(struct aiocb));
    memset(cb_pack.cb, 0, sizeof(struct aiocb));
    cb_pack.cb->aio_fildes = output->fd;
    cb_pack.fd = output->fd;

    // 发起异步 fsync
    if (aio_fsync(O_SYNC, cb_pack.cb) < 0) {
        perror("aio_fsync");
        close(cb_pack.fd);
        return;
    }
    aio_cbs.push_back(cb_pack);
}

int output_table_add(output_table* output, KeyValuePair* kv, std::vector<_aiocb>& aio_cbs)
{
    if((kv->seq & DELETE_SEQ) > 0)
        return 0;
    output->keys.push_back(kv->key);
    memcpy(output->buffer + output->buffer_cursor, kv, VALUE_OFFSET);
    output->buffer_cursor += VALUE_OFFSET;
    memcpy(output->buffer + output->buffer_cursor, kv->value, VALUE_LENGTH);
    output->buffer_cursor += VALUE_LENGTH;
    output->table_size += KV_LENGTH;
    if(output->table_size >= OUTPUT_TABLE_SIZE_THRES)
    {
        output_table_flush(output, aio_cbs);
        return -1;
    }
    return 0;
}

void do_compaction_work(level_iterator* levels, int nr_levels, uint64_t* output_file_nums, int nr_outputs, size_t* output_sizes, 
        KeyType* smallests, KeyType* largests, segment* segs, uint64_t* segs_sizes, std::vector<BloomFilter*>& filters)
{
    std::vector<_aiocb> aio_cbs;
    KeyValuePair* kvs = (KeyValuePair*)malloc(sizeof(KeyValuePair) * nr_levels);
    output_table output;
    int output_used = 0;
    output.buffer_cursor = 0;
    output.table_size = 0;
    output.segs_offset = 0;
    output.segs = segs;
    posix_memalign((void **)&(output.buffer), 4096, OUTPUT_TABLE_MAX_SIZE);
    if (output_sizes != NULL)
        for (int i = 0; i < nr_outputs; i++)
            output_sizes[i] = 0;
    
    for(int i = 0; i < nr_levels; i++)
    {
        level_iterator_next(&levels[i], &kvs[i]);
    }

    int smallest;
    int64_t table_num = -1;
    char filename[100];
    int fd;
    KeyType lastkey = 0;
    int flag = 1;
    while(1)
    {
        smallest = -1;
        for(int i = 0; i < nr_levels; i++)
        {
            if(kvs[i].value != NULL && (smallest < 0 || compare_keys(&kvs[i], &kvs[smallest]) < 0))
                smallest = i;
        }
        if(smallest < 0)
            break;
        if(table_num < 0)
        {
            table_num = output_file_nums[output_used];
            sprintf(filename, "/mnt/openssd/%ld.txt", table_num);
            fd = open(filename, O_RDWR);
            output.fd = fd;
            output.table_size = 0;
            smallests[output_used] = kvs[smallest].key;
        }
        if(flag || lastkey != kvs[smallest].key)
        {
            lastkey = kvs[smallest].key;
            flag = 0;
            if(output_table_add(&output, &kvs[smallest], aio_cbs) < 0)
            {
                table_num = -1;
                output_sizes[output_used] = output.table_size;
                largests[output_used] = kvs[smallest].key;
                segs_sizes[output_used] = output.segs_num;
                filters.push_back(output.filter);
                output_used++;
            }
        }
        level_iterator_next(&levels[smallest], &kvs[smallest]);
    }
    if(output.buffer_cursor > 0)
    {
        output_sizes[output_used] = output.table_size;
        largests[output_used] = lastkey;
        output_table_flush(&output, aio_cbs);
        output.table_size = 0;
        segs_sizes[output_used] = output.segs_num;
        filters.push_back(output.filter);
        output_used++;
    }
    free(kvs);
    free(output.buffer);

    for(auto i : aio_cbs)
    {
        while (aio_error(i.cb) == EINPROGRESS);
        int ret = aio_return(i.cb);
        if(ret != 0)
            printf("aio_fsync failed: %s\n", strerror(errno));
        close(i.fd);
    }
    
}

void compaction(compaction_args* args, segment* segs, std::vector<BloomFilter*>& filters)
{
    level_iterator* level_its = (level_iterator*)malloc(args->nr_levels * sizeof(level_iterator));
    uint64_t *levels = (uint64_t *)&args->payload[args->levels_offset];
    uint64_t* input_file_nums = (uint64_t*)&args->payload[args->input_file_nums_offset];
    uint64_t* output_file_nums = (uint64_t*)&args->payload[args->output_file_nums_offset];
    uint64_t *output_sizes = (uint64_t *)&args->payload[args->output_sizes_offset];
    KeyType *output_smallests = (KeyType *)&args->payload[args->output_smallests_offset];
    KeyType *output_largests = (KeyType *)&args->payload[args->output_largests_offset];
    uint64_t *segs_sizes = (uint64_t*)&args->payload[args->output_segs_size_offset];
    int fd;
    char filename[100];
    // printf("before compaction\n");
    struct stat stat;
    for(int i = 0; i < args->nr_levels; i++)
    {
        int nr_inputs = i < args->nr_levels - 1 ?
                        levels[i + 1] - levels[i] :
                        args->nr_inputs - levels[i];
        level_its[i].nr_inputs = nr_inputs;
        level_its[i].cur = 0;
        for(int j = 0; j < nr_inputs; j++)
        {
            sprintf(filename, "/mnt/openssd/%ld.txt", input_file_nums[j + levels[i]]);
            fd = open(filename, O_RDONLY);
            fstat(fd, &stat);
            level_its[i].input[j].size = stat.st_size;
            level_its[i].input[j].offset = 0;
            level_its[i].input[j].mapped = mmap(NULL, stat.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
            close(fd);
        }
    }
    // struct timeval start, end;
    // gettimeofday(&start, NULL);
    do_compaction_work(level_its, args->nr_levels, output_file_nums, args->nr_outputs, output_sizes, output_smallests, output_largests, 
        segs, segs_sizes, filters);
    // gettimeofday(&end, NULL);
    // long seconds = end.tv_sec - start.tv_sec;
    // long microseconds = end.tv_usec - start.tv_usec;
    // double milliseconds = seconds * 1000.0 + microseconds / 1000.0;
    // printf("Compaction time: %.2f milliseconds\n", milliseconds);

    // for(int i = 0; i < args->nr_outputs; i++)
    // {
    //     printf("output file %d, size = %ld, smallest = %ld, largest = %ld\n", i, output_sizes[i], output_smallests[i], output_largests[i]);
    // }
    
    for(int i = 0; i < args->nr_levels; i++)
    {
        if(level_its[i].nr_inputs > 0)
        {
            for(int j = 0; j < level_its[i].nr_inputs; j++)
            {
                munmap(level_its[i].input[j].mapped, level_its[i].input[j].size);
            }
        }
    }
    free(level_its);
}