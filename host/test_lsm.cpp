#include "kv.h"
#include "lsm.h"
#include "table.h"
#include "utils.h"
#include <bits/types/struct_timeval.h>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <sys/time.h>
#include <unistd.h>
#include <vector>
#include <algorithm>
#include <random>
#include <iostream>

#define BATCH_SIZE 512

struct opearation {
    int op_type;
    KeyType key;
    int length_range;
};
enum op_type
{
    READ=0,
    UPDATE,
    SCAN,
    INSERT
};
enum exp_type
{
    SEARCH_DIS=0,   
    READ_WHILE_WRITE,
    ABLATION,
    _BATCH_SIZE,       
    YCSB,
    SOSD,
    WRITE,
    MUTILTH, // 7
};
void test_write(std::string data_path, int is_seq, int is_str);
void test_search_distribution(std::string data_path, int nr_threads_r, int is_str);
void test_readwhilewrite(std::string data_path, int write_percent, int nr_threads_r, bool _csd_compaction, bool csd_search);
void ablation_exp(std::string data_path, bool csd_compaction, bool csd_search);
void test_batch_size(std::string data_path);
void test_ycsb(std::string ycsb_load_path, std::string ycsb_run_path, int nr_threads_r);
void test_sosd(std::string data_path, std::string dis_path, int nr_threads_r);
void test_mutil_threads_read(std::string data_path, int nr_threads_r, int with_csd_search);
int main(int argc, char* argv[])
{
    KeyType key = 1;
    uint8_t* value;
    lsmInit();

    int exp_type = atoi(argv[1]);
    std::string data_path, dis_path;
    std::string ycsb_load_path;
    std::string ycsb_run_path;
    int write_percent, _csd_compaction, _csd_search, batch_size, nr_threads_r, nr_threads_w, is_write_only, is_seq, is_str;

    switch (exp_type) {
        case WRITE:
            data_path = argv[2];
            is_seq = atoi(argv[3]);
            is_str = atoi(argv[4]);
            test_write(data_path, is_seq, is_str);
            break;
        case SEARCH_DIS:
            data_path = argv[2];
            nr_threads_r = atoi(argv[3]);
            is_str = atoi(argv[4]);
            test_search_distribution(data_path, nr_threads_r, is_str);
            printf("max model size = %ld\n", max_models_size);
            break;

        case READ_WHILE_WRITE:
            data_path = argv[2];
            write_percent = atoi(argv[3]);
            nr_threads_r = atoi(argv[4]);
            _csd_compaction = atoi(argv[5]);
            _csd_search = atoi(argv[6]);
            test_readwhilewrite(data_path, write_percent, nr_threads_r, _csd_compaction, _csd_search);
            break;

        case ABLATION:
            data_path = argv[2];
            _csd_compaction = atoi(argv[3]);
            _csd_search = atoi(argv[4]);
            ablation_exp(data_path, _csd_compaction, _csd_search);
            break;

        case _BATCH_SIZE:
            data_path = argv[2];
            test_batch_size(data_path);
            break;

        case YCSB:
            ycsb_load_path = argv[2];
            ycsb_run_path = argv[3];
            nr_threads_r = atoi(argv[4]);
            test_ycsb(ycsb_load_path, ycsb_run_path, nr_threads_r);
            break;
        
        case SOSD:
            data_path = argv[2];
            dis_path = argv[3];
            nr_threads_r = atoi(argv[4]);
            test_sosd(data_path, dis_path, nr_threads_r);
            break;
        
        case MUTILTH:
            data_path = argv[2];
            nr_threads_r = atoi(argv[3]);
            _csd_search = atoi(argv[4]);
            test_mutil_threads_read(data_path, nr_threads_r, _csd_search);
            break;
    }
    
    fflush(stdout);
    destroy_lsm();
    return 0;
}

void test_write(std::string data_path, int is_seq, int is_str)
{
    int keys_num = 100000000;
    std::vector<KeyType> keys;
    std::vector<std::string> keys_str;
    KeyType key;
    uint8_t* value = (uint8_t*)malloc(VALUE_LENGTH);
    memset(value, 1, 64);

    if(is_str)
    {
        std::ifstream file(data_path);
        std::string key_str;
        // 逐行读取文件
        while (getline(file, key_str)) {
            keys_str.push_back(key_str);
        }
        file.close();
    }
    else 
    {
        FILE* data_fp = fopen(data_path.c_str(), "r+");
        for(int i = 0; i < keys_num; i++)
        {   
            fscanf(data_fp, "%lu", &key);
            keys.push_back(key);
        }
        fclose(data_fp);
    }

    if(is_seq)
    {
        if(is_str)
            std::sort(keys_str.begin(), keys_str.end());
        else
            std::sort(keys.begin(), keys.end());
    }

    struct timeval start, end;
    gettimeofday(&start, NULL);
    for(int i = 0; i < keys_num; i++)
    {   
        if(is_str)
        {
            key = hash_function(keys_str[i]);
        }
        else
            key = keys[i];
        insert(key, value);
    }
    gettimeofday(&end, NULL);
    long seconds  = end.tv_sec  - start.tv_sec;
    long useconds = end.tv_usec - start.tv_usec;
    double elapsed = seconds + useconds / 1000000.0;
    printf("write time = %.6f seconds, throughput = %.1f op/s\n", elapsed, keys_num / elapsed);
}

void test_ycsb(std::string ycsb_load_path, std::string ycsb_run_path, int nr_threads_r)
{
    FILE* load = fopen(ycsb_load_path.c_str(), "r+");
    FILE* run = fopen(ycsb_run_path.c_str(), "r+");
    char ycsb_type = ycsb_load_path.back();
    int keys_num, ops_num, op_type;
    KeyType key;
    uint8_t* value = (uint8_t*)malloc(VALUE_LENGTH);
    memset(value, 1, VALUE_LENGTH);
    fscanf(load, "%d", &keys_num);
    fscanf(run, "%d", &ops_num);
    printf("%d, %d\n", keys_num, ops_num);
    for(int i = 0; i < keys_num; i++)
    {
        fscanf(load, "%d", &op_type);
        fscanf(load, "%lu", &key);
        insert(key, value);
    }
    std::vector<uint64_t> candidate_keys;
    std::vector<opearation> ops_r;
    std::vector<opearation> ops_w;
    opearation op;
    for(int i = 0; i < ops_num; i++)
    {
        fscanf(run, "%d", &op.op_type);
        fscanf(run, "%lu", &op.key);
        if(op.op_type == INSERT || op.op_type == UPDATE)
        {
            ops_w.emplace_back(op);
        }
        else if(op.op_type == SCAN)
        {
            fscanf(run, "%d", &op.length_range);
            ops_r.emplace_back(op);
        }
        else 
        {
            ops_r.emplace_back(op);
            candidate_keys.push_back(op.key);
        }
    }

    std::vector<std::thread> readers;
    std::vector<std::thread> writers;
    int nr_reads_per_thread = ops_r.size() / nr_threads_r;

    start_read = true;
    
    for(int i = 0; i < 1; i++)
    {
        writers.emplace_back([&]() {
            for(int j = 0; j < ops_w.size(); j++)
            {
                if(ops_w[j].op_type == UPDATE)
                {
                    update(ops_w[j].key, value);
                }
                else if(ops_w[j].op_type == INSERT)
                {
                    insert(ops_w[j].key, value);
                }
            }
        });
    }

    for(int i = 0; i < nr_threads_r; i++)
    {
        readers.emplace_back([&, i]() {
            if(ycsb_type == 'e')
            {
                for(int j = 0; j < nr_reads_per_thread; j++)
                {
                    int index = i * nr_reads_per_thread + j;
                    rangeQuery(ops_r[index].key, ops_r[index].length_range);
                }
            }
            else
            {
                search_in_batch_mutil_threads(candidate_keys, i * nr_reads_per_thread, nr_reads_per_thread, BATCH_SIZE);
            }
        });
    }
    
    auto s = std::chrono::high_resolution_clock::now();
    for (auto& reader : readers) {
        reader.join();
    }
    for (auto& writer : writers) {
        writer.join();
    }
    auto e = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = e - s;
    std::cout << elapsed.count() << " seconds " << ops_num / elapsed.count() << " op/s" << "\n";
    fclose(load);
    fclose(run);
}

void test_sosd(std::string data_path, std::string dis_path, int nr_threads_r)
{
    FILE* data_fp = fopen(data_path.c_str(), "r+");
    int keys_num = 100000000;
    int nr_reads = 20000000;
    KeyType key;
    uint8_t* value = (uint8_t*)malloc(VALUE_LENGTH);
    memset(value, 1, 64);

    for(int i = 0; i < keys_num; i++)
    {   
        fscanf(data_fp, "%lu", &key);
        insert(key, value);
    }
    fclose(data_fp);

    const DataType file_type = resolve_type(data_path);
    std::vector<uint64_t> distribution(nr_reads);
    if(file_type == UINT32)
    {
        std::vector<EqualityLookup<uint32_t>> lookups = load_data<EqualityLookup<uint32_t>>(dis_path);
        for(int i = 0; i < nr_reads; i++)
        {
            distribution[i] = lookups[i].key;
        }
    }
    else 
    {
        std::vector<EqualityLookup<uint64_t>> lookups = load_data<EqualityLookup<uint64_t>>(dis_path);
        for(int i = 0; i < nr_reads; i++)
        {
            distribution[i] = lookups[i].key;
        }
    }

    int nr_reads_per_thread = nr_reads / nr_threads_r;
    std::vector<std::thread> readers;
    for (int i = 0; i < nr_threads_r; ++i) {
        readers.emplace_back([&, i]() {
            search_in_batch_mutil_threads(distribution, i * nr_reads_per_thread, nr_reads_per_thread, BATCH_SIZE);
        });
    }
    struct timeval start, end;
    gettimeofday(&start, NULL);
    for (auto& reader : readers) {
        reader.join();
    }
    gettimeofday(&end, NULL);
    long seconds  = end.tv_sec  - start.tv_sec;
    long useconds = end.tv_usec - start.tv_usec;
    double elapsed = seconds + useconds / 1000000.0;
    printf("%lf\n", elapsed);
    fflush(stdout);
}

void test_search_distribution(std::string data_path, int nr_threads_r, int is_str)
{
    FILE* data_fp = fopen(data_path.c_str(), "r+");
    int keys_num = 100000000;
    int search_num = 20000000;
    std::vector<KeyType> keys;
    std::vector<KeyType> keys_sorted;
    KeyType key;
    uint8_t* v = (uint8_t*)malloc(VALUE_LENGTH);
    memset(v, 1, 64);

    if(is_str)
    {
        std::ifstream file(data_path);
        std::string key_str;
        // 逐行读取文件
        while (getline(file, key_str)) {
            key = hash_function(key_str);
            keys.push_back(key);
            keys_sorted.push_back(key);
            insert(key, v);
        }
        file.close();
    }
    else 
    {
        FILE* data_fp = fopen(data_path.c_str(), "r+");
        for(int i = 0; i < keys_num; i++)
        {
            fscanf(data_fp, "%lu", &key);
            keys.push_back(key);
            keys_sorted.push_back(key);
            insert(key, v);
        }
        fclose(data_fp);
    }

    std::vector<KeyType> keys_dis(search_num);
    FILE* zipfian_fp = fopen("../distribution/zipfian.txt", "r+");
    FILE* hotspot_fp = fopen("../distribution/hotspot.txt", "r+");
    FILE* uniform_fp = fopen("../distribution/uniform.txt", "r+");
    int operations = 3;
    uint8_t* value = (uint8_t*)malloc(VALUE_LENGTH);
    // seq
    long seconds, useconds;
    double elapsed = 0;
    struct timeval start, end;
    std::sort(keys_sorted.begin(), keys_sorted.end());
    int nr_reads_per_thread = search_num / nr_threads_r;
    // for(int op = 0; op < operations; op++)
    // {
    //     system("sync; echo 3 > sudo tee /proc/sys/vm/drop_caches");
    //     std::vector<thread> readers;
    //     for (int i = 0; i < nr_threads_r; ++i) {
    //         readers.emplace_back([&, i]() {
    //             search_in_batch_mutil_threads(keys_sorted, i * nr_reads_per_thread, nr_reads_per_thread, BATCH_SIZE);
    //             // search_in_batch(keys_sorted, search_num, BATCH_SIZE);
    //             // for(int i = 0; i < search_num; i++)
    //             // {
    //             //     KeyType key = keys_sorted[i];
    //             //     uint8_t* test_value = search(key, value, 1);
    //             // }
    //         });
    //     }

    //     gettimeofday(&start, NULL);
    //     for (auto& reader : readers) {
    //         reader.join();
    //     }
    //     gettimeofday(&end, NULL);

    //     seconds  = end.tv_sec  - start.tv_sec;
    //     useconds = end.tv_usec - start.tv_usec;
    //     elapsed += seconds + useconds / 1000000.0;
    // }
    // printf("sequential search = %.6f seconds, throughput = %.1f op/s\n", elapsed / operations, search_num * operations / elapsed);
    // fflush(stdout);

    // zipfian
    int distribute;
    elapsed = 0;
    for(int i = 0; i < search_num; i++)
    {
        fscanf(zipfian_fp, "%d", &distribute);
        keys_dis[i] = keys[distribute];
    }
    fclose(zipfian_fp);
    for(int op = 0; op < operations; op++)
    {
        system("sync; echo 3 > sudo tee /proc/sys/vm/drop_caches");
        std::vector<thread> readers;
        for (int i = 0; i < nr_threads_r; ++i) {
            readers.emplace_back([&, i]() {
                search_in_batch_mutil_threads(keys_dis, i * nr_reads_per_thread, nr_reads_per_thread, BATCH_SIZE);
                // search_in_batch(keys_dis, search_num, BATCH_SIZE);
                // for(int i = 0; i < search_num; i++)
                // {
                //     KeyType key = keys_dis[i];
                //     uint8_t* test_value = search(key, value, 1);
                // }
            });
        }

        gettimeofday(&start, NULL);
        for (auto& reader : readers) {
            reader.join();
        }
    
        gettimeofday(&end, NULL);
        seconds  = end.tv_sec  - start.tv_sec;
        useconds = end.tv_usec - start.tv_usec;
        elapsed += seconds + useconds / 1000000.0;
    }
    printf("zipfian search = %.6f seconds, throughput = %.1f op/s\n", elapsed / operations, search_num * operations / elapsed);
    
    // hotspot
    elapsed = 0;
    for(int i = 0; i < search_num; i++)
    {
        fscanf(hotspot_fp, "%d", &distribute);
        keys_dis[i] = keys[distribute];
    }
    fclose(hotspot_fp);
    for(int op = 0; op < operations; op++)
    {
        system("sync; echo 3 > sudo tee /proc/sys/vm/drop_caches");
        std::vector<thread> readers;
        for (int i = 0; i < nr_threads_r; ++i) {
            readers.emplace_back([&, i]() {
                search_in_batch_mutil_threads(keys_dis, i * nr_reads_per_thread, nr_reads_per_thread, BATCH_SIZE);
                // search_in_batch(keys_dis, search_num, BATCH_SIZE);
                // for(int i = 0; i < search_num; i++)
                // {
                //     KeyType key = keys_dis[i];
                //     uint8_t* test_value = search(key, value, 1);
                // }
            });
        }

        gettimeofday(&start, NULL);
        for (auto& reader : readers) {
            reader.join();
        }
    
        gettimeofday(&end, NULL);
        seconds  = end.tv_sec  - start.tv_sec;
        useconds = end.tv_usec - start.tv_usec;
        elapsed += seconds + useconds / 1000000.0;
    }
    printf("hotspot search = %.6f seconds, throughput = %.1f op/s\n", elapsed / operations, search_num * operations / elapsed);
    fflush(stdout);

    // uniform
    elapsed = 0;
    for(int i = 0; i < search_num; i++)
    {
        fscanf(uniform_fp, "%d", &distribute);
        keys_dis[i] = keys[distribute];
    }
    fclose(uniform_fp);
    for(int op = 0; op < operations; op++)
    {
        system("sync; echo 3 > sudo tee /proc/sys/vm/drop_caches");
        std::vector<thread> readers;
        for (int i = 0; i < nr_threads_r; ++i) {
            readers.emplace_back([&, i]() {
                search_in_batch_mutil_threads(keys_dis, i * nr_reads_per_thread, nr_reads_per_thread, BATCH_SIZE);
                // search_in_batch(keys_dis, search_num, BATCH_SIZE);
                // for(int i = 0; i < search_num; i++)
                // {
                //     KeyType key = keys_dis[i];
                //     uint8_t* test_value = search(key, value, 1);
                // }
            });
        }

        gettimeofday(&start, NULL);
        for (auto& reader : readers) {
            reader.join();
        }
        gettimeofday(&end, NULL);
        seconds  = end.tv_sec  - start.tv_sec;
        useconds = end.tv_usec - start.tv_usec;
        elapsed += seconds + useconds / 1000000.0;
    }
    printf("uniform search = %.6f seconds, throughput = %.1f op/s\n", elapsed / operations, search_num * operations / elapsed);

    // latest
    elapsed = 0;
    for(int i = 0; i < search_num; i++)
    {
        keys_dis[i] = keys[i + 80000000];
    }
    for(int op = 0; op < operations; op++)
    {
        system("sync; echo 3 > sudo tee /proc/sys/vm/drop_caches");
        std::vector<thread> readers;
        for (int i = 0; i < nr_threads_r; ++i) {
            readers.emplace_back([&, i]() {
                search_in_batch_mutil_threads(keys_dis, i * nr_reads_per_thread, nr_reads_per_thread, BATCH_SIZE);
                // search_in_batch(keys_dis, search_num, BATCH_SIZE);
                // for(int i = 0; i < search_num; i++)
                // {
                //     KeyType key = keys_dis[i];
                //     uint8_t* test_value = search(key, value, 1);
                // }
            });
        }

        gettimeofday(&start, NULL);
        for (auto& reader : readers) {
            reader.join();
        }
    
        gettimeofday(&end, NULL);
        seconds  = end.tv_sec  - start.tv_sec;
        useconds = end.tv_usec - start.tv_usec;
        elapsed += seconds + useconds / 1000000.0;
    }
    printf("latest search = %.6f seconds, throughput = %.1f op/s\n", elapsed / operations, search_num * operations / elapsed);
}

void test_readwhilewrite(std::string data_path, int write_percent, int nr_thread_r, bool _csd_compaction, bool csd_search)
{
    FILE* data_fp = fopen(data_path.c_str(), "r+");
    int keys_num = 100000000;
    std::vector<KeyType> keys;
    std::vector<KeyType> search_keys;
    KeyType key;
    uint8_t* value = (uint8_t*)malloc(VALUE_LENGTH);
    memset(value, 1, 64);

    for(int i = 0; i < keys_num; i++)
    {   
        fscanf(data_fp, "%lu", &key);
        keys.push_back(key);
        if(i < keys_num / 2)
            search_keys.push_back(key);
        insert(key, value);
        std::cout << "Key" << i << "/ " << keys_num << "\r";
    }
    fclose(data_fp);
    printf("start readwhilewrite\n");
    csd_compaction = _csd_compaction;
    int nr_ops = 50000000;
    int nr_writes = nr_ops / 100 * write_percent;
    int nr_reads = nr_ops - nr_writes;
    int nr_reads_per_thread = nr_reads / nr_thread_r;

    std::vector<thread> writers;
    std::vector<thread> readers;
    for(int i = 0; i < 1; i++)
    {
        writers.emplace_back([&]() {
            struct timeval s, e;
            gettimeofday(&s, NULL);
            for(int i = 0; i < nr_writes; i++)
            {
                insert(keys[i + keys_num / 2], value);
            }
            gettimeofday(&e, NULL);
            long seconds  = e.tv_sec  - s.tv_sec;
            long useconds = e.tv_usec - s.tv_usec;
            double elapsed = seconds + useconds / 1000000.0;
            printf("readwhilewrite write time: %.6f seconds\n", elapsed);
        });
    }
    
    for (int i = 0; i < nr_thread_r; ++i) {
        readers.emplace_back([&, i]() {
            // struct timeval s, e;
            // gettimeofday(&s, NULL);
            uint8_t* v = (uint8_t*)malloc(VALUE_LENGTH);
            if(csd_search)
            {
                if(nr_thread_r == 1)
                    search_in_batch(search_keys, nr_reads_per_thread, BATCH_SIZE);
                else
                    search_in_batch_mutil_threads(search_keys, i * nr_reads_per_thread, nr_reads_per_thread, BATCH_SIZE);
            }
            else
            {
                for(int j = 0; j < nr_reads_per_thread; j++)
                {
                    KeyType key = keys[i * nr_reads_per_thread + j];
                    uint8_t* test_value = search(key, v, 1);
                }
            }
            // gettimeofday(&e, NULL);
            // long seconds  = e.tv_sec  - s.tv_sec;
            // long useconds = e.tv_usec - s.tv_usec;
            // double elapsed = seconds + useconds / 1000000.0;
            // printf("read time: %.6f seconds\n", elapsed);
        });
    }

    struct timeval start, end;
    gettimeofday(&start, NULL);
    for (auto& reader : readers) {
        reader.join();
    }
    gettimeofday(&end, NULL);
    long seconds  = end.tv_sec  - start.tv_sec;
    long useconds = end.tv_usec - start.tv_usec;
    double elapsed = seconds + useconds / 1000000.0;
    printf("readwhilewrite read time: %.6f seconds\n", elapsed);
    for (auto& writer : writers) {
        writer.join();
    }
    gettimeofday(&end, NULL);
    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;
    elapsed = seconds + useconds / 1000000.0;
    printf("readwhilewrite total time: %.6f seconds\n", elapsed);
}

void ablation_exp(std::string data_path, bool csd_compaction, bool csd_search)
{
    if(csd_compaction)
    {
        printf("Only CSD Compaction:\n");
        test_readwhilewrite(data_path, 50, 1, 1, 0);
    }
    else if(csd_search)
    {
        printf("Only CSD Search:\n");
        test_readwhilewrite(data_path, 50, 1, 0, 1);
    }
}

void test_batch_size(std::string data_path)
{
    FILE* data_fp = fopen(data_path.c_str(), "r+");
    int keys_num = 100000000;
    int search_num = 20000000;
    std::vector<KeyType> keys;
    KeyType key;
    uint8_t* value = (uint8_t*)malloc(VALUE_LENGTH);
    memset(value, 1, 64);

    for(int i = 0; i < keys_num; i++)
    {   
        fscanf(data_fp, "%lu", &key);
        keys.push_back(key);
        insert(key, value);
    }
    fclose(data_fp);

    // int batch_sizes[] = {128, 256, 512, 640, 768, 896, 1024, 1152, 1280};
    int batch_sizes[] = {32, 64, 128, 256, 512, 640, 768, 896, 1024, 1152, 1280};
    for(int i = 0; i < sizeof(batch_sizes) / sizeof(int); i++)
    {
        struct timeval start, end;
        gettimeofday(&start, NULL);
        search_in_batch(keys, search_num, batch_sizes[i]);
        gettimeofday(&end, NULL);
        long seconds  = end.tv_sec  - start.tv_sec;
        long useconds = end.tv_usec - start.tv_usec;
        double elapsed = seconds + useconds / 1000000.0;
        printf("batch_size = %d, read time: %.6f seconds\n", batch_sizes[i], elapsed);
        fflush(stdout);
    }
}

void test_mutil_threads_read(std::string data_path, int nr_threads_r, int with_csd_search)
{
    FILE* data_fp = fopen(data_path.c_str(), "r+");
    int keys_num = 100000000;
    std::vector<KeyType> keys;
    KeyType key;
    uint8_t* value = (uint8_t*)malloc(VALUE_LENGTH);
    memset(value, 1, 64);

    for(int i = 0; i < keys_num; i++)
    {   
        fscanf(data_fp, "%lu", &key);
        keys.push_back(key);
        insert(key, value);
    }
    fclose(data_fp);

    int nr_read_operations = 20000000;
    int nr_reads = nr_read_operations / nr_threads_r;

    FILE* uniform_fp = fopen("../distribution/uniform.txt", "r+");
    std::vector<int> distribution(nr_read_operations);
    for(int i = 0; i < nr_read_operations; i++)
    {
        int dis;
        fscanf(uniform_fp, "%d", &dis);
        distribution[i] = dis;
    }
    fclose(uniform_fp);

    std::vector<thread> readers;    
    for (int i = 0; i < nr_threads_r; ++i) {
        readers.emplace_back([&, i]() {
            // struct timeval s, e;
            // gettimeofday(&s, NULL);
            int nn = 0;
            if(with_csd_search)
                search_in_batch_mutil_threads(keys, i * nr_reads, nr_reads, BATCH_SIZE);
            else
            {
                // printf("%d\n", nr_reads);
                for(int j = 0; j < nr_reads; j++)
                {
                    KeyType key = keys[i * nr_reads + j];
                    uint8_t* test_value = search(key, value, 1);
                }
            }
            // gettimeofday(&e, NULL);
            // long seconds  = e.tv_sec  - s.tv_sec;
            // long useconds = e.tv_usec - s.tv_usec;
            // double elapsed = seconds + useconds / 1000000.0;
            // printf("read time: %.6f seconds\n", elapsed);
        });
    }

    struct timeval start, end;
    gettimeofday(&start, NULL);
    for (auto& reader : readers) {
        reader.join();
    }
    gettimeofday(&end, NULL);
    long seconds  = end.tv_sec  - start.tv_sec;
    long useconds = end.tv_usec - start.tv_usec;
    double elapsed = seconds + useconds / 1000000.0;
    printf("mutilthread read time: %.6f seconds\n", elapsed);
}