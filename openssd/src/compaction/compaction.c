#include <string.h>
#include "compaction.h"
#include "utils_common.h"
#include "debug.h"
#include "xtime_l.h"
#include <stdio.h>
#include "memory_map.h"

#define GET_ARM_V8_CYCLE_COUNT(var) do { asm volatile("mrs %0, PMCCNTR_EL0" : "=r" (var) : : "memory"); } while (0)

#define ENABLE_CYCLE_COUNT

static struct cycle_counts {
    uint64_t compaction_cycles;
    uint64_t open_cycles;
    uint64_t read_cycles;
    uint64_t write_cycles;
    uint64_t close_cycles;
    uint64_t sync_read_cycles;
    uint64_t sync_write_cycles;
    uint64_t sync_close_cycles;
    uint64_t memcpy_cycles;
    uint64_t compare_cycles;
    uint64_t copy_keys_cycles;
    uint64_t input_next_cycles;
    uint64_t output_add_cycles;
    uint64_t output_finish_cycles;
    uint64_t memcpy_size;
    uint64_t read_size;
    uint64_t write_size;
} cycle_counts;

#ifdef ENABLE_CYCLE_COUNT

#define CYCLE_COUNT_BEGIN() \
    do { \
        uint64_t __begin, __end; \
        GET_ARM_V8_CYCLE_COUNT(__begin)

#define CYCLE_COUNT_END(var) \
        GET_ARM_V8_CYCLE_COUNT(__end); \
        (var) += __end - __begin; \
    } while (0)

void memcpy_wrapper(void *dst, void *src, size_t n)
{
    CYCLE_COUNT_BEGIN();
    memcpy(dst, src, n);
    CYCLE_COUNT_END(cycle_counts.memcpy_cycles);
    cycle_counts.memcpy_size += n;
}

#else

#define CYCLE_COUNT_BEGIN()
#define CYCLE_COUNT_END(field)
#define memcpy_wrapper memcpy

#endif
XCompaction compaction_fpga;
XCompaction_Config* compaction_cfg;

int compare_keys(KeyValuePair* a, KeyValuePair* b) {
    int ret;
    CYCLE_COUNT_BEGIN();
    if(a->key > b->key)
        ret = 1;
    else if(a->key < b->key)
        ret = -1;
    else
        ret = 0;

    if(ret == 0)
    {
        if(a->seq == b->seq)
        {
            printf("equal seq\n");
            ASSERT(a->seq != b->seq);
        }
        
        if(a->seq > b->seq)
            ret = -1;
        else
            ret = 1;
    }
    CYCLE_COUNT_END(cycle_counts.compare_cycles);
    return ret;
}

int table_iterator_next(table_file* table_it, KeyValuePair* kv) {
    if(table_it->offset >= table_it->size)
    {
        // *key = 0;
        kv->value = NULL;
        return -1;
    }
    memcpy_wrapper((void*)kv, table_it->mapped + table_it->offset, VALUE_OFFSET);
    table_it->offset += VALUE_OFFSET;
    kv->value = &((uint8_t*)(table_it->mapped + table_it->offset))[0];
    table_it->offset += VALUE_LENGTH;
    return 0;
}

void prepare_input_file(struct cs_file *file, struct table_file *input) {
    ASSERT(file != NULL);
    ASSERT(file->size <= OUTPUT_TABLE_MAX_SIZE);
    ASSERT(input->mapped != NULL);

    
    CYCLE_COUNT_BEGIN();
    sync_file_pending_ios(file);
    CYCLE_COUNT_END(cycle_counts.sync_read_cycles);
    
    input->size = file->size;
    input->offset = 0;
    
    CYCLE_COUNT_BEGIN();
    cs_file_close(file);
    CYCLE_COUNT_END(cycle_counts.close_cycles);
}

void level_iterator_init(struct cs_file **input_files, int nr_inputs, int level, struct level_iterator *it) {
    it->nr_inputs = nr_inputs;
    if (nr_inputs > 0) {
        /* an empty level is given if a trivial move is rejected due to overlapping with grandparents*/
        if (nr_inputs > MAX_INPUTS_PER_LEVEL) {
            ASSERT(0);
        }

        it->cur = 0;
        for (int i = 0; i < nr_inputs; i++) {
            it->input_files[i] = input_files[i];
            it->file_bufs[i] = linear_malloc(input_files[i]->size, 0);
            ASSERT(it->file_bufs[i] != NULL);
            // CYCLE_COUNT_BEGIN();
            // FLUSH_CACHE(it->file_bufs[i], input_files[i]->size);
            // cs_file_pread(input_files[i], it->file_bufs[i], input_files[i]->size, 0);
            // CYCLE_COUNT_END(cycle_counts.read_cycles);
        }
        // it->input.mapped = it->file_bufs[0];
        // prepare_input_file(input_files[0], &it->input);
        // table_iterator_init(&it->input, &it->table_it);
    }
}

void level_iterator_prefetch_file(int i, struct level_iterator *it) {
    if (i >= it->nr_inputs)
        return;

    CYCLE_COUNT_BEGIN();
    FLUSH_CACHE(it->file_bufs[i], it->input_files[i]->size);
    cs_file_pread(it->input_files[i], it->file_bufs[i], it->input_files[i]->size, 0);
    CYCLE_COUNT_END(cycle_counts.read_cycles);
    cycle_counts.read_size += it->input_files[i]->size;
}

void level_iterator_prepare(struct level_iterator *it) {
    if (it->nr_inputs == 0)
        return;

    it->input.mapped = it->file_bufs[0];
    prepare_input_file(it->input_files[0], &it->input);
    // table_iterator_init(&it->input, &it->table_it);
}

void level_iterator_destroy(struct level_iterator *it)
{
}

int level_iterator_next(level_iterator *it, KeyValuePair* kv) {
    if(it->nr_inputs == 0 || it->cur >= it->nr_inputs)
    {
        kv->value = NULL;
        return -1;
    }

    if (table_iterator_next(&it->input, kv) != 0) {
        it->cur++;
        if (it->cur >= it->nr_inputs)
            return -1;

        it->input.mapped = it->file_bufs[it->cur];
        prepare_input_file(it->input_files[it->cur], &it->input);
        // table_iterator_init(&it->input, &it->table_it);
        if (table_iterator_next(&it->input, kv) != 0)
        {
            printf("table iterator next error\n");
            ASSERT(0);
        }
    }

    return 0;
}

void output_table_init(struct output_table *output, uint64_t file_id) {
    // uint32_t open_start, open_end;

    // GET_ARM_V7_CYCLE_COUNT(open_start);
    output->file = cs_file_open(file_id);
    // GET_ARM_V7_CYCLE_COUNT(open_end);
    // open_cycles += open_end - open_start;
    ASSERT(output->file != NULL);

    output->table_size = 0;
    output->output_buf_cursor = 0;
}

void output_table_flush(output_table* output) {
	CYCLE_COUNT_BEGIN();
    FLUSH_CACHE(output->output_buf, output->output_buf_cursor);
    cs_file_write(output->file, output->output_buf, output->output_buf_cursor);
    sync_file_pending_ios(output->file);
    cs_file_close(output->file);
    CYCLE_COUNT_END(cycle_counts.write_cycles);
    cycle_counts.write_size += output->output_buf_cursor;
    output->output_buf_cursor = 0;
}

int output_table_add(output_table* output, KeyValuePair* kv) {
    if((kv->seq & DELETE_SEQ) > 0)
        return 0;
    memcpy_wrapper(output->output_buf + output->output_buf_cursor, kv, VALUE_OFFSET);
    output->output_buf_cursor += VALUE_OFFSET;
    memcpy_wrapper(output->output_buf + output->output_buf_cursor, kv->value, VALUE_LENGTH);
    output->output_buf_cursor += VALUE_LENGTH;
    output->table_size += KV_LENGTH;
    if(output->table_size >= OUTPUT_TABLE_SIZE_THRES)
    {
        output_table_flush(output);
        return -1;
    }
    return 0;
}

u32 cycle1 = 0;
u32 cycle2 = 0;
u32 cycle3 = 0;

void do_compaction(struct level_iterator *levels, int nr_levels, uint64_t *output_file_ids, 
        int nr_outputs, uint64_t *output_sizes, KeyType *smallests, KeyType *largests)
{
    KeyValuePair* kvs = linear_malloc(sizeof(KeyValuePair) * nr_levels, 0);
    output_table* output = linear_malloc(sizeof(struct output_table), 0);
    int smallest;
    int64_t file_id = -1;
    int output_used = 0;

    ASSERT(kvs != NULL);
    ASSERT(output != NULL);
    output->output_buf = linear_malloc(OUTPUT_TABLE_MAX_SIZE, 0);
    ASSERT(output->output_buf != NULL);
    if (output_sizes != NULL)
        for (int i = 0; i < nr_outputs; i++)
            output_sizes[i] = 0;

    for (int i = 0; i < nr_levels; i++) 
    {
        level_iterator_next(&levels[i], &kvs[i]);
    }
    
    KeyType lastkey;
    int flag = 1;
    while (1) 
    {
        smallest = -1;
        for (int i = 0; i < nr_levels; i++)
        {
            if(kvs[i].value != NULL && (smallest < 0 || compare_keys(&kvs[i], &kvs[smallest]) < 0))
                smallest = i;
        }

        if (smallest < 0)
            break;

        if (file_id < 0) 
        {
            ASSERT(output_used < nr_outputs);
            file_id = output_file_ids[output_used];
            output_table_init(output, file_id);
            smallests[output_used] = kvs[smallest].key;
        }

        if(flag || lastkey != kvs[smallest].key)
        {
            lastkey = kvs[smallest].key;
            flag = 0;
            if(output_table_add(output, &kvs[smallest]) < 0)
            {
                file_id = -1;
                output_sizes[output_used] = output->table_size;
                largests[output_used] = kvs[smallest].key;
                output_used++;
            }
        }

        level_iterator_next(&levels[smallest], &kvs[smallest]);
    }
    if(output->output_buf_cursor > 0)
    {
    	output_sizes[output_used] = output->table_size;
        largests[output_used] = lastkey;
        output_table_flush(output);
        output->table_size = 0;
        output_used++;
    }
}

static volatile struct shared_mem *m = (struct shared_mem *)SHARED_MEM_BASE_ADDR;
static struct host_io_req *compaction_io_reqs;
static QTAILQ_HEAD(free_compaction_io_reqs, host_io_req) free_compaction_io_reqs;
static int nr_free_compaction_io_reqs;

void init_compaction_io_reqs()
{
	compaction_io_reqs = linear_malloc(COMPACTION_FILE_REQ_QP_ENTRIES * sizeof(struct host_io_req), 0);
    nr_free_compaction_io_reqs = COMPACTION_FILE_REQ_QP_ENTRIES;

	QTAILQ_INIT(&free_compaction_io_reqs);
	for (int i = 0; i < COMPACTION_FILE_REQ_QP_ENTRIES; i++)
		QTAILQ_INSERT_TAIL(&free_compaction_io_reqs, &compaction_io_reqs[i], qent);
}

struct host_io_req *alloc_compaction_io_req()
{
	struct host_io_req *req;
	ASSERT(!QTAILQ_EMPTY(&free_compaction_io_reqs));

	req = QTAILQ_FIRST(&free_compaction_io_reqs);
	QTAILQ_REMOVE(&free_compaction_io_reqs, req, qent);

	nr_free_compaction_io_reqs--;

	return req;
}

void free_compaction_io_req(struct host_io_req *req)
{
	QTAILQ_INSERT_HEAD(&free_compaction_io_reqs, req, qent);
	nr_free_compaction_io_reqs++;
}

void process_emu_req_cq(int* nr_req, volatile struct qpair* qp)
{
	struct emu_req_cqe *cqe;

	while ((cqe = qpair_peek_cqe(qp)) != NULL) {
        struct host_io_req *req = cqe->host_io_req;
        free_compaction_io_req(req);
        qpair_consume_cqe(qp, cqe);
        *nr_req = *nr_req - 1;
	}
}

void compaction(uint8_t* uncached_buffer, uint64_t* disk_offsets)
{
    struct compaction_args *args = (struct compaction_args *)uncached_buffer;
    struct level_iterator *level_its;
    uint64_t *input_file_ids;
    uint64_t *input_file_nums;
    uint64_t *output_file_ids;
    uint64_t *output_file_nums;
    uint64_t *levels;
    uint64_t *output_sizes;
    KeyType *output_smallests;
    KeyType *output_largests;
    uint64_t* output_segs_sizes;
    struct cs_file **input_files;
    struct cs_file **output_files;

    memset(&cycle_counts, 0, sizeof(cycle_counts));

//    CYCLE_COUNT_BEGIN(); /* compaction_cycles */

    level_its = linear_malloc(args->nr_levels * sizeof(struct level_iterator), 0);
    input_file_ids = (uint64_t *)&args->payload[args->input_file_ids_offset];
    input_file_nums = (uint64_t *)&args->payload[args->input_file_nums_offset];
    output_file_ids = (uint64_t *)&args->payload[args->output_file_ids_offset];
    output_file_nums = (uint64_t*)&args->payload[args->output_file_nums_offset];
    levels = (uint64_t *)&args->payload[args->levels_offset];
    output_sizes = (uint64_t *)&args->payload[args->output_sizes_offset];
    output_smallests = (KeyType *)&args->payload[args->output_smallests_offset];
    output_largests = (KeyType *)&args->payload[args->output_largests_offset];
    output_segs_sizes = (uint64_t*)&args->payload[args->output_segs_sizes_offset];

    input_files = linear_malloc(args->nr_inputs * sizeof(struct cs_file *), 0);
    output_files = linear_malloc(args->nr_outputs * sizeof(struct cs_file *), 0);

    ASSERT(level_its != NULL);
    XTime s, e;
    for (int i = 0; i < args->nr_inputs; i++)
    {
        input_files[i] = cs_file_open(input_file_ids[i]);
    }
    
    if(args->nr_levels == 2)
    {
        struct cs_file* learned_index_file = cs_file_open(args->learned_index_file_id);
        for(int i = 0; i < args->nr_outputs; i++)
        {
            output_files[i] = cs_file_open(output_file_ids[i]);
        }
    	uint64_t* fpga_args = (uint64_t*)(CPU1_UNCACHED_MEMORY_BASE_ADDR);
    	struct segment* segs = (struct segment*)((UINTPTR)DDR4_BUFFER_BASE_ADDR + learned_index_file->disk_offset);

        struct fpga_compaction_args fpga_meta_args;
        fpga_meta_args.value_length = args->value_length;
        fpga_meta_args.output_table_size_thres = args->output_table_size_thres;
        fpga_meta_args.nr_inputs = args->nr_inputs;
        fpga_meta_args.nr_outputs = args->nr_outputs;
        fpga_meta_args.nr_levels = args->nr_levels;
        fpga_meta_args.levels_num[0] = levels[1] - levels[0];
        fpga_meta_args.levels_num[1] = levels[2] - levels[1];
        fpga_meta_args.input_files_size_offset = 0;
        fpga_meta_args.input_files_pos_offset = fpga_meta_args.input_files_size_offset + args->nr_inputs;
        fpga_meta_args.output_files_pos_offset = fpga_meta_args.input_files_pos_offset + args->nr_inputs;
        fpga_meta_args.output_sizes_offset = fpga_meta_args.output_files_pos_offset + args->nr_outputs;
        fpga_meta_args.output_smallests_offset = fpga_meta_args.output_sizes_offset + args->nr_outputs;
        fpga_meta_args.output_largests_offset = fpga_meta_args.output_smallests_offset + args->nr_outputs;
        fpga_meta_args.segs_offset = fpga_meta_args.output_largests_offset + args->nr_outputs;
        fpga_meta_args.gamma = args->gamma;
        fpga_meta_args.best_len = args->best_len;

        uint64_t* input_files_size_fpga = fpga_args + fpga_meta_args.input_files_size_offset;
        uint64_t* input_files_pos_fpga = fpga_args + fpga_meta_args.input_files_pos_offset;
        uint64_t* output_files_pos_fpga = fpga_args + fpga_meta_args.output_files_pos_offset;
        uint64_t* output_sizes_fpga = fpga_args + fpga_meta_args.output_sizes_offset;
        uint64_t* smallests_fpga = fpga_args + fpga_meta_args.output_smallests_offset;
        uint64_t* largests_fpga = fpga_args + fpga_meta_args.output_largests_offset;
        uint64_t* segs_size = fpga_args + fpga_meta_args.segs_offset;

        for(int i = 0; i < args->nr_inputs; i++)
        {
            input_files_size_fpga[i] = input_files[i]->size;
            input_files_pos_fpga[i] = input_files[i]->disk_offset / DATA_PACK;
            disk_offsets[input_file_nums[i]] = input_files[i]->disk_offset/ DATA_PACK;
        }
        for(int i = 0; i < args->nr_outputs; i++)
        {
            output_files_pos_fpga[i] = output_files[i]->disk_offset / DATA_PACK;
            disk_offsets[output_file_nums[i]] = output_files[i]->disk_offset / DATA_PACK;
            output_sizes_fpga[i] = 0;
            smallests_fpga[i] = 0;
            largests_fpga[i] = 0;
            segs_size[i] = 0;
        }
        XCompaction_Args xcompaction_args;
        memcpy(&xcompaction_args, &fpga_meta_args, sizeof(XCompaction_Args)); 

        // printf("set params\n");
        XTime_GetTime(&s);
        XCompaction_Set_args(&compaction_fpga, xcompaction_args);
        XCompaction_Set_args_ptr(&compaction_fpga, (UINTPTR)fpga_args);
        XCompaction_Set_leveli(&compaction_fpga, (UINTPTR)((uint8_t*)DDR4_BUFFER_BASE_ADDR));
        XCompaction_Set_leveli_s(&compaction_fpga, (UINTPTR)((uint8_t*)DDR4_BUFFER_BASE_ADDR));
        XCompaction_Set_leveli1(&compaction_fpga, (UINTPTR)((uint8_t*)DDR4_BUFFER_BASE_ADDR));
        XCompaction_Set_leveli1_s(&compaction_fpga, (UINTPTR)((uint8_t*)DDR4_BUFFER_BASE_ADDR));
        XCompaction_Set_out_r(&compaction_fpga, (UINTPTR)DDR4_BUFFER_BASE_ADDR);
        XCompaction_Set_segments(&compaction_fpga, (UINTPTR)segs);

        // printf("start fpga compaction\n");
        
        XCompaction_Start(&compaction_fpga);
        while(!XCompaction_IsDone(&compaction_fpga));
        XTime_GetTime(&e);
        u32 time = (e - s) * 1000000 / COUNTS_PER_SECOND;
        printf("==========input table num = %d, fpga compaction time = %d us=========\n", args->nr_inputs, time);

        int average_len = XCompaction_Get_return(&compaction_fpga);
        printf("average reprocessing length = %d\n", average_len);

        memcpy(output_sizes, output_sizes_fpga, sizeof(uint64_t) * args->nr_outputs);
        memcpy(output_smallests, smallests_fpga, sizeof(uint64_t) * args->nr_outputs);
        memcpy(output_largests, largests_fpga, sizeof(uint64_t) * args->nr_outputs);
        memcpy(output_segs_sizes, segs_size, sizeof(uint64_t) * args->nr_outputs);

//	     for(int i = 0; i < 10; i++)
//	     {
////             if(output_sizes[i] > 10 * 64 * 1024 * 1024)
//	     	   printf("file %d, output size = %ld, smallest = %ld, largest = %ld, segs_size = %ld\n", i, output_sizes[i], output_smallests[i], output_largests[i], output_segs_sizes[i]);
//	     }
//	     printf("\n");
//        for(int i = 0; i < 10; i++)
//        	printf("%ld, %lf, %lf\n", segs[i].x, segs[i].k, segs[i].b);
        // printf("nr_inputs = %ld, nr_outputs = %ld\n", fpga_meta_args.nr_inputs, fpga_meta_args.nr_outputs);
        
        int nr_emu_reqs = 0;
        struct host_io_req *req;
        struct emu_req_sqe *sqe;
        for(int i = 0; i < args->nr_inputs; i++)
        {
        	cs_file_close(input_files[i]);

            req = alloc_compaction_io_req();
	        req->is_read = true;
	        req->is_cs = false;
	        req->slba = input_files[i]->disk_offset / BYTES_PER_NVME_BLOCK;
	        req->nlb = input_files[i]->size / (32 * 1024);
	        
	        sqe = qpair_alloc_sqe(&m->compaction_file_req_qp);
	        sqe->host_io_req = req;
	        qpair_submit_sqe(&m->compaction_file_req_qp, sqe);

            nr_emu_reqs++;
        }
        
        for(int i = 0; i < args->nr_outputs; i++)
        {
            cs_file_close(output_files[i]);
            
            req = alloc_compaction_io_req();
	        req->is_read = false;
	        req->is_cs = false;
	        req->slba = output_files[i]->disk_offset / BYTES_PER_NVME_BLOCK;
	        req->nlb = output_files[i]->size / (32 * 1024);
	        
	        sqe = qpair_alloc_sqe(&m->compaction_file_req_qp);
	        sqe->host_io_req = req;
	        qpair_submit_sqe(&m->compaction_file_req_qp, sqe);

            nr_emu_reqs++;
        }
        
        while(nr_emu_reqs > 0)
        {
            process_emu_req_cq(&nr_emu_reqs, &m->compaction_file_req_qp);
        }

        cs_file_close(learned_index_file);
        sync_closing_files();
        return;
    }

    // int max_nr_inputs = 0;
    // for (int i = 0; i < args->nr_levels; i++) {
    //     int nr_inputs = i < args->nr_levels - 1 ?
    //                     levels[i + 1] - levels[i] :
    //                     args->nr_inputs - levels[i];

    //     if (nr_inputs > max_nr_inputs)
    //         max_nr_inputs = nr_inputs;
    //     level_iterator_init(&input_files[levels[i]], nr_inputs, i, &level_its[i]);
    // }
    // XTime start, end;
    // XTime_GetTime(&start);
    // for (int i = 0; i < max_nr_inputs; i++)
    //     for (int l = 0; l < args->nr_levels; l++)
    //         level_iterator_prefetch_file(i, &level_its[l]);
    // for (int i = 0; i < args->nr_levels; i++)
    //     level_iterator_prepare(&level_its[i]);
    // do_compaction(level_its, args->nr_levels,
    //               output_file_ids, args->nr_outputs, output_sizes,
    //               output_smallests, output_largests);
    // XTime_GetTime(&end);
    // u32 time = (end - start) * 1000000 / COUNTS_PER_SECOND;
	// printf("==========compaction time = %d us=========\n", time);

    // for (int i = 0; i < args->nr_levels; i++)
    //     level_iterator_destroy(&level_its[i]);

    // CYCLE_COUNT_BEGIN();
    // sync_closing_files();
    // CYCLE_COUNT_END(cycle_counts.sync_close_cycles);

    // linear_malloc_reset();

//    CYCLE_COUNT_END(cycle_counts.compaction_cycles);

    // printf("compaction time = %ldus\n", cycle_counts.compaction_cycles * 1000000 / XPAR_PSU_CORTEXA53_1_CPU_CLK_FREQ_HZ);
    // printf("read time = %ldus\n", cycle_counts.read_cycles * 1000000 / XPAR_PSU_CORTEXA53_1_CPU_CLK_FREQ_HZ);
    // printf("sync read time = %ldus\n", cycle_counts.sync_read_cycles * 1000000 / XPAR_PSU_CORTEXA53_1_CPU_CLK_FREQ_HZ);
    // printf("write time = %ldus\n", cycle_counts.write_cycles * 1000000 / XPAR_PSU_CORTEXA53_1_CPU_CLK_FREQ_HZ);
    // printf("sync write time = %ldus\n", cycle_counts.sync_write_cycles * 1000000 / XPAR_PSU_CORTEXA53_1_CPU_CLK_FREQ_HZ);
    // printf("compare keys time = %ldus\n", cycle_counts.compare_cycles * 1000000 / XPAR_PSU_CORTEXA53_1_CPU_CLK_FREQ_HZ);
    // printf("memcpy time = %ldus\n", cycle_counts.memcpy_cycles * 1000000 / XPAR_PSU_CORTEXA53_1_CPU_CLK_FREQ_HZ);
    // printf("memcpy size = %ldB\n", cycle_counts.memcpy_size);
    // printf("read size = %ldB\n", cycle_counts.read_size);
    // printf("write size = %ldB\n", cycle_counts.write_size);
}
