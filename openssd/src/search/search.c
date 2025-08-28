#include "search.h"
#include "utils_common.h"
// search_engine_0
XTop_search search_engine;
XTop_search_Config* search_engine_cfg;
int iterate_num = 100000;
int last_batch_size = 0;

static volatile struct shared_mem *m = (struct shared_mem *)SHARED_MEM_BASE_ADDR;
static struct host_io_req *search_io_reqs;
static QTAILQ_HEAD(free_search_io_reqs, host_io_req) free_search_io_reqs;
static int nr_free_search_io_reqs;

void init_search_io_reqs()
{
	search_io_reqs = linear_malloc(SEARCH_FILE_REQ_QP_ENTRIES * sizeof(struct host_io_req), 0);
    nr_free_search_io_reqs = SEARCH_FILE_REQ_QP_ENTRIES;

	QTAILQ_INIT(&free_search_io_reqs);
	for (int i = 0; i < SEARCH_FILE_REQ_QP_ENTRIES; i++)
		QTAILQ_INSERT_TAIL(&free_search_io_reqs, &search_io_reqs[i], qent);
}

struct host_io_req *alloc_search_io_req()
{
	struct host_io_req *req;
	ASSERT(!QTAILQ_EMPTY(&free_search_io_reqs));

	req = QTAILQ_FIRST(&free_search_io_reqs);
	QTAILQ_REMOVE(&free_search_io_reqs, req, qent);

	nr_free_search_io_reqs--;

	return req;
}

void free_search_io_req(struct host_io_req *req)
{
	QTAILQ_INSERT_HEAD(&free_search_io_reqs, req, qent);
	nr_free_search_io_reqs++;
}

void process_emu_req_cq(int* nr_req, volatile struct qpair* qp)
{
	struct emu_req_cqe *cqe;

	while ((cqe = qpair_peek_cqe(qp)) != NULL) {
        struct host_io_req *req = cqe->host_io_req;
        free_search_io_req(req);
        qpair_consume_cqe(qp, cqe);
        *nr_req = *nr_req - 1;
	}
}

int search(int nr_search, int batch_size, int batch_id, uint64_t args_file_disk_offset, uint64_t result_file_disk_offset, uint64_t* disk_offsets)
{
    fpga_search_args fpga_meta_args;
    fpga_meta_args.nr_search = nr_search;
    fpga_meta_args.args_file_offset = batch_id > 0 ? args_file_disk_offset + 32*1024*1024/64 : args_file_disk_offset;
    fpga_meta_args.result_file_offset = batch_id > 0 ? result_file_disk_offset + 16*1024*1024/64 : result_file_disk_offset;
    // fpga_meta_args.debug_offset = fpga_meta_args.result_file_offset + 16*1024*1024/64;
    fpga_meta_args.search_files_pos_offset = 0;
    fpga_meta_args.target_keys_offset = nr_search;
    fpga_meta_args.kv_pos_offset = 2 * nr_search;

    
    uint8_t* disk_ptr = (uint8_t*)DDR4_BUFFER_BASE_ADDR;
    uint64_t* input_files_num = (uint64_t*)(disk_ptr + args_file_disk_offset * 64);
    uint64_t* target_keys = (uint64_t*)(disk_ptr + args_file_disk_offset * 64 + nr_search * sizeof(uint64_t));
    uint64_t* kv_pos = (uint64_t*)(disk_ptr + args_file_disk_offset * 64 + 2 * nr_search * sizeof(uint64_t));

    // for(int i = 0; i < nr_search; i++)
    //     printf("%ld, %ld, %ld, %ld\n", input_files_num[i], target_keys[i], kv_pos[i] / 80, disk_offsets[input_files_num[i]]);
    
    XTime s,e;
    // XTime_GetTime(&s);
    // uint64_t* fpga_args = (uint64_t*)(CPU2_UNCACHED_MEMORY_BASE_ADDR);
    // uint64_t* input_files_pos_fpga = fpga_args + fpga_meta_args.search_files_pos_offset;
    // uint64_t* target_keys_fpga = fpga_args + fpga_meta_args.target_keys_offset;
    // uint64_t* kv_pos_fpga = fpga_args + fpga_meta_args.kv_pos_offset;
    // for(int i = 0; i < fpga_meta_args.nr_search; i++)
    // {
    //     // input_files_pos_fpga[i] = disk_offsets[input_files_num[i]];
    //     // input_files_num[i] = disk_offsets[input_files_num[i]];
    //     // target_keys_fpga[i] = target_keys[i];
    //     // kv_pos_fpga[i] = kv_pos[i];
    // }
    // XTime_GetTime(&e);
    // u32 time = (e - s) * 1000000 / COUNTS_PER_SECOND;
    // printf("==========fpga prepare time = %d us=========\n", time);

    
    
    XTop_search_Args input_args;
    memcpy(&input_args, &fpga_meta_args, sizeof(XTop_search_Args));
    XTop_search_Set_args(&search_engine, input_args);
	XTop_search_Set_input_r(&search_engine, (UINTPTR)(uint8_t*)DDR4_BUFFER_BASE_ADDR);
	XTop_search_Set_output_r(&search_engine, (UINTPTR)(uint8_t*)DDR4_BUFFER_BASE_ADDR);

    XTop_search_Start(&search_engine);
	while(!XTop_search_IsDone(&search_engine));

    u32 nr_result = 0;
    if(nr_search > 0)
        nr_result = XTop_search_Get_return(&search_engine);

//    XTime_GetTime(&e);
//    u32 time = (e - s) * 1000000 / COUNTS_PER_SECOND;
//    if(last_batch_size != batch_size)
//    {
//    	last_batch_size = batch_size;
//    	iterate_num = 20000000 / batch_size / 5;
//    }
//    if(iterate_num == 0)
//    {
//    	printf("==========batch size = %d, fpga search engine time = %d us=========\n", batch_size, time);
//    	iterate_num = 100000;
//    }
    int nr_emu_reqs = 0;
    struct host_io_req *req;
    struct emu_req_sqe *sqe;
    
    // for(int i = 0; i < nr_search; i++)
    // {
    //     req = alloc_search_io_req();
	//     req->is_read = true;
	//     req->is_cs = false;
	//     req->slba = args_file_disk_offset * 64 / BYTES_PER_NVME_BLOCK;
	//     req->nlb = 1;
	        
	//     sqe = qpair_alloc_sqe(&m->search_file_req_qp);
	//     sqe->host_io_req = req;
	//     qpair_submit_sqe(&m->search_file_req_qp, sqe);

    //     nr_emu_reqs++;
    // }
    
    // req = alloc_search_io_req();
	// req->is_read = false;
	// req->is_cs = false;
	// req->slba = result_file_disk_offset * 64 / BYTES_PER_NVME_BLOCK;
	// req->nlb = 1;
	        
	// sqe = qpair_alloc_sqe(&m->search_file_req_qp);
	// sqe->host_io_req = req;
	// qpair_submit_sqe(&m->search_file_req_qp, sqe);

    // nr_emu_reqs++;

    // while(nr_emu_reqs > 0)
    // {
    //     process_emu_req_cq(&nr_emu_reqs, &m->search_file_req_qp);
    // }
    return nr_result;
}
