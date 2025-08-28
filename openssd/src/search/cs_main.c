#include "shared_mem.h"
#include "memory_map.h"
#include "search.h"
// #include "cs_file.h"
#include "utils_common.h"
#include "debug.h"

void cs_main()
{
    volatile struct shared_mem *m = (struct shared_mem *)SHARED_MEM_BASE_ADDR;

    asm volatile("msr PMCR_EL0, %0" : : "r" ((1 << 0) | (1 << 2)));
    asm volatile("msr PMCNTENSET_EL0, %0" : : "r" (1 << 31));

    int status;
    search_engine_cfg = XTop_search_LookupConfig(XPAR_TOP_SEARCH_0_DEVICE_ID);
    status = XTop_search_CfgInitialize(&search_engine, search_engine_cfg);
    if(status != XST_SUCCESS)
    {
    	printf("FPGA init failed\n");
    }
    ASSERT(status == XST_SUCCESS);
    init_search_io_reqs();
    // for(int i = 0; i < 4096; i++)
    // {
    //     m->disk_offsets[i] = 0;
    // }
    linear_malloc_set_base();
    while (1) {
        if (m->cs_status[1] != CS_STATUS_RUNNING)
            continue;

        linear_malloc_reset();

        int batch_id = m->batch_id;
        int nr_search = m->nr_search[batch_id];
//        printf("%d, %d\n", batch_id, nr_search);
        int nr_result = search(nr_search, m->batch_size, batch_id, m->args_file_disk_offset, m->result_file_disk_offset, m->file_disk_offsets); 
        m->nr_result[batch_id] = nr_result;

        MEMORY_BARRIER();

        m->cs_status[1] = CS_STATUS_DONE;
    }
}
