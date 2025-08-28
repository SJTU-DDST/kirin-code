#include "shared_mem.h"
#include "memory_map.h"
#include "compaction.h"
#include "cs_file.h"
#include "utils_common.h"
#include "debug.h"

void cs_main()
{
    volatile struct shared_mem *m = (struct shared_mem *)SHARED_MEM_BASE_ADDR;

    asm volatile("msr PMCR_EL0, %0" : : "r" ((1 << 0) | (1 << 2)));
    asm volatile("msr PMCNTENSET_EL0, %0" : : "r" (1 << 31));

    int status;
    compaction_cfg = XCompaction_LookupConfig(XPAR_COMPACTION_0_DEVICE_ID);
    status = XCompaction_CfgInitialize(&compaction_fpga, compaction_cfg);
    if(status != XST_SUCCESS)
    {
    	printf("FPGA init failed\n");
    }
    ASSERT(status == XST_SUCCESS);

    double* bram1 = (double*)XPAR_AXI_BRAM_CTRL_0_S_AXI_BASEADDR;
    double* bram2 = (double*)XPAR_AXI_BRAM_CTRL_1_S_AXI_BASEADDR;
    double* bram3 = (double*)XPAR_AXI_BRAM_CTRL_2_S_AXI_BASEADDR;
    double temp;
    bram1[0] = bram2[0] = bram3[0] = 0;
    for(int i = 1; i < 7000; i++)
    {
    	temp = 1.0 / i;
    	bram1[i] = bram2[i] = bram3[i] = temp;
    }
    init_compaction_io_reqs();
    // for(int i = 0; i < 4096; i++)
    // {
    //     m->file_disk_offsets[i] = 0;
    // }
    linear_malloc_set_base();
    while (1) {
        if (m->cs_status[0] != CS_STATUS_RUNNING)
            continue;

        linear_malloc_reset();

        compaction((struct compaction_args *)compaction_args_buf, m->file_disk_offsets);

        MEMORY_BARRIER();

        m->cs_status[0] = CS_STATUS_DONE;
    }
}
