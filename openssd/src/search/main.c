#include <assert.h>
#include "xil_cache.h"
#include "xil_exception.h"
#include "xil_mmu.h"
#include "xscugic_hw.h"
#include "xscugic.h"
#include "memory_map.h"
#include "shared_mem.h"
#include "cs_file.h"
#include "utils_common.h"

XScuGic GicInstance;

static void check_elf_size()
{
    extern char _end;

    assert((uintptr_t)&_end <= CPU2_MEMORY_SEGMENTS_END_ADDR);
}

static void __attribute__((optimize("O0"))) signal_cpu2_up()
{
    MEMORY_BARRIER();
    ((volatile struct shared_mem *)SHARED_MEM_BASE_ADDR)->cpu2_magic = CPU2_MAGIC_NUM;
}

extern void cs_main();

int main()
{
	setup_page_table();

	check_elf_size();

	linear_malloc_init(CPU2_LINEAR_MALLOC_BASE_ADDR, CPU2_LINEAR_MALLOC_END_ADDR);

    init_cs_files();

    wait_cpu0_up();

    signal_cpu2_up();

    cs_main();

    return 0;
}
