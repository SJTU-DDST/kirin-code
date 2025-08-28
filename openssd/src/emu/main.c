#include <assert.h>
#include "xil_cache.h"
#include "xil_exception.h"
#include "xil_mmu.h"
#include "xscugic_hw.h"
#include "xscugic.h"
#include "memory_map.h"
#include "shared_mem.h"
#include "utils_common.h"

XScuGic GicInstance;

static void __attribute__((optimize("O0"))) signal_cpu3_up()
{
    MEMORY_BARRIER();
    ((volatile struct shared_mem *)SHARED_MEM_BASE_ADDR)->cpu3_magic = CPU3_MAGIC_NUM;
}

static void check_elf_size()
{
    extern char _end;

    assert((uintptr_t)&_end <= CPU3_MEMORY_SEGMENTS_END_ADDR);
}

extern void emu_main();

int main()
{
	setup_page_table();

	check_elf_size();

	linear_malloc_init(CPU3_LINEAR_MALLOC_BASE_ADDR, CPU3_LINEAR_MALLOC_END_ADDR);

    wait_cpu0_up();

    signal_cpu3_up();

    emu_main();

    return 0;
}
