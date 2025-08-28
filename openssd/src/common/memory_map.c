#include "xil_mmu.h"
#include "xil_cache.h"
#include "memory_map.h"

void setup_page_table()
{
	UINTPTR u;

	Xil_ICacheDisable();
	Xil_DCacheDisable();

	// Paging table set
	#define MB (1024*1024)
	for (u = 0; u < 4096; u+=2)
	{
		if (u < 0x2)
			Xil_SetTlbAttributes(u * MB, NORM_WB_CACHE);
		else if (u < 0x180)
			Xil_SetTlbAttributes(u * MB, NORM_NONCACHE);
		else if (u < 0x400)
			Xil_SetTlbAttributes(u * MB, NORM_WB_CACHE);
		else if (u < CPU0_CACHED_MEMORY_END_ADDR / MB)
			Xil_SetTlbAttributes(u * MB, NORM_WB_CACHE);
		else if (u < CPU0_UNCACHED_MEMORY_END_ADDR / MB)
			Xil_SetTlbAttributes(u * MB, NORM_NONCACHE);
		else if (u < CPU1_CACHED_MEMORY_END_ADDR / MB)
			Xil_SetTlbAttributes(u * MB, NORM_WB_CACHE);
		else if (u < CPU1_UNCACHED_MEMORY_END_ADDR / MB)
			Xil_SetTlbAttributes(u * MB, NORM_NONCACHE);
		else if (u < CPU2_CACHED_MEMORY_END_ADDR / MB)
			Xil_SetTlbAttributes(u * MB, NORM_WB_CACHE);
		else if (u < CPU2_UNCACHED_MEMORY_END_ADDR / MB)
			Xil_SetTlbAttributes(u * MB, NORM_NONCACHE);
		else if (u < CPU3_CACHED_MEMORY_END_ADDR / MB)
			Xil_SetTlbAttributes(u * MB, NORM_WB_CACHE);
		else if (u < CPU3_UNCACHED_MEMORY_END_ADDR / MB)
			Xil_SetTlbAttributes(u * MB, NORM_NONCACHE);
		else if (u < DMA_END_ADDR / MB)
			Xil_SetTlbAttributes(u * MB, NORM_NONCACHE);
		else
			Xil_SetTlbAttributes(u * MB, STRONG_ORDERED);
	}

	// #define GB (1024ULL * 1024 * 1024)
	// for (u = 0; u < 64; u++)
	// 	Xil_SetTlbAttributes(DDR4_BUFFER_BASE_ADDR + u * GB, NORM_NONCACHE);

	Xil_ICacheEnable();
	Xil_DCacheEnable();
}
