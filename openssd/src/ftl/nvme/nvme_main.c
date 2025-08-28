//////////////////////////////////////////////////////////////////////////////////
// nvme_main.c for Cosmos+ OpenSSD
// Copyright (c) 2016 Hanyang University ENC Lab.
// Contributed by Yong Ho Song <yhsong@enc.hanyang.ac.kr>
//				  Youngjin Jo <yjjo@enc.hanyang.ac.kr>
//				  Sangjin Lee <sjlee@enc.hanyang.ac.kr>
//				  Jaewook Kwak <jwkwak@enc.hanyang.ac.kr>
//				  Kibin Park <kbpark@enc.hanyang.ac.kr>
//
// This file is part of Cosmos+ OpenSSD.
//
// Cosmos+ OpenSSD is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 3, or (at your option)
// any later version.
//
// Cosmos+ OpenSSD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Cosmos+ OpenSSD; see the file COPYING.
// If not, see <http://www.gnu.org/licenses/>.
//////////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Company: ENC Lab. <http://enc.hanyang.ac.kr>
// Engineer: Sangjin Lee <sjlee@enc.hanyang.ac.kr>
//			 Jaewook Kwak <jwkwak@enc.hanyang.ac.kr>
//			 Kibin Park <kbpark@enc.hanyang.ac.kr>
//
// Project Name: Cosmos+ OpenSSD
// Design Name: Cosmos+ Firmware
// Module Name: NVMe Main
// File Name: nvme_main.c
//
// Version: v1.2.0
//
// Description:
//   - initializes FTL and NAND
//   - handles NVMe controller
//////////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Revision History:
//
// * v1.2.0
//   - header file for buffer is changed from "ia_lru_buffer.h" to "lru_buffer.h"
//   - Low level scheduler execution is allowed when there is no i/o command
//
// * v1.1.0
//   - DMA status initialization is added
//
// * v1.0.0
//   - First draft
//////////////////////////////////////////////////////////////////////////////////

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <arm_neon.h>

#include "xil_printf.h"
#include "debug.h"
#include "io_access.h"

#include "nvme.h"
#include "host_lld.h"
#include "nvme_main.h"
#include "nvme_admin_cmd.h"
#include "nvme_io_cmd.h"

#include "../memory_map.h"
#include "../cdma.h"
#include "../utils_common.h"
#include "../cs_args.h"
#include "../cs_io.h"
#include "../ext4_cs.h"

#include "xtop_search.h"

volatile NVME_CONTEXT g_nvmeTask;

#define DDR4_INIT_BUF_SIZE (32 * 1024 * 1024)
static void init_ddr4()
{
	uint8_t *buf = linear_malloc(DDR4_INIT_BUF_SIZE, 0);
	uint64_t start, end, total, error_count;
	uint64_t success;

	assert(buf != NULL);

	// int test_sizes[] = { 4, 8, 16, 32, 64, 128, 256, 512 };
	// for (int i = 0; i < sizeof(test_sizes) / sizeof(int); i++) {
	// 	printf("testing transfer size %d\n", test_sizes[i]);
	// 	cdma_transfer((void *)DDR4_BUFFER_BASE_ADDR + test_sizes[i], buf + test_sizes[i], test_sizes[i] - 1, 0, 0, 1, 1);
	// }
	// while (1);

	for (int i = 0; i < DDR4_INIT_BUF_SIZE; i++)
		buf[i] = 0xff;
	FLUSH_CACHE(buf, DDR4_INIT_BUF_SIZE);

	total = 0;
	for (size_t offset = 0; offset < NVME_STORAGE; offset += DDR4_INIT_BUF_SIZE) {
		start = get_time_ns();
		success = cdma_transfer((void *)(DDR4_BUFFER_BASE_ADDR + offset),
		                        buf, DDR4_INIT_BUF_SIZE, 0, 0, 1, 1);
		assert(success);
		end = get_time_ns();
		total += end - start;

		if (offset % ONE_GB == 0) {
			printf("DDR4 init: %d/%d, last transfer: %luns\n", (int)(offset / ONE_GB) + 1,
			       (int)(NVME_STORAGE / ONE_GB), end - start);
		}
	}

	printf("Filled %dGB in %.2lfs\n", (int)(NVME_STORAGE / ONE_GB),
		   total / 1000000000.0);
	printf("Measured bandwidth: %.2lfGB/s\n",
	       (NVME_STORAGE / ONE_GB) / (total / 1000000000.0));

	start = get_time_ns();
	success = cdma_transfer(buf, (void *)(DDR4_BUFFER_BASE_ADDR), 128 * 1024, 0, 0, 1, 1);
	end = get_time_ns();
	printf("CDMA 128KB transfer time = %luns\n", end - start);

	printf("Checking first 1GB of data\n");

	total = 0;
	error_count = 0;
	for (size_t offset = 0; offset < ONE_GB; offset += DDR4_INIT_BUF_SIZE) {
		memset(buf, 0, DDR4_INIT_BUF_SIZE);
		FLUSH_CACHE(buf, DDR4_INIT_BUF_SIZE);

		start = get_time_ns();
		success = cdma_transfer(buf, (void *)(DDR4_BUFFER_BASE_ADDR + offset),
		                        DDR4_INIT_BUF_SIZE, 0, 0, 1, 1);
		assert(success);
		end = get_time_ns();
		total += end - start;

		for (int i = 0; i < DDR4_INIT_BUF_SIZE / sizeof(uint64_t); i++)
			if (((uint64_t *)buf)[i] != 0xffffffffffffffff)
				error_count++;

		printf("%luMB checked, %lu errors found, transfer time %luns\n",
		       (offset + DDR4_INIT_BUF_SIZE) >> 20, error_count, end - start);
	}

	printf("Read 1GB in %.2fs\n", total / 1000000000.0);
	printf("Measured bandwidth: %.2lfGB/s\n", 1.0 / total * 1000000000.0);

	linear_malloc_reset();
}

static void init_shared_mem()
{
    struct shared_mem *m = (struct shared_mem *)SHARED_MEM_BASE_ADDR;

	assert(sizeof(struct shared_mem) <= SHARED_MEM_END_ADDR - SHARED_MEM_BASE_ADDR);

	for(int i = 0; i < 3; i++)
    	m->cs_status[i] = CS_STATUS_IDLE;
    m->fs_ready = 0;
	m->file_disk_offsets = (uint64_t*)XPAR_AXI_BRAM_CTRL_3_S_AXI_BASEADDR;

	qpair_init(&m->file_req_qp, FILE_REQ_QP_ENTRIES,
			   sizeof(struct file_req_sqe), sizeof(struct file_req_cqe));

	qpair_init(&m->compaction_file_req_qp, COMPACTION_FILE_REQ_QP_ENTRIES,
				sizeof(struct emu_req_sqe), sizeof(struct emu_req_cqe));

	qpair_init(&m->search_file_req_qp, SEARCH_FILE_REQ_QP_ENTRIES,
			   sizeof(struct emu_req_sqe), sizeof(struct emu_req_cqe));
	
	qpair_init(&m->emu_req_qp, EMU_REQ_QP_ENTRIES,
	 		   sizeof(struct emu_req_sqe), sizeof(struct emu_req_cqe));

	MEMORY_BARRIER();

	m->cpu0_magic = CPU0_MAGIC_NUM;
}

static void wait_cpu_up()
{
	volatile struct shared_mem *m = (struct shared_mem *)SHARED_MEM_BASE_ADDR;

	while (m->cpu1_magic != CPU1_MAGIC_NUM);
	printf("CPU1 is up!!!!!!\r\n");

	 while (m->cpu2_magic != CPU2_MAGIC_NUM);
	 printf("CPU2 is up!!!!!!\r\n");

	while (m->cpu3_magic != CPU3_MAGIC_NUM);
	printf("CPU3 is up!!!!!!\r\n");

	MEMORY_BARRIER();

	m->cpu0_magic = 0x00000000;
	m->cpu1_magic = 0x00000000;
	m->cpu2_magic = 0x00000000;
	m->cpu3_magic = 0x00000000;
}

void do_low_level_tasks()
{
	/* anything here must be reentrant safe */
	execute_queued_cs_args_reqs();
	check_done_cs_args_reqs();
	schedule_cs_io_reqs();
}

struct fpga_search_args
{
	int nr_search;
	uint64_t result_file_offset;
	uint64_t debug_file_offset;
	/*in*/
	int search_files_num_offset;  // file_pos = physical_address / DATA_WIDTH
	int target_keys_offset;
	int kv_pos_offset;            // kv_pos = logical_address * KV_LENGTH
};

void test_search_engine()
{
	int status;
	XTop_search search0, search1;
	XTop_search_Config* search_cfg0 = XTop_search_LookupConfig(XPAR_TOP_SEARCH_0_DEVICE_ID);
	status = XTop_search_CfgInitialize(&search0, search_cfg0);
	if(status != XST_SUCCESS)
	{
	    printf("FPGA init failed\n");
	}
	ASSERT(status == XST_SUCCESS);
//	XTop_search_Config* search_cfg1 = XTop_search_LookupConfig(XPAR_TOP_SEARCH_1_DEVICE_ID);
//	status = XTop_search_CfgInitialize(&search1, search_cfg1);
	if(status != XST_SUCCESS)
	{
		printf("FPGA init failed\n");
	}
	ASSERT(status == XST_SUCCESS);

	int nr_search = 64*2;
	int search_files_num_offset = 0;
	int target_keys_offset = 1024;
	int kv_pos_offset = 2048;
	struct fpga_search_args search_args;
	XTop_search_Args input_args;
	search_args.nr_search = nr_search;
	search_args.result_file_offset = 1024*1024*1024 / 64;
	search_args.debug_file_offset = 1024*1024 / 64;
	search_args.search_files_num_offset = search_files_num_offset / sizeof(uint64_t); // (CPU0_UNCACHED_MEMORY_BASE_ADDR + search_files_pos_offset) / sizeof(uint64_t);
	search_args.target_keys_offset = target_keys_offset / sizeof(uint64_t); //(CPU0_UNCACHED_MEMORY_BASE_ADDR + target_keys_offset) / sizeof(uint64_t);
	search_args.kv_pos_offset = kv_pos_offset / sizeof(uint64_t); //(CPU0_UNCACHED_MEMORY_BASE_ADDR + kv_pos_offset) / sizeof(uint64_t);
	uint8_t* args_ptr = (uint8_t*)(CPU0_UNCACHED_MEMORY_BASE_ADDR);
	uint8_t* result_ptr = (uint8_t*)(DDR4_BUFFER_BASE_ADDR + search_args.result_file_offset * 64);
	uint64_t* debug_ptr = (uint64_t*)(DDR4_BUFFER_BASE_ADDR + search_args.debug_file_offset * 64);
	uint8_t* disk_ptr = (uint8_t*)(DDR4_BUFFER_BASE_ADDR);
	uint64_t* disk_offsets = (uint64_t*)XPAR_AXI_BRAM_CTRL_3_S_AXI_BASEADDR;
	uint64_t search_files_pos = 8192, target_keys = 1, kv_pos = 10;
	XTime s, e;
	XTime_GetTime(&s);
	uint64_t table_id = 0;
	for(int i = 0; i < search_args.nr_search; i++)
	{
		uint64_t search_files_pos_p = search_files_pos / 64;
		memcpy(args_ptr + search_files_num_offset, &table_id, sizeof(uint64_t));
		disk_offsets[i] = search_files_pos_p;
		table_id++;
//		printf("%ld ", disk_offsets[i]);
		search_files_num_offset += sizeof(uint64_t);
		memcpy(disk_ptr + search_files_pos + (kv_pos + 10) * 80, &target_keys, sizeof(uint64_t));
		memset(disk_ptr + search_files_pos + (kv_pos + 10) * 80 + 16, 'd', 64);
		memset(disk_ptr + 1024 * 1024 * 1024 + i * 64, 0, 64);
		search_files_pos += 1024*1024;
		memcpy(args_ptr + target_keys_offset, &target_keys, sizeof(uint64_t));
		target_keys_offset += sizeof(uint64_t);
		target_keys += 100000;
		uint64_t kv_phy_pos = kv_pos * 80;
		memcpy(args_ptr + kv_pos_offset, &kv_phy_pos, sizeof(uint64_t));
		kv_pos_offset += sizeof(uint64_t);
		kv_pos += 13;
	}
	XTime_GetTime(&e);
	u32 time1 = (e - s) * 1000000 / COUNTS_PER_SECOND;
	printf("\n==========fpga prepare time = %d us=========\n", time1);


	printf("%d, %d\n", sizeof(struct fpga_search_args), sizeof(XTop_search_Args));
	// int search_files_pos_offset1 = 0;
	// int target_keys_offset1 = 1024;
	// int kv_pos_offset1 = 2048;
	// struct fpga_search_args search_args1;
	// XTop_search_Args input_args1;
	// search_args1.nr_search = nr_search;
	// search_args1.result_file_offset = (100 * 1024 * 1024) / 64;
	// search_args1.search_files_pos_offset = search_files_pos_offset1 / sizeof(uint64_t);
	// search_args1.target_keys_offset = target_keys_offset1 / sizeof(uint64_t);
	// search_args1.target_keys_offset = kv_pos_offset1 / sizeof(uint64_t);
	// uint8_t* args_ptr1 = (uint8_t*)(CPU0_UNCACHED_MEMORY_BASE_ADDR + 4096);
	// uint8_t* disk_ptr1 = (uint8_t*)(DDR4_BUFFER_BASE_ADDR + 75*1024*1024);
	// uint64_t search_files_pos1 = 8192, target_keys1 = 1, kv_pos1 = 10;
	// for(int i = 0; i < search_args1.nr_search; i++)
	// {
	// 	uint64_t search_files_pos_p = search_files_pos1 / 64;
	// 	memcpy(args_ptr1 + search_files_pos_offset1, &search_files_pos_p, sizeof(uint64_t));
	// 	search_files_pos_offset1 += sizeof(uint64_t);
	// 	memcpy(disk_ptr1 + search_files_pos1 + (kv_pos1 + 10) * 80, &target_keys1, sizeof(uint64_t));
	// 	search_files_pos1 += 1024*100;
	// 	memcpy(args_ptr1 + target_keys_offset1, &target_keys1, sizeof(uint64_t));
	// 	target_keys_offset1 += sizeof(uint64_t);
	// 	target_keys1 += 100000;
	// 	uint64_t kv_phy_pos = kv_pos1 * 80;
	// 	memcpy(args_ptr1 + kv_pos_offset1, &kv_phy_pos, sizeof(uint64_t));
	// 	kv_pos_offset1 += sizeof(uint64_t);
	// 	kv_pos1 += 13;
	// }

	memcpy(&input_args, &search_args, sizeof(XTop_search_Args));
	XTop_search_Set_args(&search0, input_args);
	XTop_search_Set_input_r(&search0, (UINTPTR)(uint8_t*)DDR4_BUFFER_BASE_ADDR);
	XTop_search_Set_output_r(&search0, (UINTPTR)(uint8_t*)DDR4_BUFFER_BASE_ADDR);

	// memcpy(&input_args1, &search_args1, sizeof(XTop_search_Args));
	// XTop_search_Set_args(&search1, input_args1);
	// XTop_search_Set_args_ptr(&search1, (UINTPTR)(uint64_t*)args_ptr1);
	// XTop_search_Set_input_r(&search1, (UINTPTR)(uint8_t*)DDR4_BUFFER_BASE_ADDR);
	// XTop_search_Set_output_r(&search1, (UINTPTR)(uint8_t*)DDR4_BUFFER_BASE_ADDR);
	for(int i = 0; i < nr_search; i++)
		printf("%ld ", ((uint64_t*)args_ptr)[i]);
	printf("\n");
	for(int i = 0; i < nr_search; i++)
		printf("%ld ", disk_offsets[((uint64_t*)args_ptr)[i]]);
	printf("\n");
	XTime_GetTime(&s);
	XTop_search_Start(&search0);
	// XTop_search_Start(&search1);
	while(!XTop_search_IsDone(&search0));
	// while(!XTop_search_IsDone(&search1));
	XTime_GetTime(&e);
	u32 time = (e - s) * 1000000 / COUNTS_PER_SECOND;
	printf("==========fpga search time = %d us=========\n", time);
	// printf("%d, %d, %d\n", 'a', disk_ptr[search_files_pos + (10 + 10) * 80 + 16], disk_ptr[1024 * 1024 * 1024 + 1]);
	for(int i = 0; i < 64*nr_search; i++)
	{
		if(result_ptr[i] != 'd')
			printf("%d ", result_ptr[i]);
	}
//	for(int i = 0; i < 4; i++)
//	{
//		printf("file id = %ld, file pos = %ld, target key = %ld, kv_pos = %ld\n", debug_ptr[i], debug_ptr[i + nr_search], debug_ptr[i + 2 * nr_search], debug_ptr[i + 3 * nr_search]);
//	}
	// printf("\n");
	// XTop_search_Args output_args = XTop_search_Get_args(&search0);
	// printf("%d, %d, %d, %d, %d, %d, %d, %d", input_args.word_0, input_args.word_1, input_args.word_2, input_args.word_3, input_args.word_4, input_args.word_5, input_args.word_6, input_args.word_7);
	// printf("%d, %d, %d, %d, %d, %d, %d, %d", output_args.word_0, output_args.word_1, output_args.word_2, output_args.word_3, output_args.word_4, output_args.word_5, output_args.word_6, output_args.word_7);
}

void nvme_main()
{
	unsigned int rstCnt = 0;

	init_ddr4();

	// test_search_engine();

	/* these must be here because functions above reset linear_malloc */
	init_shared_mem();
	wait_cpu_up();
	init_host_io_reqs();
	init_cs_args();
	init_cs_io_reqs();
	init_cs_files();

	xil_printf("[ storage capacity %d MB ]\r\n", STORAGE_CAPACITY_L / ((1024*1024) / BYTES_PER_NVME_BLOCK));

	xil_printf("Turn on the host PC \r\n");

	while(1)
	{
		do_low_level_tasks();
		process_sq();
		check_host_io_dma_done();
		process_emu_req_cq();

		if(g_nvmeTask.status == NVME_TASK_WAIT_CC_EN)
		{
			unsigned int ccEn;
			ccEn = check_nvme_cc_en();
			if(ccEn == 1)
			{
				set_nvme_admin_queue(1, 1, 1);
				set_nvme_csts_rdy(1);
				g_nvmeTask.status = NVME_TASK_RUNNING;
				xil_printf("\r\nNVMe ready!!!\r\n");
			}
		}
		else if(g_nvmeTask.status == NVME_TASK_RUNNING)
		{
			NVME_COMMAND nvmeCmd;
			unsigned int cmdValid;

			cmdValid = get_nvme_cmd(&nvmeCmd.qID, &nvmeCmd.cmdSlotTag, &nvmeCmd.cmdSeqNum, nvmeCmd.cmdDword);

			if(cmdValid == 1)
			{
				rstCnt = 0;
				if(nvmeCmd.qID == 0)
				{
					handle_nvme_admin_cmd(&nvmeCmd);
				}
				else
				{
					handle_nvme_io_cmd(&nvmeCmd);
				}
			}
		}
		else if(g_nvmeTask.status == NVME_TASK_SHUTDOWN)
		{
			NVME_STATUS_REG nvmeReg;
			nvmeReg.dword = IO_READ32(NVME_STATUS_REG_ADDR);
			if(nvmeReg.ccShn != 0)
			{
				unsigned int qID;
				set_nvme_csts_shst(1);

				for(qID = 0; qID < 8; qID++)
				{
					set_io_cq(qID, 0, 0, 0, 0, 0, 0);
					set_io_sq(qID, 0, 0, 0, 0, 0);
				}

				set_nvme_admin_queue(0, 0, 0);
				g_nvmeTask.cacheEn = 0;
				set_nvme_csts_shst(2);
				g_nvmeTask.status = NVME_TASK_WAIT_RESET;

				xil_printf("\r\nNVMe shutdown!!!\r\n");
			}
		}
		else if(g_nvmeTask.status == NVME_TASK_WAIT_RESET)
		{
			unsigned int ccEn;
			ccEn = check_nvme_cc_en();
			if(ccEn == 0)
			{
                unsigned int qID;

				g_nvmeTask.cacheEn = 0;
				set_nvme_csts_shst(0);
				set_nvme_csts_rdy(0);

                set_nvme_admin_queue(0, 0, 0);
                for(qID = 0; qID < 8; qID++)
                {
                    set_io_cq(qID, 0, 0, 0, 0, 0, 0);
                    set_io_sq(qID, 0, 0, 0, 0, 0);
                }

				g_nvmeTask.status = NVME_TASK_IDLE;
				xil_printf("\r\nNVMe disable!!!\r\n");
			}
		}
		else if(g_nvmeTask.status == NVME_TASK_RESET)
		{
			unsigned int qID;
			for(qID = 0; qID < 8; qID++)
			{
				set_io_cq(qID, 0, 0, 0, 0, 0, 0);
				set_io_sq(qID, 0, 0, 0, 0, 0);
			}

			if (rstCnt== 5){
				pcie_async_reset(rstCnt);
				rstCnt = 0;
				xil_printf("\r\nPcie iink disable!!!\r\n");
				xil_printf("Wait few minute or reconnect the PCIe cable\r\n");
			}
			else
				rstCnt++;

			g_nvmeTask.cacheEn = 0;
			set_nvme_admin_queue(0, 0, 0);
			set_nvme_csts_shst(0);
			set_nvme_csts_rdy(0);
			g_nvmeTask.status = NVME_TASK_IDLE;

			xil_printf("\r\nNVMe reset!!!\r\n");
		}
	}
}


