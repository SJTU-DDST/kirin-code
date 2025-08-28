//////////////////////////////////////////////////////////////////////////////////
// memory_map.h for Cosmos+ OpenSSD
// Copyright (c) 2017 Hanyang University ENC Lab.
// Contributed by Yong Ho Song <yhsong@enc.hanyang.ac.kr>
//                  Jaewook Kwak <jwkwak@enc.hanyang.ac.kr>
//                  Sangjin Lee <sjlee@enc.hanyang.ac.kr>
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
// Engineer: Jaewook Kwak <jwkwak@enc.hanyang.ac.kr>
//
// Project Name: Cosmos+ OpenSSD
// Design Name: Cosmos+ Firmware
// Module Name: Static Memory Allocator
// File Name: memory_map.h
//
// Version: v1.0.0
//
// Description:
//     - allocate DRAM address space (0x0010_0000 ~ 0x3FFF_FFFF) to each module
//////////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Revision History:
//
// * v1.0.0
//   - First draft
//////////////////////////////////////////////////////////////////////////////////

#include <stdint.h>

#ifndef MEMORY_MAP_H_
#define MEMORY_MAP_H_

#define DRAM_START_ADDR                 ((uintptr_t)0x00100000)
#define DRAM_END_ADDR                   ((uintptr_t)0x80000000)


#define CPU0_DRAM_START_ADDR            DRAM_START_ADDR
#define CPU0_DRAM_END_ADDR              ((uintptr_t)0x50000000)

#define CPU1_DRAM_START_ADDR            CPU0_DRAM_END_ADDR
#define CPU1_DRAM_END_ADDR              ((uintptr_t)0x60000000)

#define CPU2_DRAM_START_ADDR            CPU1_DRAM_END_ADDR
#define CPU2_DRAM_END_ADDR              ((uintptr_t)0x70000000)

#define CPU3_DRAM_START_ADDR            CPU2_DRAM_END_ADDR
#define CPU3_DRAM_END_ADDR              (DRAM_END_ADDR - 16 * 1024 * 1024)

#define DMA_START_ADDR                  CPU3_DRAM_END_ADDR
#define DMA_END_ADDR                    DRAM_END_ADDR


#define CPU0_MEMORY_SEGMENTS_START_ADDR CPU0_DRAM_START_ADDR
#define CPU0_MEMORY_SEGMENTS_END_ADDR   ((uintptr_t)0x00200000)

#define NVME_MANAGEMENT_START_ADDR      ((uintptr_t)0x00200000)
#define NVME_MANAGEMENT_END_ADDR        ((uintptr_t)0x10000000)

#define DUMMY_RD_WR_ADDR                ((uintptr_t)(0x40000000 - 0x1000)) // Reserved for NVMe IP.

#define CPU0_CACHED_MEMORY_BASE_ADDR    ((uintptr_t)0x40000000)
#define CPU0_CACHED_MEMORY_END_ADDR     (CPU0_CACHED_MEMORY_BASE_ADDR + (uintptr_t)0x08000000)

#define CPU0_UNCACHED_MEMORY_BASE_ADDR  CPU0_CACHED_MEMORY_END_ADDR
#define CPU0_UNCACHED_MEMORY_END_ADDR   CPU0_DRAM_END_ADDR

#define SHARED_MEM_BASE_ADDR            CPU0_CACHED_MEMORY_BASE_ADDR
#define SHARED_MEM_END_ADDR             (SHARED_MEM_BASE_ADDR + 16 * 1024 * 1024)

#define CPU0_LINEAR_MALLOC_BASE_ADDR    SHARED_MEM_END_ADDR
#define CPU0_LINEAR_MALLOC_END_ADDR     CPU0_CACHED_MEMORY_END_ADDR


#define CPU1_MEMORY_SEGMENTS_START_ADDR CPU1_DRAM_START_ADDR
#define CPU1_MEMORY_SEGMENTS_END_ADDR   (CPU1_DRAM_START_ADDR + (uintptr_t)0x00200000)

#define CPU1_CACHED_MEMORY_BASE_ADDR    CPU1_MEMORY_SEGMENTS_END_ADDR
#define CPU1_CACHED_MEMORY_END_ADDR     (CPU1_CACHED_MEMORY_BASE_ADDR + (uintptr_t)0x08000000)

#define CPU1_UNCACHED_MEMORY_BASE_ADDR  CPU1_CACHED_MEMORY_END_ADDR
#define CPU1_UNCACHED_MEMORY_END_ADDR   CPU1_DRAM_END_ADDR

#define CPU1_LINEAR_MALLOC_BASE_ADDR    CPU1_CACHED_MEMORY_BASE_ADDR
#define CPU1_LINEAR_MALLOC_END_ADDR     CPU1_CACHED_MEMORY_END_ADDR


#define CPU2_MEMORY_SEGMENTS_START_ADDR CPU2_DRAM_START_ADDR
#define CPU2_MEMORY_SEGMENTS_END_ADDR   (CPU2_DRAM_START_ADDR + (uintptr_t)0x00200000)

#define CPU2_CACHED_MEMORY_BASE_ADDR    CPU2_MEMORY_SEGMENTS_END_ADDR
#define CPU2_CACHED_MEMORY_END_ADDR     (CPU2_CACHED_MEMORY_BASE_ADDR + (uintptr_t)0x08000000)

#define CPU2_UNCACHED_MEMORY_BASE_ADDR  CPU2_CACHED_MEMORY_END_ADDR
#define CPU2_UNCACHED_MEMORY_END_ADDR   CPU2_DRAM_END_ADDR

#define CPU2_LINEAR_MALLOC_BASE_ADDR    CPU2_CACHED_MEMORY_BASE_ADDR
#define CPU2_LINEAR_MALLOC_END_ADDR     CPU2_CACHED_MEMORY_END_ADDR

#define CPU3_MEMORY_SEGMENTS_START_ADDR CPU3_DRAM_START_ADDR
#define CPU3_MEMORY_SEGMENTS_END_ADDR   (CPU3_DRAM_START_ADDR + (uintptr_t)0x00200000)

#define CPU3_CACHED_MEMORY_BASE_ADDR    CPU3_MEMORY_SEGMENTS_END_ADDR
#define CPU3_CACHED_MEMORY_END_ADDR     (CPU3_DRAM_END_ADDR)

#define CPU3_UNCACHED_MEMORY_BASE_ADDR  CPU3_CACHED_MEMORY_END_ADDR
#define CPU3_UNCACHED_MEMORY_END_ADDR   CPU3_DRAM_END_ADDR

#define CPU3_LINEAR_MALLOC_BASE_ADDR    CPU3_CACHED_MEMORY_BASE_ADDR
#define CPU3_LINEAR_MALLOC_END_ADDR     CPU3_CACHED_MEMORY_END_ADDR

#define DDR4_BUFFER_BASE_ADDR ((uintptr_t)XPAR_DDR4_0_C0_DDR4_MEMORY_MAP_BASEADDR)

void setup_page_table();

#endif /* MEMORY_MAP_H_ */
