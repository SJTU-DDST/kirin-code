//////////////////////////////////////////////////////////////////////////////////
// nvme_io_cmd.c for Cosmos+ OpenSSD
// Copyright (c) 2016 Hanyang University ENC Lab.
// Contributed by Yong Ho Song <yhsong@enc.hanyang.ac.kr>
//				  Youngjin Jo <yjjo@enc.hanyang.ac.kr>
//				  Sangjin Lee <sjlee@enc.hanyang.ac.kr>
//				  Jaewook Kwak <jwkwak@enc.hanyang.ac.kr>
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
//
// Project Name: Cosmos+ OpenSSD
// Design Name: Cosmos+ Firmware
// Module Name: NVMe IO Command Handler
// File Name: nvme_io_cmd.c
//
// Version: v1.0.1
//
// Description:
//   - handles NVMe IO command
//////////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Revision History:
//
// * v1.0.1
//   - header file for buffer is changed from "ia_lru_buffer.h" to "lru_buffer.h"
//
// * v1.0.0
//   - First draft
//////////////////////////////////////////////////////////////////////////////////


#include "xil_printf.h"
#include "debug.h"
#include "io_access.h"

#include "nvme.h"
#include "host_lld.h"
#include "nvme_io_cmd.h"
#include "../memory_map.h"
#include "../cs_args.h"
#include "../shared_mem.h"
#include "../utils_common.h"

static volatile struct shared_mem *m = (struct shared_mem *)SHARED_MEM_BASE_ADDR;

static struct host_io_req *host_io_reqs;
static QTAILQ_HEAD(free_host_io_reqs, host_io_req) free_host_io_reqs;
static QTAILQ_HEAD(pending_host_io_dmas, host_io_req) pending_host_io_dmas;
static uint64_t nr_free_host_io_reqs;

struct host_io_req *alloc_host_io_req()
{
	struct host_io_req *req;
	ASSERT(!QTAILQ_EMPTY(&free_host_io_reqs));

	req = QTAILQ_FIRST(&free_host_io_reqs);
	QTAILQ_REMOVE(&free_host_io_reqs, req, qent);

	nr_free_host_io_reqs--;
	return req;
}

void free_host_io_req(struct host_io_req *req)
{
	ASSERT(req != NULL);
	QTAILQ_INSERT_HEAD(&free_host_io_reqs, req, qent);
	nr_free_host_io_reqs++;
}

void init_host_io_reqs()
{
	host_io_reqs = linear_malloc(CONFIG_NR_HOST_IO_REQS * sizeof(struct host_io_req), 0);
	nr_free_host_io_reqs = CONFIG_NR_HOST_IO_REQS;

	QTAILQ_INIT(&free_host_io_reqs);
	QTAILQ_INIT(&pending_host_io_dmas);
	for (int i = 0; i < CONFIG_NR_HOST_IO_REQS; i++)
		QTAILQ_INSERT_TAIL(&free_host_io_reqs, &host_io_reqs[i], qent);
}

void check_host_io_dma_done()
{
	struct host_io_req *req;

	while (1) {
		if (QTAILQ_EMPTY(&pending_host_io_dmas))
			break;

		req = QTAILQ_FIRST(&pending_host_io_dmas);
		if (req->is_read) {
			if (!check_auto_tx_dma_partial_done(req->dma_tail, req->dma_overflow_cnt))
				break;
		} else {
			if (!check_auto_rx_dma_partial_done(req->dma_tail, req->dma_overflow_cnt))
				break;
		}

		QTAILQ_REMOVE(&pending_host_io_dmas, req, qent);

		if (req->is_read) {
			set_auto_nvme_cpl(req->cmd_slot_tag, 0, 0);
			free_host_io_req(req);
		} else {
			struct emu_req_sqe *sqe = qpair_alloc_sqe(&m->emu_req_qp);

			sqe->host_io_req = req;
			qpair_submit_sqe(&m->emu_req_qp, sqe);
		}
	}
}

extern void signal_cs_emu_req_done(struct cs_file_req *file_req);

void process_emu_req_cq()
{
	struct emu_req_cqe *cqe;

	while ((cqe = qpair_peek_cqe(&m->emu_req_qp)) != NULL) {
		struct host_io_req *req = cqe->host_io_req;

		if (!req->is_cs) {
			if (req->is_read) {
				for (int i = 0; i < req->nlb; i++) {
					uintptr_t addr = DDR4_BUFFER_BASE_ADDR + ((size_t)req->slba + i) * BYTES_PER_NVME_BLOCK;
					uint32_t addr_hi = (addr >> 32);
					uint32_t addr_lo = (addr & 0xffffffff);

					set_auto_tx_dma(req->cmd_slot_tag, i, addr_hi, addr_lo, NVME_COMMAND_AUTO_COMPLETION_OFF);
				}
				req->dma_tail = g_hostDmaStatus.fifoTail.autoDmaTx;
				req->dma_overflow_cnt = g_hostDmaAssistStatus.autoDmaTxOverFlowCnt;
				QTAILQ_INSERT_TAIL(&pending_host_io_dmas, req, qent);
			} else {
				set_auto_nvme_cpl(req->cmd_slot_tag, 0, 0);
				free_host_io_req(req);
			}
		} else {
			signal_cs_emu_req_done(req->file_req);
			free_host_io_req(req);
		}

		qpair_consume_cqe(&m->emu_req_qp, cqe);
	}
}


void handle_nvme_io_read(unsigned int cmdSlotTag, NVME_IO_COMMAND *nvmeIOCmd,
                         unsigned int qid, unsigned int cid)
{
	unsigned int requestedNvmeBlock, dmaIndex, numOfNvmeBlock, devAddrH, devAddrL;
	// unsigned long long devAddr;
	struct host_io_req *req;
	struct emu_req_sqe *sqe;

	IO_READ_COMMAND_DW12 readInfo12;
	//IO_READ_COMMAND_DW13 readInfo13;
	//IO_READ_COMMAND_DW15 readInfo15;
	unsigned int startLba[2];
	unsigned int nlb;

	readInfo12.dword = nvmeIOCmd->dword[12];
	//readInfo13.dword = nvmeIOCmd->dword[13];
	//readInfo15.dword = nvmeIOCmd->dword[15];

	startLba[0] = nvmeIOCmd->dword[10];
	startLba[1] = nvmeIOCmd->dword[11];
	nlb = readInfo12.NLB;
	ASSERT(startLba[0] < STORAGE_CAPACITY_L && (startLba[1] < STORAGE_CAPACITY_H || startLba[1] == 0));
	//ASSERT(nlb < MAX_NUM_OF_NLB);
	ASSERT((nvmeIOCmd->PRP1[0] & 0x3) == 0 && (nvmeIOCmd->PRP2[0] & 0x3) == 0); //error
	ASSERT(nvmeIOCmd->PRP1[1] < 0x10000 && nvmeIOCmd->PRP2[1] < 0x10000);

	if (readInfo12.IS_CS) {
		// transfer_cs_args(cmdSlotTag, qid, cid, nlb, CS_ARGS_TX);
		if(readInfo12.COMPACTION_ARGS)
			queue_cs_args_req(cmdSlotTag, qid, cid, nlb, COMPACTION_ARGS_TX);
		else if(readInfo12.SEARCH_ARGS)
		{
			if(readInfo12.BATCH_ID)
			{
				queue_cs_args_req(cmdSlotTag, qid, cid, nlb, SEARCH_ARGS_TX1);
			}
			else
			{
				queue_cs_args_req(cmdSlotTag, qid, cid, nlb, SEARCH_ARGS_TX0);
			}
		}
		return;
	}

    // dmaIndex = 0;
    requestedNvmeBlock = nlb + 1;
    // devAddr = (unsigned long long)DDR4_BUFFER_BASE_ADDR + (unsigned long long)startLba[0] * (unsigned long long)BYTES_PER_NVME_BLOCK;
    // devAddrH = (unsigned int)(devAddr >> 32);
    // devAddrL = (unsigned int)(devAddr & 0xFFFFFFFF);
    // numOfNvmeBlock = 0;

    // while(numOfNvmeBlock < requestedNvmeBlock)
    // {
    //     set_auto_tx_dma(cmdSlotTag, dmaIndex, devAddrH, devAddrL, NVME_COMMAND_AUTO_COMPLETION_ON);

    //     numOfNvmeBlock++;
    //     dmaIndex++;
    //     devAddr += BYTES_PER_NVME_BLOCK;
    //     devAddrH = (unsigned int)(devAddr >> 32);
    //     devAddrL = (unsigned int)(devAddr & 0xFFFFFFFF);
    // }
	req = alloc_host_io_req();
	req->is_read = true;
	req->is_cs = false;
	req->slba = startLba[0];
	req->nlb = requestedNvmeBlock;
	req->cmd_slot_tag = cmdSlotTag;
	req->qid = qid;
	req->cid = cid;

	sqe = qpair_alloc_sqe(&m->emu_req_qp);
	sqe->host_io_req = req;
	qpair_submit_sqe(&m->emu_req_qp, sqe);
}


void handle_nvme_io_write(unsigned int cmdSlotTag, NVME_IO_COMMAND *nvmeIOCmd,
                          unsigned int qid, unsigned int cid)
{
	unsigned int requestedNvmeBlock, dmaIndex, numOfNvmeBlock, devAddrH, devAddrL;
	unsigned long long devAddr;
	struct host_io_req *req;

	IO_READ_COMMAND_DW12 writeInfo12;
	IO_READ_COMMAND_DW13 writeInfo13;
	//IO_READ_COMMAND_DW15 writeInfo15;
	unsigned int startLba[2];
	unsigned int nlb;

	writeInfo12.dword = nvmeIOCmd->dword[12];
	//writeInfo13.dword = nvmeIOCmd->dword[13];
	//writeInfo15.dword = nvmeIOCmd->dword[15];

	//if(writeInfo12.FUA == 1)
	//	xil_printf("write FUA\r\n");

	startLba[0] = nvmeIOCmd->dword[10];
	startLba[1] = nvmeIOCmd->dword[11];
	nlb = writeInfo12.NLB;

	ASSERT(startLba[0] < STORAGE_CAPACITY_L && (startLba[1] < STORAGE_CAPACITY_H || startLba[1] == 0));
	//ASSERT(nlb < MAX_NUM_OF_NLB);
	ASSERT((nvmeIOCmd->PRP1[0] & 0xF) == 0 && (nvmeIOCmd->PRP2[0] & 0xF) == 0);
	ASSERT(nvmeIOCmd->PRP1[1] < 0x10000 && nvmeIOCmd->PRP2[1] < 0x10000);

	if (writeInfo12.IS_CS) {
		if (writeInfo12.COMPACTION_ARGS)
			transfer_cs_args(cmdSlotTag, qid, cid, nlb, COMPACTION_ARGS_RX, 0, 0);
		else if (writeInfo12.SEARCH_ARGS)
		{
			int nr_search = nvmeIOCmd->dword[13];
			int batch_size = nvmeIOCmd->dword[14];
			if(writeInfo12.BATCH_ID)
			{
				transfer_cs_args(cmdSlotTag, qid, cid, nlb, SEARCH_ARGS_RX1, nr_search, batch_size);
			}
			else
			{
				transfer_cs_args(cmdSlotTag, qid, cid, nlb, SEARCH_ARGS_RX0, nr_search, batch_size);
			}
			
		}
		return;
	}

    dmaIndex = 0;
    requestedNvmeBlock = nlb + 1;
    devAddr = (unsigned long long)DDR4_BUFFER_BASE_ADDR + (unsigned long long)startLba[0] * (unsigned long long)BYTES_PER_NVME_BLOCK;
    devAddrH = (unsigned int)(devAddr >> 32);
    devAddrL = (unsigned int)(devAddr & 0xFFFFFFFF);
    numOfNvmeBlock = 0;

    while(numOfNvmeBlock < requestedNvmeBlock)
    {
        set_auto_rx_dma(cmdSlotTag, dmaIndex, devAddrH, devAddrL, NVME_COMMAND_AUTO_COMPLETION_OFF);

        numOfNvmeBlock++;
        dmaIndex++;
        devAddr += BYTES_PER_NVME_BLOCK;
        devAddrH = (unsigned int)(devAddr >> 32);
        devAddrL = (unsigned int)(devAddr & 0xFFFFFFFF);
    }

	req = alloc_host_io_req();
	req->is_read = false;
	req->is_cs = false;
	req->slba = startLba[0];
	req->nlb = requestedNvmeBlock;
	req->cmd_slot_tag = cmdSlotTag;
	req->qid = qid;
	req->cid = cid;
	req->dma_tail = g_hostDmaStatus.fifoTail.autoDmaRx;
	req->dma_overflow_cnt = g_hostDmaAssistStatus.autoDmaRxOverFlowCnt;
	QTAILQ_INSERT_TAIL(&pending_host_io_dmas, req, qent);
}

void handle_nvme_io_cmd(NVME_COMMAND *nvmeCmd)
{
	NVME_IO_COMMAND *nvmeIOCmd;
	NVME_COMPLETION nvmeCPL;
	unsigned int opc;

	nvmeIOCmd = (NVME_IO_COMMAND*)nvmeCmd->cmdDword;

	opc = (unsigned int)nvmeIOCmd->OPC;

	switch(opc)
	{
		case IO_NVM_FLUSH:
		{
			PRINT("IO Flush Command\r\n");
			nvmeCPL.dword[0] = 0;
			nvmeCPL.specific = 0x0;
			set_auto_nvme_cpl(nvmeCmd->cmdSlotTag, nvmeCPL.specific, nvmeCPL.statusFieldWord);
			break;
		}
		case IO_NVM_WRITE:
		{
			PRINT("IO Write Command\r\n");
			handle_nvme_io_write(nvmeCmd->cmdSlotTag, nvmeIOCmd, nvmeCmd->qID, nvmeIOCmd->CID);
			break;
		}
		case IO_NVM_READ:
		{
			PRINT("IO Read Command\r\n");
			handle_nvme_io_read(nvmeCmd->cmdSlotTag, nvmeIOCmd, nvmeCmd->qID, nvmeIOCmd->CID);
			break;
		}
		default:
		{
			xil_printf("Not Support IO Command OPC: 0x%X\r\n", opc);
			ASSERT(0);
			break;
		}
	}

#if (__IO_CMD_DONE_MESSAGE_PRINT)
    xil_printf("OPC = 0x%X\r\n", nvmeIOCmd->OPC);
    xil_printf("PRP1[63:32] = 0x%X, PRP1[31:0] = 0x%X\r\n", nvmeIOCmd->PRP1[1], nvmeIOCmd->PRP1[0]);
    xil_printf("PRP2[63:32] = 0x%X, PRP2[31:0] = 0x%X\r\n", nvmeIOCmd->PRP2[1], nvmeIOCmd->PRP2[0]);
    xil_printf("dword10 = 0x%X\r\n", nvmeIOCmd->dword10);
    xil_printf("dword11 = 0x%X\r\n", nvmeIOCmd->dword11);
    xil_printf("dword12 = 0x%X\r\n", nvmeIOCmd->dword12);
#endif
}

