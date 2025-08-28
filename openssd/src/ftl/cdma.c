#include "cdma.h"
#include "utils_common.h"

void do_low_level_tasks();

static uint64_t transfer_seq;
static XAxiCdma_Config *cdma_cfg;
static XAxiCdma cdma_inst;

static bool cdma_check_error()
{
    uint32_t error;

    error = XAxiCdma_GetError(&cdma_inst);
    if (error != 0) {
        xil_printf("CDMA error %08x, trying reset\n", error);

        XAxiCdma_Reset(&cdma_inst);
        while (!XAxiCdma_ResetIsDone(&cdma_inst));

        error = XAxiCdma_GetError(&cdma_inst);
        if (error != 0) {
            xil_printf("CDMA error %08x after reset; something is wrong\n");
            return false;
        }
    }

    return true;
}

bool cdma_init()
{
    uint32_t status;

    transfer_seq = 1;

    cdma_cfg = XAxiCdma_LookupConfig(XPAR_AXICDMA_0_DEVICE_ID);
    if (cdma_cfg == NULL) {
        xil_printf("XAxiCdma_LookupConfig failed\n");
        return false;
    }

    status = XAxiCdma_CfgInitialize(&cdma_inst, cdma_cfg, cdma_cfg->BaseAddress);
    if (status != XST_SUCCESS) {
        xil_printf("XAxiCdma_CfgInitialize failed\n");
        return false;
    }

    XAxiCdma_IntrDisable(&cdma_inst, XAXICDMA_XR_IRQ_ALL_MASK);

    return cdma_check_error();
}

uint64_t cdma_transfer(volatile void *dst, volatile void *src, size_t size, bool flush_src,
                       bool flush_dst, bool check_error, bool synchronous)
{
    uint64_t ret = ++transfer_seq;
    uint32_t status;

    /* wait for previous async transfer to complete */
    while (XAxiCdma_IsBusy(&cdma_inst));

    if (check_error && !cdma_check_error())
        return 0;

    if (flush_src)
        FLUSH_CACHE(src, size);
    if (flush_dst)
        FLUSH_CACHE(dst, size);

    status = XAxiCdma_SimpleTransfer(&cdma_inst, (UINTPTR)src, (UINTPTR)dst, size, NULL, NULL);
    if (status != XST_SUCCESS) {
        xil_printf("XAxiCdma_SimpleTransfer failed\n");
        cdma_check_error();
        return 0;
    }

    while (synchronous && XAxiCdma_IsBusy(&cdma_inst));

    if (synchronous && check_error && !cdma_check_error())
        ret = 0;

    return ret;
}

bool cdma_is_busy()
{
    return XAxiCdma_IsBusy(&cdma_inst);
}

bool cdma_transfer_done(uint64_t seq)
{
    return seq != transfer_seq || !XAxiCdma_IsBusy(&cdma_inst);
}
