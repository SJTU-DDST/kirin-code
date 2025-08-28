#ifndef __CDMA_H
#define __CDMA_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "xaxicdma.h"
#include "xdebug.h"
#include "xil_cache.h"
#include "xparameters.h"
#include "xil_util.h"

bool cdma_init();
uint64_t cdma_transfer(volatile void *dst, volatile void *src, size_t size, bool flush_src,
                       bool flush_dst, bool check_error, bool synchronous);
bool cdma_is_busy();
bool cdma_transfer_done(uint64_t seq);

#endif
