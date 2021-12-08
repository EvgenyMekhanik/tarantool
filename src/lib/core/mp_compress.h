#pragma once
/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include <stdint.h>
#include <stdio.h>
#include "tt_compress.h"

#if defined(__cplusplus)
extern "C" {
#endif

/**
 * Calculate size of MessagePack buffer for compressed structure @a ttc.
 */
uint32_t
mp_sizeof_compress(const struct tt_compress *ttc);

/**
 * Decode compressed structure @a ttc from MessagePack @a data.
 */
struct tt_compress *
mp_decode_compress(const char **data, struct tt_compress *ttc);

/**
 * Encode compress structure @a ttc to the MessagePack buffer @a data.
 */
char *
mp_encode_compress(char *data, const struct tt_compress *ttc);

/**
 * Print compressed data string representation into a given buffer.
 */
int
mp_snprint_compress(char *buf, int size, const char **data, uint32_t len);

/**
 * Print compressed data string representation into a stream.
 */
int
mp_fprint_compress(FILE *file, const char **data, uint32_t len);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
