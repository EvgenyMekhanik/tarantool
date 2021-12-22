#pragma once
/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include <stdint.h>
#include <stdio.h>
#include "tt_compression.h"

#if defined(__cplusplus)
extern "C" {
#endif

/**
 * Compress data from @a src data array into @a dst data array
 * according to compression @a type and @a level. If data size
 * after compression is greater or equal then data size before
 * compression save data without any compression. It is caller
 * responsibility to ensure that @a dst has at least @a src_size
 * bytes. Return data + size, that the data occupied.
 */
char *
mp_compress(char *dst, const char *src, size_t src_size,
	    enum compression_type type, uint32_t level);

/**
 * If @a dst_size is equal to zero just return size of decompressed
 * data, otherwise decompress data from @a src data array into @a dst
 * data array and move @src. Return 0 in case of error, otherwise
 * return size of decompressed data.
 */
size_t
mp_decompress(const char **src, char *dst, size_t dst_size);

/**
 * Print compressed data string representation into a given buffer.
 */
int
mp_snprint_compression(char *buf, int size, const char **data, uint32_t len);

/**
 * Print compressed data string representation into a stream.
 */
int
mp_fprint_compression(FILE *file, const char **data, uint32_t len);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
