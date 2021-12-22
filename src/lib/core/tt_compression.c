/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "tt_compression.h"
#include <zstd.h>

/**
 * Compress data array from @a src data array into @a dst data array
 * according to the "zstd" algorithm with @a level. Return 0 in case
 * of error, or compressed data size if success.
 */
static int
zstd_compress_data(char *dst, size_t dst_size, const char *src,
		   size_t src_size, uint32_t level)
{
	size_t size = ZSTD_compress(dst, dst_size, src, src_size, level);
	if (ZSTD_isError(size))
		return 0;
	return size;
}

/**
 * Decompress data from @src data array to @a dst data array
 * according to "zstd" algorithm. Return 0 in case of error,
 * or decompressed data size if success.
 */
int
zstd_decompress_data(char *dst, size_t dst_size, const char *src,
		     size_t src_size)
{
	size_t size = ZSTD_decompress(dst, dst_size, src, src_size);
	if (ZSTD_isError(size))
		return 0;
	return size;
}

size_t
tt_compress(char *dst, size_t dst_size, const char *src,
	    size_t src_size, enum compression_type type,
	    uint32_t level)
{
	switch (type) {
	case COMPRESSION_TYPE_ZSTD:
		return zstd_compress_data(dst, dst_size, src, src_size, level);
	default:
		;
	}
	return 0;
}

size_t
tt_decompress(char *dst, size_t dst_size, const char *src,
	      size_t src_size, enum compression_type type)
{
	switch (type) {
	case COMPRESSION_TYPE_ZSTD:
		return zstd_decompress_data(dst, dst_size, src, src_size);
	default:
		;
	}
	return 0;
}
