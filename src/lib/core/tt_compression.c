/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "tt_compression.h"
#include "trivia/util.h"
#include <zstd.h>
#include <lz4.h>

/**
 * Compress data array from @a src data array into @a dst data array
 * according to the "zstd" algorithm with @a level. Return 0 in case
 * of error, or compressed data size if success.
 */
static size_t
tt_compress_zstd(char *dst, size_t dst_size, const char *src,
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
static size_t
tt_decompress_zstd(char *dst, size_t dst_size, const char *src,
		   size_t src_size)
{
	size_t size = ZSTD_decompress(dst, dst_size, src, src_size);
	if (ZSTD_isError(size))
		return 0;
	return size;
}

/**
 * Compress data array from @a src data array into @a dst data array
 * according to the "lz4" algorithm. Return 0 in case of error, or
 * compressed data size if success.
 */
static size_t
tt_compress_lz4(char *dst, size_t dst_size, const char *src, size_t src_size)
{
	return LZ4_compress_default(src, dst, src_size, dst_size);
}

/**
 * Decompress data from @src data array to @a dst data array
 * according to "lz4" algorithm. Return 0 in case of error,
 * or decompressed data size if success.
 */
static size_t
tt_decompress_lz4(char *dst, size_t dst_size, const char *src,
		   size_t src_size)
{
	int size = LZ4_decompress_safe(src, dst, src_size, dst_size);
	if (size < 0)
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
		return tt_compress_zstd(dst, dst_size, src, src_size, level);
	case COMPRESSION_TYPE_LZ4:
		return tt_compress_lz4(dst, dst_size, src, src_size);
	default:
		unreachable();
	}
	return 0;
}

size_t
tt_decompress(char *dst, size_t dst_size, const char *src,
	      size_t src_size, enum compression_type type)
{
	switch (type) {
	case COMPRESSION_TYPE_ZSTD:
		return tt_decompress_zstd(dst, dst_size, src, src_size);
	case COMPRESSION_TYPE_LZ4:
		return tt_decompress_lz4(dst, dst_size, src, src_size);
	default:
		unreachable();
	}
	return 0;
}
