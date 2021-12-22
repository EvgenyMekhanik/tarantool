#pragma once
/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include <stdint.h>
#include <stddef.h>

#if defined(__cplusplus)
extern "C" {
#endif

enum compression_type {
	COMPRESSION_TYPE_NONE = 0,
	COMPRESSION_TYPE_ZSTD,
	compression_type_MAX
};

/**
 * Compress data from @a src data array into @a dst data array
 * according to compression @a type and @a level. Return 0 in
 * case of error, or compressed data size if success.
 */
size_t
tt_compress(char *dst, size_t dst_size, const char *src,
	    size_t src_size, enum compression_type type,
	    uint32_t level);

/**
 * Decompress data from @src data array to @a dst data array
 * according to compression @a type. Return 0 in case of error,
 * or decompressed data size if success.
 */
size_t
tt_decompress(char *dst, size_t dst_size, const char *src,
	      size_t src_size, enum compression_type type);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
