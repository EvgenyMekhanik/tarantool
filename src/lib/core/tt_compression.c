/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2021, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "tt_compression.h"

#include <stddef.h>

#include "trivia/config.h"
#include "trivia/util.h"

#if defined(ENABLE_MP_COMPRESSION)
# error unimplemented
#endif

size_t
tt_compress(char *dst, size_t dst_size, const char *src,
	    size_t src_size, enum compression_type type,
	    uint32_t level)
{
	(void)dst;
	(void)dst_size;
	(void)src;
	(void)src_size;
	(void)type;
	(void)level;
	unreachable();
	return 0;
}

size_t
tt_decompress(char *dst, size_t dst_size, const char *src,
	      size_t src_size, enum compression_type type)
{
	(void)dst;
	(void)dst_size;
	(void)src;
	(void)src_size;
	(void)type;
	unreachable();
	return 0;
}
