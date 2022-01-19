/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2021, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "mp_compression.h"

#include <stddef.h>

#include "trivia/config.h"
#include "trivia/util.h"

#if defined(ENABLE_TUPLE_COMPRESSION)
# error unimplemented
#endif

char *
mp_compress(char *dst, const char *src, size_t src_size,
	    enum compression_type type)
{
	(void)dst;
	(void)src;
	(void)src_size;
	(void)type;
	unreachable();
	return NULL;
}

size_t
mp_decompress(const char **src, char *dst, size_t dst_size)
{
	(void)src;
	(void)dst;
	(void)dst_size;
	/** Zero is an error according to the convention. */
	return 0;
}

int
mp_snprint_compression(char *buf, int size, const char **data, uint32_t len)
{
	(void)buf;
	(void)size;
	(void)data;
	(void)len;
	return -1;
}

int
mp_fprint_compression(FILE *file, const char **data, uint32_t len)
{
	(void)file;
	(void)data;
	(void)len;
	return -1;
}
