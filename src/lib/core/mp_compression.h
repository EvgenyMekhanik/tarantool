#pragma once
/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "trivia/config.h"

#if defined(ENABLE_TUPLE_COMPRESSION)
# include "mp_compression_impl.h"
#else /* !defined(ENABLE_TUPLE_COMPRESSION) */

#include <stdint.h>
#include <stdio.h>
#include "tt_compression.h"

#if defined(__cplusplus)
extern "C" {
#endif

char *
mp_compress(char *dst, const char *src, size_t src_size,
	    enum compression_type type);

size_t
mp_decompress(const char **src, char *dst, size_t dst_size);

int
mp_snprint_compression(char *buf, int size, const char **data, uint32_t len);

int
mp_fprint_compression(FILE *file, const char **data, uint32_t len);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* !defined(ENABLE_TUPLE_COMPRESSION) */
