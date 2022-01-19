#pragma once
/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "trivia/config.h"

#if defined(ENABLE_MP_COMPRESSION)
# include "tt_compression_impl.h"
#else /* !defined(ENABLE_MP_COMPRESSION) */

#include "tt_compression_types.h"

#include <stdint.h>
#include <stddef.h>

#if defined(__cplusplus)
extern "C" {
#endif

size_t
tt_compress(char *dst, size_t dst_size, const char *src,
            size_t src_size, enum compression_type type,
            uint32_t level);

size_t
tt_decompress(char *dst, size_t dst_size, const char *src,
              size_t src_size, enum compression_type type);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* !defined(ENABLE_MP_COMPRESSION) */
