#pragma once
/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include <stdbool.h>
#include "trivia/config.h"

#if defined(__cplusplus)
extern "C" {
#endif

enum compression_type {
        COMPRESSION_TYPE_NONE = 0,
#if defined(ENABLE_TUPLE_COMPRESSION)
        COMPRESSION_TYPE_ZSTD,
        COMPRESSION_TYPE_LZ4,
#endif /* !defined(ENABLE_TUPLE_COMPRESSION) */
        compression_type_MAX
};

static inline bool
tt_check_compression_type(enum compression_type type)
{
        return (type > COMPRESSION_TYPE_NONE && type < compression_type_MAX);
}

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
