#pragma once
/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "trivia/config.h"

#if defined(ENABLE_TUPLE_COMPRESSION)
# include "tuple_compression_impl.h"
#else /* !defined(ENABLE_TUPLE_COMPRESSION) */

#include "tuple.h"

#if defined(__cplusplus)
extern "C" {
#endif

void
tuple_compress_raw(struct tuple_format *format, const char *data,
                   const char *data_end, char **cdata,
                   char **cdata_end);

struct tuple *
tuple_decompress(struct tuple *tuple);

static inline struct tuple *
tuple_maybe_decompress(struct tuple *tuple)
{
        if (!tuple_is_compressed(tuple))
                return tuple;
        return tuple_decompress(tuple);
}

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* !defined(ENABLE_TUPLE_COMPRESSION) */
