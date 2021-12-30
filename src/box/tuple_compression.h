#pragma once
/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#if defined(__cplusplus)
extern "C" {
#endif

struct space;
struct tuple;

struct tuple *
tt_compress_tuple(struct space *space, const char *data,
                  const char *data_end);

struct tuple *
tt_decompress_tuple(struct space *space, struct tuple *old_tuple);


#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
