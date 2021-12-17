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

int
msgpack_compress_fields(struct space *space, const char *data,
                        const char *data_end, char **new_data,
                        char **new_data_end);

int
msgpack_decompress_fields(const char *data, const char *data_end,
                          char **new_data, char **new_data_end);


#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */