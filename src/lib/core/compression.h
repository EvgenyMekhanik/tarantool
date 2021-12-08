#pragma once
/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include <stddef.h>
#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

size_t
none_compress_size(const char *data, const char *data_end);

int
none_compress(const char *data, const char *data_end, char *new_data,
              size_t *new_data_size);

int
none_decompress(const char **data, uint32_t len, char **new_data,
                char **new_data_end);

size_t
zstd_compress_size(const char *data, const char *data_end, int level);

int
zstd_compress(const char *data, const char *data_end, char *new_data,
              size_t *new_data_size, int level);

int
zstd_decompress(const char **data, uint32_t len, char **new_data,
                char **new_data_end);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
