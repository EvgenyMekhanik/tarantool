#pragma once
/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

int64_t
none_compressed_data_size(const uint32_t data_size);

int
none_compress_data(const char *data, const uint32_t data_size,
                   char *new_data, uint32_t *new_data_size);

int
none_decompress_data(const char **data, const uint32_t data_size,
                     char *new_data);

int64_t
zstd_compressed_data_size(const char *data, const uint32_t data_size,
                          int level);

int
zstd_compress_data(const char *data, const uint32_t data_size, char *new_data,
                   uint32_t *new_data_size, int level);

int
zstd_decompress_data(const char **data, const uint32_t data_size,
                     char *new_data);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
