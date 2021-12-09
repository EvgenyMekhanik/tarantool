/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "compression.h"
#include "fiber.h"
#include "small/region.h"
#include <zstd.h>

int64_t
none_compressed_data_size(const uint32_t data_size)
{
        return data_size;
}

int
none_compress_data(const char *data, const uint32_t data_size,
                   char *new_data, uint32_t *new_data_size)
{
        *new_data_size = data_size;
        memcpy(new_data, data, *new_data_size);
        return 0;
}

int
none_decompress_data(const char **data, const uint32_t data_size,
                     char *new_data)
{
        memcpy(new_data, *data, data_size);
        *data += data_size;
        return 0;
}

int64_t
zstd_compressed_data_size(const char *data, const uint32_t data_size,
                          int level)
{
        uint32_t max_size = ZSTD_compressBound(data_size);
        uint32_t used = region_used(&fiber()->gc);
        void *tmp = region_alloc(&fiber()->gc, max_size);
        if (tmp == NULL)
                return -1;
        uint32_t size = ZSTD_compress(tmp, max_size, data,
                                      data_size, level);
        region_truncate(&fiber()->gc, used);
        if (ZSTD_isError(size))
                return -1;
        return size;
}

int
zstd_compress_data(const char *data, const uint32_t data_size, char *new_data,
                   uint32_t *new_data_size, int level)
{
        uint32_t max_size = ZSTD_compressBound(data_size);
        uint32_t used = region_used(&fiber()->gc);
        void *tmp = region_alloc(&fiber()->gc, max_size);
        if (tmp == NULL)
                return -1;
        *new_data_size = ZSTD_compress(tmp, max_size, data,
                                       data_size, level);
        if (ZSTD_isError(*new_data_size)) {
                region_truncate(&fiber()->gc, used);
                return -1;
        }
        memcpy(new_data, tmp, *new_data_size);
        region_truncate(&fiber()->gc, used);
        return 0;
}

int
zstd_decompress_data(const char **data, const uint32_t data_size,
                     char *new_data)
{
        uint32_t max_size = ZSTD_getFrameContentSize(*data, data_size);
        if (ZSTD_isError(max_size))
                return -1;
        uint32_t used = region_used(&fiber()->gc);
        void *tmp = region_alloc(&fiber()->gc, max_size);
        if (tmp == NULL)
                return -1;
        uint32_t size = ZSTD_decompress(tmp, max_size,
                                        *data, data_size);
        if (ZSTD_isError(size)) {
                region_truncate(&fiber()->gc, used);
                return -1;
        }
        memcpy(new_data, tmp, size);
        region_truncate(&fiber()->gc, used);
        *data += data_size;
        return 0;
}
