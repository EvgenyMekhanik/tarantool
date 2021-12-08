/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "compression.h"
#include "fiber.h"
#include "small/region.h"
#include <zstd.h>

size_t
none_compress_size(const char *data, const char *data_end)
{
        return data_end - data;
}

int
none_compress(const char *data, const char *data_end, char *new_data,
              size_t *new_data_size)
{
        *new_data_size = data_end - data;
        memcpy(new_data, data, *new_data_size);
        return 0;
}

int
none_decompress(const char **data, uint32_t len, char **new_data,
                char **new_data_end)
{
        *new_data = region_alloc(&fiber()->gc, len);
        if (*new_data == NULL)
                return -1;
        memcpy(*new_data, *data, len);
        *new_data_end = *new_data + len;
        *data += len;
        return 0;
}

size_t
zstd_compress_size(const char *data, const char *data_end, int level)
{
        size_t max_size = ZSTD_compressBound(data_end - data);
        size_t used = region_used(&fiber()->gc);
        void *tmp = region_alloc(&fiber()->gc, max_size);
        if (tmp == NULL)
                return max_size;
        size_t size = ZSTD_compress(tmp, max_size, data,
                                    data_end - data, level);
        region_truncate(&fiber()->gc, used);
        if (ZSTD_isError(size))
                return max_size;
        return size;
}

int
zstd_compress(const char *data, const char *data_end, char *new_data,
              size_t *new_data_size, int level)
{
        size_t max_size = ZSTD_compressBound(data_end - data);
        size_t used = region_used(&fiber()->gc);
        void *tmp = region_alloc(&fiber()->gc, max_size);
        if (tmp == NULL)
                return -1;
        *new_data_size = ZSTD_compress(tmp, max_size, data,
                                       data_end - data, level);
        if (ZSTD_isError(*new_data_size)) {
                region_truncate(&fiber()->gc, used);
                return -1;
        }
        memcpy(new_data, tmp, *new_data_size);
        region_truncate(&fiber()->gc, used);
        return 0;
}

int
zstd_decompress(const char **data, uint32_t len, char **new_data,
                char **new_data_end)
{
        size_t size = ZSTD_getFrameContentSize(*data, len);
        if (ZSTD_isError(size))
                return -1;
        size_t used = region_used(&fiber()->gc);
        *new_data = region_alloc(&fiber()->gc, size);
        if (*new_data == NULL)
                return -1;
        size_t real_size = ZSTD_decompress(*new_data, size, *data, len);
        if (ZSTD_isError(real_size)) {
                region_truncate(&fiber()->gc, used);
                return -1;
        }
        *new_data_end = *new_data + real_size;
        *data += len;
        region_truncate(&fiber()->gc, used + real_size);
        return 0;
}
