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

enum compression_type {
        COMPRESSION_TYPE_NONE = 0,
        COMPRESSION_TYPE_ZSTD5,
        compression_type_MAX
};

struct tt_compress {
        /** Type of compression */
        enum compression_type type;
        /** Pointer to data for compression. */
        char *data;
        /** Pointer to the end of data for compression. */
        char *data_end;
};

/**
 * Return the size of the data from @a ttc, that they
 * will occupy after compression.
 */
size_t
tt_compress_size(const struct tt_compress *ttc);

/**
 * Compress data from @a ttc structure and save new compressed
 * data in @a data and size of this compressed data in @a size.
 * Return 0 if success, otherwise return -1 and doesn't affect
 * @a data and @a size.
 */
int
tt_compress_compress(const struct tt_compress *ttc, char *data, size_t *size);

/**
 * Decompress @a data and save pointers to begin and end of
 * new decompressed data in @a ttc structure. Return 0 if
 * success otherwise return -1.
 */
int
tt_compress_decompress(const char **data, uint32_t len,
                       struct tt_compress *ttc);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
