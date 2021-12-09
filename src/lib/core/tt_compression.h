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

enum compression_type {
        COMPRESSION_TYPE_NONE = 0,
        COMPRESSION_TYPE_ZSTD5,
        compression_type_MAX
};

/**
 * Structure describing data for compression or
 * decompression.
 */
struct tt_compression {
        /** Type of compression. */
        int type;
        /**
         * Size of data for compression in case when
         * this structure used for data compression or
         * expected size of decompressed data in case
         * when this struct used for decompression.
         */
        uint32_t size;
        /**
         * Pointer to data for comression
         * or decompression
         */
        char *data;
};

/**
 * Initialize @a ttc structure. In case of decompression @a size
 * and @a type have no matter - they will be obtained during @a data
 * decompression.
 */
void
tt_compression_create(struct tt_compression *ttc, char *data,
		      uint32_t size, int type);

/**
 * Return size of data from @a ttc, that they
 * will occupy after compression or -1 if error
 * occured.
 */
int64_t
tt_compression_compressed_data_size(const struct tt_compression *ttc);

/**
 * Compress data from @a ttc structure and save new compressed
 * data in @a data and size of this compressed data in @a size.
 * Return 0 if success, otherwise return -1 and doesn't affect
 * @a data and @a size. It is caller responsibility to ensure
 * that data has enought bytes.
 */
int
tt_compression_compress_data(const struct tt_compression *ttc,
                             char *data, uint32_t *size);

/**
 * Decompress @a data into data array in @a ttc structure. Return
 * 0 if success otherwise return -1. It is caller responsibility
 * to ensure that data array in @a ttc structure has enought bytes.
 */
int
tt_compression_decompress_data(const char **data, uint32_t size,
                               struct tt_compression *ttc);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
