#pragma once
/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include <stdint.h>
#include <small/region.h>

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
        /**
         * A memory region, used for allocation during
         * compression and decompression.
         */
        struct region region;
        /** Type of compression. */
        enum compression_type type;
        /**
         * Size of data for compression in case when
         * this structure used for data compression or
         * decompressed data size in case when this
         * structure used for decompression.
         */
        uint32_t size;
        /**
         * Pointer to array of uncompressed data. In case of
         * compression, data that is subject to compression
         * is stored here. In case of decompression, this is
         * where the data is saved after unpacking.
         */
        char *data;
};

/**
 * Allocate and initialize new tt_compression
 * structure.
 */
struct tt_compression *
tt_compression_new(void);

/**
 * Delete @a ttc structure and free all associated
 * resources.
 */
void
tt_compression_delete(struct tt_compression *ttc);

/**
 * Initialize @a ttc structure and all associated
 * resources.
 */
void
tt_compression_create(struct tt_compression *ttc);

/**
 * Initialize @a ttc structure for compression.
 * @a data is an array with size equal to @a size.
 * Check compression @a type and is @a data is
 * single msgpack field.
 */
int
tt_compression_init_for_compress(struct tt_compression *ttc,
                                 enum compression_type type,
                                 uint32_t size, const char *data);

/**
 * Destroy @a ttc structure and all associated
 * resources.
 */
void
tt_compression_destroy(struct tt_compression *ttc);

/**
 * Compress data from @a ttc structure and save new compressed data
 * in @a data and size in @a size. Return 0 if success, otherwise
 * return -1. It is caller responsibility to ensure that data has
 * enought bytes, but the maximum size that data can take after
 * compression is equal to @a ttc size.
 */
int
tt_compression_compress_data(const struct tt_compression *ttc, char *data,
                             uint32_t *size);

/**
 * Decompress @a data into data array in @a ttc structure. Return
 * 0 if success otherwise return -1.
 */
int
tt_compression_decompress_data(const char **data, uint32_t size,
                               struct tt_compression *ttc);

/**
 * Compare @a lhs and @a rhs structures. They are equal if there
 * size is equal and data stored in them is equal too.
 */
bool
tt_compression_is_equal(const struct tt_compression *lhs,
                        const struct tt_compression *rhs);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
