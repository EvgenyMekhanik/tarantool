/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "tt_compression.h"
#include "diag.h"
#include "fiber.h"
#include "trivia/util.h"
#include "msgpuck.h"
#include <zstd.h>


/**
 * Calculate the @a new_data_size that will take @a data array
 *from @a ttc structure after "zstd" compression with @a level.
 * Return 0 if success, otherwise return -1.
 */
static int
zstd_compressed_data_size(struct tt_compression *ttc, int level,
			  uint32_t *new_data_size)
{
	size_t max_size = ZSTD_compressBound(ttc->size);
	if (max_size > UINT32_MAX) {
		diag_set(CompressionError, "ZSTD_compressBound failed: %s\n",
			 "too big data for compression");
		return -1;
	}
	uint32_t used = region_used(&ttc->region);
	void *tmp = region_alloc(&ttc->region, max_size);
	if (tmp == NULL) {
		diag_set(OutOfMemory, max_size,
			 "region_alloc", "compression");
		return -1;
	}
	size_t real_size = ZSTD_compress(tmp, max_size, ttc->data,
					 ttc->size, level);
	region_truncate(&ttc->region, used);
	if (ZSTD_isError(real_size)) {
		diag_set(CompressionError, "ZSTD_compress failed: %s\n",
			 ZSTD_getErrorName(real_size));
		return -1;
	}
	assert(real_size <= UINT32_MAX);
	*new_data_size = real_size;
	return 0;
}

/**
 * Compress data array from @a ttc structure into @a new_data array
 * according to the "zstd" algorithm with @a level. Save @a new_data
 * array size in @a new_data_size. Return 0 if success, otherwise
 * return -1.
 */

static int
zstd_compress_data(struct tt_compression *ttc, char *new_data,
		   uint32_t *new_data_size, int level)
{
	size_t max_size = ZSTD_compressBound(ttc->size);
	if (max_size > UINT32_MAX) {
		diag_set(CompressionError, "ZSTD_compressBound failed: %s\n",
			 "too big data for compression");
			 return -1;
	}
	uint32_t used = region_used(&ttc->region);
	void *tmp = region_alloc(&ttc->region, max_size);
	if (tmp == NULL) {
		diag_set(OutOfMemory, max_size,
			 "region_alloc", "compression");
		return -1;
	}
	size_t real_size = ZSTD_compress(tmp, max_size, ttc->data,
					 ttc->size, level);
	if (ZSTD_isError(real_size)) {
		diag_set(CompressionError, "ZSTD_compress failed: %s\n",
			 ZSTD_getErrorName(real_size));
		region_truncate(&ttc->region, used);
		return -1;
	}
	assert(real_size <= UINT32_MAX);
	*new_data_size = real_size;
	memcpy(new_data, tmp, *new_data_size);
	region_truncate(&ttc->region, used);
	return 0;
}

/**
 * Decompress @data array with size @a data_size into data array
 * in @a ttc structure according to "zstd" algorithm. Return 0 if
 * success, otherwise return -1.
 */
int
zstd_decompress_data(const char **data, const uint32_t data_size,
                     struct tt_compression *ttc)
{
	size_t max_size = ZSTD_getFrameContentSize(*data, data_size);
	if (ZSTD_isError(max_size)) {
		diag_set(CompressionError,
			 "ZSTD_getFrameContentSize failed: %s\n",
			 ZSTD_getErrorName(max_size));
		return -1;
	}
	/* Checked during compression */
	assert(max_size <= UINT32_MAX);
	uint32_t used = region_used(&ttc->region);
	ttc->data = region_alloc(&ttc->region, max_size);
	if (ttc->data == NULL) {
		diag_set(OutOfMemory, max_size,
			 "region_alloc", "compression");
		return -1;
	}
	ttc->size = ZSTD_decompress(ttc->data, max_size, *data, data_size);
	if (ZSTD_isError(ttc->size)) {
		diag_set(CompressionError, "ZSTD_decompress failed: %s\n",
			 ZSTD_getErrorName(ttc->size));
		region_truncate(&ttc->region, used);
		return -1;
	}
	assert(ttc->size <= UINT32_MAX);
	region_truncate(&ttc->region, used + ttc->size);
	*data += data_size;
	return 0;
}

struct tt_compression *
tt_compression_new(void)
{
	struct tt_compression *ttc = xmalloc(sizeof(struct tt_compression));
	tt_compression_create(ttc);
	return ttc;
}

void
tt_compression_delete(struct tt_compression *ttc)
{
	tt_compression_destroy(ttc);
	TRASH(ttc);
	free(ttc);
}

void
tt_compression_create(struct tt_compression *ttc)
{
	region_create(&ttc->region, &cord()->slabc);
	ttc->type = COMPRESSION_TYPE_NONE;
	ttc->size = 0;
	ttc->data = NULL;
}

void
tt_compression_destroy(struct tt_compression *ttc)
{
	region_destroy(&ttc->region);
}

int
tt_compression_init_for_compress(struct tt_compression *ttc,
                                 enum compression_type type,
                                 uint32_t size, char *data)
{
	if (!(type > COMPRESSION_TYPE_NONE && type < compression_type_MAX)) {
	    	diag_set(IllegalParams, "invaalid compression type");
		return -1;
	}
	const char *d  = data;
	mp_next(&d);
	if (d != data + size) {
		diag_set(IllegalParams, "data for compression should be "
			 "single msgpack field");
		return -1;
	}
	ttc->type = type;
	ttc->size = size;
	ttc->data = data;
	return 0;
}

int
tt_compression_compressed_data_size(struct tt_compression *ttc,
				    uint32_t *size)
{
	switch (ttc->type) {
	case COMPRESSION_TYPE_ZSTD5:
		return zstd_compressed_data_size(ttc, 5, size);
	default:
		diag_set(IllegalParams, "invaalid compression type");
	}
	return -1;
}

int
tt_compression_compress_data(struct tt_compression *ttc, char *data,
			     uint32_t *size)
{
	switch (ttc->type) {
	case COMPRESSION_TYPE_ZSTD5:
		return zstd_compress_data(ttc, data, size, 5);
	default:
		;
	}
	return -1;
}

int
tt_compression_decompress_data(const char **data, uint32_t size,
                               struct tt_compression *ttc)
{
	switch (ttc->type) {
	case COMPRESSION_TYPE_ZSTD5:
		return zstd_decompress_data(data, size, ttc);
	default:
		diag_set(IllegalParams, "invaalid compression type");
	}
	return -1;
}
