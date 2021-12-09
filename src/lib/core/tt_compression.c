/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "tt_compression.h"
#include "trivia/util.h"
#include "compression.h"

void
tt_compression_create(struct tt_compression *ttc, char *data,
		      uint32_t size, int type)
{
	ttc->type = type;
	ttc->size = size;
	ttc->data = data;
}

int64_t
tt_compression_compressed_data_size(const struct tt_compression *ttc)
{
	switch (ttc->type) {
	case COMPRESSION_TYPE_NONE:
		return none_compressed_data_size(ttc->size);
	case COMPRESSION_TYPE_ZSTD5:
		return zstd_compressed_data_size(ttc->data, ttc->size, 5);
	default:
		;
	}
	return -1;
}

int
tt_compression_compress_data(const struct tt_compression *ttc,
                             char *data, uint32_t *size)
{
	switch (ttc->type) {
	case COMPRESSION_TYPE_NONE:
		return none_compress_data(ttc->data, ttc->size,
					  data, size);
	case COMPRESSION_TYPE_ZSTD5:
		return zstd_compress_data(ttc->data, ttc->size,
					  data, size, 5);
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
	case COMPRESSION_TYPE_NONE:
		return none_decompress_data(data, size, ttc->data);
	case COMPRESSION_TYPE_ZSTD5:
		return zstd_decompress_data(data, size, ttc->data);
	default:
		;
	}
	return -1;
}