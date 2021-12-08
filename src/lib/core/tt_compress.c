/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "tt_compress.h"
#include "trivia/util.h"
#include "compression.h"

size_t
tt_compress_size(const struct tt_compress *ttc)
{
	switch (ttc->type) {
	case COMPRESSION_TYPE_NONE:
		return none_compress_size(ttc->data, ttc->data_end);
	case COMPRESSION_TYPE_ZSTD5:
		return zstd_compress_size(ttc->data, ttc->data_end, 5);
	default:
		;
	}
	return none_compress_size(ttc->data, ttc->data_end);
}

int
tt_compress_compress(const struct tt_compress *ttc, char *data, size_t *size)
{
	switch (ttc->type) {
	case COMPRESSION_TYPE_NONE:
		return none_compress(ttc->data, ttc->data_end, data, size);
	case COMPRESSION_TYPE_ZSTD5:
		return zstd_compress(ttc->data, ttc->data_end,
				     data, size, 5);
	default:
		;
	}
	return -1;
}

int
tt_compress_decompress(const char **data, uint32_t len,
		       struct tt_compress *ttc)
{
	switch (ttc->type) {
	case COMPRESSION_TYPE_NONE:
		return none_decompress(data, len, &ttc->data, &ttc->data_end);
	case COMPRESSION_TYPE_ZSTD5:
		return zstd_decompress(data, len, &ttc->data, &ttc->data_end);
	default:
		unreachable();
	}
	return -1;
}