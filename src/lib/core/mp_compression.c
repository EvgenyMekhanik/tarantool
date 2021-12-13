/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "mp_compression.h"
#include "mp_extension_types.h"
#include "msgpuck.h"
#include "tt_compression.h"
#include "fiber.h"
#include <small/region.h>

uint32_t
mp_sizeof_compression_max(const struct tt_compression *ttc)
{
	return mp_sizeof_ext(ttc->size + 1);
}

int
compression_unpack(const char **data, uint32_t len,
		   struct tt_compression *ttc)
{
	const char *const svp = *data;
	ttc->type = mp_load_u8(data);
	len -= *data - svp;
	if (tt_compression_decompress_data(data, len, ttc) != 0) {
		*data = svp;
		return -1;
	}
	return 0;
}

int
mp_decode_compression(const char **data, struct tt_compression *ttc)
{
	if (mp_typeof(**data) != MP_EXT)
		return -1;
	int8_t type;
	const char *const svp = *data;
	uint32_t len = mp_decode_extl(data, &type);
	if (type != MP_COMPRESSION ||
	    compression_unpack(data, len, ttc) != 0) {
		*data = svp;
		return -1;
	}
	return 0;
}

char *
mp_encode_compression(char *data, const struct tt_compression *ttc)
{
	/*
	 * Compressed data size should be less than or equal to
	 * uncompressed data size.
	 */
	uint32_t used = region_used(&fiber()->gc);
	char *tmp = region_alloc(&fiber()->gc, ttc->size);
	if (tmp == NULL)
		return NULL;
	uint32_t size;
	if (tt_compression_compress_data(ttc, tmp, &size) != 0)
		return NULL;
	assert(size <= ttc->size);
	data = mp_encode_extl(data, MP_COMPRESSION, size + 1);
	/*
	 * If compressed data size is equal to uncompressed data size,
	 * it's means that compression increases data size and we don't
	 * use it.
	 */
	enum compression_type type =
		size < ttc->size ? ttc->type : COMPRESSION_TYPE_NONE;
	data = mp_store_u8(data, type);
	memcpy(data, tmp, size);
	region_truncate(&fiber()->gc, used);
	return data + size;
}

int
mp_snprint_compression(char *buf, int size, const char **data, uint32_t len)
{
	struct tt_compression ttc;
	tt_compression_create(&ttc);
	int rc = 0;
	if (compression_unpack(data, len, &ttc) != 0) {
		rc = -1;
		goto finish;
	}
	rc = mp_snprint(buf, size, ttc.data);
finish:
	tt_compression_destroy(&ttc);
	return rc;
}

int
mp_fprint_compression(FILE *file, const char **data, uint32_t len)
{
	struct tt_compression ttc;
	tt_compression_create(&ttc);
	int rc = 0;
	if (compression_unpack(data, len, &ttc) != 0) {
		rc = -1;
		goto finish;
	}
	rc = mp_fprint(file, ttc.data);
finish:
	tt_compression_destroy(&ttc);
	return rc;
}
