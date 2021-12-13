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

static inline int
mp_sizeof_for_compression_raw(const struct tt_compression *ttc, uint32_t *size)
{
	if (tt_compression_compressed_data_size((struct tt_compression *)ttc,
						     size) < 0)
		return -1;
	*size = 5 + *size;
	return 0;
}

int
mp_sizeof_for_compression(const struct tt_compression *ttc, uint32_t *size)
{
	if (mp_sizeof_for_compression_raw(ttc, size) < 0)
		return -1;
	*size = mp_sizeof_ext(*size);
	return 0;
}

int
mp_sizeof_for_decompression(const char **data, uint32_t *size)
{
       if (mp_typeof(**data) != MP_EXT)
		return -1;
	int8_t type;
	const char *d = *data;
	mp_decode_extl(&d, &type);
	if (type != MP_COMPRESSION)
		return -1;
	mp_load_u8(&d);
	*size = mp_load_u32(&d);
	return 0;
}

static char *
compression_pack(char *data, const struct tt_compression *ttc)
{
	char *const svp = data;
	data = mp_store_u8(data, ttc->type);
	data = mp_store_u32(data, ttc->size);
	uint32_t size;
	if (tt_compression_compress_data((struct tt_compression *)ttc,
					     data, &size) != 0) {
		data = svp;
		return NULL;
	}
	return data + size;
}

static int
compression_unpack(const char **data, uint32_t len, struct tt_compression *ttc)
{
	const char *const svp = *data;
	ttc->type = mp_load_u8(data);
	size_t size = mp_load_u32(data);
	len -= *data - svp;
	if (tt_compression_decompress_data(data, len, ttc) != 0 ||
	    ttc->size != size) {
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
	uint32_t size;
	if (mp_sizeof_for_compression_raw(ttc, &size) < 0)
		return NULL;
	char *const svp = data;
	data = mp_encode_extl(data, MP_COMPRESSION, size);
	data = compression_pack(data, ttc);
	if (data == NULL) {
		data = svp;
		return NULL;
	}
	return data;
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
