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

static inline int64_t
mp_sizeof_for_compression_raw(const struct tt_compression *ttc)
{
	int64_t size = tt_compression_compressed_data_size(ttc);
	if (size < 0)
		return size;
	return 5 + size;
}

int64_t
compression_unpack_size(const char **data)
{
	const char *d = *data;
	mp_load_u8(&d);
	return mp_load_u32(&d);
}

int64_t
mp_sizeof_for_compression(const struct tt_compression *ttc)
{
	int64_t size = mp_sizeof_for_compression_raw(ttc);
	if (size < 0)
		return size;
	return mp_sizeof_ext(size);
}

int64_t
mp_sizeof_for_decompression(const char **data)
{
       if (mp_typeof(**data) != MP_EXT)
		return -1;
	int8_t type;
	const char *d = *data;
	mp_decode_extl(&d, &type);
	if (type != MP_COMPRESSION)
		return -1;
	return compression_unpack_size(&d);
}

char *
compression_pack(char *data, const struct tt_compression *ttc)
{
	char *const svp = data;
	data = mp_store_u8(data, ttc->type);
	data = mp_store_u32(data, ttc->size);
	uint32_t size;
	if (tt_compression_compress_data(ttc, data, &size) != 0) {
		data = svp;
		return NULL;
	}
	return data + size;
}

struct tt_compression *
compression_unpack(const char **data, uint32_t len, struct tt_compression *ttc)
{
	const char *const svp = *data;
	ttc->type = mp_load_u8(data);
	ttc->size = mp_load_u32(data);
	len -= *data - svp;
	if (tt_compression_decompress_data(data, len, ttc) != 0) {
		*data = svp;
		return NULL;
	}
	return ttc;
}

struct tt_compression *
mp_decode_compression(const char **data, struct tt_compression *ttc)
{
	if (mp_typeof(**data) != MP_EXT)
		return NULL;
	int8_t type;
	const char *const svp = *data;
	uint32_t len = mp_decode_extl(data, &type);
	if (type != MP_COMPRESSION ||
	    compression_unpack(data, len, ttc) == NULL) {
		*data = svp;
		return NULL;
	}
	return ttc;
}

char *
mp_encode_compression(char *data, const struct tt_compression *ttc)
{
	int64_t size = mp_sizeof_for_compression_raw(ttc);
	if (size < 0)
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
	(void)buf;
	(void)size;
	(void)data;
	(void)len;
	return 0;
}

int
mp_fprint_compression(FILE *file, const char **data, uint32_t len)
{
	(void)file;
	(void)data;
	(void)len;
	return 0;
}
