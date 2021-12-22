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

char *
mp_compress(char *dst, size_t dst_size, const char *src, size_t src_size,
            enum compression_type type, uint32_t level)
{
	assert(dst_size >= src_size);
	size_t size = tt_compress(dst, dst_size, src, src_size, type, level);
	/*
	 * If error occurs or compressed data size with MP_EXT header
	 * and meta information is greater or equal to @a src_size, just
	 * copy @a src to @a dst.
	 */
	if (size == 0 || mp_sizeof_ext(size + 9) >= src_size) {
		memcpy(dst, src, src_size);
		return dst + src_size;
	}
	memmove(dst + mp_sizeof_extl(size + 9) + 9, dst, size);
	dst = mp_encode_extl(dst, MP_COMPRESSION, size + 9);
	dst = mp_store_u8(dst, type);
	dst = mp_store_u64(dst, size);
	return dst + size;
}

size_t
mp_decompress(char *dst, size_t dst_size, const char **src)
{
	if (mp_typeof(**src) != MP_EXT)
		return 0;
	int8_t ext_type;
	const char *const svp = *src;
	uint32_t len = mp_decode_extl(src, &ext_type);
	if (ext_type != MP_COMPRESSION) {
		*src = svp;
		return 0;
	}
	enum compression_type type = mp_load_u8(src);
	size_t size = mp_load_u64(src);
	if (dst_size == 0) {
		*src = svp;
		return size;
	}
	len -= *src - svp;
	size_t real_size = tt_decompress(dst, dst_size, *src, len, type);
	if (real_size != size) {
		*src = svp;
		return 0;
	}
	*src += real_size;
	return real_size;
}

int
mp_snprint_compression(char *buf, int size, const char **data, uint32_t len)
{
	const char *const svp = *data;
	enum compression_type type = mp_load_u8(data);
	size_t dst_size = mp_load_u64(data);
	uint32_t used = region_used(&fiber()->gc);
	void *dst = region_alloc(&fiber()->gc, dst_size);
	if (dst == NULL) {
		*data = svp;
		return -1;
	}
	len -= *data - svp;
	size_t real_size = tt_decompress(dst, dst_size, *data, len, type);
	if (real_size != dst_size) {
		region_truncate(&fiber()->gc, used);
		*data = svp;
		return -1;
	}
	int rc = 0;
	const char *d = dst;
	if (mp_check(&d, d + dst_size) != 0) {
		*data = svp;
		rc = mp_snprint_ext_default(buf, size, data, MP_PRINT_MAX_DEPTH);
	} else {
		rc = mp_snprint(buf, size, dst);
	}
	region_truncate(&fiber()->gc, used);
	return rc;
}

int
mp_fprint_compression(FILE *file, const char **data, uint32_t len)
{
	const char *const svp = *data;
	enum compression_type type = mp_load_u8(data);
	size_t dst_size = mp_load_u64(data);
	uint32_t used = region_used(&fiber()->gc);
	void *dst = region_alloc(&fiber()->gc, dst_size);
	if (dst == NULL) {
		*data = svp;
		return -1;
	}
	len -= *data - svp;
	size_t real_size = tt_decompress(dst, dst_size, *data, len, type);
	if (real_size != dst_size) {
		region_truncate(&fiber()->gc, used);
		*data = svp;
		return -1;
	}
	int rc = 0;
	const char *d = dst;
	if (mp_check(&d, d + dst_size) != 0) {
		*data = svp;
		rc = mp_fprint_ext_default(file, data, MP_PRINT_MAX_DEPTH);
	} else {
		rc = mp_fprint(file, dst);
	}
	region_truncate(&fiber()->gc, used);
	return rc;
}
