/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "mp_compression.h"
#include "mp_extension_types.h"
#include "msgpuck.h"
#include "tt_compression.h"
#include "trivia/util.h"

size_t
mp_decompress_raw(const char **src, uint32_t src_len, const char *svp,
		  char *dst, size_t dst_size)
{
	enum compression_type type = mp_decode_uint(src);
	size_t size = mp_decode_uint(src);
	if (dst_size == 0) {
		*src = svp;
		return size;
	}
	if (!tt_check_compression_type(type)) {
		*src = svp;
		return 0;
	}
	src_len -= mp_sizeof_uint(type) + mp_sizeof_uint(size);
	size_t real_size = tt_decompress(dst, dst_size, *src, src_len, type);
	if (real_size != size) {
		*src = svp;
		return 0;
	}
	*src += real_size;
	return real_size;
}

char *
mp_compress(char *dst, const char *src, size_t src_size,
	    enum compression_type type, uint32_t level)
{
	if (!tt_check_compression_type(type))
		goto no_compression;
	size_t size = tt_compress(dst, src_size, src, src_size, type, level);
	if (size == 0)
		goto no_compression;
	/*
	 * If compressed data size with MP_EXT header and meta information is
	 * greater or equal to @a src_size, just copy @a src to @a dst.
	 */
	uint32_t extra_size = mp_sizeof_uint(type) + mp_sizeof_uint(src_size);
	if (mp_sizeof_ext(size + extra_size) >= src_size)
		goto no_compression;
	memmove(dst + mp_sizeof_extl(size + extra_size) + extra_size, dst, size);
	dst = mp_encode_extl(dst, MP_COMPRESSION, size + extra_size);
	dst = mp_encode_uint(dst, type);
	dst = mp_encode_uint(dst, src_size);
	return dst + size;

no_compression:
	memcpy(dst, src, src_size);
	return dst + src_size;
}

size_t
mp_decompress(const char **src, char *dst, size_t dst_size)
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
	return mp_decompress_raw(src, len, svp, dst, dst_size);
}

int
mp_snprint_compression(char *buf, int size, const char **data, uint32_t len)
{
	const char *const svp = *data;
	size_t dst_size = mp_decompress_raw(data, len, svp, NULL, 0);
	char *dst = xmalloc(dst_size);

	if (mp_decompress_raw(data, len, svp, dst, dst_size) == 0) {
		*data = svp;
		free(dst);
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
	free(dst);
	return rc;
}

int
mp_fprint_compression(FILE *file, const char **data, uint32_t len)
{
	const char *const svp = *data;
	size_t dst_size = mp_decompress_raw(data, len, svp, NULL, 0);
	char *dst = xmalloc(dst_size);

	if (mp_decompress_raw(data, len, svp, dst, dst_size) == 0) {
		*data = svp;
		free(dst);
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
	free(dst);
	return rc;
}
