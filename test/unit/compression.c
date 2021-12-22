/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "msgpuck.h"

#include "mp_compression.h"
#include "mp_extension_types.h"
#include "trivia/util.h"
#include "unit.h"

#include "zstd.h"

#include <stdlib.h>

static int
test_mp_decompress(const char *data, const size_t data_size, const char *cdata)
{
	plan(3);

	const char *cdata_end = cdata;
	size_t size = mp_decompress(&cdata_end, NULL, 0);
	is(size, data_size, "decompress data size");
	char *ddata = xmalloc(size);
	is(mp_decompress(&cdata_end, ddata, size), size, "mp_decompress");
	is(memcmp(data, ddata, data_size), 0, "compression/decompression");
	free(ddata);

	return check_plan();
}

static int
test_mp_compress_small(enum compression_type compression_type)
{
	plan(2);

	char data[9];
	char *data_end = data;
	data_end = mp_encode_uint(data_end, 123456);

	char *cdata = xmalloc(data_end - data);

	char *cdata_end = mp_compress(cdata, data, data_end - data,
				      compression_type, ZSTD_maxCLevel());
	is(cdata_end - cdata == data_end - data, true, "mp_compress");
	is(memcmp(data, cdata, data_end - data), 0, "no compression");

	free(cdata);

	return check_plan();
}

static int
test_mp_compress_large(enum compression_type compression_type)
{
	plan(2);

	char source[512], data[1024];
	memset(source, 'a', sizeof(source) - 1);
	source[sizeof(source) - 1] = '\0';
	char *data_end = data;
	data_end = mp_encode_str(data_end, source, strlen(source));

	char *cdata = xmalloc(data_end - data);

	char *cdata_end = mp_compress(cdata, data, data_end - data,
				      compression_type, ZSTD_maxCLevel());
	is(cdata_end - cdata < data_end - data, true, "mp_compress");
	test_mp_decompress(data, data_end - data, cdata);

	free(cdata);

	return check_plan();
}

static int
test_mp_compress_errors(void)
{
	plan(4);

	char source[512], data[1024];
	memset(source, 'a', sizeof(source) - 1);
	source[sizeof(source) - 1] = '\0';
	char *data_end = data;
	data_end = mp_encode_str(data_end, source, strlen(source));

	/*
	 * In case of errors during compression, we just save
	 * uncompressed data.
	 */
	char *cdata = xmalloc(data_end - data);
	char *cdata_end;

	/* Invalid compression type */
	cdata_end = mp_compress(cdata, data, data_end - data,
				      compression_type_MAX, ZSTD_maxCLevel());

	is(cdata_end - cdata == data_end - data, true, "mp_compress");
	is(memcmp(data, cdata, data_end - data), 0, "no compression");

	/* Invalid compression size */
	cdata_end = mp_compress(cdata, data, 1, COMPRESSION_TYPE_ZSTD,
				ZSTD_maxCLevel());
	is(cdata_end - cdata == 1, true, "mp_compress");
	is(memcmp(data, cdata, data_end - data), 0, "no compression");

	return check_plan();
}

static int
test_mp_decompress_errors(void)
{
	plan(4);

	char source[512], data[1024], ddata[1024];
	memset(source, 'a', sizeof(source) - 1);
	source[sizeof(source) - 1] = '\0';

	char *data_end = data;
	data_end = mp_encode_str(data_end, source, strlen(source));
	/* Compressed data should have MP_EXT header. */
	is(mp_decompress((const char **)&data_end, ddata, sizeof(ddata)),
	   0, "mp_decompress");

	data_end = data;
	data_end = mp_encode_ext(data_end, MP_UNKNOWN_EXTENSION,
				 source, strlen(source));
	/* MP_EXT should have MP_COMPRESSION type */
	is(mp_decompress((const char **)&data_end, ddata, sizeof(ddata)),
	   0, "mp_decompress");

	data_end = data;
	data_end = mp_encode_str(data_end, source, strlen(source));

	const char *d;
	char *cdata = xmalloc(data_end - data);
	char *cdata_end = mp_compress(cdata, data, data_end - data,
				      COMPRESSION_TYPE_ZSTD, ZSTD_maxCLevel());
	d = cdata;
	/* Too small size of dst buffer */
	is(mp_decompress(&d, ddata, 1),
	   0, "mp_decompress");

	cdata_end[-2] = 177;
	d = cdata;
	/* Corrupt data */
	is(mp_decompress(&d, ddata, sizeof(ddata)),
	   0, "mp_decompress");

	return check_plan();
}

int
main(void)
{
	plan(compression_type_MAX - COMPRESSION_TYPE_NONE - 1 + 2);

        for (enum compression_type type = COMPRESSION_TYPE_NONE + 1;
             type < compression_type_MAX; type++) {
		test_mp_compress_small(type);
		test_mp_compress_large(type);
	}

	test_mp_compress_errors();
	test_mp_decompress_errors();

	return check_plan();
}
