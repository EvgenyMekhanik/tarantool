/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "msgpuck.h"

#include "mp_extension_types.h"
#include "mp_decimal.h"
#include "mp_uuid.h"
#include "mp_error.h"
#include "mp_datetime.h"
#include "mp_compression.h"

#include "diag.h"
#include "error.h"
#include "trivia/util.h"
#include "unit.h"
#include "random.h"
#include "msgpack.h"
#include "fiber.h"
#include "memory.h"

#include "zstd.h"

#include <stdlib.h>


#define FIELD_SIZE_MAX 2048 * 8

static void
rand_string(char *str, size_t len)
{
	for (size_t i = 0; i < len - 1; i++)
		str[i] = 'a' + rand() % ('z' - 'a');
	str[len - 1] = '\0';
}

static void
rand_bin(char *bin, size_t len)
{
	for (size_t i = 0; i < len; i++)
		bin[i] = rand() % 2;
}

static char *
mp_encode_TYPE(char *data, int type, int compression_type)
{
	char *data_end = data;
	decimal_t decimal;
	struct tt_uuid uuid;
	struct error *error;
	struct datetime now;
	char buf[FIELD_SIZE_MAX / 2];
	int ext_type;
	char *end = buf;

	switch (type) {
	case MP_NIL:
		data_end = mp_encode_nil(data_end);
		break;
	case MP_UINT:
		data_end = mp_encode_uint(data_end, rand());
		break;
	case MP_INT:
		data_end = mp_encode_int(data_end, -rand());
		break;
	case MP_STR:
		rand_string(buf, sizeof(buf));
		data_end = mp_encode_str(data_end, buf, strlen(buf));
		break;
	case MP_BIN:
		rand_bin(buf, sizeof(buf));
		data_end = mp_encode_bin(data_end, buf, strlen(buf));
		break;
	case MP_ARRAY:
		data_end = mp_encode_array(data_end, 3);
		data_end = mp_encode_uint(data_end, rand());
		rand_string(buf, sizeof(buf));
		data_end = mp_encode_str(data_end, buf, strlen(buf));
		data_end = mp_encode_nil(data_end);
		break;
	case MP_MAP:
		data_end = mp_encode_map(data_end, 3);
		data_end = mp_encode_str(data_end, "1", 1);
		data_end = mp_encode_uint(data_end, 212);
		data_end = mp_encode_uint(data_end, 1);
		rand_string(buf, sizeof(buf));
		data_end = mp_encode_str(data_end, buf, strlen(buf));
		data_end = mp_encode_uint(data_end, 3);
		data_end = mp_encode_nil(data_end);
		break;
	case MP_BOOL:
		data_end = mp_encode_bool(data_end, rand() % 2);
		break;
	case MP_FLOAT:
		data_end = mp_encode_float(data_end, rand() / 1.12345);
		break;
	case MP_DOUBLE:
		data_end = mp_encode_double(data_end, rand() / 1.12345);
		break;
	/* MP_EXT */
	default:
		ext_type = type - MP_EXT;
		switch (ext_type) {
		case MP_UNKNOWN_EXTENSION:
			rand_string(buf, sizeof(buf));
			data_end = mp_encode_ext(data_end, MP_UNKNOWN_EXTENSION,
						 buf, strlen(buf));
			break;
		case MP_DECIMAL:
			decimal_from_double(&decimal, rand() / 1.12345);
			data_end = mp_encode_decimal(data_end, &decimal);
			break;
		case MP_UUID:
			tt_uuid_create(&uuid);
			data_end = mp_encode_uuid(data_end, &uuid);
			break;
		case MP_ERROR:
			diag_set(ClientError, ER_INJECTION, "compression");
			error = diag_last_error(diag_get());
			data_end = mp_encode_error(data_end, error);
			diag_clear(diag_get());
			break;
		case MP_DATETIME:
			datetime_now(&now);
			data_end = mp_encode_datetime(data_end, &now);
			break;
		case MP_COMPRESSION:
			end = mp_encode_TYPE(buf, rand() % 2 ? MP_BIN : MP_STR,
					     compression_type);
			data_end = mp_compress(data_end, FIELD_SIZE_MAX, buf,
					       end - buf, compression_type, 1);
			break;
		default:
			/* No msgpack just copy string */
			rand_string(buf, sizeof(buf));
			strcpy(data, buf);
			data_end += strlen(buf) + 1;
			break;
		}
		break;
	}
	return data_end;
}

static int
mp_compression_check_snprintf(char *data, const char *cdata)
{
	char data_str[FIELD_SIZE_MAX], cdata_str[FIELD_SIZE_MAX];
	plan(2);
	int s1 = mp_snprint(data_str, FIELD_SIZE_MAX, data);
	int s2 = mp_snprint(cdata_str, FIELD_SIZE_MAX, cdata);
	is(s1, s2, "string representation size");
	is(memcmp(data_str, cdata_str, s1),
	   0, "string representation");
	return check_plan();
}

static int
mp_compression_check_fprintf(char *data, const char *cdata)
{
	int rc;
	char data_str[FIELD_SIZE_MAX], cdata_str[FIELD_SIZE_MAX];
	plan(2);
	FILE *f1 = tmpfile();
	FILE *f2 = tmpfile();
	assert(f1 != NULL && f2 != NULL);
	int s1 = mp_fprint(f1, data);
	int s2 = mp_fprint(f2, cdata);
	rewind(f1);
	rewind(f2);
	is(s1, s2, "file representation size");
	rc = fread(data_str, 1, sizeof(data_str), f1);
	rc = fread(cdata_str, 1, sizeof(cdata_str), f2);
	is(memcmp(data_str, cdata_str, s1),
	   0, "file representation");
	fclose(f1);
	fclose(f2);
	return check_plan();
}

static int
mp_compression_check_decompression(const char *data, const size_t data_size,
				   const char *cdata, bool is_compressed)
{
	plan(is_compressed ? 3 : 1);
	if (is_compressed) {
		const char *cdata_end = cdata;
		size_t size = mp_decompress(NULL, 0, &cdata_end);
		is(size, data_size, "decompress data size");
		char *ddata = xmalloc(size);
		is(mp_decompress(ddata, size, &cdata_end), size,
		   "decompression");
		is(memcmp(data, ddata, data_size),
		   0, "compression/decompression");
		free(ddata);
	} else {
		is(memcmp(data, cdata, data_size),
		   0, "none compression/decompression");
	}
	return check_plan();
}

static int
mp_compression_test(enum compression_type compression_type)
{
	plan ((MP_EXT + mp_extension_type_MAX + 1) * 4);

	char data[FIELD_SIZE_MAX];
	for (int type = MP_NIL; type <= MP_EXT + mp_extension_type_MAX;
	     type++) {
		char *data_end = mp_encode_TYPE(data, type, compression_type);
		size_t size = data_end - data;
		char *cdata = xmalloc(size);
		char *cdata_end = mp_compress(cdata, size, data, size,
					      compression_type,
					      ZSTD_maxCLevel());
		size_t csize = cdata_end - cdata;
		is (csize <= size, true, "compressed data size");
		bool is_compressed = (csize < size ? true : false);
		mp_compression_check_snprintf(data, cdata);
		mp_compression_check_fprintf(data, cdata);
		mp_compression_check_decompression(data, size, cdata,
						   is_compressed);
		free(cdata);
	}

	return check_plan();
}

int
main(void)
{
	plan(compression_type_MAX - COMPRESSION_TYPE_NONE - 1);
	msgpack_init();
	random_init();
	memory_init();
	fiber_init(fiber_c_invoke);

        for (enum compression_type type = COMPRESSION_TYPE_NONE + 1;
             type < compression_type_MAX; type++) {
		mp_compression_test(type);
	}

	fiber_free();
	memory_free();
	random_free();

	return check_plan();
}
