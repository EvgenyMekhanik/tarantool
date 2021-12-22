/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "mp_compression.h"
#include "msgpuck.h"
#include "mp_extension_types.h"
#include "mp_uuid.h"
#include "trivia/util.h"
#include "unit.h"
#include "random.h"
#include "msgpack.h"

#include <stdlib.h>

#define FIELD_SIZE_MAX 2048

struct field {
	char *data_end;
	char data_str[FIELD_SIZE_MAX];
};

static struct field
mp_encode_field(char *data, int type)
{
	struct field field;
	struct tt_uuid uuid;
	field.data_end = data;
	char tmp[FIELD_SIZE_MAX / 2];
	memset(tmp, 'X', sizeof(tmp));
	tmp[sizeof(tmp) - 1] = '\0';
	int ext_type;

	switch (type) {
	case MP_NIL:
		field.data_end = mp_encode_nil(field.data_end);
		strcpy(field.data_str, "null");
		break;
	case MP_UINT:
		field.data_end = mp_encode_uint(field.data_end, 123456789);
		strcpy(field.data_str, "123456789");
		break;
	case MP_INT:
		field.data_end = mp_encode_int(field.data_end, -123456789);
		strcpy(field.data_str, "-123456789");
		break;
	case MP_STR:
	case MP_BIN:
		if (type == MP_STR) {
			field.data_end = mp_encode_str(field.data_end,
						       tmp, strlen(tmp));
		} else {
			field.data_end = mp_encode_bin(field.data_end,
						       tmp, strlen(tmp));
		}
		strcpy(field.data_str, "\"");
		strcat(field.data_str, tmp);
		strcat(field.data_str, "\"");
		break;
	case MP_ARRAY:
		field.data_end = mp_encode_array(field.data_end, 3);
		field.data_end = mp_encode_uint(field.data_end, 212);
		field.data_end = mp_encode_str(field.data_end, tmp, strlen(tmp));
		field.data_end = mp_encode_nil(field.data_end);
		strcpy(field.data_str, "[212, ");
		strcat(field.data_str, "\"");
		strcat(field.data_str, tmp);
		strcat(field.data_str, "\"");
		strcat(field.data_str, ", null]");
		break;
	case MP_MAP:
		field.data_end = mp_encode_map(field.data_end, 3);
		field.data_end = mp_encode_str(field.data_end, "1", 1);
		field.data_end = mp_encode_uint(field.data_end, 212);
		field.data_end = mp_encode_uint(field.data_end, 1);
		field.data_end = mp_encode_str(field.data_end, tmp, strlen(tmp));
		field.data_end = mp_encode_uint(field.data_end, 3);
		field.data_end = mp_encode_nil(field.data_end);
		strcpy(field.data_str, "{\"1\": 212, 1: ");
		strcat(field.data_str, "\"");
		strcat(field.data_str, tmp);
		strcat(field.data_str, "\"");
		strcat(field.data_str, ", 3: null}");
		break;
	case MP_BOOL:
		field.data_end = mp_encode_bool(field.data_end, true);
		strcpy(field.data_str, "true");
		break;
	case MP_FLOAT:
		field.data_end = mp_encode_float(field.data_end, 1.375);
		strcpy(field.data_str, "1.375");
		break;
	case MP_DOUBLE:
		field.data_end = mp_encode_double(field.data_end, 1.375);
		strcpy(field.data_str, "1.375");
		break;
	/* MP_EXT */
	default:
		ext_type = type - MP_EXT;
		switch (ext_type) {
		case MP_UNKNOWN_EXTENSION:
			strcpy(field.data_end, tmp);
			field.data_end += strlen(tmp) + 1;
			strcpy(field.data_str, "88");
			break;
		case MP_DECIMAL:

		case MP_UUID:
			tt_uuid_create(&uuid);
			field.data_end = mp_encode_uuid(field.data_end, &uuid);
			strcpy(field.data_str, tt_uuid_str(&uuid));
			break;
		case MP_ERROR:

		case MP_COMPRESSION:
		default:
			break;
		}
		break;
	}
	return field;
}

static int
mp_compression_check_snprintf(char *data, const char *cdata, char *str)
{
	char data_str[FIELD_SIZE_MAX], ttc_data_str[FIELD_SIZE_MAX];
	plan(3);
	int s1 = mp_snprint(data_str, FIELD_SIZE_MAX, data);
	int s2 = mp_snprint(ttc_data_str, FIELD_SIZE_MAX, cdata);
	is(s1, s2, "string representation size");
	is(memcmp(data_str, ttc_data_str, s1),
	   0, "string representation");
	is(memcmp(data_str, str, s1),
	   0, "string representation");
	return check_plan();
}

static int
mp_compression_check_fprintf(char *data, const char *cdata, char *str)
{
	int rc;
	char data_str[FIELD_SIZE_MAX], ttc_data_str[FIELD_SIZE_MAX];
	plan(3);
	FILE *f1 = tmpfile();
	FILE *f2 = tmpfile();
	assert(f1 != NULL && f2 != NULL);
	int s1 = mp_fprint(f1, data);
	int s2 = mp_fprint(f2, cdata);
	rewind(f1);
	rewind(f2);
	is(s1, s2, "file representation size");
	rc = fread(data_str, 1, sizeof(data_str), f1);
	rc = fread(ttc_data_str, 1, sizeof(ttc_data_str), f2);
	is(memcmp(data_str, ttc_data_str, s1),
	   0, "file representation");
	is(memcmp(data_str, str, s1),
	   0, "string representation");
	fclose(f1);
	fclose(f2);
	return check_plan();

}

static int
mp_compression_check_decompression(const char *data, const size_t data_size,
				   const char *cdata)
{
	bool is_compressed = false;
	if (mp_typeof(*cdata) == MP_EXT) {
		const char *cdata_end = cdata;
		int8_t ext_type;
		mp_decode_extl(&cdata_end, &ext_type);
		if (ext_type == MP_COMPRESSION)
			is_compressed = true;
	}
	plan(is_compressed ? 4 : 1);
	if (is_compressed) {
		const char *cdata_end = cdata;
		size_t size = mp_decompress(NULL, 0, &cdata_end);
		is(size, data_size, "decompress data size");
		is(cdata_end, cdata, "decompress data size");
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
	plan ((MP_EXT + 2) * 3);

	char data[FIELD_SIZE_MAX];
	for (int type = MP_NIL; type <= MP_EXT + 1; type++) {
		struct field field = mp_encode_field(data, type);
		size_t size = field.data_end - data;
		char *cdata = xmalloc(size);
		mp_compress(cdata, size, data, size, compression_type, 5);
		mp_compression_check_snprintf(data, cdata, field.data_str);
		mp_compression_check_fprintf(data, cdata, field.data_str);
		mp_compression_check_decompression(data, size, cdata);
		free(cdata);
	}

	return check_plan();
}

int
main(void)
{
	plan(compression_type_MAX - COMPRESSION_TYPE_NONE - 1);
	random_init();
	msgpack_init();

        for (enum compression_type type = COMPRESSION_TYPE_NONE + 1;
             type < compression_type_MAX; type++) {
		mp_compression_test(type);
	}

	return check_plan();
}
