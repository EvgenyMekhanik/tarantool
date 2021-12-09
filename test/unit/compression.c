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
#include "memory.h"
#include "fiber.h"

#include <stdlib.h>

#define FIELD_SIZE_MAX 100

static size_t
field_size_max(enum mp_type type)
{
	switch (type) {
	case MP_NIL:
		return mp_sizeof_nil();
	case MP_UINT:
	case MP_INT:
		return 9;
	case MP_STR:
	case MP_BIN:
	case MP_ARRAY:
	case MP_MAP:
		return FIELD_SIZE_MAX;
	case MP_BOOL:
		return 1;
	case MP_FLOAT:
		return 5;
	case MP_DOUBLE:
		return 9;
	case MP_EXT:
		return mp_sizeof_uuid();
	default:
		;
	}
	abort();
}

static char *
mp_encode_random_str(char *data)
{
        char field[FIELD_SIZE_MAX - 5];
        for (uint32_t i = 0; i < FIELD_SIZE_MAX - 5; i++)
                field[i] = rand() % 128;
        return mp_encode_str(data, field, FIELD_SIZE_MAX - 5);
}

static char *
mp_encode_random_bin(char *data)
{
        char field[FIELD_SIZE_MAX - 5];
        for (uint32_t i = 0; i < FIELD_SIZE_MAX - 5; i++)
                field[i] = rand() % 255;
        return mp_encode_bin(data, field, FIELD_SIZE_MAX - 5);
}

static char *
mp_encode_random_array(char *data)
{
        uint32_t total_field_count = (FIELD_SIZE_MAX - 5) / 9;
        char *data_end = data;
        data_end = mp_encode_array(data_end, total_field_count);
        for (uint32_t i = 0; i < total_field_count; i++)
                data_end = mp_encode_uint(data_end, rand());
        return data_end;
}

static char *
mp_encode_random_map(char *data)
{
	uint32_t total_field_count = (FIELD_SIZE_MAX - 5) / (2 * 9);
	char *data_end = data;
	data_end = mp_encode_map(data_end, total_field_count);
	for (uint32_t i = 0; i < total_field_count; i++) {
		data_end = mp_encode_uint(data_end, rand());
		data_end = mp_encode_uint(data_end, rand());
	}
	return data_end;
}

static char *
mp_encode_random_ext(char *data)
{
	struct tt_uuid uuid;
	tt_uuid_create(&uuid);
	return mp_encode_uuid(data, &uuid);
}

static char *
mp_encode_random_field(char *data, enum mp_type type)
{
	switch (type) {
	case MP_NIL:
		return mp_encode_nil(data);
	case MP_UINT:
		return mp_encode_uint(data, rand());
	case MP_INT:
		return mp_encode_int(data, -rand());
	case MP_STR:
		return mp_encode_random_str(data);
	case MP_BIN:
		return mp_encode_random_bin(data);
	case MP_ARRAY:
		return mp_encode_random_array(data);
	case MP_MAP:
		return mp_encode_random_map(data);
	case MP_BOOL:
		return mp_encode_bool(data, rand() % 2);
	case MP_FLOAT:
		return mp_encode_float(data, rand() / 1.375);
	case MP_DOUBLE:
		return mp_encode_double(data, rand() / 1.375);
	case MP_EXT:
		return mp_encode_random_ext(data);
	default:;
	}
	abort();
}

int
check_mp_compression(int type)
{
	/** MP_ARRAY header max size */
	size_t total_size = 5;
	unsigned field_count = 0;
	for (enum mp_type type = MP_NIL; type <= MP_EXT; type++, field_count++)
		total_size += field_size_max(type);
	char *data = xcalloc(1, total_size);
	char *data_end = data;
	data_end = mp_encode_array(data_end, field_count);
	for (enum mp_type type = MP_NIL; type <= MP_EXT; type++)
		data_end = mp_encode_random_field(data_end, type);
	plan(field_count * 3 + 1);
	const char *d = data;
	uint32_t size = mp_decode_array(&d);
	assert(size == field_count);
	size_t total_size_save = data_end - data;
	total_size = 5;
	struct tt_compression ttc;
	ttc.type = type;
	for (uint32_t i = 0; i < size; i++) {
		ttc.data = (char *)d;
		mp_next(&d);
		ttc.size = d - ttc.data;
		total_size += mp_sizeof_for_compression(&ttc);
	}
	d = data;
	mp_decode_array(&d);
	char *cdata = xcalloc(1, total_size);
	char *cdata_end = cdata;
	cdata_end = mp_encode_array(cdata_end, field_count);
	for (uint32_t i = 0; i < field_count; i++) {
		ttc.data = (char *)d;
		mp_next(&d);
		ttc.size = (char *)d - ttc.data;
		cdata_end = mp_encode_compression(cdata_end, &ttc);
		isnt(cdata_end, NULL, "mp_compress");
	}
	d = cdata;
	mp_decode_array(&d);
	char *ddata = xcalloc(1, total_size_save);
	char *ddata_end = ddata;
	ddata_end = mp_encode_array(ddata_end, field_count);
	for (uint32_t i = 0; i < field_count; i++) {
		int64_t size = mp_sizeof_for_decompression(&d);
		char *tmp = xmalloc(size);
		ttc.data = tmp;
		isnt(mp_decode_compression(&d, &ttc), NULL, "mp_decompress");
		is(ttc.type, type, "compression type");
		memcpy(ddata_end, ttc.data, ttc.size);
		ddata_end += ttc.size;
		free(tmp);
	}
	assert(ddata_end == ddata + total_size_save);
	int rc = memcmp(data, ddata, total_size_save);
	is(rc, 0, "success");

	return check_plan();
}


int
main(void)
{
	plan(2);
	memory_init();
        fiber_init(fiber_c_invoke);
        random_init();

	check_mp_compression(COMPRESSION_TYPE_NONE);
	check_mp_compression(COMPRESSION_TYPE_ZSTD5);

	return check_plan();
}