/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "tuple_compression.h"
#include "schema.h"
#include "msgpack.h"
#include "space.h"
#include "fiber.h"
#include "tuple_format.h"
#include "tuple.h"
#include <small/region.h>

struct tuple *
compress_tuple_new(struct space *space, const char *data,
		   const char *data_end)
{
	if (space_is_system(space) || space_is_ephemeral(space)) {
		return space->format->vtab.tuple_new(space->format, data,
						     data_end);
	}
	uint32_t field_count = space->def->field_count;
	const char *d = data;
	uint32_t used = region_used(&fiber()->gc);
	void *tmp_data = region_alloc(&fiber()->gc, data_end - data);
	if (tmp_data == NULL) {
		diag_set(OutOfMemory, data_end - data, "region_alloc", "buf");
		return NULL;
	}
	uint32_t array_size = mp_decode_array(&d);
	void *tmp_data_end = tmp_data;
	memcpy(tmp_data_end, data, d - data);
	tmp_data_end += d - data;
	for (uint32_t i = 0; i < field_count && i < array_size; i++) {
		const char *dp = d;
		mp_next(&d);
		enum compression_type type =
			space->def->fields[i].compression_type;
		if (type == COMPRESSION_TYPE_NONE) {
			memcpy(tmp_data_end, dp, d - dp);
			tmp_data_end += d - dp;
			continue;
		}
		tmp_data_end = mp_compress(tmp_data_end, d - dp, dp,
					   d - dp, type, 5);
	}
	assert(d <= data_end);
	memcpy(tmp_data_end, d, data_end - d);
	tmp_data_end += data_end - d;
	assert(tmp_data_end <= tmp_data + (data_end - data));
	struct tuple *new_tuple =
		space->format->vtab.tuple_new(space->format, tmp_data,
					      tmp_data_end);
	if (new_tuple != NULL)
		new_tuple->is_compressed = true;
	region_truncate(&fiber()->gc, used);
	return new_tuple;
}

struct tuple *
decompress_tuple_new(struct space *space, struct tuple *old_tuple)
{
	uint32_t bsize;
	const char *old_data = tuple_data_range(old_tuple, &bsize);
	if (space_is_system(space) || space_is_ephemeral(space)) {
		return space->format->vtab.tuple_new(space->format, old_data,
						     old_data + bsize);
	}
	const char *d = old_data;
	uint32_t total_size = 0;
	uint32_t array_size = mp_decode_array(&d);
	total_size += d - old_data;
	for (uint32_t i = 0; i < array_size; i++) {
		const char *dp = d;
		mp_next(&d);
		if (mp_typeof(*dp) != MP_EXT) {
			total_size += d - dp;
			continue;
		}
		int8_t ext_type;
		const char *header = dp;
		mp_decode_extl(&dp, &ext_type);
		dp = header;
		if (ext_type != MP_COMPRESSION) {
			total_size += d - dp;
			continue;
		}
		uint32_t size = mp_decompress(NULL, 0, &dp);
		/* ext_type was checked previously */
		assert(size != 0);
		total_size += size;
	}
	uint32_t used = region_used(&fiber()->gc);
	void *tmp_data = region_alloc(&fiber()->gc, total_size);
	if (tmp_data == NULL) {
		diag_set(OutOfMemory, total_size, "region_alloc", "buf");
		return NULL;
	}
	void *tmp_data_end = tmp_data;
	d = old_data;
	mp_decode_array(&d);
	memcpy(tmp_data_end, old_data, d - old_data);
	total_size -= d - old_data;
	tmp_data_end += d - old_data;
	uint32_t field_count = space->def->field_count;
	for (uint32_t i = 0; i < field_count && i < array_size; i++) {
		const char *dp = d;
		mp_next(&d);
		if (mp_typeof(*dp) != MP_EXT) {
			memcpy(tmp_data_end, dp, d - dp);
			total_size -= d - dp;
			tmp_data_end += d - dp;
			continue;
		}
		uint32_t size = mp_decompress(tmp_data_end, total_size, &dp);
		if (size == 0) {
			diag_set(ClientError, ER_COMPRESSION,
				 "failed to decompress data");
			region_truncate(&fiber()->gc, used);
			return NULL;
		}
		total_size -= size;
		tmp_data_end += size;
	}
	memcpy(tmp_data_end, d, (old_data + bsize) - d);
	total_size -= (old_data + bsize) - d;
	tmp_data_end += (old_data + bsize) - d;
	assert(total_size == 0);
	struct tuple *new_tuple =
		space->format->vtab.tuple_new(space->format, tmp_data,
					      tmp_data_end);
	region_truncate(&fiber()->gc, used);
	return new_tuple;
}
