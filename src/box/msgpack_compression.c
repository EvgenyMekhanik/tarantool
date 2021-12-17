/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "msgpack_compression.h"
#include "msgpack.h"
#include "space.h"
#include "fiber.h"
#include <small/region.h>

int
msgpack_compress_fields(struct space *space, const char *data,
                        const char *data_end, char **new_data,
                        char **new_data_end)
{
        struct space_def *def = space->def;
        const char *d = data;
        uint32_t total_size = 0;

        uint32_t array_size = mp_decode_array(&d);
        total_size += d - data;

        if (def == NULL || def->field_count == 0) {
                *new_data = (char *)data;
                *new_data_end = (char *)data_end;
                return 0;
        }

        uint32_t field_count = def->field_count;
        for (uint32_t i = 0; i < field_count && i < array_size; i++) {
                const char *dp = d;
                mp_next(&d);
                enum compression_type type = def->fields[i].compression_type;
                if (type == COMPRESSION_TYPE_NONE) {
                        total_size += d - dp;
                        continue;
                }
                struct tt_compression *ttc = tt_compression_new(d - dp, type);
                if (mp_set_data_for_compression(dp, d - dp, ttc) != 0) {
                        tt_compression_delete(ttc);
                        return -1;
                }
                uint32_t tmp;
                if (mp_sizeof_for_compression(ttc, &tmp) != 0) {
                        tt_compression_delete(ttc);
                        return -1;
                }
                tt_compression_delete(ttc);
                total_size += tmp;
        }
        total_size += data_end - d;
        *new_data = region_alloc(&fiber()->gc, total_size);
        if (*new_data == NULL)
                return -1;
        *new_data_end = *new_data;
        *new_data_end = mp_encode_array(*new_data_end, array_size);

        d = data;
        mp_decode_array(&d);
        for (uint32_t i = 0; i < field_count && i < array_size; i++) {
                const char *dp = d;
                mp_next(&d);
                enum compression_type type = def->fields[i].compression_type;
                if (type == COMPRESSION_TYPE_NONE) {
                        memcpy(*new_data_end, dp, d - dp);
                        *new_data_end += d -dp;
                        continue;
                }
                struct tt_compression *ttc = tt_compression_new(d - dp, type);
                if (mp_set_data_for_compression(dp, d - dp, ttc) != 0) {
                        tt_compression_delete(ttc);
                        return -1;
                }
                *new_data_end = mp_encode_compression(*new_data_end, ttc);
                if (*new_data_end == NULL) {
                        tt_compression_delete(ttc);
                        return -1;
                }
                tt_compression_delete(ttc);
        }
        assert(d <= data_end);
        memcpy(*new_data_end, d, data_end - d);
        *new_data_end += data_end - d;
        return 0;
}

int
msgpack_decompress_fields(const char *data, const char *data_end,
                          char **new_data, char **new_data_end)
{
        const char *d = data;
        uint32_t total_size = 0;

        uint32_t array_size = mp_decode_array(&d);
        total_size += d - data;

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
                uint32_t size;
                if (mp_sizeof_for_decompression(&dp, &size) != 0)
                        return -1;
                total_size += size;
        }

        *new_data = region_alloc(&fiber()->gc, total_size);
        if (*new_data == NULL)
                return -1;
        *new_data_end = *new_data;
        *new_data_end = mp_encode_array(*new_data_end, array_size);
        d = data;
        mp_decode_array(&d);
        for (uint32_t i = 0; i < array_size; i++) {
                const char *dp = d;
                mp_next(&d);
                if (mp_typeof(*dp) != MP_EXT) {
                        memcpy(*new_data_end, dp, d - dp);
                        *new_data_end += d - dp;
                        continue;
                }
                int8_t ext_type;
                const char *header = dp;
                mp_decode_extl(&dp, &ext_type);
                dp = header;
                if (ext_type != MP_COMPRESSION) {
                        memcpy(*new_data_end, dp, d - dp);
                        *new_data_end += d - dp;
                        continue;
                }
                uint32_t size;
                if (mp_sizeof_for_decompression(&dp, &size) != 0)
                        return -1;
                struct tt_compression *ttc =
                        tt_compression_new(size, COMPRESSION_TYPE_NONE);
                if (mp_decode_compression(&dp, ttc) == NULL) {
                        tt_compression_delete(ttc);
                        return -1;
                }
                memcpy(*new_data_end, ttc->data, ttc->size);
                *new_data_end += ttc->size;
        }
        assert(d == data_end);
        (void)data_end;
        return 0;
}