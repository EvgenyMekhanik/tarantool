/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "mp_compress.h"
#include "mp_extension_types.h"
#include "msgpuck.h"
#include "tt_compress.h"
#include "fiber.h"

static inline uint32_t
mp_sizeof_compress_raw(const struct tt_compress *ttc)
{
        size_t size = tt_compress_size(ttc);
        return 1 + size;
}


uint32_t
mp_sizeof_compress(const struct tt_compress *ttc)
{
        return mp_sizeof_ext(mp_sizeof_compress_raw(ttc));
}

char *
compress_pack(char *data, const struct tt_compress *ttc)
{
        char *const svp = data;
        data = mp_store_u8(data, ttc->type);
        size_t size = ttc->data_end - ttc->data;
        if (tt_compress_compress(ttc, data, &size) != 0) {
                data = svp;
                data = mp_store_u8(data, COMPRESSION_TYPE_NONE);
                memcpy(data, ttc->data, size);
        }
        return data + size;
}

struct tt_compress *
compress_unpack(const char **data, uint32_t len, struct tt_compress *ttc)
{
        const char *const svp = *data;
        ttc->type = mp_load_u8(data);
        len -= *data - svp;
        if (tt_compress_decompress(data, len, ttc) != 0) {
                *data = svp;
                return NULL;
        }
        return ttc;
}

struct tt_compress *
mp_decode_compress(const char **data, struct tt_compress *ttc)
{
        if (mp_typeof(**data) != MP_EXT)
                return NULL;
        int8_t type;
        const char *const svp = *data;
        uint32_t len = mp_decode_extl(data, &type);
        if (type != MP_COMPRESS || compress_unpack(data, len, ttc) == NULL) {
                *data = svp;
                return NULL;
        }
        return ttc;
}

char *
mp_encode_compress(char *data, const struct tt_compress *ttc)
{
        uint32_t len = mp_sizeof_compress_raw(ttc);
        data = mp_encode_extl(data, MP_COMPRESS, len);
        return compress_pack(data, ttc);
}

#if 0
int
mp_snprint_compress(char *buf, int size, const char **data, uint32_t len)
{
        (void)buf;
        (void)size;
        (void)data;
        (void)len;
        return 0;
}

int
mp_fprint_compress(FILE *file, const char **data, uint32_t len)
{
        (void)file;
        (void)data;
        (void)len;
        return 0;
}
#endif //TODO