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

#if defined(ENABLE_TUPLE_COMPRESSION)
# error unimplemented
#endif

void
tuple_compress_raw(struct tuple_format *format, const char *data,
                   const char *data_end, char **cdata,
                   char **cdata_end)
{
	(void)format;
	(void)data;
	(void)data_end;
	(void)cdata;
	(void)cdata_end;
	unreachable();
}

struct tuple *
tuple_decompress(struct tuple *tuple)
{
	(void)tuple;
	diag_set(IllegalParams, "Tuple compression is not "
		 "available in this build");
	return NULL;
}
