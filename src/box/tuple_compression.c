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

struct tuple *
tt_compress_tuple_new(struct space *space, const char *data,
		      const char *data_end)
{
	return space->format->vtab.tuple_new(space->format, data,
					     data_end);
}

struct tuple *
tt_decompress_tuple_new(struct space *space, struct tuple *old_tuple)
{
	(void) space;
	(void) old_tuple;
	diag_set(IllegalParams, "Tuple compression is not "
		 "available in this build");
	return NULL;
}
