#include "msgpack.h"
#include "fiber.h"
#include "memory.h"
#include "mp_extension_types.h"
#include "trivia/util.h"
#include "unit.h"
#include <stdio.h>

int
main(void)
{
	plan(2);

	memory_init();
	fiber_init(fiber_c_invoke);
	msgpack_init();

	char expected[] = "(extension: type 0, len 10)";
	char got[sizeof(expected)] = {0};
	char data[] = { 0xca, 0xca, 0xca, 0xca, 0xca, 0xca, 0xca, 0xca, 0xca, 0xca };

	uint32_t size = mp_sizeof_ext(sizeof(data));
	char *ext_data = xmalloc(size);
	char *data_end = ext_data;
	data_end = mp_encode_ext(data_end, MP_UNKNOWN_EXTENSION, data, sizeof(data));
	mp_snprint(got, sizeof(got), ext_data);
	is(strcmp(expected, got), 0, "mp_snprint");

	memset(got, 0, sizeof(got));
	FILE *f  = tmpfile();
	assert(f != NULL);
	mp_fprint(f, ext_data);
	rewind(f);
	fread(got, 1, sizeof(got), f);
	is(strcmp(expected, got), 0, "mp_fprint");

	free(ext_data);

	fiber_free();
	memory_free();

	return check_plan();
}
