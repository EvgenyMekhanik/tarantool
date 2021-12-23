#include "msgpack.h"
#include <random.h> /* tt_uuid_create */
#include "diag.h"
#include "error.h"
#include "mp_extension_types.h"
#include "mp_decimal.h"
#include "mp_uuid.h"
#include "mp_error.h"
#include "mp_datetime.h"
#include "trivia/util.h"
#include "unit.h"
#include <stdio.h>

static int
test_mp_print(const char *sample, const char *ext_data)
{
	plan(2);

	char got[200] = {0};

	mp_snprint(got, sizeof(got), ext_data);
	is(strcmp(sample, got), 0, "mp_snprint");

	memset(got, 0, sizeof(got));
	FILE *f  = tmpfile();
	assert(f != NULL);
	mp_fprint(f, ext_data);
	rewind(f);
	fread(got, 1, sizeof(got), f);
	is(strcmp(sample, got), 0, "mp_fprint");

	return check_plan();
}

static void
test_mp_print_nil(void)
{
	header();
	plan(1);

	char sample[] = "null";
	char ext_data[mp_sizeof_nil()];

	char *data_end = ext_data;
	data_end = mp_encode_nil(data_end);

	test_mp_print(sample, ext_data);

	check_plan();
	footer();
}

static void
test_mp_print_uint(void)
{
	header();
	plan(1);

	char sample[] = "123456";
	char *ext_data = xmalloc(mp_sizeof_uint(123456));

	char *data_end = ext_data;
	data_end = mp_encode_uint(data_end, 123456);

	test_mp_print(sample, ext_data);

	free(ext_data);

	check_plan();
	footer();
}

static void
test_mp_print_int(void)
{
	header();
	plan(1);

	char sample[] = "-123456";
	char *ext_data = xmalloc(mp_sizeof_int(-123456));

	char *data_end = ext_data;
	data_end = mp_encode_int(data_end, -123456);

	test_mp_print(sample, ext_data);

	free(ext_data);

	check_plan();
	footer();
}

static void
test_mp_print_str(void)
{
	header();
	plan(1);

	char *source = "test_mp_print_str";
	char sample[] = "\"test_mp_print_str\"";
	char *ext_data = xmalloc(mp_sizeof_str(strlen(source)));

	char *data_end = ext_data;
	data_end = mp_encode_str(data_end, source, strlen(source));

	test_mp_print(sample, ext_data);

	free(ext_data);

	check_plan();
	footer();
}

static void
test_mp_print_bin(void)
{
	header();
	plan(1);

	char source[] = {18, 85, 93, 6, 77};
	char sample[] = "\"\\u0012U]\\u0006M\"";

	char *ext_data = xmalloc(mp_sizeof_bin(sizeof(source)));

	char *data_end = ext_data;
	data_end = mp_encode_bin(data_end, source, sizeof(source));

	test_mp_print(sample, ext_data);

	free(ext_data);

	check_plan();
	footer();
}

static void
test_mp_print_array(void)
{
	header();
	plan(1);

	char *source = "test_mp_print_array";
	char sample[] = "[null, \"test_mp_print_array\", 123456]";

	uint32_t size = mp_sizeof_array(3) + mp_sizeof_nil() +
		mp_sizeof_str(strlen(source)) + mp_sizeof_uint(123456);
	char *ext_data = xmalloc(size);

	char *data_end = ext_data;
	data_end = mp_encode_array(data_end, 3);
	data_end = mp_encode_nil(data_end);
	data_end = mp_encode_str(data_end, source, strlen(source));
	data_end = mp_encode_uint(data_end, 123456);

	test_mp_print(sample, ext_data);

	free(ext_data);

	check_plan();
	footer();
}

static void
test_mp_print_map(void)
{
	header();
	plan(1);

	char *source = "test_mp_print_array";
	char sample[] = "{1: null, \"1\": \"test_mp_print_array\", 3: 123456}";

	uint32_t size = mp_sizeof_array(3) +
		mp_sizeof_uint(1) + mp_sizeof_nil() +
		mp_sizeof_str(1) + mp_sizeof_str(strlen(source)) +
		mp_sizeof_uint(3) + mp_sizeof_uint(123456);
	char *ext_data = xmalloc(size);

	char *data_end = ext_data;
	data_end = mp_encode_map(data_end, 3);
	data_end = mp_encode_uint(data_end, 1);
	data_end = mp_encode_nil(data_end);
	data_end = mp_encode_str(data_end, "1", 1);
	data_end = mp_encode_str(data_end, source, strlen(source));
	data_end = mp_encode_uint(data_end, 3);
	data_end = mp_encode_uint(data_end, 123456);

	test_mp_print(sample, ext_data);

	free(ext_data);

	check_plan();
	footer();
}

static void
test_mp_print_bool(void)
{
	header();
	plan(1);

	char sample[] = "true";

	char *ext_data = xmalloc(mp_sizeof_bool(true));
	char *data_end = ext_data;
	data_end = mp_encode_bool(data_end, true);

	test_mp_print(sample, ext_data);

	free(ext_data);

	check_plan();
	footer();
}

static void
test_mp_print_float(void)
{
	header();
	plan(1);

	char sample[] = "-123.456";

	char *ext_data = xmalloc(mp_sizeof_float(-123.456));
	char *data_end = ext_data;
	data_end = mp_encode_float(data_end, -123.456);

	test_mp_print(sample, ext_data);

	free(ext_data);

	check_plan();
	footer();
}

static void
test_mp_print_double(void)
{
	header();
	plan(1);

	char sample[] = "-123.456";

	char *ext_data = xmalloc(mp_sizeof_double(-123.456));
	char *data_end = ext_data;
	data_end = mp_encode_double(data_end, -123.456);

	test_mp_print(sample, ext_data);

	free(ext_data);

	check_plan();
	footer();
}

static void
test_mp_print_unknown_extention(void)
{
	header();
	plan(1);

	char sample[] = "(extension: type 0, len 10)";
	char data[] = { 0xca, 0xca, 0xca, 0xca, 0xca, 0xca, 0xca, 0xca, 0xca, 0xca };
	char *ext_data = xmalloc(mp_sizeof_ext(sizeof(data)));

	char *data_end = ext_data;
	data_end = mp_encode_ext(data_end, MP_UNKNOWN_EXTENSION, data, sizeof(data));

	test_mp_print(sample, ext_data);

	free(ext_data);

	check_plan();
	footer();
}

static void
test_mp_print_decimal(void)
{
	header();
	plan(1);

	char sample[] = "-123.456";
	decimal_t dec, *pdec;
	pdec = decimal_from_string(&dec, "-123.456");
	assert(pdec != NULL);
	char *ext_data = xmalloc(mp_sizeof_decimal(pdec));

	char *data_end = ext_data;
	data_end = mp_encode_decimal(data_end, pdec);

	test_mp_print(sample, ext_data);

	free(ext_data);

	check_plan();
	footer();
}

static void
test_mp_print_uuid(void)
{
	header();
	plan(1);

	struct tt_uuid uu;
	tt_uuid_create(&uu);
	char *ext_data = xmalloc(mp_sizeof_uuid());

	char *data_end = ext_data;
	data_end = mp_encode_uuid(data_end, &uu);

	test_mp_print(tt_uuid_str(&uu), ext_data);

	free(ext_data);

	check_plan();
	footer();
}

static void
test_mp_print_error(void)
{
	header();
	plan(1);

	struct error *error = BuildClientError("file", 1, ER_INJECTION, "test");
	char *ext_data = xmalloc(mp_sizeof_error(error));

	char *data_end = ext_data;
	data_end = mp_encode_error(data_end, error);

	test_mp_print("{\"stack\": [{\"type\": \"ClientError\", \"line\": 1,"
		      " \"file\": \"file\", \"message\": "
		      "\"Error injection 'test'\", \"errno\": 0, \"code\": 8}]}",
		      ext_data);

	free(ext_data);
	error_payload_destroy(&error->payload);
	error->destroy(error);

	check_plan();
	footer();
}

static void
test_mp_print_datetime(void)
{
	header();
	plan(1);

	char sample[64];
	struct datetime date = {0, 0, 0, 0}; // 1970-01-01T00:00Z
	char *ext_data = xmalloc(mp_sizeof_datetime(&date));

	char *data_end = ext_data;
	data_end = mp_encode_datetime(data_end, &date);
	datetime_to_string(&date, sample, sizeof(sample));

	test_mp_print(sample, ext_data);

	free(ext_data);

	check_plan();
	footer();
}

int
main(void)
{
	plan(14);

	random_init();
	msgpack_init();

	test_mp_print_nil();
	test_mp_print_uint();
	test_mp_print_int();
	test_mp_print_str();
	test_mp_print_bin();
	test_mp_print_array();
	test_mp_print_map();
	test_mp_print_bool();
	test_mp_print_float();
	test_mp_print_double();
	test_mp_print_decimal();
	test_mp_print_uuid();
	test_mp_print_error();
	test_mp_print_datetime();

	return check_plan();
}
