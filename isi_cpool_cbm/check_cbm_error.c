#include <sys/isi_malloc_ostream.h>
#include <sys/isi_ostream.h>
#include <sys/isi_buf_istream.h>
#include <sys/isi_istream.h>

#include "isi_cbm_error.h"


#include <isi_util/checker.h>

SUITE_DEFINE_FOR_FILE(check_isi_cbm_error,
    .mem_check = CK_MEM_LEAKS,
    .timeout = 60,
    .fixture_timeout = 1200);


TEST(test_cbm_error)
{
	struct isi_error *error = NULL, *decoded_error = NULL;
	struct isi_malloc_ostream mos = { };
	struct isi_ostream *os = NULL;
	struct isi_buf_istream *bisp = NULL;
	struct isi_istream *is = NULL;
	char *buf = NULL;
	struct fmt fmt = FMT_INITIALIZER;
	const char *exp_error_msg = NULL, *error_msg = NULL;

	error = isi_cbm_error_new(CBM_PERM_ERROR, "something %s happened",
	    "bad");
	fail_if (error == NULL);
	fail_unless (isi_error_is_a(error, CBM_ERROR_CLASS));
	exp_error_msg = "something bad happened: [error code: CBM_PERM_ERROR]";
	error_msg = isi_error_get_message(error);
	fail_if((strcmp(error_msg, exp_error_msg) != 0),
	    "expected >%s< but got >%s< instead", exp_error_msg, error_msg);
	fail_unless (isi_cbm_error_is_a(error, CBM_PERM_ERROR),
	    "expect CBM_PERM_ERROR, got: %s", isi_error_fmt(error));

	isi_error_add_decodable_class(CBM_ERROR_CLASS);

	isi_malloc_ostream_init(&mos, 0);
	os = &mos.super;

	isi_error_encode(error, os);

	buf = malloc(isi_malloc_ostream_used(&mos));
	memcpy(buf, isi_malloc_ostream_buf(&mos),
	    isi_malloc_ostream_used(&mos));
	bisp = malloc(sizeof *bisp);
	isi_buf_istream_init(bisp, buf, isi_malloc_ostream_used(&mos));
	is = &bisp->super;

	decoded_error = isi_error_decode(is);
	fail_if (decoded_error == NULL);
	fail_unless (isi_error_is_a(decoded_error, CBM_ERROR_CLASS));

	error_msg = isi_error_get_message(decoded_error);
	fail_if((strcmp(error_msg, exp_error_msg) != 0),
	    "expected >%s< but got >%s< instead", exp_error_msg, error_msg);

	fail_unless (isi_cbm_error_is_a(error, CBM_PERM_ERROR),
	    "expect CBM_PERM_ERROR, got: %s", isi_error_fmt(error));

	free(bisp);
	free(buf);
	isi_malloc_ostream_clean(&mos);
	isi_error_clear_decodable_classes();
	isi_error_free(decoded_error);
	isi_error_free(error);
	fmt_clean(&fmt);
}

TEST(test_cbm_error_fmt)
{
	char buf[64];
	struct fmt FMT_INIT_CLEAN(fmt);

	/*
	 * Verify that each error enum value has a valid string representation.
	 * (This is to verify that the generator script didn't mess up.)
	 */
	for (int i = 0; i < MAX_CBM_ERROR_TYPE; ++i) {
		snprintf(buf, sizeof(buf), "<invalid:0x%x>", i);
		fmt_clean(&fmt);
		fmt_print(&fmt, "%#{}", cbm_error_type_fmt(i));
		fail_if(strcmp(fmt_string(&fmt), buf) == 0,
		    "unexpected error format found (i: %d)", i);
	}

	/*
	 * Verify that the max error value doesn't have a valid string
	 * representation.
	 */
	snprintf(buf, sizeof(buf), "<invalid:0x%x>", MAX_CBM_ERROR_TYPE);
	fmt_clean(&fmt);
	fmt_print(&fmt, "%#{}", cbm_error_type_fmt(MAX_CBM_ERROR_TYPE));

	fail_if(strcmp(fmt_string(&fmt), buf) != 0,
	    "unexpected error format found for MAX_CBM_ERROR_TYPE "
	    "(exp: |%s| act: |%s|)", buf, fmt_string(&fmt));
}
