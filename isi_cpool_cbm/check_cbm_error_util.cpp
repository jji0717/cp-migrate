
#define ISI_CBM_ERROR_PROTECTED
#include "isi_cbm_error_protected.h"
#undef ISI_CBM_ERROR_PROTECTED

#include <isi_cpool_cbm/isi_cbm_error_util.h>
#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
SUITE_DEFINE_FOR_FILE(check_cbm_error_util,
    .mem_check = CK_MEM_LEAKS,
    .timeout = 60,
    .fixture_timeout = 1200);

TEST(test_cbm_error)
{
	struct isi_error *error = NULL;
	struct fmt fmt = FMT_INITIALIZER;
	const char *expected_txt = "context test_cbm_error";
	error = isi_cbm_error_new(CBM_PERM_ERROR, "%s", "something wrong");
	fail_if(error == NULL, "failed to convert cbm error");
	fail_if(!isi_error_is_a(error, CBM_ERROR_CLASS),
	    "must be CBM_ERROR_CLASS");
	fail_unless( isi_cbm_error_is_a(error, CBM_PERM_ERROR),
	    "expect CBM_PERM_ERROR, got: %s", isi_error_fmt(error));

	isi_error_add_context(error, "%s", expected_txt);
	fmt_clean(&fmt);
        fmt_print(&fmt, "%#{}", isi_error_fmt(error));
        //printf("%s\n", fmt_string(&fmt));
        fail_unless(NULL != strstr(fmt_string(&fmt), expected_txt),
            "Couldn't find %s in\n%s", expected_txt, fmt_string(&fmt));
        fmt_clean(&fmt);
	isi_error_free(error);
}

TEST(test_cbm_clapi_exception_conversion)
{
	using namespace isi_cloud;
	struct isi_error *error = NULL;

	// one test case
	cl_exception clexp(CL_AUTHENTICATION_FAILED, "passwd incorrect");
	error = convert_cl_exception_to_cbm_error(clexp);
	fail_if(error == NULL, "failed to convert cbm error");

	fail_unless(isi_cbm_error_is_a(error, CBM_CLAPI_AUTHENTICATION_FAILED),
	    "except error CBM_CLAPI_AUTHENTICATION_FAILED, got %s",
	    isi_error_fmt(error));
	isi_error_free(error);

	// second test case
	cl_exception clexp2(CL_PRVD_INTERNAL_ERROR, "internal error");
	error = convert_cl_exception_to_cbm_error(clexp2);

	fail_if(error == NULL, "failed to convert cbm error");
	fail_unless(isi_cbm_error_is_a(error, CBM_CLAPI_UNEXPECTED),
	    "except error CBM_CLAPI_UNEXPECTED, got %s",
	    isi_error_fmt(error));
	isi_error_free(error);
}

