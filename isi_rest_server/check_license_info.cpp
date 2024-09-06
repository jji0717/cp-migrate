#include <stdlib.h>

#include "license_info.h"

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

static void test_setup(void *context);

SUITE_DEFINE_FOR_FILE(check_license_info,
    .mem_check = CK_MEM_DISABLE /*XXX*/, .dmalloc_opts = NULL,
    .suite_setup = test_setup, .suite_teardown = NULL,
    .test_setup = test_setup);

const char *BAD_LIC = "Bad";

char const *const LICS[] = 
{
	"SmartQuotas",
	"SnapshotIQ",
	"InsightIQ",
	"SmartPools",
	"SmartLock",
};

void
test_setup(void *context)
{
}

/* tests license_info api without modifying an licenses */
TEST(license_info_basic)
{
	license_info &info = license_info::instance();

	fail_unless(info.status(BAD_LIC) == ISI_LICENSING_UNLICENSED);

	fail_unless_throws_any(info.check(BAD_LIC));
}
