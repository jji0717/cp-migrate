#include <check.h>
#include <unistd.h>

#include <isi_util/check_helper.h>
#include "siq_policy.h"

#define FAKE_NAME "fakename1234567890abcdef"

SUITE_DEFINE_FOR_FILE(siq_policy);

TEST(siq_spec_copy_from_policy_check, .attrs="overnight")
{
	struct siq_policy *pol = NULL;
	struct siq_spec *spec;

	pol = siq_policy_create();
	pol->common->name = strdup(FAKE_NAME);

	spec = siq_spec_copy_from_policy(pol);

	siq_policy_free(pol);

	fail_unless(strcmp(spec->common->name, FAKE_NAME) == 0);
	siq_report_spec_free(spec);
}
