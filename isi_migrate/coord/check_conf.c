#include <stdio.h>
#include <stdlib.h>
#include <isi_util/isi_error.h>

#include <check.h>
#include <isi_util/check_helper.h>

//#include <isi_migrate/config/siq_job.h>
//#include <isi_migrate/config/siq_util.h>

#include "coord.h"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);

SUITE_DEFINE_FOR_FILE(conf, .suite_setup = suite_setup,
    .suite_teardown = suite_teardown, .timeout = 60);
//    .mem_check = CK_MEM_DISABLE);

static char *policyName = NULL;
static char *srcClusterName = NULL;

TEST_FIXTURE(suite_setup)
{
	policyName = strdup("myPolicy1");
	srcClusterName = strdup("bjohnsont");
}

TEST_FIXTURE(suite_teardown)
{
	if (policyName)
		free(policyName);
	if (srcClusterName)
		free(srcClusterName);
}

TEST(expand_snap_patterns_valid)
{
	char *toExpand = NULL;
	char *result = NULL;
	char expected[MAXPATHLEN];

	snprintf(expected, MAXPATHLEN,
		"TEST-%s-%s-%%H-%%M-%%S", srcClusterName, policyName);
	toExpand = strdup("TEST-%{SrcCluster}-%{PolicyName}-%H-%M-%S");

	result = expand_snapshot_patterns(&toExpand,
		    policyName, srcClusterName);

	fail_if(strlen(toExpand) > MAXPATHLEN);
	fail_unless(strcmp(expected, toExpand) == 0);
	fail_unless(result == toExpand);

	free(toExpand);
}

TEST(expand_snap_patterns_no_patterns)
{
	char *result = NULL;
	char *toExpand = NULL;
	char expected[MAXPATHLEN];

	snprintf(expected, MAXPATHLEN, "TEST-%%H-%%M-%%S");
	toExpand = strdup("TEST-%H-%M-%S");

	result = expand_snapshot_patterns(&toExpand,
		    policyName, srcClusterName);

	fail_if(strlen(toExpand) > MAXPATHLEN);
	fail_unless(strcmp(expected, toExpand) == 0);
	fail_unless(result == toExpand);

	free(toExpand);
}

TEST(expand_snap_patterns_no_policy)
{
	char *toExpand = NULL;
	char *result = NULL;

	toExpand = strdup("TEST-%{SrcCluster}-%{PolicyName}-%H-%M-%S");

	result = expand_snapshot_patterns(&toExpand, NULL, NULL);

	fail_unless(result != toExpand);
	fail_unless(result == NULL);

	free(toExpand);
}

TEST(expand_snap_patterns_repeated)
{
	char *toExpand = NULL;
	char *result = NULL;
	char expected[MAXPATHLEN];

	snprintf(expected, MAXPATHLEN,
		"TEST-%s-%s-%s-%s-%s-%s-%%H-%%M-%%S",
		srcClusterName, srcClusterName, srcClusterName,
		policyName, policyName, policyName);
	toExpand = strdup("TEST-%{SrcCluster}-%{SrcCluster}-%{SrcCluster}-"
			"%{PolicyName}-%{PolicyName}-%{PolicyName}-%H-%M-%S");

	result = expand_snapshot_patterns(&toExpand,
		    policyName, srcClusterName);

	fail_if(strlen(toExpand) > MAXPATHLEN);
	fail_unless(strcmp(expected, toExpand) == 0);
	fail_unless(result == toExpand);

	free(toExpand);
}

TEST(expand_snap_patterns_null_entry)
{
	char *toExpand = NULL;
	char *result = NULL;

	result = expand_snapshot_patterns(&toExpand,
		    policyName, srcClusterName);

	fail_unless(toExpand == NULL);
	fail_unless(result == NULL);
}

