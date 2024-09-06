#include <sys/file.h>
#include <sys/socket.h>
#include <sys/sysctl.h>
#include <stdlib.h>
#include <stdio.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <string.h>
#include "check.h"
#include "isirep.h"

int result;

char *name_restriction;
char **out_msg;
struct isi_error **out_error;

TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(isi_migrate, .test_setup = test_setup,
    .test_teardown = test_teardown, .timeout=50);

TEST_FIXTURE(test_setup)
{
}

TEST_FIXTURE(test_teardown)
{
}

TEST(without_subnet_pool, .mem_check=CK_MEM_DISABLE)
{
	name_restriction = NULL;
	result = get_local_cluster_info(NULL, NULL, NULL, 0, 0, name_restriction, 
	    false, false, out_msg,out_error);
	fail_if(result == 0, "Error retrieving IP addresses for prim cluster");
}
