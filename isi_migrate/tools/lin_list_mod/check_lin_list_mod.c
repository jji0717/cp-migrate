#include <check.h>
#include <isi_util/check_helper.h>
#include <isi_migrate/migr/lin_list.h>
#include "lin_list_mod.h"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
SUITE_DEFINE_FOR_FILE(lin_list_mod);

TEST(lin_list_mod_print_info_types)
{
	struct lin_list_info_entry llie = {};
	for (int i = 0; i < MAX_LIN_LIST; i++) {
		llie.type = i;
		print_info(llie);
	}
}

TEST(lin_list_mod_print_entry_types)
{
	struct lin_list_entry ent = {};
	uint64_t lin = 0;
	enum lin_list_type type;

	for (int i = 0; i < MAX_LIN_LIST; i++) {
		type = i;
		print_entry(lin, ent, type);
	}
}

