#include <isi_migrate/migr/check_helper.h>
#include <isi_migrate/migr/isirep.h>

#include "sworker.h"

#define TESTPOLICYID "11111111111111111111111111111111"
#define TESTNAME "check_target_work"

TEST_FIXTURE(suite_teardown)
{
	struct isi_error *error = NULL;

	//Just in case, try to delete everything
	remove_lin_list(TESTPOLICYID, TESTNAME, true, &error);
	isi_error_free(error);
	error = NULL;

	remove_compliance_map(TESTPOLICYID, TESTNAME, true, &error);
	isi_error_free(error);
}

SUITE_DEFINE_FOR_FILE(migr_target_work, .suite_teardown = suite_teardown);

TEST(commit_work_split, .attrs="overnight")
{
	struct migr_sworker_comp_ctx ctx = {};
	struct lin_list_info_entry llie = {};
	struct lin_list_entry lle = {};
	ifs_lin_t split_lin = INVALID_LIN;
	struct isi_error *error = NULL;

	//Setup context
	ctx.rt = WORK_COMP_COMMIT;

	//Check existence  of lin list and delete if it already exists
	remove_lin_list(TESTPOLICYID, TESTNAME, true, &error);
	fail_if_error(error);

	//Create the test lin list
	create_lin_list(TESTPOLICYID, TESTNAME, false, &error);
	fail_if_error(error);
	open_lin_list(TESTPOLICYID, TESTNAME, &ctx.worm_commit_ctx, &error);
	fail_if_error(error);
	llie.type = COMMIT;
	set_lin_list_info(&llie, ctx.worm_commit_ctx, false, &error);
	fail_if_error(error);

	//Add a LIN to work on
	set_lin_list_entry(1000, lle, ctx.worm_commit_ctx, &error);
	fail_if_error(error);

	//Min = 0x2, Max = 0x1000
	//1 work item at 1000 means there will be no split
	split_lin = comp_find_split(&ctx, ROOT_LIN, 1000,
	    &error);
	fail_unless(split_lin == INVALID_LIN);
	//But at 1001, there will be a split
	split_lin = comp_find_split(&ctx, ROOT_LIN, 1001,
	    &error);
	fail_unless(split_lin == 501);

	//Min = 0x2, Max = -1
	//1 work item at 1000, this should be splittable
	split_lin = comp_find_split(&ctx, ROOT_LIN, -1, &error);
	fail_unless(split_lin != INVALID_LIN);

	//Add another LIN
	set_lin_list_entry(2000, lle, ctx.worm_commit_ctx, &error);
	fail_if_error(error);

	//Min = 0x2, Max = 0x2001
	//This should split between 1000 and 2000
	split_lin = comp_find_split(&ctx, ROOT_LIN, 2001,
	    &error);
	fail_unless(split_lin == 1001);
	//But this will have a different range because it does not include 2000
	split_lin = comp_find_split(&ctx, ROOT_LIN, 2000,
	    &error);
	fail_unless(split_lin == 501);

	//This should be a clear split at 3003
	set_lin_list_entry(3000, lle, ctx.worm_commit_ctx, &error);
	fail_if_error(error);
	set_lin_list_entry(3001, lle, ctx.worm_commit_ctx, &error);
	fail_if_error(error);
	set_lin_list_entry(3002, lle, ctx.worm_commit_ctx, &error);
	fail_if_error(error);
	set_lin_list_entry(3003, lle, ctx.worm_commit_ctx, &error);
	fail_if_error(error);
	set_lin_list_entry(3004, lle, ctx.worm_commit_ctx, &error);
	fail_if_error(error);
	set_lin_list_entry(3005, lle, ctx.worm_commit_ctx, &error);
	fail_if_error(error);
	split_lin = comp_find_split(&ctx, 3000, 3006, &error);
	fail_unless(split_lin == 3003);

	close_lin_list(ctx.worm_commit_ctx);
}

TEST(comp_map_work_split, .attrs="overnight")
{
	ifs_lin_t split_lin = INVALID_LIN;
	struct migr_sworker_comp_ctx ctx = {};
	struct isi_error *error = NULL;

	//Setup context
	ctx.rt = WORK_COMP_MAP_PROCESS;

	//Check existence of compliance map and delete if it already exists
	remove_compliance_map(TESTPOLICYID, TESTNAME, true, &error);
	fail_if_error(error);

	//Create the test compliance map
	create_compliance_map(TESTPOLICYID, TESTNAME, false, &error);
	fail_if_error(error);
	open_compliance_map(TESTPOLICYID, TESTNAME, &ctx.comp_map_ctx, &error);
	fail_if_error(error);

	//Add a LIN to work on
	set_compliance_mapping(1000, 350, ctx.comp_map_ctx, &error);
	fail_if_error(error);

	//Min = 0x2, Max = 0x1000
	//1 work item at 1000 means there will be no split
	split_lin = comp_find_split(&ctx, ROOT_LIN, 1000, &error);
	fail_unless(split_lin == INVALID_LIN);
	//But at 1001, there will be a split
	split_lin = comp_find_split(&ctx, ROOT_LIN, 1001, &error);
	fail_unless(split_lin == 501);

	//Min = 0x2, Max = -1
	//1 work item at 1000, this should be splittable
	split_lin = comp_find_split(&ctx, ROOT_LIN, -1, &error);
	fail_unless(split_lin != INVALID_LIN);

	//Add another LIN
	set_compliance_mapping(2000, 250, ctx.comp_map_ctx, &error);
	fail_if_error(error);

	//Min = 0x2, Max = 0x2001
	//This should split between 1000 and 2000
	split_lin = comp_find_split(&ctx, ROOT_LIN, 2001, &error);
	fail_unless(split_lin == 1001);
	//But this will have a different range because it does not include 2000
	split_lin = comp_find_split(&ctx, ROOT_LIN, 2000, &error);
	fail_unless(split_lin == 501);

	//This should be a clear split at 3003
	set_compliance_mapping(3000, 1, ctx.comp_map_ctx, &error);
	fail_if_error(error);
	set_compliance_mapping(3001, 2, ctx.comp_map_ctx, &error);
	fail_if_error(error);
	set_compliance_mapping(3002, 3, ctx.comp_map_ctx, &error);
	fail_if_error(error);
	set_compliance_mapping(3003, 4, ctx.comp_map_ctx, &error);
	fail_if_error(error);
	set_compliance_mapping(3004, 5, ctx.comp_map_ctx, &error);
	fail_if_error(error);
	set_compliance_mapping(3005, 6, ctx.comp_map_ctx, &error);
	fail_if_error(error);
	split_lin = comp_find_split(&ctx, 3000, 3006, &error);
	fail_unless(split_lin == 3003);

	close_compliance_map(ctx.comp_map_ctx);
}

TEST(commit_dir_dels_split, .attrs="overnight")
{
	struct migr_sworker_comp_ctx ctx = {};
	struct lin_list_info_entry llie = {};
	struct lin_list_entry lle = {};
	ifs_lin_t split_lin = INVALID_LIN;
	struct isi_error *error = NULL;

	//Setup context
	ctx.rt = WORK_CT_COMP_DIR_DELS;

	//Check existence  of lin list and delete if it already exists
	remove_lin_list(TESTPOLICYID, TESTNAME, true, &error);
	fail_if_error(error);

	//Create the test lin list
	create_lin_list(TESTPOLICYID, TESTNAME, false, &error);
	fail_if_error(error);
	open_lin_list(TESTPOLICYID, TESTNAME, &ctx.worm_dir_dels_ctx, &error);
	fail_if_error(error);
	llie.type = DIR_DELS;
	set_lin_list_info(&llie, ctx.worm_dir_dels_ctx, false, &error);
	fail_if_error(error);

	//Add a LIN to work on
	set_lin_list_entry(1000, lle, ctx.worm_dir_dels_ctx, &error);
	fail_if_error(error);

	//Min = 0x2, Max = 0x1000
	//1 work item at 1000 means there will be no split
	split_lin = comp_find_split(&ctx, ROOT_LIN, 1000,
	    &error);
	fail_unless(split_lin == INVALID_LIN);
	//But at 1001, there will be a split
	split_lin = comp_find_split(&ctx, ROOT_LIN, 1001,
	    &error);
	fail_unless(split_lin == 501);

	//Min = 0x2, Max = -1
	//1 work item at 1000, this should be splittable
	split_lin = comp_find_split(&ctx, ROOT_LIN, -1, &error);
	fail_unless(split_lin != INVALID_LIN);

	//Add another LIN
	set_lin_list_entry(2000, lle, ctx.worm_dir_dels_ctx, &error);
	fail_if_error(error);

	//Min = 0x2, Max = 0x2001
	//This should split between 1000 and 2000
	split_lin = comp_find_split(&ctx, ROOT_LIN, 2001,
	    &error);
	fail_unless(split_lin == 1001);
	//But this will have a different range because it does not include 2000
	split_lin = comp_find_split(&ctx, ROOT_LIN, 2000,
	    &error);
	fail_unless(split_lin == 501);

	//This should be a clear split at 3003
	set_lin_list_entry(3000, lle, ctx.worm_dir_dels_ctx, &error);
	fail_if_error(error);
	set_lin_list_entry(3001, lle, ctx.worm_dir_dels_ctx, &error);
	fail_if_error(error);
	set_lin_list_entry(3002, lle, ctx.worm_dir_dels_ctx, &error);
	fail_if_error(error);
	set_lin_list_entry(3003, lle, ctx.worm_dir_dels_ctx, &error);
	fail_if_error(error);
	set_lin_list_entry(3004, lle, ctx.worm_dir_dels_ctx, &error);
	fail_if_error(error);
	set_lin_list_entry(3005, lle, ctx.worm_dir_dels_ctx, &error);
	fail_if_error(error);
	split_lin = comp_find_split(&ctx, 3000, 3006, &error);
	fail_unless(split_lin == 3003);

	close_lin_list(ctx.worm_dir_dels_ctx);
}
