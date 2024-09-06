#include <libgen.h>

#include <isi_migrate/migr/check_helper.h>
#include <isi_quota/check_quota_helper.h>
#include <ifs/ifs_lin_open.h>
#include "sworker.h"

TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);
SUITE_DEFINE_FOR_FILE(migr_multi_tmp, .test_setup = test_setup,
    .test_teardown = test_teardown);
/* BEJ: TODO: CODE COVERAGE: use merge_copy_collison_entries once. */

/****************************HELPER FUNCTIONS*********************/
/* simple helper to create and setup a quota on a new directory */
inline static void
create_and_setup_quota(struct quota_domain *domain, struct quota_domain_id *qdi)
{
	SETUP_DOMAIN(domain, qdi);
	GET_DOMAIN(domain, qdi);
}

/* Setup a basic quota with the following nested dir setup:
	test.quota.XXXX [with quota_outer]
		test.quota.inner [with quota_inner]
			test.noquota
   Creates new dirs separate from test setup() function
   This setup is used for several quota tests */
static void
standard_two_quota_setup(
    char *quota_outer_path,
    char *policy_id,
    struct stat *quota_outer_st,
    struct quota_domain *domain_outer,
    char *quota_inner_path,
    struct stat *quota_inner_st,
    struct quota_domain *domain_inner,
    char *dir_path,
    struct stat *dir_st)
{
	struct quota_domain_id qdi;
	struct quota_domain_id qdi_outer;
	ifs_lin_t quota_lin_outer;
	int ret = -1;
	int errorno = 0;
	char *pid = NULL;

	/* setup outermost quota */
	create_test_dir(quota_outer_path, &quota_lin_outer, &qdi_outer);
	create_and_setup_quota(domain_outer, &qdi_outer);
	ret = stat(quota_outer_path, quota_outer_st);
	fail_unless(ret == 0);
	pid = siq_create_policy_id(false);
	fail_if(pid == NULL);
	strlcpy(policy_id, pid, POLICY_ID_LEN + 1);
	free(pid);
	/* manually set up inner quota */
	sprintf(quota_inner_path, "%s/test.quota.inner", quota_outer_path);
	errorno = mkdir(quota_inner_path, 0755);
	fail_unless(errorno == 0);
	fail_unless(stat(quota_inner_path, quota_inner_st) == 0);
	qdi_init(&qdi, quota_inner_st->st_ino, QUOTA_DOMAIN_ALL, false,
	    ANY_INSTANCE);
	create_and_setup_quota(domain_inner, &qdi);

	/* make a dir in the innermost quota */
	sprintf(dir_path, "%s/test.noquota", quota_inner_path);
	errorno = mkdir(dir_path, 0755);
	fail_unless(errorno == 0);
	fail_unless(stat(dir_path, dir_st) == 0);
}

/* removes all quotas from a dir */
static void
remove_quota_dir(struct quota_domain *domain, char *quota_path,
    ifs_lin_t quota_lin)
{
	struct isi_error *error = NULL;
	quota_domain_delete(domain, &error);
	fail_if_error(error);
	clean_test_dir(quota_path, quota_lin);
}

/* runs initialize_tmp_map on a path, checks to make
   sure setup was completed successfully and the map exists.
   The quota paths/stat parameters are optional */
static void
run_test_setup_init_tmp_dirs(
    struct migr_sworker_ctx *sw_ctx,
    const char *test_path,
    const char *quota_outer_path,
    const struct stat *quota_outer_st,
    const char *quota_inner_path,
    const struct stat *quota_inner_st,
    const struct stat *dir_st,
    bool restore,
    bool expect_quotas)
{
	int ret = -1;
	int tmp_fd = -1;
	ifs_lin_t *tmp_lin = NULL, tlin, ptlin;
	char tmp_path[MAXPATHLEN + 1];
	char tmp_dir_name[MAXPATHLEN + 1];
	struct stat tmp_st;
	struct isi_error *error = NULL;

	/* setup junk */
	/* BEJ: XXX: hack to stop the lack of tmonitor from nuking test */
	sw_ctx->tmonitor.coord_fd = -1;
	/* BEJ: XXX: hack to prevent calling merge_copy_collision_entries */
	sw_ctx->tmonitor.stf_sync = 0;
	sw_ctx->tmonitor.restore = restore;
	get_tmp_working_dir_name(tmp_dir_name, restore);

	/* validate tmp dirs don't already exist */
	if (quota_outer_path != NULL) {
		snprintf(tmp_path, sizeof(tmp_path), "%s/%s", quota_outer_path,
		    tmp_dir_name);
		ret = stat(tmp_path, &tmp_st);
		fail_unless(ret != 0, "Bad setup, tmps already exist");
		fail_unless(errno == ENOENT, "Bad setup, tmps already exist");
	}
	if (quota_inner_path != NULL) {
		snprintf(tmp_path, sizeof(tmp_path), "%s/%s", quota_inner_path,
		    tmp_dir_name);
		ret = stat(tmp_path, &tmp_st);
		fail_unless(ret != 0, "Bad setup, tmps already exist");
		fail_unless(errno == ENOENT, "Bad setup, tmps already exist");
	}

	/* setup_tmp_dirs */
	sw_ctx->global.enable_hash_tmpdir = true;
	tmp_fd = initialize_tmp_map(test_path, sw_ctx->tmonitor.policy_id,
	    &sw_ctx->global, restore, true, &error);
	fail_if_error(error);
	close(tmp_fd);
	/* validate tmp dirs were made */
	snprintf(tmp_path, sizeof(tmp_path), "%s/%s", test_path, tmp_dir_name);
	ret = stat(tmp_path, &tmp_st);
	fail_unless(ret == 0);
	if (quota_outer_path != NULL) {
		tlin = get_tmp_lin(quota_outer_st->st_ino, &sw_ctx->global,
		    &ptlin, &error);
		fail_if_error(error);
		snprintf(tmp_path, sizeof(tmp_path), "%s/%s", quota_outer_path,
		    tmp_dir_name);
		ret = stat(tmp_path, &tmp_st);
		fail_unless(ret == 0 && ptlin == tmp_st.st_ino,
		    "tmp_path: %s ret: %d ptlin: %llx tmp_st.st_ino: %llx",
		    tmp_path, ret, ptlin, tmp_st.st_ino);
	}
	if (quota_inner_path != NULL)  {
		tlin = get_tmp_lin(dir_st->st_ino, &sw_ctx->global, &ptlin,
		    &error);
		fail_if_error(error);
		fail_if(tlin == 0);
		snprintf(tmp_path, sizeof(tmp_path), "%s/%s", quota_inner_path,
		    tmp_dir_name);
		ret = stat(tmp_path, &tmp_st);
		fail_unless(ret == 0 && ptlin == tmp_st.st_ino,
		    "tmp_path: %s ret: %d ptlin: %llx tmp_st.st_ino: %llx",
		    tmp_path, ret, ptlin, tmp_st.st_ino);
	}

	/* validate that quotas_present was set in the global constructor */
	if (!expect_quotas)
		fail_unless(sw_ctx->global.quotas_present == false);
	else
		fail_unless(sw_ctx->global.quotas_present == true);

	/* validate tmp map that was created correctly */
	snprintf(tmp_path, sizeof(tmp_path), "%s/%s", test_path, tmp_dir_name);
	ret = stat(tmp_path, &tmp_st);
	fail_unless(sw_ctx->global.root_tmp_lin == tmp_st.st_ino,
	    "root tmp lin (%llx) does not match expected lin (%llx)",
	    sw_ctx->global.root_tmp_lin, tmp_st.st_ino);
	if (quota_outer_path != NULL && quota_outer_st != NULL) {
		snprintf(tmp_path, sizeof(tmp_path), "%s/%s", quota_outer_path,
		    tmp_dir_name);
		ret = stat(tmp_path, &tmp_st);
		fail_unless(ret == 0);
		tmp_lin = tmp_map_find(&sw_ctx->global,
		    quota_outer_st->st_ino, &error);
		fail_if_error(error);

		if (tmp_st.st_ino == sw_ctx->global.root_tmp_lin) {
			/* root tmp lin is not supposed to be in the mapping */
			fail_if(tmp_lin != NULL, "root tmp mapping exists.");
		} else {
			fail_if(tmp_lin == NULL);
			fail_if(*tmp_lin != tmp_st.st_ino,
			    "mapping lin (%llx) does not match expected lin "
			    "(%llx)", *tmp_lin, tmp_st.st_ino);
		}
	}
	if (quota_inner_path != NULL && quota_inner_st != NULL) {
		snprintf(tmp_path, sizeof(tmp_path), "%s/%s", quota_inner_path,
		    tmp_dir_name);
		ret = stat(tmp_path, &tmp_st);
		fail_unless(ret == 0);
		tmp_lin = tmp_map_find(&sw_ctx->global,
		    quota_inner_st->st_ino, &error);
		fail_if_error(error);

		if (tmp_st.st_ino == sw_ctx->global.root_tmp_lin) {
			/* root tmp lin is not supposed to be in the mapping */
			fail_if(tmp_lin != NULL, "root tmp mapping exists.");
		} else {
			fail_if(tmp_lin == NULL);
			fail_if(*tmp_lin != tmp_st.st_ino,
			    "mapping lin (%llx) does not match expected lin "
			    "(%llx)", *tmp_lin, tmp_st.st_ino);
		}
	}
}

/* runs cleanup_tmp_dirs on a path, checks to make
   sure all tmp dirs were successfully removed.
   The quota paths/stat parameters are optional */
static void
run_test_cleanup_tmp_dirs(
    struct migr_sworker_ctx *sw_ctx,
    const char *test_path,
    const char *quota_outer_path,
    const struct stat *quota_outer_st,
    const char *quota_inner_path,
    const struct stat *quota_inner_st,
    bool restore)
{
	int ret = -1;
	char tmp_path[MAXPATHLEN + 1];
	char tmp_dir_name[MAXPATHLEN + 1];
	struct stat tmp_st;
	struct isi_error *error = NULL;

	/* setup junk */
	get_tmp_working_dir_name(tmp_dir_name, restore);

	/* cleanup_tmp_dirs */
cleanup:
	cleanup_tmp_dirs(test_path, restore, sw_ctx, &error);
	fail_if_error(error);

	/* validate tmp dirs were removed */
	sprintf(tmp_path, "%s/%s", test_path, tmp_dir_name);
	ret = stat(tmp_path, &tmp_st);
	fail_unless(ret != 0);
	fail_unless(errno == ENOENT);
	if (quota_outer_path != NULL) {
		sprintf(tmp_path, "%s/%s", quota_outer_path, tmp_dir_name);
		ret = stat(tmp_path, &tmp_st);
		fail_unless(ret != 0);
		fail_unless(errno == ENOENT);
	}
	if (quota_inner_path != NULL) {
		sprintf(tmp_path, "%s/%s", quota_inner_path, tmp_dir_name);
		ret = stat(tmp_path, &tmp_st);
		fail_unless(ret != 0);
		fail_unless(errno == ENOENT);
	}
}

/* given a test lin to check, use get_tmp_lin, get_tmp_fd, and is_tmp_lin.
   Validate the results are as expected with the tmp dir returned the same
   as the one given by the tmp_dir_parent/.tmp_working_dir[_restore] */
static void
check_against_tmp_map(ifs_lin_t to_check,
    char *tmp_dir_parent,
    bool restore,
    struct migr_sworker_ctx *sw_ctx)
{
	ifs_lin_t lin_returned;
	ifs_lin_t tlin;
	ifs_lin_t mask;
	int fd_returned = -1;
	int ret;
	char tmp_dir_name[MAXPATHLEN + 1];
	char tmp_path[MAXPATHLEN + 1];
	char *hash_dir_name = NULL;
	struct stat tmp_st;
	struct stat fd_st;
	struct isi_error *error = NULL;
	bool is_tmp_return = false;
	bool expected_tmp = false;

	get_tmp_working_dir_name(tmp_dir_name, restore);
	snprintf(tmp_path, sizeof(tmp_path), "%s/%s", tmp_dir_parent,
	    tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0, "tmp_dir_parent: %s",
	    tmp_dir_parent);

	lin_returned = get_tmp_lin(to_check, &sw_ctx->global, &tlin, &error);
	fail_if_error(error);
	/* checking the tmp lin of a tmp lin should return the root */
	if (to_check == tmp_st.st_ino) {
		fail_if(tlin != sw_ctx->global.root_tmp_lin,
		    "get_tmp_lin returned %llx, "
		    "expected %llx, when checking %llx",
		    tlin, sw_ctx->global.root_tmp_lin, to_check);
	} else {
		fail_if(tlin != tmp_st.st_ino,
		    "get_tmp_lin returned %llx, "
		    "expected %llx, when checking %llx",
		    tlin, tmp_st.st_ino, to_check);
	}

	mask = get_hash_dirname(to_check, &hash_dir_name);
	fail_if(hash_dir_name == NULL);
	fd_returned = ifs_lin_open(tlin, HEAD_SNAPID, O_RDONLY);
	fail_unless(fd_returned != -1);
	ret = enc_fstatat(fd_returned, hash_dir_name, ENC_DEFAULT,
	    &tmp_st, AT_SYMLINK_NOFOLLOW);
	close(fd_returned);
	fail_if(ret != 0, "stat at %llx %s: %s", tlin, hash_dir_name,
	    strerror(errno));

	fd_returned = get_tmp_fd(to_check, &sw_ctx->global, &error);
	fail_if_error(error);
	fail_if(fstat(fd_returned, &fd_st) < 0);
	fail_if(fd_st.st_ino != lin_returned, "get_tmp_fd returned fd lin "
	    "%llx, expected %llx, when checking %llx",
	    fd_st.st_ino, lin_returned, to_check);
	close(fd_returned);

	is_tmp_return = is_tmp_lin(to_check, &sw_ctx->global, &error);
	fail_if_error(error);
	expected_tmp = (to_check == tlin);
	fail_if(is_tmp_return != expected_tmp,
	    "is_tmp_return: expected: %d, received %d",
	    expected_tmp, is_tmp_return);

	free(hash_dir_name);
}

/****************************GLOBALS******************************/
static char quota_outer_path[MAXPATHLEN + 1];
static char policy_id[POLICY_ID_LEN + 1];
static struct stat quota_outer_st;
static struct quota_domain domain_outer;
static char quota_inner_path[MAXPATHLEN + 1];
static struct stat quota_inner_st;
static struct quota_domain domain_inner;
static char dir_path[MAXPATHLEN + 1];
static struct stat dir_st;

/****************************SETUP/TEARDOWN***********************/
TEST_FIXTURE(test_setup)
{
	standard_two_quota_setup(
	    quota_outer_path,
	    policy_id,
	    &quota_outer_st,
	    &domain_outer,
	    quota_inner_path,
	    &quota_inner_st,
	    &domain_inner,
	    dir_path,
	    &dir_st);
}

TEST_FIXTURE(test_teardown)
{
	remove_quota_dir(&domain_inner, quota_inner_path,
	    quota_inner_st.st_ino);
	remove_quota_dir(&domain_outer, quota_outer_path,
	    quota_outer_st.st_ino);
}

/****************************TESTS********************************/
TEST(test_tmpdir_map_2q, .attrs="overnight")
{
	bool restore = false;
	struct migr_sworker_ctx sw_ctx = {};
	char tmp_dir_name[MAXPATHLEN + 1];
	char tmp_path[MAXPATHLEN + 1];
	struct stat tmp_st;
	get_tmp_working_dir_name(tmp_dir_name, restore);

	/* test and validate setup */
	sw_ctx.tmonitor.policy_id = policy_id;
	sw_ctx.global.restore = restore;
	run_test_setup_init_tmp_dirs(
	    &sw_ctx, quota_outer_path,
	    quota_outer_path, &quota_outer_st,
	    quota_inner_path, &quota_inner_st, &dir_st,
	    restore, true);

	/* now try get fd,lin and is_tmp_lin on the dirents of the quota */
	check_against_tmp_map(dir_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);
	check_against_tmp_map(quota_inner_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);
	check_against_tmp_map(quota_outer_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);

	/* now get some of the tmp dirs and check those too */
	sprintf(tmp_path, "%s/%s", quota_outer_path, tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0);
	check_against_tmp_map(tmp_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);
	sprintf(tmp_path, "%s/%s", quota_inner_path, tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0);
	check_against_tmp_map(tmp_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);

	/* test and validate cleanup */
	run_test_cleanup_tmp_dirs(
	    &sw_ctx, quota_outer_path,
	    quota_outer_path, &quota_outer_st,
	    quota_inner_path, &quota_inner_st,
	    restore);

}

TEST(test_tmpdir_map_2q_restore, .attrs="overnight")
{
	bool restore = true;
	struct migr_sworker_ctx sw_ctx = {};
	char tmp_dir_name[MAXPATHLEN + 1];
	char tmp_path[MAXPATHLEN + 1];
	struct stat tmp_st;
	get_tmp_working_dir_name(tmp_dir_name, restore);

	/* test and validate setup */
	sw_ctx.tmonitor.policy_id = policy_id;
	sw_ctx.global.restore = restore;
	run_test_setup_init_tmp_dirs(
	    &sw_ctx, quota_outer_path,
	    quota_outer_path, &quota_outer_st,
	    quota_inner_path, &quota_inner_st, &dir_st,
	    restore, true);

	/* now try get fd,lin and is_tmp_lin on the dirents of the quota */
	check_against_tmp_map(dir_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);
	check_against_tmp_map(quota_inner_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);
	check_against_tmp_map(quota_outer_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);

	/* now get some of the tmp dirs and check those too */
	sprintf(tmp_path, "%s/%s", quota_outer_path, tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0);
	check_against_tmp_map(tmp_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);
	sprintf(tmp_path, "%s/%s", quota_inner_path, tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0);
	check_against_tmp_map(tmp_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);

	/* test and validate cleanup */
	run_test_cleanup_tmp_dirs(
	    &sw_ctx, quota_outer_path,
	    quota_outer_path, &quota_outer_st,
	    quota_inner_path, &quota_inner_st,
	    restore);
}

TEST(test_tmpdir_map_1q, .attrs="overnight")
{
	bool restore = false;
	struct migr_sworker_ctx sw_ctx = {};
	char tmp_dir_name[MAXPATHLEN + 1];
	char tmp_path[MAXPATHLEN + 1];
	struct stat tmp_st;
	get_tmp_working_dir_name(tmp_dir_name, restore);

	/* test and validate setup */
	sw_ctx.tmonitor.policy_id = policy_id;
	sw_ctx.global.restore = restore;
	run_test_setup_init_tmp_dirs(
	    &sw_ctx, quota_inner_path,
	    NULL, NULL,
	    quota_inner_path, &quota_inner_st, &dir_st,
	    restore, false);

	/* now try get fd,lin and is_tmp_lin on the dirents of the quota */
	check_against_tmp_map(dir_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);
	check_against_tmp_map(quota_inner_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);

	/* now get some of the tmp dirs and check those too */
	sprintf(tmp_path, "%s/%s", quota_inner_path, tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0);
	check_against_tmp_map(tmp_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);

	/* test and validate cleanup */
	run_test_cleanup_tmp_dirs(
	    &sw_ctx, quota_inner_path,
	    NULL, NULL,
	    quota_inner_path, &quota_inner_st,
	    restore);
}

TEST(test_tmpdir_map_1q_restore, .attrs="overnight")
{
	bool restore = true;
	struct migr_sworker_ctx sw_ctx = {};
	char tmp_dir_name[MAXPATHLEN + 1];
	char tmp_path[MAXPATHLEN + 1];
	struct stat tmp_st;
	get_tmp_working_dir_name(tmp_dir_name, restore);

	/* test and validate setup */
	sw_ctx.tmonitor.policy_id = policy_id;
	sw_ctx.global.restore = restore;
	run_test_setup_init_tmp_dirs(
	    &sw_ctx, quota_inner_path,
	    NULL, NULL,
	    quota_inner_path, &quota_inner_st, &dir_st,
	    restore, false);

	/* now try get fd,lin and is_tmp_lin on the dirents of the quota */
	check_against_tmp_map(dir_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);
	check_against_tmp_map(quota_inner_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);

	/* now get some of the tmp dirs and check those too */
	sprintf(tmp_path, "%s/%s", quota_inner_path, tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0);
	check_against_tmp_map(tmp_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);

	/* test and validate cleanup */
	run_test_cleanup_tmp_dirs(
	    &sw_ctx, quota_inner_path,
	    NULL, NULL,
	    quota_inner_path, &quota_inner_st,
	    restore);
}

TEST(test_tmpdir_map_0q, .attrs="overnight")
{
	bool restore = false;
	struct migr_sworker_ctx sw_ctx = {};
	char tmp_dir_name[MAXPATHLEN + 1];
	char tmp_path[MAXPATHLEN + 1];
	struct stat tmp_st;
	get_tmp_working_dir_name(tmp_dir_name, restore);

	/* test and validate setup */
	sw_ctx.tmonitor.policy_id = policy_id;
	sw_ctx.global.restore = restore;
	run_test_setup_init_tmp_dirs(
	    &sw_ctx, dir_path,
	    NULL, NULL,
	    NULL, NULL, NULL,
	    restore, false);

	/* now try get fd,lin and is_tmp_lin on the dirents of the quota */
	check_against_tmp_map(dir_st.st_ino, dir_path,
	    restore, &sw_ctx);

	/* now get some of the tmp dirs and check those too */
	sprintf(tmp_path, "%s/%s", dir_path, tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0);
	check_against_tmp_map(tmp_st.st_ino, dir_path,
	    restore, &sw_ctx);

	/* test and validate cleanup */
	run_test_cleanup_tmp_dirs(
	    &sw_ctx, dir_path,
	    NULL, NULL,
	    NULL, NULL,
	    restore);
}

TEST(test_tmpdir_map_0q_restore, .attrs="overnight")
{
	bool restore = true;
	struct migr_sworker_ctx sw_ctx = {};
	char tmp_dir_name[MAXPATHLEN + 1];
	char tmp_path[MAXPATHLEN + 1];
	struct stat tmp_st;
	get_tmp_working_dir_name(tmp_dir_name, restore);

	/* test and validate setup */
	sw_ctx.tmonitor.policy_id = policy_id;
	sw_ctx.global.restore = restore;
	run_test_setup_init_tmp_dirs(
	    &sw_ctx, dir_path,
	    NULL, NULL,
	    NULL, NULL, NULL,
	    restore, false);

	/* now try get fd,lin and is_tmp_lin on the dirents of the quota */
	check_against_tmp_map(dir_st.st_ino, dir_path,
	    restore, &sw_ctx);

	/* now get some of the tmp dirs and check those too */
	sprintf(tmp_path, "%s/%s", dir_path, tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0);
	check_against_tmp_map(tmp_st.st_ino, dir_path,
	    restore, &sw_ctx);

	/* test and validate cleanup */
	run_test_cleanup_tmp_dirs(
	    &sw_ctx, dir_path,
	    NULL, NULL,
	    NULL, NULL,
	    restore);
}

TEST(test_tmpdir_map_2q_with_domain, .attrs="overnight")
{
	bool restore = false;
	struct migr_sworker_ctx sw_ctx = {};
	char tmp_dir_name[MAXPATHLEN + 1];
	char tmp_path[MAXPATHLEN + 1];
	struct stat tmp_st;
	get_tmp_working_dir_name(tmp_dir_name, restore);
	ifs_domainid_t domid = 0;
	uint32_t generation;

	/* this time we will actually use the domainid */
	fail_if(dom_make_domain(dir_st.st_ino, &domid, &generation));
	sw_ctx.global.domain_id = domid;

	/* test and validate setup */
	sw_ctx.tmonitor.policy_id = policy_id;
	sw_ctx.global.restore = restore;
	run_test_setup_init_tmp_dirs(
	    &sw_ctx, quota_outer_path,
	    quota_outer_path, &quota_outer_st,
	    quota_inner_path, &quota_inner_st, &dir_st,
	    restore, true);

	/* now try get fd,lin and is_tmp_lin on the dirents of the quota */
	check_against_tmp_map(dir_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);
	check_against_tmp_map(quota_inner_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);
	check_against_tmp_map(quota_outer_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);

	/* now get some of the tmp dirs and check those too */
	sprintf(tmp_path, "%s/%s", quota_outer_path, tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0);
	check_against_tmp_map(tmp_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);
	sprintf(tmp_path, "%s/%s", quota_inner_path, tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0);
	check_against_tmp_map(tmp_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);

	/* test and validate cleanup */
	run_test_cleanup_tmp_dirs(
	    &sw_ctx, quota_outer_path,
	    quota_outer_path, &quota_outer_st,
	    quota_inner_path, &quota_inner_st,
	    restore);

	fail_if(dom_remove_domain(domid));
}
#define DELETED_ENTRY "deleted"
#define NO_QUOTA_ENTRY "no-quota"
TEST(test_tmpdir_map_resume, .attrs="overnight")
{
	bool restore = false;
	struct migr_sworker_ctx sw_ctx = { 0 };
	char tmp_dir_name[MAXPATHLEN + 1];
	char tmp_path[MAXPATHLEN + 1];
	char *hashdir = NULL;
	struct stat tmp_st, st;
	struct isi_error *error = NULL;
	ifs_lin_t tlin, deleted_lin, no_quota_lin, no_quota_tmp_lin;
	int tmp_fd = -1, fd = -1;
	bool found;

	get_tmp_working_dir_name(tmp_dir_name, restore);

	/* test and validate setup */
	sw_ctx.tmonitor.policy_id = policy_id;
	sw_ctx.global.restore = restore;
	run_test_setup_init_tmp_dirs(
	    &sw_ctx, quota_outer_path,
	    quota_outer_path, &quota_outer_st,
	    quota_inner_path, &quota_inner_st, &dir_st,
	    restore, true);

	/* check tmpdir hashing init */
	fail_if(sw_ctx.global.tmpdir_hash == NULL);

	/* set mapping to INVALID_LIN */
	set_mapping(quota_inner_st.st_ino, INVALID_LIN,
	    sw_ctx.global.tmp_working_map_ctx, &error);
	fail_if_error(error);

	/* delete mapping */
	remove_mapping(quota_outer_st.st_ino,
	    sw_ctx.global.tmp_working_map_ctx, &error);
	fail_if_error(error);
	close_linmap(sw_ctx.global.tmp_working_map_ctx);
	sw_ctx.global.tmp_working_map_ctx = NULL;
	uint64_uint64_map_clean(sw_ctx.global.tmp_working_map);
	free(sw_ctx.global.tmp_working_map);
	sw_ctx.global.tmp_working_map = NULL;
	tmp_fd = initialize_tmp_map(quota_outer_path,
	    sw_ctx.tmonitor.policy_id,
	    &sw_ctx.global, restore, true, &error);
	fail_if_error(error);

	fail_if(sw_ctx.global.tmpdir_hash == NULL);

	/* test is_tmp_lin for tmp hash dir */
	get_hash_dirname(INVALID_LIN, &hashdir);
	fail_if(hashdir == NULL);
	fail_unless(enc_fstatat(tmp_fd, hashdir, ENC_DEFAULT, &st,
	    AT_SYMLINK_NOFOLLOW) == 0);
	found = is_tmp_lin(st.st_ino, &sw_ctx.global, &error);
	fail_if_error(error);
	fail_unless(found);

	/* replace hash sentinel with file */
	fail_unless(enc_unlinkat(tmp_fd, hashdir, ENC_DEFAULT,
	    AT_REMOVEDIR) == 0);
	fd = enc_openat(tmp_fd, hashdir, ENC_DEFAULT,
	    O_RDWR|O_CREAT|O_NOFOLLOW|O_OPENLINK, 0666);
	fail_if(fd == -1);
	close(fd);
	fd = -1;
	fail_unless(fstat(tmp_fd, &st) == 0);
	close(tmp_fd);
	tmp_fd = -1;

	/* now try get fd,lin and is_tmp_lin on the dirents of the quota */
	check_against_tmp_map(dir_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);
	check_against_tmp_map(quota_inner_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);
	check_against_tmp_map(quota_outer_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);

	/* now get some of the tmp dirs and check those too */
	sprintf(tmp_path, "%s/%s", quota_outer_path, tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0);
	check_against_tmp_map(tmp_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);
	sprintf(tmp_path, "%s/%s", quota_inner_path, tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0);
	check_against_tmp_map(tmp_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);

	/* set mapping to different LIN */
	set_mapping(quota_inner_st.st_ino, MAX_LIN,
	    sw_ctx.global.tmp_working_map_ctx, &error);
	fail_if_error(error);

	/* add fake mapping */
	tmp_fd = ifs_lin_open(quota_outer_st.st_ino, HEAD_SNAPID, O_RDONLY);
	fail_if(tmp_fd == -1);
	fail_unless(enc_mkdirat(tmp_fd, DELETED_ENTRY, ENC_DEFAULT,
	    0700) == 0);
	fail_unless(enc_fstatat(tmp_fd, DELETED_ENTRY, ENC_DEFAULT, &tmp_st,
	    AT_SYMLINK_NOFOLLOW) == 0);
	fail_unless(enc_unlinkat(tmp_fd, DELETED_ENTRY, ENC_DEFAULT,
	    AT_REMOVEDIR) == 0);
	close(tmp_fd);
	deleted_lin = tmp_st.st_ino;
	set_mapping(deleted_lin, INVALID_LIN,
	    sw_ctx.global.tmp_working_map_ctx, &error);
	fail_if_error(error);

	/* add no quota mapping */
	tmp_fd = ifs_lin_open(quota_outer_st.st_ino, HEAD_SNAPID, O_RDONLY);
	fail_if(tmp_fd == -1);
	fail_unless(enc_mkdirat(tmp_fd, NO_QUOTA_ENTRY, ENC_DEFAULT,
	    0700) == 0);
	fail_unless(enc_fstatat(tmp_fd, NO_QUOTA_ENTRY, ENC_DEFAULT, &tmp_st,
	    AT_SYMLINK_NOFOLLOW) == 0);
	close(tmp_fd);
	no_quota_lin = tmp_st.st_ino;
	tmp_fd = ifs_lin_open(no_quota_lin, HEAD_SNAPID, O_RDONLY);
	fail_if(tmp_fd == -1);
	fail_unless(enc_mkdirat(tmp_fd, tmp_dir_name, ENC_DEFAULT, 0700) == 0);
	fail_unless(enc_fstatat(tmp_fd, tmp_dir_name, ENC_DEFAULT, &tmp_st,
	    AT_SYMLINK_NOFOLLOW) == 0);
	close(tmp_fd);
	no_quota_tmp_lin = tmp_st.st_ino;
	set_mapping(no_quota_lin, no_quota_tmp_lin,
	    sw_ctx.global.tmp_working_map_ctx, &error);
	fail_if_error(error);

	close_linmap(sw_ctx.global.tmp_working_map_ctx);
	sw_ctx.global.tmp_working_map_ctx = NULL;
	uint64_uint64_map_clean(sw_ctx.global.tmp_working_map);
	free(sw_ctx.global.tmp_working_map);
	sw_ctx.global.tmp_working_map = NULL;


	/* check invalid hash sentinel */
	tmp_fd = initialize_tmp_map(quota_outer_path,
	    sw_ctx.tmonitor.policy_id,
	    &sw_ctx.global, restore, true, &error);
	fail_unless_error(error, "initialize_tmp_map");
	fail_unless(tmp_fd == -1);
	tmp_fd = ifs_lin_open(st.st_ino, HEAD_SNAPID, O_RDONLY);
	fail_if(tmp_fd == -1);
	fail_unless(enc_unlinkat(tmp_fd, hashdir, ENC_DEFAULT, 0) == 0);
	close(tmp_fd);
	tmp_fd = -1;
	cleanup_tmp_dirs(quota_outer_path, restore, &sw_ctx, &error);
	fail_if_error(error);

	tmp_fd = initialize_tmp_map(quota_outer_path,
	    sw_ctx.tmonitor.policy_id,
	    &sw_ctx.global, restore, true, &error);
	fail_if_error(error);
	close(tmp_fd);

	/* check that entry for deleted directory is removed */
	found = get_mapping(deleted_lin, &tlin,
	    sw_ctx.global.tmp_working_map_ctx, &error);
	fail_if_error(error);
	fail_if(found);

	/* check that entry for no-quota directory exists */
	found = get_mapping(no_quota_lin, &tlin,
	    sw_ctx.global.tmp_working_map_ctx, &error);
	fail_if_error(error);
	fail_unless(found);
	fail_unless(tlin == no_quota_tmp_lin);

	/* now try get fd,lin and is_tmp_lin on the dirents of the quota */
	check_against_tmp_map(dir_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);
	check_against_tmp_map(quota_inner_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);
	check_against_tmp_map(quota_outer_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);

	/*
	 * Set memory mapping to INVALID_LIN and confirm memory mapping
	 * updated with disk mapping.
	 */
	tlin = INVALID_LIN;
	uint64_uint64_map_add(sw_ctx.global.tmp_working_map,
	    quota_inner_st.st_ino, &tlin);
	check_against_tmp_map(quota_inner_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);

	/* now get some of the tmp dirs and check those too */
	sprintf(tmp_path, "%s/%s", quota_outer_path, tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0);
	check_against_tmp_map(tmp_st.st_ino, quota_outer_path,
	    restore, &sw_ctx);
	sprintf(tmp_path, "%s/%s", quota_inner_path, tmp_dir_name);
	fail_if(stat(tmp_path, &tmp_st) < 0);
	check_against_tmp_map(tmp_st.st_ino, quota_inner_path,
	    restore, &sw_ctx);

	/* test and validate cleanup */
	run_test_cleanup_tmp_dirs(
	    &sw_ctx, quota_outer_path,
	    quota_outer_path, &quota_outer_st,
	    quota_inner_path, &quota_inner_st,
	    restore);

	free(hashdir);
}
