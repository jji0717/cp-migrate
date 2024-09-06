
#include <isi_migrate/migr/check_helper.h>
#include <isi_quota/check_quota_helper.h>
#include <ifs/ifs_lin_open.h>
#include "sworker.h"

/* Test Globals */
static char *file1 = "testfile1";
static char *file2 = "testfile2";
static char path[MAXPATHLEN + 1];
static ifs_lin_t bad_lin = 0x99999999;
static char policy_id[POLICY_ID_LEN + 1];

struct migr_sworker_ctx sw_ctx;

TEST_FIXTURE(test_setup)
{
        char *pid = NULL;
        // Empty for now
        memset(&sw_ctx, 0, sizeof(struct migr_sworker_ctx));
        init_statistics(&sw_ctx.stats.stf_cur_stats);
        pid = siq_create_policy_id(false);
        fail_if(pid == NULL);
        strlcpy(policy_id, pid, sizeof(policy_id));
        free(pid);
        pid = NULL;
        sw_ctx.tmonitor.policy_id = policy_id;
        sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
        sw_ctx.tmonitor.coord_fd = -1;
}

TEST_FIXTURE(test_teardown)
{
        // Empty for now
        if (sw_ctx.stats.stf_cur_stats) {
                siq_stat_free(sw_ctx.stats.stf_cur_stats);
                sw_ctx.stats.stf_cur_stats = NULL;
        }
}


SUITE_DEFINE_FOR_FILE(migr_quota, .test_setup = test_setup,
	.test_teardown = test_teardown);

/*
 * fd is the parent fd for the new file
 * enc is the desired encoding
 */
inline static char *
create_new_dir_with_enc(int fd, int enc)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	int i, ret;

	for (i = 0;; i++) {
		fmt_print(&fmt, "test.siq.enc.%04d", i);
		ret = enc_mkdirat(fd, fmt_string(&fmt), enc, 0777);
		if (ret == 0)
		    break;
		else if (errno != EEXIST)
			fail_unless(errno == EEXIST, "errno = %d", errno);
		fmt_clean(&fmt);
	}

	return strdup(fmt_string(&fmt));
}

/* checks for any temporary directories made by a treemv.
 * Removes them if found, and also deletes them when found */
inline static bool
clean_mvtmp_in_dir(char *testdir_path)
{
	char *tmp_path = NULL;
	bool found_tmp = false;
	DIR *testdir = NULL;
	struct dirent *d = NULL;
	testdir = opendir(testdir_path);
	do {
		d = readdir(testdir);
		if (d && 0 == strncmp(d->d_name, TW_MV_TMPNAME, 6)) {
			sprintf(path, "%s/%s", testdir_path, d->d_name);
			found_tmp = true;
			tmp_path = strdup(path);
			clean_dir(tmp_path); /* frees tmp_path */
		}
	} while (d != NULL);

	if (testdir)
		closedir(testdir);

	return found_tmp;
}

/* creates an ads file */
inline static void
create_ads(const char *container, const char* stream)
{
	int fd, ads_fd, flags;
	struct stat st;

	fail_if(stat(container, &st) < 0, "stat %s:%s: %s", container, stream,
	    strerror(errno));
	if (S_ISDIR(st.st_mode))
		flags = O_RDONLY;
	else
		flags = O_RDWR;

	fd = open(container, flags);
	fail_if(fd < 0, "open %s: %s", container, strerror(errno));
	ads_fd = enc_openat(fd, stream, ENC_DEFAULT,
	    O_CREAT | O_RDWR | O_XATTR, 0775);
	fail_if(ads_fd < 0, "enc_openat %s: %s", stream, strerror(errno));
	close(ads_fd);
	close(fd);
}

inline static void
create_and_setup_quota(struct quota_domain *domain, struct quota_domain_id *qdi)
{
	SETUP_DOMAIN(domain, qdi);
	GET_DOMAIN(domain, qdi);
}

inline static void
clean_basedir(char *dir)
{
        struct fmt FMT_INIT_CLEAN(fmt);

        fmt_print(&fmt, "rm -rf -- %s", dir);
        system(fmt_string(&fmt));
}

/****************************QUOTA CROSS TESTS****************************/

/* single same quota test */
TEST(test_quota_cross_1q_2q, .attrs="overnight")
{
	struct stat file1_st;
	struct stat file2_st;
	char quota_path[MAXPATHLEN + 1];
	struct quota_domain_id qdi;
	struct quota_domain domain;
	ifs_lin_t quota_lin;
	int ret = -1;
	struct isi_error *error = NULL;
	bool func_ret;

	/* setup */
	create_test_dir(quota_path, &quota_lin, &qdi);
	create_and_setup_quota(&domain, &qdi);

	create_file(quota_path, file1);
	sprintf(path, "%s/%s", quota_path, file1);
	ret = stat(path, &file1_st);
	fail_unless(ret == 0);

	create_file(quota_path, file2);
	sprintf(path, "%s/%s", quota_path, file2);
	ret = stat(path, &file2_st);
	fail_unless(ret == 0);

	/* test */
	func_ret = crossed_quota(file1_st.st_ino, file2_st.st_ino, &error);

	/* validate */
	fail_if_error(error);
	fail_unless(func_ret == false);

	/* cleanup */
	quota_domain_delete(&domain, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_path, quota_lin);
}

/* only a quota on file1, none on file 2*/
TEST(test_quota_cross_1q_2n, .attrs="overnight")
{
	struct stat file1_st;
	struct stat file2_st;
	char quota_path[MAXPATHLEN + 1];
	struct quota_domain_id qdi;
	struct quota_domain domain;
	ifs_lin_t quota_lin;
	int ret = -1;
	bool func_ret;
	struct isi_error *error = NULL;
	char *srcdir_path = NULL;

	/* setup */
	create_test_dir(quota_path, &quota_lin, &qdi);
	create_and_setup_quota(&domain, &qdi);

	create_file(quota_path, file1);
	sprintf(path, "%s/%s", quota_path, file1);
	ret = stat(path, &file1_st);
	fail_unless(ret == 0);

	srcdir_path = create_new_dir();
	create_file(srcdir_path, file2);
	sprintf(path, "%s/%s", srcdir_path, file2);
	ret = stat(path, &file2_st);
	fail_unless(ret == 0);

	/* test */
	func_ret = crossed_quota(file1_st.st_ino, file2_st.st_ino, &error);

	/* validate */
	fail_if_error(error);
	fail_unless(func_ret == true);

	/* cleanup */
	quota_domain_delete(&domain, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_path, quota_lin);
	clean_dir(srcdir_path);
}

/* only a quota on file2, none on file 1*/
TEST(test_quota_cross_2q_1n, .attrs="overnight")
{
	struct stat file1_st;
	struct stat file2_st;
	char quota_path[MAXPATHLEN + 1];
	struct quota_domain_id qdi;
	struct quota_domain domain;
	ifs_lin_t quota_lin;
	int ret = -1;
	bool func_ret;
	struct isi_error *error = NULL;
	char *srcdir_path = NULL;

	/* setup */
	create_test_dir(quota_path, &quota_lin, &qdi);
	create_and_setup_quota(&domain, &qdi);

	create_file(quota_path, file2);
	sprintf(path, "%s/%s", quota_path, file2);
	ret = stat(path, &file2_st);
	fail_unless(ret == 0);

	srcdir_path = create_new_dir();
	create_file(srcdir_path, file1);
	sprintf(path, "%s/%s", srcdir_path, file1);
	ret = stat(path, &file1_st);
	fail_unless(ret == 0);

	/* test */
	func_ret = crossed_quota(file1_st.st_ino, file2_st.st_ino, &error);

	/* validate */
	fail_if_error(error);
	fail_unless(func_ret == true);

	/* cleanup */
	quota_domain_delete(&domain, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_path, quota_lin);
	clean_dir(srcdir_path);
}

/* multiple same quota test */
TEST(test_quota_cross_1qq_2qq, .attrs="overnight")
{
	struct stat file1_st;
	struct stat file2_st;
	char quota_path[MAXPATHLEN + 1];
	char quota_path_outer[MAXPATHLEN + 1];
	struct quota_domain_id qdi;
	struct quota_domain_id qdi_outer;
	struct quota_domain domain;
	struct quota_domain domain_outer;
	ifs_lin_t quota_lin;
	ifs_lin_t quota_lin_outer;
	int ret = -1;
	int num = 0;
	struct isi_error *error = NULL;
	bool func_ret;
	struct stat st;

	/* setup */
	create_test_dir(quota_path_outer, &quota_lin_outer, &qdi_outer);
	create_and_setup_quota(&domain_outer, &qdi_outer);

	/* manually set up inner quota */
	do {
		sprintf(quota_path, "%s/test.quotas.%d", quota_path_outer, num++);
		ret = mkdir(quota_path, 0755);
		fail_unless(ret == 0 || errno == EEXIST);
	} while (error);
	fail_unless(stat(quota_path, &st) == 0);
	quota_lin = st.st_ino;
	qdi_init(&qdi, quota_lin, QUOTA_DOMAIN_ALL, false, ANY_INSTANCE);
	create_and_setup_quota(&domain, &qdi);

	create_file(quota_path, file1);
	sprintf(path, "%s/%s", quota_path, file1);
	ret = stat(path, &file1_st);
	fail_unless(ret == 0);

	create_file(quota_path, file2);
	sprintf(path, "%s/%s", quota_path, file2);
	ret = stat(path, &file2_st);
	fail_unless(ret == 0);

	/* test */
	func_ret = crossed_quota(file1_st.st_ino, file2_st.st_ino, &error);

	/* validate */
	fail_if_error(error);
	fail_unless(func_ret == false);

	/* cleanup */
	quota_domain_delete(&domain, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_path, quota_lin);

	quota_domain_delete(&domain_outer, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_path_outer, quota_lin_outer);
}

/* no quotas at all */
TEST(test_quota_cross_no_q, .attrs="overnight")
{
	struct stat file1_st;
	struct stat file2_st;
	int ret = -1;
	bool func_ret;
	struct isi_error *error = NULL;
	char *srcdir_path = NULL;
	char *tgtdir_path = NULL;

	/* setup */
	tgtdir_path = create_new_dir();
	create_file(tgtdir_path, file1);
	sprintf(path, "%s/%s", tgtdir_path, file1);
	ret = stat(path, &file1_st);
	fail_unless(ret == 0);

	srcdir_path = create_new_dir();
	create_file(srcdir_path, file2);
	sprintf(path, "%s/%s", srcdir_path, file2);
	ret = stat(path, &file2_st);
	fail_unless(ret == 0);

	/* test */
	func_ret = crossed_quota(file1_st.st_ino, file2_st.st_ino, &error);

	/* validate */
	fail_if_error(error);
	fail_unless(func_ret == false);

	/* cleanup */
	clean_dir(srcdir_path);
	clean_dir(tgtdir_path);
}


/* only a quota on file1, none on file 2*/
TEST(test_quota_cross_1qa_2qb, .attrs="overnight")
{
	struct stat file1_st;
	struct stat file2_st;

	char quota_path_outer[MAXPATHLEN + 1];
	struct quota_domain_id qdi_outer;
	struct quota_domain domain_outer;
	ifs_lin_t quota_lin_outer;

	char quota_path_a[MAXPATHLEN + 1];
	struct quota_domain_id qdi_a;
	struct quota_domain domain_a;
	ifs_lin_t quota_lin_a;

	char quota_path_b[MAXPATHLEN + 1];
	struct quota_domain_id qdi_b;
	struct quota_domain domain_b;
	ifs_lin_t quota_lin_b;

	int ret = -1;
	int num = 0;
	struct isi_error *error = NULL;
	bool func_ret;
	struct stat st;

	/* setup */
	create_test_dir(quota_path_outer, &quota_lin_outer, &qdi_outer);
	create_and_setup_quota(&domain_outer, &qdi_outer);

	/* manually set up inner quota a */
	do {
		sprintf(quota_path_a, "%s/test.quotas.%d",
		    quota_path_outer, num++);
		ret = mkdir(quota_path_a, 0755);
		fail_unless(ret == 0 || errno == EEXIST);
	} while (error);
	fail_unless(stat(quota_path_a, &st) == 0);
	quota_lin_a = st.st_ino;
	qdi_init(&qdi_a, quota_lin_a, QUOTA_DOMAIN_ALL, false, ANY_INSTANCE);
	create_and_setup_quota(&domain_a, &qdi_a);

	/* manually set up inner quota b */
	do {
		sprintf(quota_path_b, "%s/test.quotas.%d",
		    quota_path_outer, num++);
		ret = mkdir(quota_path_b, 0755);
		fail_unless(ret == 0 || errno == EEXIST);
	} while (error);
	fail_unless(stat(quota_path_b, &st) == 0);
	quota_lin_b = st.st_ino;
	qdi_init(&qdi_b, quota_lin_b, QUOTA_DOMAIN_ALL, false, ANY_INSTANCE);
	create_and_setup_quota(&domain_b, &qdi_b);

	/* create file1 in quota a */
	create_file(quota_path_a, file1);
	sprintf(path, "%s/%s", quota_path_a, file1);
	ret = stat(path, &file1_st);
	fail_unless(ret == 0);

	/* create file1 in quota b */
	create_file(quota_path_b, file2);
	sprintf(path, "%s/%s", quota_path_b, file2);
	ret = stat(path, &file2_st);
	fail_unless(ret == 0);

	/* test */
	func_ret = crossed_quota(file1_st.st_ino, file2_st.st_ino, &error);

	/* validate */
	fail_if_error(error);
	fail_unless(func_ret == true);

	/* cleanup */
	quota_domain_delete(&domain_a, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_path_a, quota_lin_a);

	quota_domain_delete(&domain_b, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_path_b, quota_lin_b);

	quota_domain_delete(&domain_outer, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_path_outer, quota_lin_outer);
}

/* garbage lin a */
TEST(test_quota_cross_bad_a_lin, .attrs="overnight")
{
	struct stat file2_st;
	int ret = -1;
	bool func_ret;
	struct isi_error *error = NULL;
	char *srcdir_path = NULL;

	/* setup */
	srcdir_path = create_new_dir();
	create_file(srcdir_path, file2);
	sprintf(path, "%s/%s", srcdir_path, file2);
	ret = stat(path, &file2_st);
	fail_unless(ret == 0);

	/* test */
	func_ret = crossed_quota(bad_lin, file2_st.st_ino, &error);

	/* validate */
	fail_if_error(error);
	fail_unless(func_ret == false);

	/* cleanup */
	clean_dir(srcdir_path);
}

/* garbage lin b */
TEST(test_quota_cross_bad_b_lin, .attrs="overnight")
{
	struct stat file1_st;
	int ret = -1;
	bool func_ret;
	struct isi_error *error = NULL;
	char *srcdir_path = NULL;

	/* setup */
	srcdir_path = create_new_dir();
	create_file(srcdir_path, file1);
	sprintf(path, "%s/%s", srcdir_path, file1);
	ret = stat(path, &file1_st);
	fail_unless(ret == 0);

	/* test */
	func_ret = crossed_quota(file1_st.st_ino, bad_lin, &error);

	/* validate */
	fail_if_error(error);
	fail_unless(func_ret == false);

	/* cleanup */
	clean_dir(srcdir_path);
}

/****************************CHECK QUOTA READY TESTS**********************/

/* ready quota test */
TEST(test_quota_ready_yes, .attrs="overnight")
{
	char quota_path[MAXPATHLEN + 1];
	struct quota_domain_id qdi;
	struct quota_domain domain;
	ifs_lin_t quota_lin;
	struct isi_error *error = NULL;
	bool result = false;

	/* setup */
	create_test_dir(quota_path, &quota_lin, &qdi);
	create_and_setup_quota(&domain, &qdi);

	/* test */
	result = check_quota_ready(&domain, &error);

	/* validate */
	fail_if_error(error);
	fail_unless(result == true);

	/* cleanup */
	quota_domain_delete(&domain, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_path, quota_lin);
}

/* non ready quota test */
TEST(test_quota_ready_nope, .attrs="overnight")
{
	char quota_path[MAXPATHLEN + 1];
	struct quota_domain_id qdi;
	struct quota_domain domain;
	ifs_lin_t quota_lin;
	struct isi_error *error = NULL;
	bool result = false;

	/* setup */
	create_test_dir(quota_path, &quota_lin, &qdi);

	/* test */
	result = check_quota_ready(&domain, &error);

	/* validate */
	fail_unless(result == false);
	fail_unless_error(error);

	/* cleanup */
	clean_test_dir(quota_path, quota_lin);
}

/* null quota test */
TEST(test_quota_ready_null, .attrs="overnight")
{
	struct isi_error *error = NULL;
	bool result = false;

	/* setup */

	/* test */
	result = check_quota_ready(NULL, &error);

	/* validate */
	fail_unless(result == false);
	fail_if_error(error);

	/* cleanup */
}

/* valid quota, but no err ptr test */
TEST(test_quota_ready_errnull, .attrs="overnight")
{
	char quota_path[MAXPATHLEN + 1];
	struct quota_domain_id qdi;
	struct quota_domain domain;
	ifs_lin_t quota_lin;
	struct isi_error *error = NULL;
	bool result = false;

	/* setup */
	create_test_dir(quota_path, &quota_lin, &qdi);
	create_and_setup_quota(&domain, &qdi);

	/* test */
	result = check_quota_ready(&domain, NULL);

	/* validate */
	fail_unless(result == true);

	/* cleanup */
	quota_domain_delete(&domain, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_path, quota_lin);
}

/* non ready quota test */
TEST(test_quota_ready_nope_errnull, .attrs="overnight")
{
	char quota_path[MAXPATHLEN + 1];
	struct quota_domain_id qdi;
	struct quota_domain domain;
	ifs_lin_t quota_lin;
	bool result = false;

	/* setup */
	create_test_dir(quota_path, &quota_lin, &qdi);

	/* test */
	result = check_quota_ready(&domain, NULL);

	/* validate */
	fail_unless(result == false);

	/* cleanup */
	clean_test_dir(quota_path, quota_lin);
}

/****************************GET QUOTAS FOR LIN TESTS*********************/
static void
run_test_on_get_quotas_suite(ifs_lin_t lin_to_test,
    const struct quota_domain_id *qdi_outer,
    const struct quota_domain_id *qdi_inner,
    enum quota_lin_query search_type,
    bool expect_inner_found, bool expect_outer_found,
    int func_count)
{
	bool found_quota_inner = false;
	bool found_quota_outer = false;
	struct qdi_wrapper *entry, *tmp;
	struct quota_queue_head *returned_quotas = NULL;
	struct isi_error *error = NULL;

	/* test the lin */
	returned_quotas = get_quotas_for_lin(lin_to_test, search_type,
	    &error);
	fail_if_error(error, "error on get_quotas_for_lin:%d", func_count);
	fail_if(returned_quotas == NULL,
	    "expected quotas, but none returned %d", func_count);
	STAILQ_FOREACH_SAFE(entry, returned_quotas, next, tmp) {
		if (qdi_equal(&entry->qdi, qdi_outer))
			found_quota_outer = true;
		if (qdi_equal(&entry->qdi, qdi_inner))
			found_quota_inner = true;
		STAILQ_REMOVE_HEAD(returned_quotas, next);
		free(entry);
	}
	fail_if(found_quota_outer != expect_outer_found,
	    "failure on finding quota outer:%d: expected: %d, found: %d",
	    func_count, found_quota_outer, expect_outer_found);
	fail_if(found_quota_inner != expect_inner_found,
	    "failede on finding quota inner:%d: expected: %d, found: %d",
	    func_count, found_quota_inner, expect_inner_found);

	if (returned_quotas != NULL)
		free(returned_quotas);
}

/* Run multiple iterations of get_quotas_for_lin.
   On setups with 0,1,2 quotas above/below path. */
TEST(test_get_quotas_suite, .attrs="overnight")
{
	struct stat file1_st;
	struct stat quota_inner_st;
	struct stat quota_outer_st;
	char quota_path[MAXPATHLEN + 1];
	char quota_path_outer[MAXPATHLEN + 1];
	struct quota_domain_id qdi;
	struct quota_domain_id qdi_outer;
	struct quota_domain domain;
	struct quota_domain domain_outer;
	ifs_lin_t quota_lin;
	ifs_lin_t quota_lin_outer;
	int ret = -1;
	int num = 0;
	struct isi_error *error = NULL;
	struct stat st;

	/* setup */
	create_test_dir(quota_path_outer, &quota_lin_outer, &qdi_outer);
	create_and_setup_quota(&domain_outer, &qdi_outer);

	/* manually set up inner quota */
	do {
		sprintf(quota_path, "%s/test.quotas.%d",
		    quota_path_outer, num++);
		ret = mkdir(quota_path, 0755);
		fail_unless(ret == 0 || errno == EEXIST);
	} while (error);
	fail_unless(stat(quota_path, &st) == 0);
	quota_lin = st.st_ino;
	qdi_init(&qdi, quota_lin, QUOTA_DOMAIN_ALL, false, ANY_INSTANCE);
	create_and_setup_quota(&domain, &qdi);

	ret = stat(quota_path_outer, &quota_outer_st);
	fail_unless(ret == 0);

	ret = stat(quota_path, &quota_inner_st);
	fail_unless(ret == 0);

	create_file(quota_path, file1);
	sprintf(path, "%s/%s", quota_path, file1);
	ret = stat(path, &file1_st);
	fail_unless(ret == 0);

	/* test the outside lin */
	run_test_on_get_quotas_suite(quota_outer_st.st_ino,
	    quota_domain_get_id(&domain_outer), quota_domain_get_id(&domain),
	    QUOTA_LIN_QUERY_RECURSIVE,
	    true, true, 1);
	/* now going up */
	run_test_on_get_quotas_suite(quota_outer_st.st_ino,
	    quota_domain_get_id(&domain_outer), quota_domain_get_id(&domain),
	    QUOTA_LIN_QUERY_RECURSIVE_UP,
	    false, true, 2);

	/* test the inside lin */
	run_test_on_get_quotas_suite(quota_inner_st.st_ino,
	    quota_domain_get_id(&domain_outer), quota_domain_get_id(&domain),
	    QUOTA_LIN_QUERY_RECURSIVE,
	    true, false, 3);
	/* now going up */
	run_test_on_get_quotas_suite(quota_inner_st.st_ino,
	    quota_domain_get_id(&domain_outer), quota_domain_get_id(&domain),
	    QUOTA_LIN_QUERY_RECURSIVE_UP,
	    true, true, 4);

	/* test the file lin */
	run_test_on_get_quotas_suite(file1_st.st_ino,
	    quota_domain_get_id(&domain_outer), quota_domain_get_id(&domain),
	    QUOTA_LIN_QUERY_RECURSIVE,
	    false, false, 5);
	/* now going up */
	run_test_on_get_quotas_suite(file1_st.st_ino,
	    quota_domain_get_id(&domain_outer), quota_domain_get_id(&domain),
	    QUOTA_LIN_QUERY_RECURSIVE_UP,
	    true, true, 6);

	/* cleanup */
	quota_domain_delete(&domain, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_path, quota_lin);

	quota_domain_delete(&domain_outer, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_path_outer, quota_lin_outer);
}

/* test how hardlinks across quotas will return for get_quotas_for_lin */
TEST(test_get_quotas_hardlink_case, .attrs="overnight")
{
	struct stat file1_st;
	struct stat quota_a_st;
	struct stat quota_b_st;
	struct stat quota_outer_st;
	char quota_a_path[MAXPATHLEN + 1];
	char quota_b_path[MAXPATHLEN + 1];
	char quota_path_outer[MAXPATHLEN + 1];
	char path_link[MAXPATHLEN + 1];
	struct quota_domain_id qdi_a;
	struct quota_domain_id qdi_b;
	struct quota_domain_id qdi_outer;
	struct quota_domain domain_a;
	struct quota_domain domain_b;
	struct quota_domain domain_outer;
	ifs_lin_t quota_lin_outer;
	int ret = -1;
	int num = 0;
	struct isi_error *error = NULL;
	struct qdi_wrapper *entry, *tmp;
	struct quota_queue_head *returned_quotas = NULL;
	bool found_outer = false, found_a = false, found_b = false;

	/* setup */
	create_test_dir(quota_path_outer, &quota_lin_outer, &qdi_outer);
	create_and_setup_quota(&domain_outer, &qdi_outer);
	ret = stat(quota_path_outer, &quota_outer_st);
	fail_unless(ret == 0);

	/* manually set up inner quota A */
	do {
		sprintf(quota_a_path, "%s/test.quotas.%d",
		    quota_path_outer, num++);
		ret = mkdir(quota_a_path, 0755);
		fail_unless(ret == 0 || errno == EEXIST);
	} while (error);
	ret = stat(quota_a_path, &quota_a_st);
	fail_unless(ret == 0);
	qdi_init(&qdi_a, quota_a_st.st_ino, QUOTA_DOMAIN_ALL,
	    false, ANY_INSTANCE);
	create_and_setup_quota(&domain_a, &qdi_a);

	/* manually set up inner quota B */
	do {
		sprintf(quota_b_path, "%s/test.quotas.%d",
		    quota_path_outer, num++);
		ret = mkdir(quota_b_path, 0755);
		fail_unless(ret == 0 || errno == EEXIST);
	} while (error);
	ret = stat(quota_b_path, &quota_b_st);
	fail_unless(ret == 0);
	qdi_init(&qdi_b, quota_b_st.st_ino, QUOTA_DOMAIN_ALL,
	    false, ANY_INSTANCE);
	create_and_setup_quota(&domain_b, &qdi_b);

	/* make the file in quota a and hardlink into quota b */
	create_file(quota_a_path, file1);
	sprintf(path, "%s/%s", quota_a_path, file1);
	ret = stat(path, &file1_st);
	fail_unless(ret == 0);

	sprintf(path_link, "%s/%s", quota_b_path, file1);
	link(path, path_link);

	/* test the file lin */
	returned_quotas = get_quotas_for_lin(file1_st.st_ino,
	    QUOTA_LIN_QUERY_RECURSIVE_UP,
	    &error);
	fail_if_error(error, "error on get_quotas_for_lin");
	fail_if(returned_quotas == NULL,
	    "expected quotas, but none returned");
	STAILQ_FOREACH_SAFE(entry, returned_quotas, next, tmp) {
		if (qdi_equal(&entry->qdi,
		    quota_domain_get_id(&domain_outer)))
			found_outer = true;
		if (qdi_equal(&entry->qdi,
		    quota_domain_get_id(&domain_a)))
			found_a = true;
		if (qdi_equal(&entry->qdi,
		    quota_domain_get_id(&domain_b)))
			found_b = true;
		STAILQ_REMOVE_HEAD(returned_quotas, next);
		free(entry);
	}

	fail_if(found_outer != true,
	    "failure on finding quota outer: expected: %d, found: %d",
	    true, found_outer);
	/* the following test is expected because the quotas returned by the
	   hardlink will only be for one of the links (usually the original
	   of where the file was made). */
	fail_if(found_a == found_b,
	    "Expected xor of finding quotas a and b, instead found_a:%d, "
	    "found_b:%d", found_a, found_b);

	/* cleanup */
	quota_domain_delete(&domain_a, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_a_path, quota_a_st.st_ino);

	quota_domain_delete(&domain_b, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_b_path, quota_b_st.st_ino);

	quota_domain_delete(&domain_outer, &error);
	fail_if_error(error, "quota_domain_delete");
	clean_test_dir(quota_path_outer, quota_lin_outer);

	if (returned_quotas != NULL)
		free(returned_quotas);
}

TEST(test_get_tmp_lin, .attrs="overnight")
{
	char quota_path[MAXPATHLEN + 1];
	char quota_path2[MAXPATHLEN + 1];
	int ret;
	ifs_lin_t quota_lin;
	ifs_lin_t test_lin = 10;
	ifs_lin_t test_lin2 = 20;
	ifs_lin_t tmp_lin;
	struct stat file1_st;
	struct stat quota2_st;
	struct quota_domain_id qdi;
	struct quota_domain domain;
	struct migr_sworker_global_ctx global = {};
	struct uint64_uint64_map *new_map = NULL;
	struct isi_error *error = NULL;

	global.quotas_present = true;
	global.root_tmp_lin = 5;

	new_map = malloc(sizeof(*new_map));
	fail_if(new_map == NULL);
	uint64_uint64_map_init(new_map);
	global.tmp_working_map = new_map;

	/* Bug 215632 - perform test without hashing */
	global.enable_hash_tmpdir = false;
	global.tmpdir_hash = NULL;

	/* setup */
	create_test_dir(quota_path, &quota_lin, &qdi);
	create_and_setup_quota(&domain, &qdi);

	snprintf(quota_path2, sizeof(quota_path2), "%s/nested_dir", quota_path);
	fail_unless(mkdir(quota_path2, 0755) == 0);
	fail_unless(stat(quota_path2, &quota2_st) == 0);

	create_file(quota_path2, file1);
	snprintf(path, sizeof(path), "%s/%s", quota_path2, file1);
	ret = stat(path, &file1_st);
	fail_unless(ret == 0);

	uint64_uint64_map_add(new_map, quota_lin, &test_lin);
	uint64_uint64_map_add(new_map, quota2_st.st_ino, &test_lin2);

	tmp_lin = get_tmp_lin(file1_st.st_ino, &global, NULL, &error);
	fail_if_error(error, "get_tmp_lin");
	fail_unless(tmp_lin == test_lin2, "%{} != %{}", lin_fmt(tmp_lin),
	    lin_fmt(test_lin2));

	free(new_map);
}

#define DIR_NAME "dir"
#define DIR_DST_NAME "dir_new"
#define REG_NAME "reg"
#define REG_DST_NAME "reg_new"
#define LINK_TGT_NAME "/link"
#define LINK_NAME "link"
#define LINK_DST_NAME "link_new"
#define CHAR_NAME "char"
#define CHAR_DST_NAME "char_new"
#define BLOCK_NAME "block"
#define BLOCK_DST_NAME "block_new"
#define SOCK_NAME "sock"
#define SOCK_DST_NAME "sock_new"
#define FIFO_NAME "fifo"
#define FIFO_DST_NAME "fifo_new"
TEST(test_siq_enc_renameat, .attrs="overnight")
{
	struct migr_sworker_ctx sw_ctx = {};
	struct isi_error *error = NULL;
	char dir_path[MAXPATHLEN + 1];
	ifs_lin_t dir_lin = INVALID_LIN;
	int dir_fd = -1, fd = -1, ret;

	bzero(&sw_ctx, sizeof(struct migr_sworker_ctx));
	/* setup */
	create_test_dir(dir_path, &dir_lin, NULL);
	fail_if(dir_lin == INVALID_LIN, "failed to create tmp dir");

	path_init(&sw_ctx.worker.cur_path, dir_path, 0);
	dir_fd = ifs_lin_open(dir_lin, HEAD_SNAPID, O_RDWR);
	fail_if(dir_fd == -1, "failed to open %{}: %s", lin_fmt(dir_lin),
	    strerror(errno));

	/* SIQ_FT_DIR  */
	ret = enc_mkdirat(dir_fd, DIR_NAME, ENC_DEFAULT, 0775);
	fail_if(ret != 0, "failed to enc_mkdirat: %s: %s", DIR_DST_NAME,
	    strerror(errno));
	siq_enc_renameat(dir_fd, DIR_NAME, ENC_DEFAULT,
	    dir_fd, DIR_DST_NAME, ENC_DEFAULT, &error);
	fail_if_error(error, "failed to siq_enc_renameat: %s => %s: %s",
	    DIR_NAME, DIR_DST_NAME, strerror(errno));

	ret = enc_unlinkat(dir_fd, DIR_DST_NAME, ENC_DEFAULT, AT_REMOVEDIR);
	fail_if(ret != 0, "failed to enc_unlinkat: %s: %s", DIR_DST_NAME,
	    strerror(errno));

	/* SIQ_FT_REG */
	fd = open_or_create_reg(&sw_ctx, dir_fd, REG_NAME, ENC_DEFAULT, NULL,
	    &error);
	fail_if_error(error);
	fail_if(fd == -1);
	close(fd);
	fd = -1;
	siq_enc_renameat(dir_fd, REG_NAME, ENC_DEFAULT,
	    dir_fd, REG_DST_NAME, ENC_DEFAULT, &error);
	fail_if_error(error, "failed to siq_enc_renameat: %s => %s: %s",
	    REG_NAME, REG_DST_NAME, strerror(errno));
	ret = enc_unlinkat(dir_fd, REG_DST_NAME, ENC_DEFAULT, 0);
	fail_if(ret != 0, "failed to enc_unlinkat: %s: %s", REG_DST_NAME,
	    strerror(errno));

	/* SIQ_FT_SYM */
	fd = open_or_create_sym(&sw_ctx, dir_fd, LINK_TGT_NAME, ENC_DEFAULT,
	    LINK_NAME, ENC_DEFAULT, NULL, &error);
	fail_if_error(error);
	fail_if(fd == -1);
	close(fd);
	fd = -1;
	siq_enc_renameat(dir_fd, LINK_NAME, ENC_DEFAULT,
	    dir_fd, LINK_DST_NAME, ENC_DEFAULT, &error);
	fail_if_error(error, "failed to siq_enc_renameat: %s => %s: %s",
	    LINK_NAME, LINK_DST_NAME, strerror(errno));
	ret = enc_unlinkat(dir_fd, LINK_DST_NAME, ENC_DEFAULT, 0);
	fail_if(ret != 0, "failed to enc_unlinkat: %s: %s", LINK_DST_NAME,
	    strerror(errno));

	/* SIQ_FT_CHAR */
	fd = open_or_create_special(&sw_ctx, dir_fd, CHAR_NAME, ENC_DEFAULT,
	    SIQ_FT_CHAR, 1, 1, NULL, &error);
	fail_if_error(error);
	fail_if(fd == -1);
	close(fd);
	fd = -1;
	siq_enc_renameat(dir_fd, CHAR_NAME, ENC_DEFAULT,
	    dir_fd, CHAR_DST_NAME, ENC_DEFAULT, &error);
	fail_if_error(error, "failed to siq_enc_renameat: %s => %s: %s",
	    CHAR_NAME, CHAR_DST_NAME, strerror(errno));
	ret = enc_unlinkat(dir_fd, CHAR_DST_NAME, ENC_DEFAULT, 0);
	fail_if(ret != 0, "failed to enc_unlinkat: %s: %s", CHAR_DST_NAME,
	    strerror(errno));

	/* SIQ_FT_BLOCK */
	fd = open_or_create_special(&sw_ctx, dir_fd, BLOCK_NAME, ENC_DEFAULT,
	    SIQ_FT_BLOCK, 1, 1, NULL, &error);
	fail_if_error(error);
	fail_if(fd == -1);
	close(fd);
	fd = -1;
	siq_enc_renameat(dir_fd, BLOCK_NAME, ENC_DEFAULT,
	    dir_fd, BLOCK_DST_NAME, ENC_DEFAULT, &error);
	fail_if_error(error, "failed to siq_enc_renameat: %s => %s: %s",
	    BLOCK_NAME, BLOCK_DST_NAME, strerror(errno));
	ret = enc_unlinkat(dir_fd, BLOCK_DST_NAME, ENC_DEFAULT, 0);
	fail_if(ret != 0, "failed to enc_unlinkat: %s: %s", BLOCK_DST_NAME,
	    strerror(errno));

	/* SIQ_FT_SOCK */
	fd = open_or_create_socket(&sw_ctx, dir_fd, SOCK_NAME, ENC_DEFAULT,
	    NULL, &error);
	fail_if_error(error);
	fail_if(fd == -1);
	close(fd);
	fd = -1;
	siq_enc_renameat(dir_fd, SOCK_NAME, ENC_DEFAULT,
	    dir_fd, SOCK_DST_NAME, ENC_DEFAULT, &error);
	fail_if_error(error, "failed to siq_enc_renameat: %s => %s: %s",
	    SOCK_NAME, SOCK_DST_NAME, strerror(errno));
	ret = enc_unlinkat(dir_fd, SOCK_DST_NAME, ENC_DEFAULT, 0);
	fail_if(ret != 0, "failed to enc_unlinkat: %s: %s", SOCK_DST_NAME,
	    strerror(errno));

	/* SIQ_FT_FIFO */
	fd = open_or_create_fifo(&sw_ctx, dir_fd, FIFO_NAME, ENC_DEFAULT, NULL,
	    &error);
	fail_if_error(error);
	fail_if(fd == -1);
	close(fd);
	fd = -1;
	siq_enc_renameat(dir_fd, FIFO_NAME, ENC_DEFAULT,
	    dir_fd, FIFO_DST_NAME, ENC_DEFAULT, &error);
	fail_if_error(error, "failed to siq_enc_renameat: %s => %s: %s",
	    FIFO_NAME, FIFO_DST_NAME, strerror(errno));
	ret = enc_unlinkat(dir_fd, FIFO_DST_NAME, ENC_DEFAULT, 0);
	fail_if(ret != 0, "failed to enc_unlinkat: %s: %s", FIFO_DST_NAME,
	    strerror(errno));

	/* clean up */
	close(dir_fd);
	dir_fd = -1;
	clean_test_dir(dir_path, dir_lin);
	path_clean(&sw_ctx.worker.cur_path);
}
/* This test creates many links which crosses quota boundary and a subdir
 * which should be moved to tmp-working-dir belonging to its own
 * quota root. Reason for many links is that we need one of the links to
 * processed before the subdir to hit bug 237064 for which test is being
 * created.
 */
TEST(test_delete_lin_hardlink_to_diff_quota, .attrs="overnight")
{
        char quota_path[MAXPATHLEN + 1];
        char del_dir[MAXPATHLEN + 1];
        char path[MAXPATHLEN + 1];
        char basepath[MAXPATHLEN + 1];
        char link_path[MAXPATHLEN + 1];
        char tmp_name[MAXPATHLEN + 1];
        struct quota_domain_id qdi;
        struct quota_domain domain;
        ifs_lin_t quota_lin;
        struct isi_error *error = NULL;
        int tmpfd = -1, num = 0, err = 0, num_files = 10;
        const char *file1 = "testfile1";
        const char *file2 = "testfile2";
        struct stat dir_st = {}, st = {}, tmp_st = {};

        /* setup - create a dir with quota */
        do {
                snprintf(basepath, sizeof(basepath),
                                "/ifs/test.sync.%d", num++);
                err = mkdir(basepath, 0755);
                fail_unless(err == 0 || (err && errno == EEXIST));
        } while (err);

        snprintf(quota_path, sizeof(quota_path), "%s/quota_dir", basepath);
        fail_unless(mkdir(quota_path, 0755) == 0);
        fail_unless(stat(quota_path, &st) == 0);
        quota_lin = st.st_ino;
        qdi_init(&qdi, quota_lin, QUOTA_DOMAIN_ALL, false, ANY_INSTANCE);

        create_and_setup_quota(&domain, &qdi);

        tmpfd = initialize_tmp_map(basepath, sw_ctx.tmonitor.policy_id,
            &sw_ctx.global, false, true, &error);
        fail_if_error(error);
        close(tmpfd);
        tmpfd = -1;

        /* Create dir outside of quota dir which will be deleted */
        create_file(quota_path, file1);
        snprintf(del_dir, sizeof(del_dir), "%s/to_be_del", basepath);
        fail_unless(mkdir(del_dir, 0755) == 0);

        /* Create 10 links to file in a quota dir outside of dir to be
         * deleted
         */
        snprintf(path, sizeof(path), "%s/%s", quota_path, file1);
        for (int i = 0; i < num_files; i++) {
                snprintf(link_path, sizeof(link_path), "%s/file%d", del_dir, i);
                link(path, link_path);
        }
        fail_unless(stat(path, &st) == 0);
        fail_unless(st.st_nlink == num_files + 1);

        snprintf(path, sizeof(path), "%s/tmpdir", del_dir);
        fail_unless(mkdir(path, 0755) == 0);
        fail_unless(stat(path, &tmp_st) == 0);
        create_file(path, file2);

        fail_unless(stat(del_dir, &dir_st) == 0);
        delete_lin(&sw_ctx, dir_st.st_ino, &error);
        fail_if_error(error);

        /* Validate that linked files are not moved to tmp */
        get_tmp_working_dir_name(tmp_name, false);

        for (int i = 0; i < num_files; i++) {
                snprintf(link_path, sizeof(link_path),
                        "%s/%s/%lx/file%d", basepath, tmp_name, tmp_st.st_ino, i);
                fail_if(stat(link_path, &st) == 0);
                fail_unless(errno == ENOENT);
        }

        /* Validate the links are deleted */
        snprintf(path, sizeof(path), "%s/%s", quota_path, file1);
        fail_unless(stat(path, &st) == 0);
        fail_unless(st.st_nlink == 1);

        /* Validate file2 is moved */
        snprintf(path, sizeof(link_path),
                "%s/%s/%lx/%s", basepath, tmp_name, tmp_st.st_ino, file2);
        fail_unless(stat(path, &st) == 0);
        quota_domain_delete(&domain, &error);
        fail_if_error(error, "quota_domain_delete");

        cleanup_tmp_dirs(basepath, false, &sw_ctx, &error);
        fail_if_error(error);
        clean_basedir(basepath);
}
