#include <stdio.h>
#include <stdlib.h>
#include <isi_util/isi_error.h>

#include <check.h>
#include <isi_util/check_helper.h>
#include <isi_snapshot/snapshot_manage.h>

#include "coord.h"
#include "isi_migrate/config/siq_target.h"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);

SUITE_DEFINE_FOR_FILE(fofb, .suite_setup = suite_setup,
   .suite_teardown = suite_teardown);

static char test_snap_1_name[6] = "snap1";
static char test_snap_2_name[6] = "snap2";
static char test_snap_3_name[6] = "snap3";
static char test_alias_1_name[7] = "alias1";
static char test_alias_2_name[7] = "alias2";

static ifs_snapid_t test_snap_1 = INVALID_SNAPID;
static ifs_snapid_t test_snap_2 = INVALID_SNAPID;
static ifs_snapid_t test_snap_3 = INVALID_SNAPID;
static ifs_snapid_t test_snap_4 = INVALID_SNAPID;
static ifs_snapid_t test_alias_1 = INVALID_SNAPID;
static ifs_snapid_t test_alias_2 = INVALID_SNAPID;

static bool test_found_target = false;
static bool test_found_alias = false;

static char test_fake_pid[POLICY_ID_LEN + 1] = "fakepolicyidfortestd34dc0d3d34d0";
static char test_fake_mirror_pid[POLICY_ID_LEN + 1] = "fakemirrorpolicyforchecktes00000";
static char test_dne_pid[POLICY_ID_LEN + 1] = "dontmakeapolicythispidisbadpid00";
static char test_bad_pid[POLICY_ID_LEN + 1] = "notlongenoughtobeapid";

//helper for tests, check that the alias refered to by alias_name
//is pointing at the target_snap.  Do nothing if it is, otherwise fail test.
static void
validate(char *alias_name, ifs_snapid_t target_snapid)
{
	SNAP *temp = NULL;
	struct isi_str temp_str;
	struct isi_error *error = NULL;
	ifs_snapid_t temp_snapid;

	isi_str_init(&temp_str, alias_name, 7, ENC_DEFAULT,
	    ISI_STR_NO_MALLOC);

	//validate setup
	temp = snapshot_open_by_name(&temp_str, &error);
	fail_if_error(error);
	fail_unless(temp != NULL);
	if (temp->target_sid != target_snapid) {
		snapshot_get_alias_target(temp, &temp_snapid);
		fail("alias '%s' target '%ld' is not same as expected '%ld'",
		    alias_name, temp_snapid, target_snapid);
	}
	snapshot_close(temp);
}

//resets global alias_1 to point at snap_1
static void
reset_alias_1(void)
{
	struct isi_error *error = NULL;
	move_snapshot_alias_helper(test_snap_1, test_alias_1, NULL, NULL,
	    &error);
	fail_if_error(error);
	validate(test_alias_1_name, test_snap_1);
}

static void set_source_record_latest(char *pid,
	ifs_snapid_t snap, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct siq_source_record *srec = NULL;

	//make fake source for fake mirror
	if (!siq_source_has_record(pid)) {
		siq_source_create(pid, &error);
		if (error)
			goto out;
	}
	srec = siq_source_record_open(pid, &error);
	if (error)
		goto out;

	siq_source_set_latest(srec, snap);
	siq_source_record_write(srec, &error);
	if (error)
		goto out;

out:
	if (srec) {
		siq_source_record_unlock(srec);
		siq_source_record_free(srec);
	}
	isi_error_handle(error, error_out);
}

static void set_target_record_last_archive_snap_alias(char *pid,
	ifs_snapid_t snap, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	struct target_record *fb_trec = NULL;
	int lock_fd = -1;


	//make fake policy
	lock_fd = siq_target_acquire_lock(pid, O_SHLOCK, &error);
	if (error || lock_fd == -1)
		goto out;

	if (siq_target_record_exists(pid)) {
		fb_trec = siq_target_record_read(pid, &error);
		if (error)
			goto out;
	}

	if (!fb_trec) {
		fb_trec = calloc(1, sizeof(struct target_record));
		fb_trec->id = strdup(pid);
	}

	fb_trec->latest_archive_snap_alias = snap;
	siq_target_record_write(fb_trec, &error);

out:
	if (lock_fd != -1)
		close(lock_fd);

	siq_target_record_free(fb_trec);

	isi_error_handle(error, error_out);
}

TEST_FIXTURE(suite_setup)
{
	SNAP *temp = NULL;
	struct isi_str temp_str;
	struct isi_error *error = NULL;

	test_snap_1 = takesnap(test_snap_1_name, "/ifs", 0,
	    test_alias_1_name, &error);
	if (error)
		goto out;
	test_snap_2 = takesnap(test_snap_2_name, "/ifs", 0,
	    test_alias_2_name, &error);
	if (error)
		goto out;
	test_snap_3 = takesnap(test_snap_3_name, "/ifs", 0, NULL, &error);
	if (error)
		goto out;
	test_snap_4 = test_snap_3 + 1;

	isi_str_init(&temp_str, test_alias_1_name, 7, ENC_DEFAULT,
	    ISI_STR_NO_MALLOC);
	temp = snapshot_open_by_name(&temp_str, NULL);
	if (temp) {
		snapshot_get_snapid(temp, &test_alias_1);
		snapshot_close(temp);
	}

	isi_str_init(&temp_str, test_alias_2_name, 7, ENC_DEFAULT,
	    ISI_STR_NO_MALLOC);
	temp = snapshot_open_by_name(&temp_str, NULL);
	if (temp) {
		snapshot_get_snapid(temp, &test_alias_2);
		snapshot_close(temp);
	}

	//make fake source for fake mirror
	set_source_record_latest(test_fake_mirror_pid, test_snap_3, &error);
	if (error)
		goto out;

	//make fake policy
	set_target_record_last_archive_snap_alias(test_fake_pid, test_alias_1,
	    &error);
	if (error)
		goto out;

out:
	if (error) {
		printf("\nERR:%s\n", isi_error_get_message(error));
		isi_error_free(error);
		error = NULL;
	}
}

TEST_FIXTURE(suite_teardown)
{
	struct isi_error *error = NULL;

	//Delete Snapshots
	delete_snapshot_helper(test_snap_1);
	delete_snapshot_helper(test_snap_2);
	delete_snapshot_helper(test_snap_3);

	//Delete Fake Source Record
	if (siq_source_has_record(test_fake_mirror_pid)) {
		siq_source_delete(test_fake_mirror_pid, &error);
	}

	if (g_ctx.record) {
		siq_source_record_free(g_ctx.record);
		g_ctx.record = NULL;
	}

	//Delete Fake Target Record
	siq_target_delete_record(test_fake_pid, false, false, &error);
	if (error)
		goto out;

out:
	if (error) {
		printf("\nERR:%s\n", isi_error_get_message(error));
		isi_error_free(error);
		error = NULL;
	}
}

/*
 * *************************************************
 * the following are move_snapshot_alias_helper tests
 * *************************************************
 */

//move an alias to point to a different snapshot
TEST(move_snapshot_alias_helper_test_a2s)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(test_snap_3, test_alias_1,
	    &test_found_target, &test_found_alias, &error);
	fail_if_error(error);
	validate(test_alias_1_name, test_snap_3);
	fail_unless(test_found_target);
	fail_unless(test_found_alias);

	//cleanup
	reset_alias_1();
}

//move an alias to point to the same snapshot
TEST(move_snapshot_alias_helper_test_a2sames)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(test_snap_1, test_alias_1, NULL, NULL,
	    &error);
	fail_if_error(error);
	validate(test_alias_1_name, test_snap_1);
}

//move an alias to point to head
TEST(move_snapshot_alias_helper_test_a2h)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(HEAD_SNAPID, test_alias_1, NULL, NULL,
	    &error);
	fail_if_error(error);
	validate(test_alias_1_name, HEAD_SNAPID);

	//cleanup
	reset_alias_1();
}

//attempt to move an alias to point at an invalid snap
TEST(move_snapshot_alias_helper_test_a2i)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(INVALID_SNAPID, test_alias_1,
	    &test_found_target, NULL, &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//validate nothing happened
	validate(test_alias_1_name, test_snap_1);
	fail_if(test_found_target);
}

//move the alias target to another alias
TEST(move_snapshot_alias_helper_test_a2a)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(test_alias_2, test_alias_1, NULL, NULL,
	    &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//validate original setup
	validate(test_alias_1_name, test_snap_1);
}

//attempt to move an alias to reference itself
TEST(move_snapshot_alias_helper_test_a2self)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(test_alias_1, test_alias_1, NULL, NULL,
	    &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//validate original setup
	validate(test_alias_1_name, test_snap_1);
}

//try to move a snapshot to another snapshot.
TEST(move_snapshot_alias_helper_test_s2s)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(test_snap_1, test_snap_2, NULL, NULL,
	    &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//validate original setup
	validate(test_alias_1_name, test_snap_1);
}

//try to move a snapshot to itself
TEST(move_snapshot_alias_helper_test_s2self)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(test_snap_1, test_snap_1, NULL, NULL,
	    &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//validate original setup
	validate(test_alias_1_name, test_snap_1);
}

//try to move an invalid snapid to a snap
TEST(move_snapshot_alias_helper_test_i2s)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(INVALID_SNAPID, test_snap_1, NULL, NULL,
	    &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//validate original setup
	validate(test_alias_1_name, test_snap_1);
}

//try to move a head snapid to a snap
TEST(move_snapshot_alias_helper_test_h2s)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(HEAD_SNAPID, test_snap_1, NULL, NULL,
	    &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//validate original setup
	validate(test_alias_1_name, test_snap_1);
}

//Move a snapshot alias to a snapshot that does not exist
TEST(move_snapshot_alias_helper_test_a2notreal)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(HEAD_SNAPID, test_snap_4, NULL, NULL,
	    &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//validate original setup
	validate(test_alias_1_name, test_snap_1);
}

//move an alias that is already at head to head
TEST(move_snapshot_alias_helper_test_athtoh)
{
	struct isi_error *error = NULL;

	//setup
	move_snapshot_alias_helper(HEAD_SNAPID, test_alias_1, NULL, NULL,
	    &error);
	fail_if_error(error);
	validate(test_alias_1_name, HEAD_SNAPID);

	//now try to move to head
	move_snapshot_alias_helper(HEAD_SNAPID, test_alias_1, NULL, NULL,
	    &error);
	fail_if_error(error);
	validate(test_alias_1_name, HEAD_SNAPID);

	//cleanup
	reset_alias_1();
}

//attempt to move an actual snap to head
TEST(move_snapshot_alias_helper_test_stoh)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(test_snap_1, HEAD_SNAPID, NULL, NULL,
	    &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//validate original setup
	validate(test_alias_1_name, test_snap_1);
}

//attempt to move an invalid snapid to head
TEST(move_snapshot_alias_helper_test_itoh)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(INVALID_SNAPID, HEAD_SNAPID, NULL, NULL,
	    &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//validate original setup
	validate(test_alias_1_name, test_snap_1);
}

//attempt to move a nonexisting snapid to head
TEST(move_snapshot_alias_helper_test_dnetoh)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(test_snap_4, HEAD_SNAPID, NULL, NULL,
	    &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//validate original setup
	validate(test_alias_1_name, test_snap_1);
}

//attempt to move a head snapid to head
TEST(move_snapshot_alias_helper_test_htoh)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(HEAD_SNAPID, HEAD_SNAPID, NULL, NULL,
	    &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//validate original setup
	validate(test_alias_1_name, test_snap_1);
}

/*
 * *************************************************
 * the following are move_snapshot_alias_from_head tests
 * *************************************************
 */

TEST(move_snapshot_alias_from_head_a1tos1)
{
	struct isi_error *error = NULL;

	//setup
	set_source_record_latest(test_fake_mirror_pid,
		test_snap_3, &error);
	fail_if_error(error);
	set_target_record_last_archive_snap_alias(test_fake_pid,
		test_alias_1, &error);
	fail_if_error(error);
	move_snapshot_alias_helper(test_snap_1, test_alias_1, NULL, NULL,
	    &error);
	fail_if_error(error);

	//test
	move_snapshot_alias_from_head(test_fake_mirror_pid, test_fake_pid,
		&error);
	fail_if_error(error);
	validate(test_alias_1_name, test_snap_3);

	//cleanup
	reset_alias_1();
	if (g_ctx.record) {
		siq_source_record_free(g_ctx.record);
		g_ctx.record = NULL;
	}
}

TEST(move_snapshot_alias_from_head_dnemirrorpid)
{
	struct isi_error *error = NULL;

	//setup
	set_target_record_last_archive_snap_alias(test_fake_pid,
		test_alias_1, &error);
	fail_if_error(error);
	move_snapshot_alias_helper(test_snap_1, test_alias_1, NULL, NULL,
	    &error);
	fail_if_error(error);

	//test
	move_snapshot_alias_from_head(test_dne_pid, test_fake_pid,
		&error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, ENOENT));
	isi_error_free(error);
	error = NULL;

	//cleanup
	validate(test_alias_1_name, test_snap_1);
	if (g_ctx.record) {
		siq_source_record_free(g_ctx.record);
		g_ctx.record = NULL;
	}
}

TEST(move_snapshot_alias_from_head_badmirrorpid)
{
	struct isi_error *error = NULL;

	//setup
	set_target_record_last_archive_snap_alias(test_fake_pid,
		test_alias_1, &error);
	fail_if_error(error);
	move_snapshot_alias_helper(test_snap_1, test_alias_1, NULL, NULL,
	    &error);
	fail_if_error(error);

	//test
	move_snapshot_alias_from_head(test_bad_pid, test_fake_pid,
		&error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//cleanup
	validate(test_alias_1_name, test_snap_1);
	if (g_ctx.record) {
		siq_source_record_free(g_ctx.record);
		g_ctx.record = NULL;
	}
}



TEST(move_snapshot_alias_from_head_nomirrorpid)
{
	struct isi_error *error = NULL;

	//setup
	set_target_record_last_archive_snap_alias(test_fake_pid,
		test_alias_1, &error);
	fail_if_error(error);
	move_snapshot_alias_helper(test_snap_1, test_alias_1, NULL, NULL,
	    &error);
	fail_if_error(error);

	//test
	move_snapshot_alias_from_head(NULL, test_fake_pid,
		&error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//cleanup
	validate(test_alias_1_name, test_snap_1);
	if (g_ctx.record) {
		siq_source_record_free(g_ctx.record);
		g_ctx.record = NULL;
	}
}

TEST(move_snapshot_alias_from_head_dnepid)
{
	struct isi_error *error = NULL;

	//setup
	set_target_record_last_archive_snap_alias(test_fake_pid,
		test_alias_1, &error);
	fail_if_error(error);
	move_snapshot_alias_helper(test_snap_1, test_alias_1, NULL, NULL,
	    &error);
	fail_if_error(error);

	//test
	move_snapshot_alias_from_head(test_fake_mirror_pid, test_dne_pid,
		&error);
	fail_unless(error != NULL);
	fail_unless(isi_siq_error_get_siqerr((struct isi_siq_error *)error)
	    == E_SIQ_CONF_NOENT);
	isi_error_free(error);
	error = NULL;

	//cleanup
	validate(test_alias_1_name, test_snap_1);
	if (g_ctx.record) {
		siq_source_record_free(g_ctx.record);
		g_ctx.record = NULL;
	}
}

TEST(move_snapshot_alias_from_head_badpid)
{
	struct isi_error *error = NULL;

	//setup
	set_source_record_latest(test_fake_mirror_pid,
		test_snap_3, &error);
	fail_if_error(error);
	move_snapshot_alias_helper(test_snap_1, test_alias_1, NULL, NULL,
	    &error);
	fail_if_error(error);

	//test
	move_snapshot_alias_from_head(test_fake_mirror_pid, test_bad_pid,
		&error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//cleanup
	validate(test_alias_1_name, test_snap_1);
	if (g_ctx.record) {
		siq_source_record_free(g_ctx.record);
		g_ctx.record = NULL;
	}
}

TEST(move_snapshot_alias_from_head_nopid)
{
	struct isi_error *error = NULL;

	//setup
	set_source_record_latest(test_fake_mirror_pid,
		test_snap_3, &error);
	fail_if_error(error);
	move_snapshot_alias_helper(test_snap_1, test_alias_1, NULL, NULL,
	    &error);
	fail_if_error(error);

	//test
	move_snapshot_alias_from_head(test_fake_mirror_pid, NULL,
		&error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;

	//cleanup
	reset_alias_1();
	if (g_ctx.record) {
		siq_source_record_free(g_ctx.record);
		g_ctx.record = NULL;
	}
}

TEST(move_snapshot_alias_from_head_a1tos1_preload)
{
	struct isi_error *error = NULL;

	//setup
	set_source_record_latest(test_fake_mirror_pid,
		test_snap_3, &error);
	fail_if_error(error);
	set_target_record_last_archive_snap_alias(test_fake_pid,
		test_alias_1, &error);
	fail_if_error(error);
	move_snapshot_alias_helper(test_snap_1, test_alias_1, NULL, NULL,
	    &error);
	fail_if_error(error);

	//test
	move_snapshot_alias_from_head(test_fake_mirror_pid, test_fake_pid,
		&error);
	fail_if_error(error);
	validate(test_alias_1_name, test_snap_3);

	//cleanup
	reset_alias_1();
	if (g_ctx.record) {
		siq_source_record_free(g_ctx.record);
		g_ctx.record = NULL;
	}
}

//attempt to move an invalid snapid to an invalid alias
TEST(move_snapshot_alias_helper_test_itoi)
{
	struct isi_error *error = NULL;

	move_snapshot_alias_helper(INVALID_SNAPID, INVALID_SNAPID,
	    &test_found_target, &test_found_alias, &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free(error);
	error = NULL;
	fail_if(test_found_target);
	fail_if(test_found_alias);
}
