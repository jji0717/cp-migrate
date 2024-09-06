#include <check.h>
#include <isi_sbtree/sbtree.h>

#include "isirep.h"
#include "work_item.h"
#include "repstate.h"
#include "check_helper.h"

#define fail_if_isi_error(err) \
do {\
        fail_unless(!err, "%s", error ?isi_error_get_message(error): "");\
} while(0)

#define rand_bool rand() % 2;

static int res;
static bool found;
static struct rep_ctx *ctx = NULL;
static struct rep_entry rep_a;
static struct rep_entry rep_b;
static struct rep_info_entry rei;
static char NAME[32];
static char POL[32];

static btree_key_t *
lin_to_key(uint64_t lin, btree_key_t *key)
{
	ASSERT(key);
	key->keys[0] = lin;
	key->keys[1] = 0;
	return key;
}

static void
reset_entry(struct rep_entry *rep)
{
	memset(rep, 0, sizeof(struct rep_entry));
}

static void
default_entry(struct rep_entry *rep)
{
	rep->lcount = 1;
	rep->non_hashed = false;
	rep->is_dir = false;
	rep->exception = false;
	rep->incl_for_child = false;
	rep->new_dir_parent = false;
	rep->not_on_target = false;
	rep->changed = false;
	rep->flipped = false;
	rep->lcount_set = true;
	rep->for_path = false;
	rep->hl_dir_lin = 0;
	rep->hl_dir_offset = 0;
	rep->excl_lcount = 0;
}

static void
randomize_entry(struct rep_entry *rep)
{
	rep->lcount = rand();
	rep->non_hashed = rand_bool;
	rep->is_dir = rand_bool;
	rep->exception = rand_bool;
	rep->incl_for_child = rand_bool;
	rep->new_dir_parent = rand_bool;
	rep->not_on_target = rand_bool;
	rep->changed = rand_bool;
	rep->flipped = rand_bool;
	rep->lcount_set = rand_bool;
	rep->for_path = rand_bool;
	rep->hl_dir_lin = rand();
	rep->hl_dir_offset = rand();
	rep->excl_lcount = rand();
}

static int
compare_entries(struct rep_entry *rep_a, struct rep_entry *rep_b)
{
	return memcmp(rep_a, rep_b, sizeof(struct rep_entry));
}

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(migr_repstate,
    .mem_check = CK_MEM_DISABLE,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = test_setup,
    .test_teardown = test_teardown);

TEST_FIXTURE(suite_setup)
{
	static int sub = 0;
	snprintf(POL, 32, "nn%d.%d", getpid(), sub++);
	snprintf(NAME, 32, "nn%d.%d", getpid(), sub++);
}

TEST_FIXTURE(suite_teardown)
{
}

TEST_FIXTURE(test_setup)
{
	struct isi_error *error = NULL;
	char pat[255];

	get_repeated_rep_pattern(pat, POL, false);
	prune_old_repstate(POL, pat, NULL, 0, &error);
	fail_if_isi_error(error);

	create_repstate(POL, NAME, false, &error);
	fail_if_isi_error(error);

	open_repstate(POL, NAME, &ctx, &error);
	fail_if_isi_error(error);

	reset_entry(&rep_a);
	reset_entry(&rep_b);
}

TEST_FIXTURE(test_teardown)
{
	struct isi_error *error = NULL;
	close_repstate(ctx);

	remove_repstate(POL, NAME, false, &error);
	fail_if_isi_error(error);
}

TEST(create_open_close_remove, .attrs="overnight")
{
	// just do set_up and tear_down
}

TEST(open_non_existing_repstate, .attrs="overnight")
{
	struct rep_ctx *ctx = NULL;
	struct isi_error *error = NULL;

	open_repstate(POL, "bar", &ctx, &error);
	fail_if(error == NULL || ctx != NULL);
	isi_error_free(error);
}

TEST(get_invalid_entry, .attrs="overnight")
{
	struct isi_error *error = NULL;

	found = get_entry(999, &rep_a, ctx, &error);
	fail_if_isi_error(error);
	fail_if(found);
}

TEST(add_get_new_entry, .attrs="overnight")
{
	struct isi_error *error = NULL;

	set_entry(1000, &rep_a, ctx, 0, NULL, &error);
	fail_if_isi_error(error);
	found = get_entry(1000, &rep_b, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(found);
	res = compare_entries(&rep_a, &rep_b);	
	fail_unless(res == 0);
}

TEST(get_removed_entry, .attrs="overnight")
{
	struct isi_error *error = NULL;

	set_entry(1000, &rep_a, ctx, 0, NULL, &error);
	fail_if_isi_error(error);
	remove_entry(1000, ctx, &error);
	fail_if_isi_error(error);
	found = get_entry(1000, &rep_b, ctx, &error);
	fail_if_isi_error(error);
	fail_if(found);
}

TEST(change_existing_entry, .attrs="overnight")
{
	struct isi_error *error = NULL;

	set_entry(1000, &rep_a, ctx, 0, NULL, &error);
	fail_if_isi_error(error);
	randomize_entry(&rep_a);
	set_entry(1000, &rep_a, ctx, 0, NULL, &error);
	fail_if_isi_error(error);
	found = get_entry(1000, &rep_b, ctx, &error);
	fail_if_isi_error(error);
	fail_if(!found);
	res = compare_entries(&rep_a, &rep_b);
	fail_unless(res == 0);
}

TEST(iterate_empty_sbt, .attrs="overnight")
{
	struct rep_iter *iter = NULL;
	struct isi_error *error = NULL;

	u_int64_t lin_out = 0;
	struct rep_entry rep_out;
	bool got_one;

	iter = new_rep_iter(ctx);
	fail_if(iter == NULL);

	got_one = rep_iter_next(iter, &lin_out, &rep_out, &error);
	fail_if_isi_error(error);
	fail_if(got_one);
}

#define ITER_TEST_SIZE 100
TEST(iterate_populated_sbt, .attrs="overnight")
{
	int i = 0;
	u_int64_t lin_out = 0;
	struct rep_iter *iter = NULL;
	bool got_one;
	struct rep_entry rep_in[ITER_TEST_SIZE];
	struct rep_entry rep_out;
	struct isi_error *error = NULL;

	/* prevent padding bytes from causing false compare mismatches */
	memset(rep_in, 0, sizeof(struct rep_entry) * ITER_TEST_SIZE);

	for (i = 0; i < ITER_TEST_SIZE; i++) {
		randomize_entry(&rep_in[i]);
		set_entry(i + 2, &rep_in[i], ctx, 0, NULL, &error);
		fail_if_isi_error(error);
	}

	iter = new_rep_iter(ctx);
	fail_if(iter == NULL);

	i = 0;
	while (true) {
		got_one = rep_iter_next(iter, &lin_out, &rep_out, &error);
		fail_if_isi_error(error);
		if (!got_one)
			break;
		i++;
		fail_unless(compare_entries(&rep_out, &rep_in[lin_out - 2]) == 0);
	}

	fail_unless(i == ITER_TEST_SIZE, "Entry count mismatch %d %d", i,
	    ITER_TEST_SIZE);
}

TEST(add_get_all_default_entry, .attrs="overnight")
{
	/* if this test fails it means a default was changed and needs to be
	 * updated in this test, or a new field was added to the rep entry */
	size_t sbt_buf_size;
	char sbt_buf[1024];
	struct btree_flags bt_flags = {};
	btree_key_t key;
	struct isi_error *error = NULL;

	default_entry(&rep_a);
	set_entry(1000, &rep_a, ctx, 0, NULL, &error);
	fail_if_isi_error(error);
	found = get_entry(1000, &rep_b, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(found);
	res = compare_entries(&rep_a, &rep_b);
	fail_unless(res == 0);

	/* sbt entry buffer size should be zero for default entry */
	res = ifs_sbt_get_entry_at(ctx->rep_fd, lin_to_key(1000, &key), sbt_buf,
		1024, &bt_flags, &sbt_buf_size);
	fail_unless(res == 0);
	fail_unless(sbt_buf_size == 0);
}

#define LARGE_SIZE (30 * 1024)
TEST(tw_serialized_data, .attrs="overnight")
{
	char buf[LARGE_SIZE];
	char *new_buf;
	size_t new_size;
	int i;
	struct work_restart_state work;
	struct isi_error *error = NULL;

	/* First fill the buffer */
	for(i = 0; i < LARGE_SIZE; i++)
		buf[i] = '1';	

	set_work_entry(9000, &work, buf, LARGE_SIZE, ctx, &error);
	fail_if_isi_error(error);
	
	get_work_entry(9000, &work, &new_buf, &new_size, ctx, &error);
	fail_if_isi_error(error);

	for (i = 0; i < LARGE_SIZE; i++)
		fail_unless (buf[i] == new_buf[i]);

	if (new_buf)
		free(new_buf);	
}

TEST(stf_phase_data, .attrs="overnight")
{
	struct stf_job_state job_phase;
	char *buf = NULL;
	size_t size;
	bool success;
	struct isi_error *error = NULL;

	init_job_phase_ctx(JOB_TW, &job_phase);
	buf = fill_job_state(&job_phase, &size);
	success = set_job_phase_cond(ctx, 9000, buf, size,
	    NULL, 0, &error);
	fail_if_isi_error(error);
	fail_unless(success == true);
	if (buf)
		free (buf);

	success = stf_job_phase_cond_enable(9000, STF_PHASE_CT_LIN_HASH_EXCPT,
	    ctx, &error);
	fail_if_isi_error(error);
	fail_unless(success == true);	
	
}

TEST(prune_keep_single, .attrs="overnight")
{
	char name1[255];
	char name2[255];
	char name3[255];
	char name4[255];
	char pat[255];
	ifs_snapid_t keptid;
	struct isi_error *error = NULL;

	sprintf(name1,"%s_snap_%u", POL, 23);
	create_repstate(POL, name1, false, &error);
	fail_if_isi_error(error);
	sprintf(name2,"%s_snap_%u", POL, 29);
	create_repstate(POL, name2, false, &error);
	fail_if_isi_error(error);
	sprintf(name3,"%s_snap_%u", POL, 2000);
	create_repstate(POL, name3, false, &error);
	fail_if_isi_error(error);
	sprintf(name4,"%s_snap_%u", POL, 1900);
	create_repstate(POL, name4, false, &error);
	fail_if_isi_error(error);

	fail_unless(repstate_exists(POL, name1, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name2, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name3, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name4, &error));
	fail_if_isi_error(error);

	keptid = 29;
	get_repeated_rep_pattern(pat, POL, false);
	prune_old_repstate(POL, pat, &keptid, 1, &error);
	fail_if_isi_error(error);
	fail_if(repstate_exists(POL, name1, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name2, &error));
	fail_if_isi_error(error);
	fail_if(repstate_exists(POL, name3, &error));
	fail_if_isi_error(error);
	fail_if(repstate_exists(POL, name4, &error));
	fail_if_isi_error(error);
}

TEST(prune_keep_none, .attrs="overnight")
{
	char name1[255];
	char name2[255];
	char name3[255];
	char name4[255];
	char pat[255];
	ifs_snapid_t keptid;
	struct isi_error *error = NULL;

	sprintf(name1,"%s_snap_%u", POL, 23);
	create_repstate(POL, name1, false, &error);
	fail_if_isi_error(error);
	sprintf(name2,"%s_snap_%u", POL, 29);
	create_repstate(POL, name2, false, &error);
	fail_if_isi_error(error);
	sprintf(name3,"%s_snap_%u", POL, 2000);
	create_repstate(POL, name3, false, &error);
	fail_if_isi_error(error);
	sprintf(name4,"%s_snap_%u", POL, 1900);
	create_repstate(POL, name4, false, &error);
	fail_if_isi_error(error);

	fail_unless(repstate_exists(POL, name1, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name2, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name3, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name4, &error));
	fail_if_isi_error(error);

	keptid = 29;
	get_repeated_rep_pattern(pat, POL, false);
	prune_old_repstate(POL, pat, &keptid, 0, &error);
	fail_if_isi_error(error);
	fail_if(repstate_exists(POL, name1, &error));
	fail_if_isi_error(error);
	fail_if(repstate_exists(POL, name2, &error));
	fail_if_isi_error(error);
	fail_if(repstate_exists(POL, name3, &error));
	fail_if_isi_error(error);
	fail_if(repstate_exists(POL, name4, &error));
	fail_if_isi_error(error);
}

TEST(prune_keep_base, .attrs="overnight")
{
	char name1[255];
	char name2[255];
	char name3[255];
	char name4[255];
	char nameb[255];
	char pat[255];
	struct isi_error *error = NULL;

	sprintf(nameb,"%s_snap_rep_base", POL);
	create_repstate(POL, nameb, false, &error);
	fail_if_isi_error(error);
	sprintf(name1,"%s_snap_%u", POL, 23);
	create_repstate(POL, name1, false, &error);
	fail_if_isi_error(error);
	sprintf(name2,"%s_snap_%u", POL, 29);
	create_repstate(POL, name2, false, &error);
	fail_if_isi_error(error);
	sprintf(name3,"%s_snap_%u", POL, 2000);
	create_repstate(POL, name3, false, &error);
	fail_if_isi_error(error);
	sprintf(name4,"%s_snap_%u", POL, 1900);
	create_repstate(POL, name4, false, &error);
	fail_if_isi_error(error);

	fail_unless(repstate_exists(POL, nameb, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name1, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name2, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name3, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name4, &error));
	fail_if_isi_error(error);

	get_repeated_rep_pattern(pat, POL, false);
	prune_old_repstate(POL, pat, NULL, 0, &error);
	fail_unless(repstate_exists(POL, nameb, &error));
	fail_if_isi_error(error);
	fail_if(repstate_exists(POL, name1, &error));
	fail_if_isi_error(error);
	fail_if(repstate_exists(POL, name2, &error));
	fail_if_isi_error(error);
	fail_if(repstate_exists(POL, name3, &error));
	fail_if_isi_error(error);
	fail_if(repstate_exists(POL, name4, &error));
	fail_if_isi_error(error);
}


TEST(prune_keep_two, .attrs="overnight")
{
	char name1[255];
	char name2[255];
	char name3[255];
	char name4[255];
	char pat[255];
	ifs_snapid_t keptids[2];
	struct isi_error *error = NULL;

	sprintf(name1,"%s_snap_%u", POL, 23);
	create_repstate(POL, name1, false, &error);
	fail_if_isi_error(error);
	sprintf(name2,"%s_snap_%u", POL, 29);
	create_repstate(POL, name2, false, &error);
	fail_if_isi_error(error);
	sprintf(name3,"%s_snap_%u", POL, 2000);
	create_repstate(POL, name3, false, &error);
	fail_if_isi_error(error);
	sprintf(name4,"%s_snap_%u", POL, 1900);
	create_repstate(POL, name4, false, &error);
	fail_if_isi_error(error);

	fail_unless(repstate_exists(POL, name1, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name2, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name3, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name4, &error));
	fail_if_isi_error(error);

	keptids[0] = 29;
	keptids[1] = 2000;
	get_repeated_rep_pattern(pat, POL, false);
	prune_old_repstate(POL, pat, keptids, 2, &error);
	fail_if_isi_error(error);
	fail_if(repstate_exists(POL, name1, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name2, &error));
	fail_if_isi_error(error);
	fail_unless(repstate_exists(POL, name3, &error));
	fail_if_isi_error(error);
	fail_if(repstate_exists(POL, name4, &error));
	fail_if_isi_error(error);
}


TEST(set_get_rep_info, .attrs="overnight")
{
	struct isi_error *error = NULL;

	rei.initial_sync_snap = 347;
	set_rep_info(&rei, ctx, false, &error);
	fail_if_isi_error(error);

	rei.initial_sync_snap = 0;
	get_rep_info(&rei, ctx, &error);
	fail_unless(rei.initial_sync_snap == 347);
}

TEST(doubleset_get_rep_info, .attrs="overnight")
{
	struct isi_error *error = NULL;

	rei.initial_sync_snap = 347;
	set_rep_info(&rei, ctx, false, &error);
	fail_if_isi_error(error);
	rei.initial_sync_snap = 347;
	set_rep_info(&rei, ctx, false, &error);
	fail_if_isi_error(error);

	rei.initial_sync_snap = 0;
	get_rep_info(&rei, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(rei.initial_sync_snap == 347);
}
TEST(doubleset_different, .attrs="overnight")
{
	struct isi_error *error = NULL;

	rei.initial_sync_snap = 347;
	set_rep_info(&rei, ctx, false, &error);
	fail_if_isi_error(error);
	rei.initial_sync_snap = 348;
	set_rep_info(&rei, ctx, false, &error);
	fail_unless(NULL != error);
	isi_error_free(error);
	error = NULL;
	
	rei.initial_sync_snap = 0;
	get_rep_info(&rei, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(rei.initial_sync_snap == 347);
}

TEST(dynamic_rep_entry_lookup, .attrs="overnight")
{
	char *dir1, *dir2;
	struct stat dir_st, file_st, link_st;
	char *file = "reg_file";
	char *link_file = "link_file";
	char *link_name = "link";
	char path[1024];
	char link_path[1024];
	int ret;
	struct rep_entry rep;
	bool exists = false;
	struct isi_error *error = NULL;

	/*
	 * 		TEST CASE 
	 * Create two directories, a regular file
	 * and hard linked file. Make sure that
	 * rep entry returned is valid.
	 * Remove the file and make sure we 
	 * return false
	 */

	/* First set the dynamic attribute in context */
	set_dynamic_rep_lookup(ctx, HEAD_SNAPID, 0);
	
	dir1 = create_new_dir();
	dir2 = create_new_dir();
	ret = stat(dir1, &dir_st);
	fail_unless(ret == 0);
	
	create_file(dir1, file);
	sprintf(path, "%s/%s", dir1, file);
	ret = stat(path, &file_st);
	fail_unless(ret == 0);

	create_file(dir1, link_file);
		
	sprintf(path, "%s/%s", dir1, link_file);
	sprintf(link_path, "%s/%s", dir2, link_name);
	ret = link(path, link_path);
	fail_unless(ret == 0);
		
	ret = stat(link_path, &link_st);
	fail_unless(ret == 0);

	
	/* get rep entry for directory */
	exists = get_entry(dir_st.st_ino, &rep, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(exists == true);
	fail_unless(rep.lcount == 1 && rep.is_dir == true);

	/* Get rep entry for regular file */
	exists = get_entry(file_st.st_ino, &rep, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(exists == true);
	fail_unless(rep.lcount == 1);

	
	/* Get rep entry for hard linked file */
	exists = get_entry(link_st.st_ino, &rep, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(exists == true);
	fail_unless(rep.lcount == 2);

	/* Lets remove regular file and make sure get entry returns false */
	
	sprintf(path, "%s/%s", dir1, file);
	unlink(path);

	/* Get rep entry for hard linked file */
	exists = get_entry(file_st.st_ino, &rep, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(exists == false);
}
