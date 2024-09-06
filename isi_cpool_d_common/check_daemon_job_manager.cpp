#include <ifs/ifs_syscalls.h>
#include <isi_sbtree/sbtree.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include <isi_cpool_config/scoped_cfg.h>

#include "cpool_d_debug.h"
#include "cpool_fd_store.h"
#include "cpool_test_fail_point.h"
#include "daemon_job_configuration.h"
#include "daemon_job_member.h"
#include "daemon_job_record.h"
#include "daemon_job_types.h"
#include "hsbt.h"
#include "scoped_object_list.h"
#include "task.h"
#include "unit_test_helpers.h"

#define CPOOL_DJM_PRIVATE
#include "daemon_job_manager_private.h"
#undef CPOOL_DJM_PRIVATE

#include "daemon_job_manager.h"

using namespace isi_cloud;
using namespace isi_cloud::daemon_job_manager;

const char *short_file_fmt =  "/ifs/data/query_job_ut_%03zu";

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);

SUITE_DEFINE_FOR_FILE(check_daemon_job_manager,
	.mem_check = CK_MEM_LEAKS,
	.suite_setup = setup_suite,
	.suite_teardown = teardown_suite,
	.test_setup = setup_test,
	.timeout = 0,
	.fixture_timeout = 1200);

TEST_FIXTURE(setup_suite)
{
	/* to prevent false positive memory leaks */
	cpool_fd_store::getInstance();
	isi_cloud_object_id object_id;

	init_failpoints_ut();
}

TEST_FIXTURE(teardown_suite)
{
	// restart isi_cpool_d
	system("isi_for_array 'killall -9 isi_cpool_d' &> /dev/null");
}

/*
 * Deletes SBTs for all task types up to priority DEL_PRIORITY_MAX and jobs up
 * to DEL_JOBS_MAX.
 */
#define DEL_JOBS_MAX 30
#define DEL_PRIORITY_MAX 10
TEST_FIXTURE(setup_test)
{
	struct isi_error *error = NULL;
	char *name_buf = NULL;
	const cpool_operation_type *op_type = NULL;

	for (djob_id_t job_id = 0; job_id < DEL_JOBS_MAX; ++job_id) {
		name_buf = get_djob_file_list_sbt_name(job_id);
		delete_sbt(name_buf, &error);
		fail_if (error != NULL, "failed to delete %s: %#{}",
		    name_buf, isi_error_fmt(error));
		free(name_buf);
	}

	name_buf = get_daemon_jobs_sbt_name();
	delete_sbt(name_buf, &error);
	fail_if (error != NULL, "failed to delete %s: %#{}",
	    name_buf, isi_error_fmt(error));
	free(name_buf);

	OT_FOREACH(op_type) {
		for (int pri = 0; pri < DEL_PRIORITY_MAX; ++pri) {
			name_buf = get_pending_sbt_name(op_type->get_type(),
			    pri);
			delete_sbt(name_buf, &error);
			fail_if (error != NULL, "failed to delete %s: %#{}",
			    name_buf, isi_error_fmt(error));
			free(name_buf);
		}

		name_buf = get_inprogress_sbt_name(op_type->get_type());
		delete_sbt(name_buf, &error);
		fail_if (error != NULL, "failed to delete %s: %#{}",
		    name_buf, isi_error_fmt(error));
		free(name_buf);
	}
}

/*
 * If this class is modified to do anything other than modify visibility of
 * methods, some tests which cast between daemon_job_record and
 * daemon_job_record_ut may need to be re-examined.
 */
class daemon_job_record_ut : public daemon_job_record
{
public:
	void set_completion_time(time_t new_time)
	{
		daemon_job_record::set_completion_time(new_time);
	}

	void set_num_members(int num_members)
	{
		ASSERT(num_members >= 0, "num_members: %d", num_members);

		for (int i = 0; i < num_members; ++i)
			increment_num_members();
	}

	void set_num_succeeded(int num_succeeded)
	{
		ASSERT(num_succeeded >= 0, "num_succeeded: %d", num_succeeded);

		for (int i = 0; i < num_succeeded; ++i)
			increment_num_succeeded();
	}
};

/*
 * Check for the existence of the daemon job record and daemon job member SBT
 * for the given daemon job ID.
 */
static void
check_for_record_and_member_sbt(djob_id_t id, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	btree_key_t key;
	char *djob_member_sbt_name = NULL;
	int djob_member_sbt_fd = -1;

	int djobs_sbt_fd = cpool_fd_store::getInstance().\
	    get_djobs_sbt_fd(false, &error);
	ON_ISI_ERROR_GOTO(out, error, "failed to retrieve daemon jobs SBT fd");

	key = daemon_job_record::get_key(id);
	if (ifs_sbt_get_entry_at(djobs_sbt_fd, &key, NULL, 0, NULL, NULL)
	    != 0) {
		error = isi_system_error_new(errno,
		    "failed to get daemon jobs SBT entry for id %d", id);
		goto out;
	}

	djob_member_sbt_name = get_djob_file_list_sbt_name(id);
	if (djob_member_sbt_name == NULL) {
		error = isi_system_error_new(EINVAL,
		    "djob_member_sbt_name returned NULL");
		goto out;
	}

	djob_member_sbt_fd = open_sbt(djob_member_sbt_name, false,
	    &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to open daemon job member SBT for %d", id);

 out:
	if (djob_member_sbt_fd != -1)
		close_sbt(djob_member_sbt_fd);

	if (djob_member_sbt_name != NULL)
		free(djob_member_sbt_name);

	isi_error_handle(error, error_out);
}

TEST(test_at_startup)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = NULL;

	at_startup(&error);
	fail_if(error != NULL, "unexpected error: %#{}", isi_error_fmt(error));

	/*
	 * For each of the non-multi-job op_types, verify the existence of its
	 * daemon job record and daemon job member SBT.
	 */
	OT_FOREACH(op_type) {
		if (!op_type->is_multi_job()) {
			check_for_record_and_member_sbt(
			    op_type->get_single_job_id(), &error);
			fail_if (error != NULL,
			    "check_for_record_and_member_sbt failed "
			    "for %{}: %#{}",
			    task_type_fmt(op_type->get_type()),
			    isi_error_fmt(error));
		}
	}

	/*
	 * Run at_startup again to simulate isi_cpool_d starting up after the
	 * initial startup.
	 */

	at_startup(&error);
	fail_if(error != NULL, "unexpected error: %#{}", isi_error_fmt(error));

	/*
	 * And again verify the record and member SBT for each of the
	 * non-multi-job op_types.
	 */
	OT_FOREACH(op_type) {
		if (!op_type->is_multi_job()) {
			check_for_record_and_member_sbt(
			    op_type->get_single_job_id(), &error);
			fail_if (error != NULL,
			    "check_for_record_and_member_sbt failed "
			    "for %{}: %#{}",
			    task_type_fmt(op_type->get_type()),
			    isi_error_fmt(error));
		}
	}
}

TEST(test_create_job__non_multi_job)
{
	struct isi_error *error = NULL;
	djob_id_t djob_id;

	/*
	 * We don't want to run at_startup here because that would create the
	 * local GC daemon job record and member SBT, which is what the
	 * function we're trying to test does - but we do need the daemon
	 * job configuration.
	 */
	create_daemon_job_config(&error);
	fail_if (error != NULL,
	    "failed to create daemon job configuration: %#{}",
	    isi_error_fmt(error));

	const cpool_operation_type *op_type = OT_LOCAL_GC; // a non-multi job

	djob_id = create_job(op_type, &error);
	fail_if (error != NULL, "error creating %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	fail_if (djob_id != op_type->get_single_job_id(), "(exp: %d act: %d)",
	    op_type->get_single_job_id(), djob_id);

	check_for_record_and_member_sbt(djob_id, &error);
	fail_if (error != NULL,
	    "check_for_record_and_member_sbt failed for %d: %#{}",
	    djob_id, isi_error_fmt(error));

	/* "Create" the job again, verifying we get the same daemon job ID. */
	djob_id = create_job(op_type, &error);
	fail_if (error != NULL, "error creating %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	fail_if (djob_id != op_type->get_single_job_id(), "(exp: %d act: %d)",
	    op_type->get_single_job_id(), djob_id);

	check_for_record_and_member_sbt(djob_id, &error);
	fail_if (error != NULL,
	    "check_for_record_and_member_sbt failed for %d: %#{}",
	    djob_id, isi_error_fmt(error));
}

TEST(test_create_job__multi_job)
{
	struct isi_error *error = NULL;
	djob_id_t djob_id;

	/*
	 * Unlike test_create_job__non_multi_job, we can run at_startup here,
	 * because that won't create any multi-jobs.
	 */
	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	const cpool_operation_type *op_type = OT_RECALL; // a multi job

	djob_id = create_job(op_type, &error);
	fail_if (error != NULL, "error creating %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	fail_if (daemon_job_configuration::get_multi_job_id_base() != djob_id,
	    "(exp: %d act: %d)",
	    daemon_job_configuration::get_multi_job_id_base(), djob_id);

	check_for_record_and_member_sbt(djob_id, &error);
	fail_if (error != NULL,
	    "check_for_record_and_member_sbt failed for %d: %#{}",
	    djob_id, isi_error_fmt(error));

	/*
	 * Create another recall job, and verify that we get a different daemon
	 * job ID.
	 */
	djob_id = create_job(op_type, &error);
	fail_if (error != NULL, "error creating %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	fail_if (daemon_job_configuration::get_multi_job_id_base() + 1 !=
	    djob_id, "(exp: %d act: %d)",
	    daemon_job_configuration::get_multi_job_id_base() + 1, djob_id);

	check_for_record_and_member_sbt(djob_id, &error);
	fail_if (error != NULL,
	    "check_for_record_and_member_sbt failed for %d: %#{}",
	    djob_id, isi_error_fmt(error));

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

TEST(test_get_daemon_job_config)
{
	struct isi_error *error = NULL;
	daemon_job_configuration *djob_config = NULL;

	/* Before initializing, there should be no configuration to get. */
	djob_config = get_daemon_job_config(&error);
	fail_if (djob_config != NULL);
	fail_if (error == NULL, "get_daemon_job_config should have failed");
	fail_if (!isi_system_error_is_a(error, ENOENT),
	    "expected ENOENT error from get_daemon_job_config: %#{}",
	    isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* After initializing, retrieving the configuration should work. */
	djob_config = get_daemon_job_config(&error);
	fail_if (error != NULL, "get_daemon_job_config failed: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_config == NULL);

	delete djob_config;
}

static void
update_daemon_job_configuration(const daemon_job_configuration* new_config)
{
	ASSERT(new_config != NULL);

	struct isi_error *error = NULL;

	int djob_sbt_fd = cpool_fd_store::getInstance().\
	    get_djobs_sbt_fd(false, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* true: block, false: shared lock */
	lock_daemon_job_config(true, false, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	daemon_job_configuration *djob_config = get_daemon_job_config(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	const void *old_serialization, *new_serialization;
	size_t old_serialization_size, new_serialization_size;

	djob_config->get_serialization(old_serialization,
	    old_serialization_size);
	new_config->get_serialization(new_serialization,
	    new_serialization_size);

	btree_key_t djob_config_key = daemon_job_record::get_key(
	    daemon_job_configuration::get_daemon_job_id());

	fail_if (ifs_sbt_cond_mod_entry(djob_sbt_fd, &djob_config_key,
	    new_serialization, new_serialization_size, NULL, BT_CM_BUF,
	    old_serialization, old_serialization_size, NULL) != 0,
	    "failed to update daemon job configuration: %s", strerror(errno));

	unlock_daemon_job_config(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	delete djob_config;
}

struct lock_checker_arg {
	djob_id_t id_;			/* input */
	bool exists_;			/* output */
	bool is_locked_;		/* output */
	struct isi_error **error_;	/* input/output */
};

static void *
lock_checker(void *arg)
{
	ASSERT(arg != NULL);

	struct lock_checker_arg *lc_arg =
	    static_cast<struct lock_checker_arg *>(arg);
	ASSERT(lc_arg != NULL);

	struct isi_error *error = NULL;
	bool is_config =
	    (lc_arg->id_ == daemon_job_configuration::get_daemon_job_id());

	/* First determine if the record or configuration exists. */
	if (is_config) {
		daemon_job_configuration *djob_config =
		    get_daemon_job_config(&error);
		if (error == NULL) {
			lc_arg->exists_ = true;

			delete djob_config;
			djob_config = NULL;
		}
		else if (isi_system_error_is_a(error, ENOENT)) {
			lc_arg->exists_ = false;

			isi_error_free(error);
			error = NULL;
		}
		ON_ISI_ERROR_GOTO(out, error);
	}
	else {
		daemon_job_record *djob_record =
		    get_daemon_job_record(lc_arg->id_, &error);
		if (error == NULL) {
			lc_arg->exists_ = true;

			delete djob_record;
			djob_record = NULL;
		}
		else if (isi_system_error_is_a(error, ENOENT)) {
			lc_arg->exists_ = false;

			isi_error_free(error);
			error = NULL;
		}
		ON_ISI_ERROR_GOTO(out, error);
	}

	/*
	 * Next determine if the record or configuration is locked.  We do this
	 * even if a record or configuration doesn't exist to catch any errant
	 * locks.
	 */
	if (is_config)
		/* false: non-blocking, false: shared lock */
		lock_daemon_job_config(false, false, &error);
	else
		/* false: non-blocking, false: shared lock */
		lock_daemon_job_record(lc_arg->id_, false, false, &error);

	if (error == NULL) {
		lc_arg->is_locked_ = false;
		is_config ?
		    unlock_daemon_job_config(&error) :
		    unlock_daemon_job_record(lc_arg->id_, &error);
	}
	else if (isi_system_error_is_a(error, EWOULDBLOCK)) {
		lc_arg->is_locked_ = true;

		isi_error_free(error);
		error = NULL;
	}
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, lc_arg->error_);

	pthread_exit(NULL);
}

static void
get_daemon_job_locks(const daemon_job_configuration *config,
    std::set<djob_id_t> &existing, std::set<djob_id_t> &locked,
    struct isi_error **error_out)
{
	ASSERT(config != NULL);

	struct isi_error *error = NULL;

	/*
	 * This will check more IDs than we will probably have, but it's OK
	 * to cast a wide net here.
	 */
	djob_id_t max_checked_id =
	    config->get_multi_job_id_base() +
	    config->get_last_used_daemon_job_id();

	for (djob_id_t id = 0; id <= max_checked_id; ++id) {
		struct lock_checker_arg lc_arg;
		lc_arg.id_ = id;
		lc_arg.error_ = &error;

		pthread_t lc_thread;
		pthread_attr_t lc_thread_attr;

		fail_if (pthread_attr_init(&lc_thread_attr) != 0,
		    "failed to initialize thread attributes (id: %d): %s",
		    id, strerror(errno));
		fail_if (pthread_attr_setdetachstate(&lc_thread_attr,
		    PTHREAD_CREATE_JOINABLE) != 0,
		    "failed to set joinable attribute (id: %d): %s",
		    id, strerror(errno));
		fail_if (pthread_create(&lc_thread, &lc_thread_attr,
		    lock_checker, &lc_arg) != 0,
		    "failed to create lock checker thread (id: %d): %s",
		    id, strerror(errno));
		fail_if (pthread_join(lc_thread, NULL) != 0,
		    "failed to join lock checker thread (id: %d): %s",
		    id, strerror(errno));
		fail_if (pthread_attr_destroy(&lc_thread_attr) != 0,
		    "failed to destroy thread attributes (id: %d): %s",
		    id, strerror(errno));

		ON_ISI_ERROR_GOTO(out, error);

		if (lc_arg.exists_)
			existing.insert(id);
		if (lc_arg.is_locked_)
			locked.insert(id);
	}

 out:
	isi_error_handle(error, error_out);
}

static void
fail_on_mismatched_djob_id_set(const std::set<djob_id_t> &exp,
    const std::set<djob_id_t> &act)
{
	ASSERT (exp != act);

	struct fmt FMT_INIT_CLEAN(fmt);

	std::set<djob_id_t>::const_iterator exp_iter = exp.begin();
	std::set<djob_id_t>::const_iterator act_iter = act.begin();

	fmt_print(&fmt, "expected %zu locked record(s): {", exp.size());
	for (; exp_iter != exp.end(); ++exp_iter) {
		if (exp_iter != exp.begin())
			fmt_print(&fmt, ", ");
		fmt_print(&fmt, "%d", *exp_iter);
	}
	fmt_print(&fmt, "}, not %zu: {", act.size());
	for (; act_iter != act.end(); ++act_iter) {
		if (act_iter != act.begin())
			fmt_print(&fmt, ", ");
		fmt_print(&fmt, "%d", *act_iter);
	}
	fmt_print(&fmt, "}");

	fail ("%s", fmt_string(&fmt));
}

static void
check_for_expected_job_id_locks(daemon_job_configuration *djob_config,
    int num_job_ids, ...)
{
	/*
	 * This test can have false positive failures from other (non-test)
	 * cloudpool processing attempting to take locks at the same time.
	 * Those failures should be transient, so we expect to see them resolved
	 * after a retry.
	 * Note that the real fix is to make sure tests are using their own
	 * SBTs and not sharing with the live system.  That work is out of scope
	 * for Riptide phase I, but we should revisit as soon as possible
	 */
	const int num_retries = 5;

	struct isi_error *error = NULL;
	std::set<djob_id_t> existing;
	std::set<djob_id_t> locked;
	std::set<djob_id_t> expected_locks;

	va_list al;
	va_start(al, num_job_ids);
	for (int i = 0; i < num_job_ids; i++) {
		djob_id_t curr_job_id = va_arg(al, djob_id_t);
		expected_locks.insert(curr_job_id);
	}
	va_end(al);

	for (int retry = 0; retry < num_retries; retry++) {
		/* Clear any residue from an early iteration */
		locked.clear();

		get_daemon_job_locks(djob_config, existing, locked, &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));

		if (expected_locks == locked)
			return;

		sleep(1);
	}

	/*
	 * If we got here, we know that expected_locks != locked 5 times over,
	 * force a failure and dump
	 */
	fail_on_mismatched_djob_id_set(expected_locks, locked);
}

/*
 * No daemon job record exists for the daemon job ID one greater than the last
 * used ID, verify that get_next_available_job_id() returns it.
 */
TEST(test_get_next_available_job_id_1, CK_MEM_LEAKS)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	{
		daemon_job_configuration djob_config;
		djob_config.set_last_used_daemon_job_id(9);
		update_daemon_job_configuration(&djob_config);
	}

	int max_jobs = 50;
	daemon_job_configuration *djob_config = NULL;
	scoped_object_list objects_to_delete;
	djob_id_t djob_id = get_next_available_job_id(max_jobs, djob_config,
	    objects_to_delete, &error);
	fail_if (error != NULL, "error getting next available job ID: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_id != 10, "(exp: 10 act: %d)", djob_id);
	fail_if (djob_config == NULL);
	fail_if (djob_config->get_num_active_jobs() != 1,
	    "num active jobs mismatch (exp: 1 act: %d)",
	    djob_config->get_num_active_jobs());

	check_for_expected_job_id_locks(djob_config, 2, 0, 10);
}

/*
 * A daemon job record exists for the daemon job ID one greater than the last
 * used ID, verify that get_next_available_job_id() returns two greater than
 * the last used ID.
 *
 * Note - we get false-positive leaking of 80 bytes due to static pthread
 * initialization here. Depending on whether it is run in the build system or
 * in a sandbox, the leak may or may not appear.  Disabling mem check for this
 * test temporarily to avoid confusion.
 */
TEST(test_get_next_available_job_id_2, CK_MEM_DISABLE)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	{
		daemon_job_configuration djob_config;
		djob_config.set_last_used_daemon_job_id(20);
		update_daemon_job_configuration(&djob_config);
	}

	/* Create a daemon job record with id 21. */
	int djobs_sbt_fd = cpool_fd_store::getInstance().\
	    get_djobs_sbt_fd(false, &error);
	fail_if (error != NULL, "failed to get daemon jobs SBT fd: %#{}",
	    isi_error_fmt(error));

	daemon_job_record djob_rec;
	djob_rec.set_daemon_job_id(21);

	const void *serialized_djob_rec;
	size_t serialized_djob_rec_size;
	djob_rec.get_serialization(serialized_djob_rec,
	    serialized_djob_rec_size);

	btree_key_t djob_rec_key = djob_rec.get_key();

	fail_if (ifs_sbt_add_entry(djobs_sbt_fd, &djob_rec_key,
	    serialized_djob_rec, serialized_djob_rec_size, NULL) != 0,
	    "failed to add filler daemon job record: %s", strerror(errno));

	int max_jobs = 50;
	daemon_job_configuration *djob_config = NULL;
	scoped_object_list objects_to_delete;
	djob_id_t djob_id = get_next_available_job_id(max_jobs, djob_config,
	    objects_to_delete, &error);
	fail_if (error != NULL, "error getting next available job ID: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_id != 22, "(exp: 22 act: %d)", djob_id);
	fail_if (djob_config == NULL);
	fail_if (djob_config->get_num_active_jobs() != 1,
	    "num active jobs mismatch (exp: 1 act: %d)",
	    djob_config->get_num_active_jobs());

	check_for_expected_job_id_locks(djob_config, 2, 0, 22);
}

/*
 * The maximum number of daemon job records exist, verify that
 * get_next_available_job_id() returns the proper error.
 */
TEST(test_get_next_available_job_id_3)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	int max_jobs = 0;
	daemon_job_configuration *djob_config = NULL;
	scoped_object_list objects_to_delete;
	djob_id_t djob_id = get_next_available_job_id(max_jobs, djob_config,
	    objects_to_delete, &error);
	fail_if (error == NULL, "expected EBUSY");
	fail_if (!isi_system_error_is_a(error, EBUSY), "expected EBUSY: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_id != DJOB_RID_INVALID,
	    "djob_id mismatch (exp: %d act: %d)", DJOB_RID_INVALID, djob_id);

	isi_error_free(error);
	error = NULL;
}

/*
 * All daemon job IDs have been exhausted, verify that
 * get_next_available_job_id() returns the proper error.
 */
TEST(test_get_next_available_job_id_4)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	{
		daemon_job_configuration djob_config;
		djob_config.set_last_used_daemon_job_id((djob_id_t)(-3));
		update_daemon_job_configuration(&djob_config);
	}

	/* Create a daemon job record with id (djob_id_t)(-2). */
	int djobs_sbt_fd = cpool_fd_store::getInstance().\
	    get_djobs_sbt_fd(false, &error);
	fail_if (error != NULL, "failed to get daemon jobs SBT fd: %#{}",
	    isi_error_fmt(error));

	daemon_job_record djob_rec;
	djob_rec.set_daemon_job_id(-2);

	const void *serialized_djob_rec;
	size_t serialized_djob_rec_size;
	djob_rec.get_serialization(serialized_djob_rec,
	    serialized_djob_rec_size);

	btree_key_t djob_rec_key = djob_rec.get_key();

	fail_if (ifs_sbt_add_entry(djobs_sbt_fd, &djob_rec_key,
	    serialized_djob_rec, serialized_djob_rec_size, NULL) != 0,
	    "failed to add filler daemon job record: %s", strerror(errno));

	int max_jobs = 100;
	daemon_job_configuration *djob_config = NULL;
	scoped_object_list objects_to_delete;
	djob_id_t djob_id = get_next_available_job_id(max_jobs, djob_config,
	    objects_to_delete, &error);
	fail_if (error == NULL, "expected ENOSPC");
	fail_if (!isi_system_error_is_a(error, ENOSPC),
	    "expected ENOSPC: %#{}", isi_error_fmt(error));
	fail_if (djob_id != DJOB_RID_INVALID,
	    "djob_id mismatch (exp: %d act: %d)", DJOB_RID_INVALID, djob_id);

	isi_error_free(error);
	error = NULL;
}

/*
 * An infinite number of daemon job records is allowed, verify that
 * get_next_available_job_id() returns a valid daemon job ID.
 */
TEST(test_get_next_available_job_id_5, CK_MEM_LEAKS)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	{
		daemon_job_configuration djob_config;
		djob_config.set_last_used_daemon_job_id(452);
		update_daemon_job_configuration(&djob_config);
	}

	/* Create a daemon job record with id 453. */
	int djobs_sbt_fd = cpool_fd_store::getInstance().\
	    get_djobs_sbt_fd(false, &error);
	fail_if (error != NULL, "failed to get daemon jobs SBT fd: %#{}",
	    isi_error_fmt(error));

	daemon_job_record djob_rec;
	djob_rec.set_daemon_job_id(453);

	const void *serialized_djob_rec;
	size_t serialized_djob_rec_size;
	djob_rec.get_serialization(serialized_djob_rec,
	    serialized_djob_rec_size);

	btree_key_t djob_rec_key = djob_rec.get_key();

	fail_if (ifs_sbt_add_entry(djobs_sbt_fd, &djob_rec_key,
	    serialized_djob_rec, serialized_djob_rec_size, NULL) != 0,
	    "failed to add filler daemon job record: %s", strerror(errno));

	int max_jobs = daemon_job_configuration::UNLIMITED_DAEMON_JOBS;
	daemon_job_configuration *djob_config = NULL;
	scoped_object_list objects_to_delete;
	djob_id_t djob_id = get_next_available_job_id(max_jobs, djob_config,
	    objects_to_delete, &error);
	fail_if (error != NULL, "error getting next available job ID: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_id != 454, "(exp: 454 act: %d)", djob_id);
	fail_if (djob_config == NULL);
	fail_if (djob_config->get_num_active_jobs() != 1,
	    "num active jobs mismatch (exp: 1 act: %d)",
	    djob_config->get_num_active_jobs());

	check_for_expected_job_id_locks(djob_config, 2, 0, 454);
}

TEST(test_get_daemon_job_record)
{
	struct isi_error *error = NULL;
	btree_key_t key;
	daemon_job_record *retrieved_djob_rec = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	int djobs_sbt_fd = cpool_fd_store::getInstance().\
	    get_djobs_sbt_fd(false, &error);
	fail_if (error != NULL, "failed to get daemon jobs SBT fd: %#{}",
	    isi_error_fmt(error));

	/*
	 * Create a daemon job record to be added to the daemon jobs
	 * SBT and subsequently found.
	 */
	daemon_job_record to_be_found;
	to_be_found.set_daemon_job_id(23);
	to_be_found.set_operation_type(OT_ARCHIVE);
	key = to_be_found.get_key();

	const void *to_be_found_serialized;
	size_t to_be_found_serialized_size;
	to_be_found.get_serialization(to_be_found_serialized,
	    to_be_found_serialized_size);

	fail_if (ifs_sbt_add_entry(djobs_sbt_fd, &key, to_be_found_serialized,
	    to_be_found_serialized_size, NULL) != 0,
	    "failed to add to-be-found daemon job record: %s",
	    strerror(errno));

	/* Get the just-added daemon job record. */
	retrieved_djob_rec = get_daemon_job_record(23, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (retrieved_djob_rec == NULL);
	fail_if (retrieved_djob_rec->get_daemon_job_id() != 23,
	    "(exp: 23 act: %d", retrieved_djob_rec->get_daemon_job_id());
	fail_if (retrieved_djob_rec->get_operation_type() != OT_ARCHIVE);
	delete retrieved_djob_rec;

	/* Attempt to get a non-existent daemon job record. */
	retrieved_djob_rec = get_daemon_job_record(81, &error);
	fail_if (error == NULL);
	fail_if (!isi_system_error_is_a(error, ENOENT),
	    "expected ENOENT: %#{}", isi_error_fmt(error));
	isi_error_free(error);
}

TEST(test_start_job_no_job_engine)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t arc_djob_id_1, arc_djob_id_2, lgc_djob_id_1, lgc_djob_id_2;

	const cpool_operation_type *local_gc_op_type = OT_LOCAL_GC;
	djob_id_t local_gc_djob_id = local_gc_op_type->get_single_job_id();

	/*
	 * Start multiple archive jobs - each call should return a different
	 * daemon job ID succeed since archive is a "multi-job".
	 */
	arc_djob_id_1 = start_job_no_job_engine(OT_ARCHIVE, &error);
	fail_if (error != NULL,
	    "start_job_no_job_engine failed for archive: %#{}",
	    isi_error_fmt(error));
	fail_if (arc_djob_id_1 == DJOB_RID_INVALID);

	arc_djob_id_2 = start_job_no_job_engine(OT_ARCHIVE, &error);
	fail_if (error != NULL,
	    "start_job_no_job_engine failed for archive: %#{}",
	    isi_error_fmt(error));
	fail_if (arc_djob_id_2 == DJOB_RID_INVALID);
	fail_if (arc_djob_id_1 == arc_djob_id_2);

	/*
	 * Start multiple local GC jobs - each call should return the same
	 * daemon job ID since local GC is not a "multi-job".
	 */
	lgc_djob_id_1 = start_job_no_job_engine(OT_LOCAL_GC, &error);
	fail_if (error != NULL,
	    "start_job_no_job_engine failed for local GC: %#{}",
	    isi_error_fmt(error));
	fail_if (lgc_djob_id_1 != local_gc_djob_id);

	lgc_djob_id_2 = start_job_no_job_engine(OT_LOCAL_GC, &error);
	fail_if (error != NULL,
	    "start_job_no_job_engine failed for local GC: %#{}",
	    isi_error_fmt(error));
	fail_if (lgc_djob_id_2 != local_gc_djob_id);

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

static void
get_task_state_runner(djob_op_job_task_state operation_state,
    std::vector<djob_op_job_task_state> job_states,
    djob_op_job_task_state task_state,
    djob_op_job_task_state expected_task_state)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * The specific operation type doesn't matter, but it needs to be a
	 * multi-job since job_states can have size greater than 1.
	 */
	const cpool_operation_type *op_type = OT_ARCHIVE;
	isi_cloud::task *task = isi_cloud::task::create_task(op_type);
	fail_if (task == NULL);

	int jobs_sbt_fd = cpool_fd_store::getInstance().\
	    get_djobs_sbt_fd(false, &error);
	fail_if (error != NULL,
	    "failed to get daemon jobs SBT fd: %#{}", isi_error_fmt(error));

	/* Set the operation state. */
	daemon_job_configuration *djob_config = get_daemon_job_config(&error);
	fail_if (error != NULL,
	    "failed to retrieve daemon job configuration: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_config == NULL);

	if (djob_config->get_operation_state(op_type) != operation_state) {
		djob_config->set_operation_state(op_type, operation_state);

		const void *serialization;
		size_t serialization_size;
		djob_config->get_serialization(serialization,
		    serialization_size);

		btree_key_t key =
		    daemon_job_record::get_key(
		    djob_config->get_daemon_job_id());

		fail_if (ifs_sbt_cond_mod_entry(jobs_sbt_fd, &key,
		    serialization, serialization_size, NULL, BT_CM_NONE,
		    NULL, 0, NULL) != 0,
		    "failed to change daemon job configuration for test: %s",
		    strerror(errno));
	}

	/* Set the job state(s).  Also add the job IDs to the task. */
	for (size_t i = 0; i < job_states.size(); ++i) {
		djob_id_t job_id = create_job(op_type, &error);
		fail_if (error != NULL,
		    "failed to create %{} job (i: %zu): %#{}",
		    task_type_fmt(op_type->get_type()), i,
		    isi_error_fmt(error));

		task->add_daemon_job(job_id, &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));

		daemon_job_record *djob_rec = get_daemon_job_record(job_id,
		    &error);
		fail_if (error != NULL,
		    "failed to get daemon job record for id %d: %#{}",
		    job_id, isi_error_fmt(error));
		fail_if (djob_rec == NULL);

		if (djob_rec->get_state() != job_states[i]) {
			djob_rec->set_state(job_states[i]);

			const void *serialization;
			size_t serialization_size;
			djob_rec->get_serialization(serialization,
			    serialization_size);

			btree_key_t key = djob_rec->get_key();

			fail_if (ifs_sbt_cond_mod_entry(jobs_sbt_fd, &key,
			    serialization, serialization_size, NULL,
			    BT_CM_NONE, NULL, 0, NULL) != 0,
			    "failed to change daemon job record (id: %d) "
			    "for test: %s", job_id, strerror(errno));

		}

		delete djob_rec;
	}

	/* Set the task state. */
	task->set_state(task_state);

	/* Compare the resultant state with the expected state. */
	djob_op_job_task_state resultant_state = get_state(task, &error);
	fail_if (error != NULL, "failed to get state for task: %#{}",
	    isi_error_fmt(error));
	fail_if (resultant_state != expected_task_state,
	    "state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(expected_task_state),
	    djob_op_job_task_state_fmt(resultant_state));

	delete task;
	delete djob_config;

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * These next 24 tests test getting the state of a task with the task, its
 * (single) owning job, and its operation in various states - basically the
 * combination of each state of:
 *
 * operations:	{ running, paused }
 * jobs:	{ running, paused, cancelled }
 * tasks:	{ running, paused, cancelled, error }
 */

/* running operation + running job + running task = running task */
TEST(test_get_state__task_01)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_RUNNING);
	get_task_state_runner(DJOB_OJT_RUNNING, job_states, DJOB_OJT_RUNNING,
	    DJOB_OJT_RUNNING);
}

/* running operation + running job + paused task = paused task */
TEST(test_get_state__task_02)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_RUNNING);
	get_task_state_runner(DJOB_OJT_RUNNING, job_states, DJOB_OJT_PAUSED,
	    DJOB_OJT_PAUSED);
}

/* running operation + running job + cancelled task = cancelled task */
TEST(test_get_state__task_03)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_RUNNING);
	get_task_state_runner(DJOB_OJT_RUNNING, job_states, DJOB_OJT_CANCELLED,
	    DJOB_OJT_CANCELLED);
}

/* running operation + running job + error task = error task */
TEST(test_get_state__task_04)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_RUNNING);
	get_task_state_runner(DJOB_OJT_RUNNING, job_states, DJOB_OJT_ERROR,
	    DJOB_OJT_ERROR);
}

/* running operation + paused job + running task = paused task */
TEST(test_get_state__task_05)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_PAUSED);
	get_task_state_runner(DJOB_OJT_RUNNING, job_states, DJOB_OJT_RUNNING,
	    DJOB_OJT_PAUSED);
}

/* running operation + paused job + paused task = paused task */
TEST(test_get_state__task_06)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_PAUSED);
	get_task_state_runner(DJOB_OJT_RUNNING, job_states, DJOB_OJT_PAUSED,
	    DJOB_OJT_PAUSED);
}

/* running operation + paused job + cancelled task = cancelled task */
TEST(test_get_state__task_07)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_PAUSED);
	get_task_state_runner(DJOB_OJT_RUNNING, job_states, DJOB_OJT_CANCELLED,
	    DJOB_OJT_CANCELLED);
}

/* running operation + paused job + error task = error task */
TEST(test_get_state__task_08)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_PAUSED);
	get_task_state_runner(DJOB_OJT_RUNNING, job_states, DJOB_OJT_ERROR,
	    DJOB_OJT_ERROR);
}

/* running operation + cancelled job + running task = cancelled task */
TEST(test_get_state__task_09)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_CANCELLED);
	get_task_state_runner(DJOB_OJT_RUNNING, job_states, DJOB_OJT_RUNNING,
	    DJOB_OJT_CANCELLED);
}

/* running operation + cancelled job + paused task = cancelled task */
TEST(test_get_state__task_10)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_CANCELLED);
	get_task_state_runner(DJOB_OJT_RUNNING, job_states, DJOB_OJT_PAUSED,
	    DJOB_OJT_CANCELLED);
}

/* running operation + cancelled job + cancelled task = cancelled task */
TEST(test_get_state__task_11)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_CANCELLED);
	get_task_state_runner(DJOB_OJT_RUNNING, job_states, DJOB_OJT_CANCELLED,
	    DJOB_OJT_CANCELLED);
}

/* running operation + cancelled job + error task = error task */
TEST(test_get_state__task_12)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_CANCELLED);
	get_task_state_runner(DJOB_OJT_RUNNING, job_states, DJOB_OJT_ERROR,
	    DJOB_OJT_ERROR);
}

/* paused operation + running job + running task = paused task */
TEST(test_get_state__task_13)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_RUNNING);
	get_task_state_runner(DJOB_OJT_PAUSED, job_states, DJOB_OJT_RUNNING,
	    DJOB_OJT_PAUSED);
}

/* paused operation + running job + paused task = paused task */
TEST(test_get_state__task_14)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_RUNNING);
	get_task_state_runner(DJOB_OJT_PAUSED, job_states, DJOB_OJT_PAUSED,
	    DJOB_OJT_PAUSED);
}

/* paused operation + running job + cancelled task = cancelled task */
TEST(test_get_state__task_15)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_RUNNING);
	get_task_state_runner(DJOB_OJT_PAUSED, job_states, DJOB_OJT_CANCELLED,
	    DJOB_OJT_CANCELLED);
}

/* paused operation + running job + error task = error task */
TEST(test_get_state__task_16)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_RUNNING);
	get_task_state_runner(DJOB_OJT_PAUSED, job_states, DJOB_OJT_ERROR,
	    DJOB_OJT_ERROR);
}

/* paused operation + paused job + running task = paused task */
TEST(test_get_state__task_17)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_PAUSED);
	get_task_state_runner(DJOB_OJT_PAUSED, job_states, DJOB_OJT_RUNNING,
	    DJOB_OJT_PAUSED);
}

/* paused operation + paused job + paused task = paused task */
TEST(test_get_state__task_18)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_PAUSED);
	get_task_state_runner(DJOB_OJT_PAUSED, job_states, DJOB_OJT_PAUSED,
	    DJOB_OJT_PAUSED);
}

/* paused operation + paused job + cancelled task = cancelled task */
TEST(test_get_state__task_19)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_PAUSED);
	get_task_state_runner(DJOB_OJT_PAUSED, job_states, DJOB_OJT_CANCELLED,
	    DJOB_OJT_CANCELLED);
}

/* paused operation + paused job + error task = error task */
TEST(test_get_state__task_20)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_PAUSED);
	get_task_state_runner(DJOB_OJT_PAUSED, job_states, DJOB_OJT_ERROR,
	    DJOB_OJT_ERROR);
}

/* paused operation + cancelled job + running task = cancelled task */
TEST(test_get_state__task_21)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_CANCELLED);
	get_task_state_runner(DJOB_OJT_PAUSED, job_states, DJOB_OJT_RUNNING,
	    DJOB_OJT_CANCELLED);
}

/* paused operation + cancelled job + paused task = cancelled task */
TEST(test_get_state__task_22)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_CANCELLED);
	get_task_state_runner(DJOB_OJT_PAUSED, job_states, DJOB_OJT_PAUSED,
	    DJOB_OJT_CANCELLED);
}

/* paused operation + cancelled job + cancelled task = cancelled task */
TEST(test_get_state__task_23)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_CANCELLED);
	get_task_state_runner(DJOB_OJT_PAUSED, job_states, DJOB_OJT_CANCELLED,
	    DJOB_OJT_CANCELLED);
}

/* paused operation + cancelled job + error task = error task */
TEST(test_get_state__task_24)
{
	std::vector<djob_op_job_task_state> job_states(1, DJOB_OJT_CANCELLED);
	get_task_state_runner(DJOB_OJT_PAUSED, job_states, DJOB_OJT_ERROR,
	    DJOB_OJT_ERROR);
}

TEST(test_add_task__op_mismatch)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	isi_cloud::archive_task archive_task;
	const cpool_operation_type *writeback_op_type = OT_WRITEBACK;

	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;

	add_task(&archive_task, NULL, writeback_op_type->get_single_job_id(),
	    sbt_ops, objects_to_delete, &error);
	fail_if (error == NULL, "expected EINVAL");
	fail_if (!isi_system_error_is_a(error, EINVAL),
	    "expected EINVAL: %#{}", isi_error_fmt(error));

	isi_error_free(error);
}

TEST(test_add_task__nonexistent_daemon_job)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	isi_cloud::archive_task archive_task;
	djob_id_t bogus_job_id = 57;

	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;

	add_task(&archive_task, NULL, bogus_job_id, sbt_ops, objects_to_delete,
	    &error);
	fail_if (error == NULL, "expected ENOENT");
	fail_if (!isi_system_error_is_a(error, ENOENT),
	    "expected ENOENT: %#{}", isi_error_fmt(error));

	isi_error_free(error);
}

TEST(test_add_member__single_job__nonexisting_member)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	isi_cloud::writeback_task writeback_task;
	writeback_task.set_lin(13579);
	writeback_task.set_snapid(987665);

	const cpool_operation_type *op_type = OT_WRITEBACK;
	djob_id_t writeback_job_id = op_type->get_single_job_id();
	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;

	add_member__single_job(&writeback_task, writeback_job_id, NULL,
	    sbt_ops, objects_to_delete, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (sbt_ops.size() != 1, "sbt_ops.size(): %zu", sbt_ops.size());

	struct hsbt_bulk_entry mem_op = sbt_ops[0];
	fail_if (mem_op.is_hash_btree,
	    "writeback does not use hashed b-trees");
	fail_if (mem_op.bulk_entry.op_type != SBT_SYS_OP_ADD,
	    "mem_op.bulk_entry.op_type: %d", mem_op.bulk_entry.op_type);
}

TEST(test_add_task_helper__single_job__existing_member)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	isi_cloud::cache_invalidation_task cache_invalidation_task;
	cache_invalidation_task.set_lin(12345);
	cache_invalidation_task.set_snapid(98765);

	const cpool_operation_type *op_type = OT_CACHE_INVALIDATION;
	djob_id_t job_id = op_type->get_single_job_id();

	/* Add this task as a member of the cache invalidation job. */
	int member_sbt_fd = open_member_sbt(job_id, false, &error);
	fail_if (error != NULL, "open_member_sbt failed: %#{}",
	    isi_error_fmt(error));
	fail_if (member_sbt_fd == -1);

	daemon_job_member existing_member(cache_invalidation_task.get_key());
	const void *existing_member_serialization;
	size_t existing_member_serialization_size;
	existing_member.get_serialization(existing_member_serialization,
	    existing_member_serialization_size);

	add_sbt_entry_ut(member_sbt_fd, cache_invalidation_task.get_key(),
	    existing_member_serialization, existing_member_serialization_size);

	close_sbt(member_sbt_fd);

	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;

	add_member__single_job(&cache_invalidation_task, job_id, NULL, sbt_ops,
	    objects_to_delete, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (sbt_ops.size() != 1, "sbt_ops.size(): %zu", sbt_ops.size());
}

TEST(test_add_task_helper__multi_job__nonexisting_member)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	isi_cloud::recall_task recall_task;
	recall_task.set_lin(13579);
	recall_task.set_snapid(2468);

	const cpool_operation_type *op_type = OT_RECALL;
	djob_id_t recall_job_id = create_job(op_type, &error);
	fail_if (error != NULL, "create_job failed for %{}: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;

	add_member__multi_job(&recall_task, recall_job_id, NULL, sbt_ops,
	    objects_to_delete, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (sbt_ops.size() != 1, "sbt_ops.size(): %zu", sbt_ops.size());

	struct hsbt_bulk_entry mem_op = sbt_ops[0];
	fail_if (mem_op.is_hash_btree,
	    "recall does not use hashed b-trees");
	fail_if (mem_op.bulk_entry.op_type != SBT_SYS_OP_ADD,
	    "mem_op.bulk_entry.op_type: %d", mem_op.bulk_entry.op_type);

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

TEST(test_add_task_helper__multi_job__existing_member)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	isi_cloud::archive_task task;
	task.set_lin(12345);
	task.set_snapid(98765);

	const cpool_operation_type *op_type = OT_ARCHIVE;
	djob_id_t job_id = create_job(op_type, &error);
	fail_if (error != NULL, "failed to create %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* Add this task as a member of the cache invalidation job. */
	int member_sbt_fd = open_member_sbt(job_id, false, &error);
	fail_if (error != NULL, "open_member_sbt failed: %#{}",
	    isi_error_fmt(error));
	fail_if (member_sbt_fd == -1);

	daemon_job_member existing_member(task.get_key());
	const void *existing_member_serialization;
	size_t existing_member_serialization_size;
	existing_member.get_serialization(existing_member_serialization,
	    existing_member_serialization_size);

	add_sbt_entry_ut(member_sbt_fd, task.get_key(),
	    existing_member_serialization, existing_member_serialization_size);

	close_sbt(member_sbt_fd);

	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;

	add_member__multi_job(&task, job_id, NULL, sbt_ops, objects_to_delete,
	    &error);
	fail_if (error == NULL, "expected EEXIST");
	fail_if (!isi_system_error_is_a(error, EEXIST),
	    "expected EEXIST: %#{}", isi_error_fmt(error));
	fail_if (sbt_ops.size() != 0, "sbt_ops.size(): %zu", sbt_ops.size());

	isi_error_free(error);

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

TEST(test_add_task__single_job)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	isi_cloud::writeback_task writeback_task;
	writeback_task.set_lin(12345);
	writeback_task.set_snapid(23456);
	writeback_task.set_location(TASK_LOC_PENDING);

	const cpool_operation_type *writeback_op = OT_WRITEBACK;
	djob_id_t writeback_job_id = writeback_op->get_single_job_id();
	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;

	daemon_job_record *old_record = get_daemon_job_record(writeback_job_id,
	    &error);
	fail_if (error != NULL, "failed to get existing record: %#{}",
	    isi_error_fmt(error));

	const void *old_serialization;
	size_t old_serialization_size;
	old_record->get_serialization(old_serialization,
	    old_serialization_size);

	btree_key_t djob_rec_key = old_record->get_key();

	add_task(&writeback_task, NULL, writeback_job_id, sbt_ops,
	    objects_to_delete, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (sbt_ops.size() != 2, "sbt_ops.size(): %zu", sbt_ops.size());

	int daemon_jobs_sbt_fd =
	    cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	struct hsbt_bulk_entry rec_op = sbt_ops[0];
	fail_if (rec_op.is_hash_btree,
	    "writeback does not use hashed b-trees");
	fail_if (rec_op.bulk_entry.fd != daemon_jobs_sbt_fd,
	    "rec_op.bulk_entry.fd: %d daemon_jobs_sbt_fd: %d",
	    rec_op.bulk_entry.fd, daemon_jobs_sbt_fd);
	fail_if (rec_op.bulk_entry.op_type != SBT_SYS_OP_MOD,
	    "rec_op.bulk_entry.op_type: %d", rec_op.bulk_entry.op_type);
	fail_if((unsigned int)rec_op.bulk_entry.old_entry_size !=
	    old_serialization_size,
	    "old entry size mismatch (exp: %zu act: %d)",
	    old_serialization_size, rec_op.bulk_entry.old_entry_size);
	fail_if (memcmp(rec_op.bulk_entry.old_entry_buf, old_serialization,
	    old_serialization_size) != 0, "old entry buf mismatch");
	fail_if (rec_op.bulk_entry.key.keys[0] != djob_rec_key.keys[0] ||
	    rec_op.bulk_entry.key.keys[1] != djob_rec_key.keys[1],
	    "key mismatch (exp: %lu/%lu act: %lu/%lu)",
	    djob_rec_key.keys[0], djob_rec_key.keys[1],
	    rec_op.bulk_entry.key.keys[0], rec_op.bulk_entry.key.keys[1]);

	daemon_job_record new_record;
	new_record.set_serialization(rec_op.bulk_entry.entry_buf,
	    rec_op.bulk_entry.entry_size, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (new_record.get_daemon_job_id() != writeback_job_id,
	    "daemon job ID mismatch (exp: %d act: %d)",
	    writeback_job_id, new_record.get_daemon_job_id());
	fail_if (new_record.get_stats()->get_num_members() != 1,
	    "num_members mismatch (exp: 1 act: %d)",
	    new_record.get_stats()->get_num_members());
	fail_if (new_record.get_stats()->get_num_succeeded() != 0,
	    "num_succeeded mismatch (exp: 0 act: %d)",
	    new_record.get_stats()->get_num_succeeded());
	fail_if (new_record.get_stats()->get_num_failed() != 0,
	    "num_failed mismatch (exp: 0 act: %d)",
	    new_record.get_stats()->get_num_failed());
	fail_if (new_record.get_stats()->get_num_pending() != 1,
	    "num_pending mismatch (exp: 1 act: %d)",
	    new_record.get_stats()->get_num_pending());
	fail_if (new_record.get_stats()->get_num_inprogress() != 0,
	    "num_inprogress mismatch (exp: 0 act: %d)",
	    new_record.get_stats()->get_num_inprogress());

	delete old_record;
}

TEST(test_add_task__multi_job)
{
	struct isi_error *error = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	isi_cloud::recall_task task;
	task.set_lin(12345);
	task.set_snapid(54321);
	task.set_location(TASK_LOC_PENDING);

	const cpool_operation_type *op_type = OT_RECALL;
	djob_id_t job_id = create_job(op_type, &error);
	fail_if (error != NULL, "failed to create %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;

	daemon_job_record *old_record = get_daemon_job_record(job_id, &error);
	fail_if (error != NULL, "failed to get existing record: %#{}",
	    isi_error_fmt(error));

	const void *old_serialization;
	size_t old_serialization_size;
	old_record->get_serialization(old_serialization,
	    old_serialization_size);

	btree_key_t djob_rec_key = old_record->get_key();

	const void *key;
	size_t key_length;
	task.get_key()->get_serialized_key(key, key_length);
	fail_if (key_length != sizeof(btree_key_t),
	    "key_length mismatch (exp: %zu act: %zu)",
	    sizeof(btree_key_t), key_length);
	const btree_key_t *bt_key = static_cast<const btree_key_t *>(key);
	fail_if (bt_key == NULL);

	add_task(&task, NULL, job_id, sbt_ops, objects_to_delete, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (sbt_ops.size() != 2, "sbt_ops.size(): %zu", sbt_ops.size());

	int daemon_jobs_sbt_fd =
	    cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	struct hsbt_bulk_entry rec_op = sbt_ops[0];
	fail_if (rec_op.is_hash_btree,
	    "recall does not use hashed b-trees");
	fail_if (rec_op.bulk_entry.fd != daemon_jobs_sbt_fd,
	    "rec_op.bulk_entry.fd: %d daemon_jobs_sbt_fd: %d",
	    rec_op.bulk_entry.fd, daemon_jobs_sbt_fd);
	fail_if (rec_op.bulk_entry.op_type != SBT_SYS_OP_MOD,
	    "rec_op.bulk_entry.op_type: %d", rec_op.bulk_entry.op_type);
	fail_if((unsigned int)rec_op.bulk_entry.old_entry_size !=
	    old_serialization_size,
	    "old entry size mismatch (exp: %zu act: %d)",
	    old_serialization_size, rec_op.bulk_entry.old_entry_size);
	fail_if (memcmp(rec_op.bulk_entry.old_entry_buf, old_serialization,
	    old_serialization_size) != 0, "old entry buf mismatch");
	fail_if (rec_op.bulk_entry.key.keys[0] != djob_rec_key.keys[0] ||
	    rec_op.bulk_entry.key.keys[1] != djob_rec_key.keys[1],
	    "key mismatch (exp: %lu/%lu act: %lu/%lu)",
	    djob_rec_key.keys[0], djob_rec_key.keys[1],
	    rec_op.bulk_entry.key.keys[0], rec_op.bulk_entry.key.keys[1]);

	daemon_job_record new_record;
	new_record.set_serialization(rec_op.bulk_entry.entry_buf,
	    rec_op.bulk_entry.entry_size, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (new_record.get_daemon_job_id() != job_id,
	    "daemon job ID mismatch (exp: %d act: %d)",
	    job_id, new_record.get_daemon_job_id());
	fail_if (new_record.get_stats()->get_num_members() != 1,
	    "num_members mismatch (exp: 1 act: %d)",
	    new_record.get_stats()->get_num_members());
	fail_if (new_record.get_stats()->get_num_succeeded() != 0,
	    "num_succeeded mismatch (exp: 0 act: %d)",
	    new_record.get_stats()->get_num_succeeded());
	fail_if (new_record.get_stats()->get_num_failed() != 0,
	    "num_failed mismatch (exp: 0 act: %d)",
	    new_record.get_stats()->get_num_failed());
	fail_if (new_record.get_stats()->get_num_pending() != 1,
	    "num_pending mismatch (exp: 1 act: %d)",
	    new_record.get_stats()->get_num_pending());
	fail_if (new_record.get_stats()->get_num_inprogress() != 0,
	    "num_inprogress mismatch (exp: 0 act: %d)",
	    new_record.get_stats()->get_num_inprogress());

	delete old_record;

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

TEST(test_add_task__cancelled_job)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id = start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	cancel_job(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Attempt to add a task to the cancelled job. */
	isi_cloud::archive_task task;
	task.set_lin(23456);
	task.set_snapid(43285);
	task.set_policy_id(11111);

	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;
	add_task(&task, NULL, djob_id, sbt_ops, objects_to_delete, &error);
	fail_if (error == NULL);
	fail_if (!isi_system_error_is_a(error, EINVAL),
	    "expected EINVAL: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

static void
test_add_task__completed_job_helper(task_process_completion_reason reason)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	daemon_job_record *djob_record = NULL;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id = start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Add a task, finish it according to the incoming reason, then try to
	 * add another.
	 */
	isi_cloud::archive_task task_1;
	task_1.set_lin(23456);
	task_1.set_snapid(43285);
	task_1.set_policy_id(11111);

	/*
	 * (Some of these next steps would have been done by either the client
	 * or server task_map.  Some of them are done within an inner scope to
	 * unlock / delete objects.)
	 */
	task_1.add_daemon_job(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	task_1.set_location(TASK_LOC_PENDING);

	{
		std::vector<struct hsbt_bulk_entry> sbt_ops;
		scoped_object_list objects_to_delete;
		add_task(&task_1, NULL, djob_id, sbt_ops, objects_to_delete,
		    &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));

		hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}

	{
		std::vector<struct hsbt_bulk_entry> sbt_ops;
		scoped_object_list objects_to_delete;
		start_task(&task_1, sbt_ops, objects_to_delete, &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));

		hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}

	{
		std::vector<struct hsbt_bulk_entry> sbt_ops;
		scoped_object_list objects_to_delete;
		finish_task(&task_1, reason, sbt_ops, objects_to_delete,
		    &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));

		hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}

	/*
	 * Verify that the daemon job is in the expected state before
	 * proceeding.
	 */
	djob_record = get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_record == NULL);
	if (reason == CPOOL_TASK_SUCCESS)
		fail_if (djob_record->get_state() != DJOB_OJT_COMPLETED,
		    "daemon job state mismatch (exp: %{} act: %{})",
		    djob_op_job_task_state_fmt(DJOB_OJT_COMPLETED),
		    djob_op_job_task_state_fmt(djob_record->get_state()));
	else if (reason == CPOOL_TASK_NON_RETRYABLE_ERROR)
		fail_if (djob_record->get_state() != DJOB_OJT_ERROR,
		    "daemon job state mismatch (exp: %{} act: %{})",
		    djob_op_job_task_state_fmt(DJOB_OJT_ERROR),
		    djob_op_job_task_state_fmt(djob_record->get_state()));
	else
		fail ("this task completion reason (%{}) "
		    "is not supported by this test helper function",
		    task_process_completion_reason_fmt(reason));

	delete djob_record;
	djob_record = NULL;

	/* Add a new task to the completed daemon job. */
	isi_cloud::archive_task task_2;
	task_2.set_lin(34523);
	task_2.set_snapid(898989);
	task_2.set_policy_id(22222);

	/* (This would have been done by the client_task_map.) */
	task_2.add_daemon_job(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	task_2.set_location(TASK_LOC_PENDING);

	{
		std::vector<struct hsbt_bulk_entry> sbt_ops;
		scoped_object_list objects_to_delete;
		add_task(&task_2, NULL, djob_id, sbt_ops, objects_to_delete,
		    &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));

		hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}

	/* Verify the daemon job is back to the running state. */
	djob_record = get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_record == NULL);
	fail_if (djob_record->get_state() != DJOB_OJT_RUNNING,
	    "daemon job state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;
	djob_record = NULL;

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

TEST(test_add_task__completed_job)
{
	test_add_task__completed_job_helper(CPOOL_TASK_SUCCESS);
}

TEST(test_add_task__completed_job_with_errors)
{
	test_add_task__completed_job_helper(CPOOL_TASK_NON_RETRYABLE_ERROR);
}

TEST(test_pause_operation)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_CLOUD_GC;
	daemon_job_configuration *djob_config = NULL;

	isi_cloud::daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_config =
	    isi_cloud::daemon_job_manager::get_daemon_job_config(&error);
	fail_if (error != NULL,
	    "failed to get initial daemon job configuration: %#{}",
	    isi_error_fmt(error));

	/*
	 * Initial configuration should show cloud GC is in the running state.
	 */
	fail_if (djob_config->get_operation_state(op_type) != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(
	    djob_config->get_operation_state(op_type)));

	delete djob_config;

	/* Pause the operation and verify. */
	isi_cloud::daemon_job_manager::pause_operation(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_config =
	    isi_cloud::daemon_job_manager::get_daemon_job_config(&error);
	fail_if (error != NULL,
	    "failed to get daemon job configuration: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_config->get_operation_state(op_type) != DJOB_OJT_PAUSED,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_PAUSED),
	    djob_op_job_task_state_fmt(
	    djob_config->get_operation_state(op_type)));

	delete djob_config;

	/* Pause the operation again and verify no error is returned. */
	isi_cloud::daemon_job_manager::pause_operation(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	isi_error_free(error);
}

TEST(test_resume_operation)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_CACHE_INVALIDATION;
	daemon_job_configuration *djob_config = NULL;

	isi_cloud::daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_config =
	    isi_cloud::daemon_job_manager::get_daemon_job_config(&error);
	fail_if (error != NULL,
	    "failed to get initial daemon job configuration: %#{}",
	    isi_error_fmt(error));

	/*
	 * Initial configuration should show cache invalidation is in the
	 * running state.
	 */
	fail_if (djob_config->get_operation_state(op_type) != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(
	    djob_config->get_operation_state(op_type)));

	delete djob_config;

	/* Resume the operation and verify no error is returned. */
	isi_cloud::daemon_job_manager::resume_operation(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	/* Pause the operation and verify. */
	isi_cloud::daemon_job_manager::pause_operation(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_config =
	    isi_cloud::daemon_job_manager::get_daemon_job_config(&error);
	fail_if (error != NULL,
	    "failed to get daemon job configuration: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_config->get_operation_state(op_type) != DJOB_OJT_PAUSED,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_PAUSED),
	    djob_op_job_task_state_fmt(
	    djob_config->get_operation_state(op_type)));

	delete djob_config;

	/* Resume the operation and verify. */
	isi_cloud::daemon_job_manager::resume_operation(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_config =
	    isi_cloud::daemon_job_manager::get_daemon_job_config(&error);
	fail_if (error != NULL,
	    "failed to get daemon job configuration: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_config->get_operation_state(op_type) != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(
	    djob_config->get_operation_state(op_type)));

	delete djob_config;
}

TEST(test_pause_single_job)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_WRITEBACK;
	daemon_job_record *djob_record = NULL;

	isi_cloud::daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id =
	    isi_cloud::daemon_job_manager::start_job_no_job_engine(op_type,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * The single job should initially be in the running state.
	 */
	fail_if (djob_record->get_state() != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/* Pause the job and verify. */
	isi_cloud::daemon_job_manager::pause_job(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_record->get_state() != DJOB_OJT_PAUSED,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_PAUSED),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/* Pause the job again and verify an error is returned. */
	isi_cloud::daemon_job_manager::pause_job(djob_id, &error);
	fail_if (error == NULL, "expected EALREADY");
	fail_if (!isi_system_error_is_a(error, EALREADY),
	    "expected EALREADY: %#{}", isi_error_fmt(error));

	isi_error_free(error);
}

TEST(test_pause_multi_job)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	daemon_job_record *djob_record = NULL;

	isi_cloud::daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id_1 =
	    isi_cloud::daemon_job_manager::start_job_no_job_engine(op_type,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	djob_id_t djob_id_2 =
	    isi_cloud::daemon_job_manager::start_job_no_job_engine(op_type,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id_1,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * A new multi-job should initially be in the running state.
	 */
	fail_if (djob_record->get_state() != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/* Pause the job and verify. */
	isi_cloud::daemon_job_manager::pause_job(djob_id_1, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id_1,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_record->get_state() != DJOB_OJT_PAUSED,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_PAUSED),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/* Verify the other job is still running. */
	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id_2,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_record->get_state() != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/* Pause the first job again and verify an error is returned. */
	isi_cloud::daemon_job_manager::pause_job(djob_id_1, &error);
	fail_if (error == NULL, "expected EALREADY");
	fail_if (!isi_system_error_is_a(error, EALREADY),
	    "expected EALREADY: %#{}", isi_error_fmt(error));

	isi_error_free(error);

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

TEST(test_resume_single_job)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_LOCAL_GC;
	daemon_job_record *djob_record = NULL;

	isi_cloud::daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id =
	    isi_cloud::daemon_job_manager::start_job_no_job_engine(op_type,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * The single job should initially be in the running state.
	 */
	fail_if (djob_record->get_state() != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/* Resume the job and verify an error is returned. */
	isi_cloud::daemon_job_manager::resume_job(djob_id, &error);
	fail_if (error == NULL, "expected EALREADY");
	fail_if (!isi_system_error_is_a(error, EALREADY),
	    "expected EALREADY: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	/* Pause the job and verify. */
	isi_cloud::daemon_job_manager::pause_job(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_record->get_state() != DJOB_OJT_PAUSED,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_PAUSED),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/* Resume the job and verify. */
	isi_cloud::daemon_job_manager::resume_job(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_record->get_state() != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;
}

TEST(test_resume_multi_job)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_RECALL;
	daemon_job_record *djob_record = NULL;

	isi_cloud::daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id_1 =
	    isi_cloud::daemon_job_manager::start_job_no_job_engine(op_type,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	djob_id_t djob_id_2 =
	    isi_cloud::daemon_job_manager::start_job_no_job_engine(op_type,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id_1,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * A new multi-job should initially be in the running state.
	 */
	fail_if (djob_record->get_state() != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/* Resume the job and verify an error is returned. */
	isi_cloud::daemon_job_manager::resume_job(djob_id_1, &error);
	fail_if (error == NULL, "expected EALREADY");
	fail_if (!isi_system_error_is_a(error, EALREADY),
	    "expected EALREADY: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	/* Pause the job and verify. */
	isi_cloud::daemon_job_manager::pause_job(djob_id_1, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id_1,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_record->get_state() != DJOB_OJT_PAUSED,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_PAUSED),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/* Verify the other job is still running. */
	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id_2,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_record->get_state() != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/* Resume the first job and verify. */
	isi_cloud::daemon_job_manager::resume_job(djob_id_1, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id_1,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_record->get_state() != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

TEST(test_cancel_single_job)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_CLOUD_GC;
	daemon_job_configuration *djob_config = NULL;
	daemon_job_record *djob_record = NULL;

	isi_cloud::daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id =
	    isi_cloud::daemon_job_manager::start_job_no_job_engine(op_type,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Get the number of active jobs - this number shouldn't change as the
	 * cancel function should fail.
	 */
	djob_config =
	    isi_cloud::daemon_job_manager::get_daemon_job_config(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	int num_active_jobs = djob_config->get_num_active_jobs();

	delete djob_config;

	/*
	 * The single job should initially be in the running state.
	 */
	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_record->get_state() != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/*
	 * Attempt to cancel the single job and verify that an error is
	 * returned.
	 */
	isi_cloud::daemon_job_manager::cancel_job(djob_id, &error);
	fail_if (error == NULL, "expected EINVAL");
	fail_if (!isi_system_error_is_a(error, EINVAL),
	    "expected EINVAL: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	/* Verify the number of active jobs hasn't changed. */
	djob_config =
	    isi_cloud::daemon_job_manager::get_daemon_job_config(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_config->get_num_active_jobs() != num_active_jobs,
	    "num active jobs mismatch (exp: %d act: %d)",
	    num_active_jobs, djob_config->get_num_active_jobs());

	delete djob_config;

	/* Verify the job is still in the running state. */
	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_record->get_state() != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;
}

TEST(test_cancel_multi_job)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_RECALL;
	daemon_job_record *djob_record = NULL;
	daemon_job_configuration *djob_config = NULL;
	int num_active_jobs;

	isi_cloud::daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id_1 =
	    isi_cloud::daemon_job_manager::start_job_no_job_engine(op_type,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	djob_id_t djob_id_2 =
	    isi_cloud::daemon_job_manager::start_job_no_job_engine(op_type,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Get the number of active jobs - this number should decrease by 1
	 * after we cancel one of the just-created jobs.
	 */
	djob_config =
	    isi_cloud::daemon_job_manager::get_daemon_job_config(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	num_active_jobs = djob_config->get_num_active_jobs();

	delete djob_config;

	/*
	 * A new multi-job should initially be in the running state.
	 */
	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id_1,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_record->get_state() != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/* Cancel one of the jobs and verify. */
	isi_cloud::daemon_job_manager::cancel_job(djob_id_1, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id_1,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_record->get_state() != DJOB_OJT_CANCELLED,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_CANCELLED),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/* Verify the number of active jobs has decreased by 1. */
	djob_config =
	    isi_cloud::daemon_job_manager::get_daemon_job_config(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_config->get_num_active_jobs() != num_active_jobs - 1,
	    "num active jobs mismatch (exp: %d act: %d)",
	    num_active_jobs - 1, djob_config->get_num_active_jobs());

	delete djob_config;

	/* Verify the other job is still running. */
	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id_2,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_record->get_state() != DJOB_OJT_RUNNING,
	    "operation state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(djob_record->get_state()));

	delete djob_record;

	/*
	 * Attempt to cancel the first job again and verify that an error is
	 * returned.
	 */
	isi_cloud::daemon_job_manager::cancel_job(djob_id_1, &error);
	fail_if (error == NULL, "expected EALREADY");
	fail_if (!isi_system_error_is_a(error, EALREADY),
	    "expected EALREADY: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	/*
	 * Attempt to resume the first job and verify that an error is
	 * returned.
	 */
	isi_cloud::daemon_job_manager::resume_job(djob_id_1, &error);
	fail_if (error == NULL, "expected EINVAL");
	fail_if (!isi_system_error_is_a(error, EINVAL),
	    "expected EINVAL: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	/*
	 * Attempt to pause the first job and verify that an error is
	 * returned.
	 */
	isi_cloud::daemon_job_manager::pause_job(djob_id_1, &error);
	fail_if (error == NULL, "expected EINVAL");
	fail_if (!isi_system_error_is_a(error, EINVAL),
	    "expected EINVAL: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

TEST(test_prc_cancelled_job)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_RECALL;

	isi_cloud::daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id =
	    isi_cloud::daemon_job_manager::start_job_no_job_engine(op_type,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Cancel the job. */
	isi_cloud::daemon_job_manager::cancel_job(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Attempt the pause the cancelled job. */
	isi_cloud::daemon_job_manager::pause_job(djob_id, &error);
	fail_if (error == NULL, "expected EINVAL");
	fail_if (!isi_system_error_is_a(error, EINVAL),
	    "expected EINVAL: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	/* Attempt the resume the cancelled job. */
	isi_cloud::daemon_job_manager::resume_job(djob_id, &error);
	fail_if (error == NULL, "expected EINVAL");
	fail_if (!isi_system_error_is_a(error, EINVAL),
	    "expected EINVAL: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	/* Attempt the cancel the cancelled job. */
	isi_cloud::daemon_job_manager::cancel_job(djob_id, &error);
	fail_if (error == NULL, "expected EALREADY");
	fail_if (!isi_system_error_is_a(error, EALREADY),
	    "expected EALREADY: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * When cancelling a daemon job with an associated running JobEngine job, the
 * JE job is also cancelled.  However, a race occurs between the JE job
 * finishing and updating the daemon job record with this information, so a JE
 * job cancellation request can be made against an already-finished JE job,
 * which results in the cancellation request failing with EINVAL.
 *
 * To avoid misleading log messages (while not ignoring them completely), if a
 * JE job cancellation fails with EINVAL, we re-check the state of the JE job
 * according to the daemon job record.  Tests that use this helper function may
 * inject failures into this execution path.  Also, a number of ENOTCONN
 * failures are simulated to test that error's retry logic.
 */
static void
cancel_job__je_job_finish__race__helper(int num_ENOTCONN_errors,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	djob_id_t djob_id;
	daemon_job_record *djob_record = NULL;
	int djob_record_sbt_fd = -1;
	btree_key_t djob_record_key;
	const void *old_ser, *new_ser;
	size_t old_ser_size, new_ser_size;
	cpool_test_fail_point
	    je_EINVAL_fp("cpool__djob_cancel__force_je_EINVAL"),
	    je_ENOTCONN_fp("cpool__djob_cancel__force_je_ENOTCONN");
	struct fmt FMT_INIT_CLEAN(fmt);

	isi_cloud::daemon_job_manager::at_startup(&error);
	ON_ISI_ERROR_GOTO(out, error);

	/*
	 * In order to not involve the actual JobEngine, start a job without
	 * starting a JE job, then add a bogus JE job ID.  We'll use failpoints
	 * to work around the fact that there isn't actually a JE job.
	 */
	djob_id =
	    isi_cloud::daemon_job_manager::start_job_no_job_engine(op_type,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	djob_record->save_serialization();
	djob_record->set_job_engine_job_id(123456);

	djob_record_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(
	    false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	djob_record_key = djob_record->get_key();

	djob_record->get_saved_serialization(old_ser, old_ser_size);
	djob_record->get_serialization(new_ser, new_ser_size);

	if (ifs_sbt_cond_mod_entry(djob_record_sbt_fd, &djob_record_key,
	    new_ser, new_ser_size, NULL, BT_CM_BUF, old_ser, old_ser_size,
	    NULL) != 0) {
		error = isi_system_error_new(errno,
		    "failed to modify daemon job record "
		    "after adding (fake) JE job ID");
		goto out;
	}

	/*
	 * With the mocked-up daemon job record in place, cancel the daemon
	 * job.  Use one failpoint to inject some number of ENOTCONN failures,
	 * and a second to both prevent an actual cancel request to JE and to
	 * ensure an EINVAL return.
	 */
	fmt_print(&fmt, "return(%d)", num_ENOTCONN_errors);
	je_ENOTCONN_fp.set(fmt_string(&fmt));

	je_EINVAL_fp.set("return(1)");

	isi_cloud::daemon_job_manager::cancel_job(djob_id, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	je_ENOTCONN_fp.set("off");
	je_EINVAL_fp.set("off");

	if (djob_record != NULL) {
		delete djob_record;
		djob_record = NULL;
	}

	isi_error_handle(error, error_out);
}

/*
 * (See cancel_job__je_job_finish__race__helper comment for context.)
 *
 * Simulate a failure to re-lock the daemon job record when re-reading after JE
 * job cancellation returned EINVAL.  Should have two errors: one for the lock
 * and one for the cancel.  For this test, don't inject any ENOTCONN errors.
 */
TEST(cancel_job__djob_relock_failure)
{
	struct isi_error *error = NULL;
	int num_ENOTCONN_failures = 0;
	cpool_test_fail_point
	    djob_relock_fp("cpool__djob_cancel__djob_lock_failure"),
	    je_job_finished_fp("cpool__djob_cleanup__force_je_job_finished");

	// set je job not finished
	je_job_finished_fp.set("return(0)");

	djob_relock_fp.set("return(1)");
	cancel_job__je_job_finish__race__helper(num_ENOTCONN_failures, &error);
	fail_if (error == NULL);

	int num_errs = isi_multi_error_get_suberr_count(error);
	fail_unless(num_errs == 2, "exp: 2 act: %d", num_errs);

	struct isi_error *first = isi_multi_error_get_suberr_idx(error, 0);
	struct isi_error *second = isi_multi_error_get_suberr_idx(error, 1);

	if (isi_system_error_is_a(first, EINVAL)) {
		fail_unless(isi_system_error_is_a(second, ENOTSOCK),
		    "unexpected error: %#{}", isi_error_fmt(second));
	} else if (isi_system_error_is_a(first, ENOTSOCK)) {
		fail_unless(isi_system_error_is_a(second, EINVAL),
		    "unexpected error: %#{}", isi_error_fmt(second));
	} else {
		fail("unexpected error: %#{}", isi_error_fmt(first));
	}

	isi_error_free(error);
	error = NULL;

	djob_relock_fp.set("off");
	je_job_finished_fp.set("off");

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * (See cancel_job__je_job_finish__race__helper comment for context.)
 *
 * Simulate a failure to re-read the daemon job record after JE job
 * cancellation returned EINVAL.  Should have two errors: one for the read and
 * one for the cancel.  Inject a "temporary" number (i.e. not enough to exceed
 * the number of attempts) of ENOTCONN errors.
 */
TEST(cancel_job__djob_reread_failure)
{
	struct isi_error *error = NULL;
	int num_ENOTCONN_failures =
	    isi_cloud::daemon_job_manager::MAX_JE_JOB_CANCEL_ATTEMPTS - 3;
	cpool_test_fail_point
	    djob_reread_fp("cpool__djob_cancel__djob_read_failure"),
	    je_job_finished_fp("cpool__djob_cleanup__force_je_job_finished");

	fail_if (num_ENOTCONN_failures <= 0,
	    "number of injected ENOTCONN failures (%d) "
	    "must be greater than 0 -- fix this test",
	    num_ENOTCONN_failures);

	// set je job not finished
	je_job_finished_fp.set("return(0)");

	djob_reread_fp.set("return(1)");
	cancel_job__je_job_finish__race__helper(num_ENOTCONN_failures, &error);
	fail_if (error == NULL);

	int num_errs = isi_multi_error_get_suberr_count(error);
	fail_unless(num_errs == 2, "exp: 2 act: %d", num_errs);

	struct isi_error *first = isi_multi_error_get_suberr_idx(error, 0);
	struct isi_error *second = isi_multi_error_get_suberr_idx(error, 1);

	if (isi_system_error_is_a(first, EINVAL)) {
		fail_unless(isi_system_error_is_a(second, ENOTSOCK),
		    "unexpected error: %#{}", isi_error_fmt(second));
	} else if (isi_system_error_is_a(first, ENOTSOCK)) {
		fail_unless(isi_system_error_is_a(second, EINVAL),
		    "unexpected error: %#{}", isi_error_fmt(second));
	} else {
		fail("unexpected error: %#{}", isi_error_fmt(first));
	}

	isi_error_free(error);
	error = NULL;

	djob_reread_fp.set("off");
	je_job_finished_fp.set("off");

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * (See cancel_job__je_job_finish__race__helper comment for context.)
 *
 * Simulate a failure to unlock the daemon job record after JE job
 * cancellation returned EINVAL.  Should have two errors: one for the unlock
 * and one for the cancel.  Inject a "temporary" number (i.e. not enough to
 * exceed the number of attempts) of ENOTCONN errors.
 */
TEST(cancel_job__djob_unlock_failure)
{
	struct isi_error *error = NULL;
	int num_ENOTCONN_failures =
	    isi_cloud::daemon_job_manager::MAX_JE_JOB_CANCEL_ATTEMPTS - 3;
	cpool_test_fail_point
	    djob_unlock_fp("cpool__djob_cancel__djob_unlock_failure"),
	    je_job_finished_fp("cpool__djob_cleanup__force_je_job_finished");

	fail_if (num_ENOTCONN_failures <= 0,
	    "number of injected ENOTCONN failures (%d) "
	    "must be greater than 0 -- fix this test",
	    num_ENOTCONN_failures);

	// set je job not finished
	je_job_finished_fp.set("return(0)");

	djob_unlock_fp.set("return(1)");
	cancel_job__je_job_finish__race__helper(num_ENOTCONN_failures, &error);
	fail_if (error == NULL);

	int num_errs = isi_multi_error_get_suberr_count(error);
	fail_unless(num_errs == 2, "exp: 2 act: %d", num_errs);

	struct isi_error *first = isi_multi_error_get_suberr_idx(error, 0);
	struct isi_error *second = isi_multi_error_get_suberr_idx(error, 1);

	if (isi_system_error_is_a(first, EINVAL)) {
		fail_unless(isi_system_error_is_a(second, ENOTSOCK),
		    "unexpected error: %#{}", isi_error_fmt(second));
	} else if (isi_system_error_is_a(first, ENOTSOCK)) {
		fail_unless(isi_system_error_is_a(second, EINVAL),
		    "unexpected error: %#{}", isi_error_fmt(second));
	} else {
		fail("unexpected error: %#{}", isi_error_fmt(first));
	}

	isi_error_free(error);
	error = NULL;

	djob_unlock_fp.set("off");
	je_job_finished_fp.set("off");

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * (See cancel_job__je_job_finish__race__helper comment for context.)
 *
 * Simulate that the JE job associated with the daemon job is still marked as
 * running for each of the times it is reread.  In this case, let the EINVAL
 * from cancelling the JE job continue up to the caller.  Inject a "temporary"
 * number (i.e. not enough to exceed the number of attempts) of ENOTCONN
 * errors.
 */
TEST(cancel_job__je_job_still_running)
{
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(fmt);
	int num_ENOTCONN_failures =
	    isi_cloud::daemon_job_manager::MAX_JE_JOB_CANCEL_ATTEMPTS - 1;
	cpool_test_fail_point
	    je_job_fp("cpool__djob_cancel__je_job_running"),
	    je_job_finished_fp("cpool__djob_cleanup__force_je_job_finished");

	fail_if (num_ENOTCONN_failures <= 0,
	    "number of injected ENOTCONN failures (%d) "
	    "must be greater than 0 -- fix this test",
	    num_ENOTCONN_failures);

	// set je job not finished
	je_job_finished_fp.set("return(0)");

	fmt_print(&fmt, "return(%d)",
	    isi_cloud::daemon_job_manager::MAX_JE_JOB_REREAD_ATTEMPTS + 1);
	je_job_fp.set(fmt_string(&fmt));
	cancel_job__je_job_finish__race__helper(num_ENOTCONN_failures, &error);
	fail_if (error == NULL || !isi_system_error_is_a(error, EINVAL),
	    "expected EINVAL: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	je_job_fp.set("off");
	je_job_finished_fp.set("off");

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * (See cancel_job__je_job_finish__race__helper comment for context.)
 *
 * Simulate that the JE job associated with the daemon job is marked as
 * finished after the JE job cancellation attempt returns EINVAL, but only
 * after a few rereads of the daemon job record.  In this case, eat the EINVAL.
 */
TEST(cancel_job__je_job_finished)
{
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(fmt);
	int num_ENOTCONN_failures =
	    isi_cloud::daemon_job_manager::MAX_JE_JOB_CANCEL_ATTEMPTS - 1;
	cpool_test_fail_point
	    je_job_fp("cpool__djob_cancel__je_job_running");

	fail_if (num_ENOTCONN_failures <= 0,
	    "number of injected ENOTCONN failures (%d) "
	    "must be greater than 0 -- fix this test",
	    num_ENOTCONN_failures);

	fmt_print(&fmt, "return(%d)",
	    isi_cloud::daemon_job_manager::MAX_JE_JOB_REREAD_ATTEMPTS - 1);
	je_job_fp.set(fmt_string(&fmt));
	cancel_job__je_job_finish__race__helper(num_ENOTCONN_failures, &error);
	fail_if (error != NULL,
	    "unexpected error: %#{}", isi_error_fmt(error));

	je_job_fp.set("return(0)");

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * (See cancel_job__je_job_finish__race__helper comment for context.)
 *
 * Use a too-large number of ENOTCONN errors to simulate a non-responsive
 * JobEngine.  In this case, the caller should receive ENOTCONN.
 */
TEST(cancel_job__je_job_ENOTCONN)
{
	struct isi_error *error = NULL;
	const int max_cancel_attempts =
	    isi_cloud::daemon_job_manager::MAX_JE_JOB_CANCEL_ATTEMPTS;
	int num_ENOTCONN_failures = max_cancel_attempts + 1;
	cpool_test_fail_point
	    je_job_finished_fp("cpool__djob_cleanup__force_je_job_finished");

	// set je job not finished
	je_job_finished_fp.set("return(0)");

	fail_if (num_ENOTCONN_failures < max_cancel_attempts,
	    "number of injected ENOTCONN failures (%d) "
	    "must be greater than max attempts (%d) -- fix this test",
	    num_ENOTCONN_failures, max_cancel_attempts);

	/*
	 * We don't need to turn on a failpoint here to simulate a failure; the
	 * helper function will use one to simulate the ENOTCONN failures,
	 * which is what we're expecting to be returned.  (In other words, any
	 * failpoint we could set here shouldn't be reached by the code since
	 * the too-many ENOTCONN failures should prevent it.)
	 */
	cancel_job__je_job_finish__race__helper(num_ENOTCONN_failures, &error);
	fail_if (error == NULL || !isi_system_error_is_a(error, ENOTCONN),
	    "unexpected ENOTCONN: %#{}", isi_error_fmt(error));

	je_job_finished_fp.set("off");
	isi_error_free(error);
	error = NULL;

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

struct test_finish_task_single_job_operation_helper_arg {
	/* INPUT */

	/* the new job engine state */
	enum jobstat_state new_je_state_;
	/* does this job have other unfinished tasks? */
	bool add_one_inprogress_task_;
	/* why was this task finished? */
	task_process_completion_reason reason_;

	/* OUTPUT */

	/* the expected state of the daemon job */
	djob_op_job_task_state new_djob_state_;
};

static void
test_finish_task_single_job_operation_helper(
    struct test_finish_task_single_job_operation_helper_arg &arg)
{
	struct isi_error *error = NULL;

	cloud_gc_task cgc_task;
	cgc_task.set_cloud_object_id(isi_cloud_object_id());

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Simulate the daemon job management part of adding a task to a job.
	 */
	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(cgc_task.get_op_type(),
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* (These steps would have been performed by the client_task_map.) */
	cgc_task.set_location(TASK_LOC_PENDING);
	cgc_task.add_daemon_job(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	{
		/*
		 * Use an inner scope here so that sbt_ops and, more
		 * importantly, objects_to_delete fall out of scope.
		 */
		std::vector<struct hsbt_bulk_entry> sbt_ops;
		scoped_object_list objects_to_delete;
		daemon_job_manager::add_task(&cgc_task, NULL, djob_id, sbt_ops,
		    objects_to_delete, &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));

		hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}

	/*
	 * Now simulate the task being started.  (This next step would have
	 * been peformed by the server_task_map.)
	 */
	cgc_task.set_location(TASK_LOC_INPROGRESS);

	{
		std::vector<struct hsbt_bulk_entry> sbt_ops;
		scoped_object_list objects_to_delete;
		daemon_job_manager::start_task(&cgc_task, sbt_ops,
		    objects_to_delete, &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));

		hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}

	/*
	 * Before finishing the task, update the job record to reflect the new
	 * job engine job state and to add any other (bogus) tasks to this job.
	 */
	daemon_job_record *djob_record =
	    daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_record == NULL);

	djob_record->save_serialization();

	djob_record->set_job_engine_job_id(5);
	djob_record->set_job_engine_state(arg.new_je_state_);

	if (arg.add_one_inprogress_task_) {
		djob_record->increment_num_members();
		djob_record->increment_num_inprogress();
	}

	const void *old_rec_serialization, *new_rec_serialization;
	size_t old_rec_serialization_size, new_rec_serialization_size;

	djob_record->get_saved_serialization(old_rec_serialization,
	    old_rec_serialization_size);
	djob_record->get_serialization(new_rec_serialization,
	    new_rec_serialization_size);

	btree_key_t djob_record_key = djob_record->get_key();

	int djob_record_sbt_fd = cpool_fd_store::getInstance().\
	    get_djobs_sbt_fd(false, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (ifs_sbt_cond_mod_entry(djob_record_sbt_fd, &djob_record_key,
	    new_rec_serialization, new_rec_serialization_size, NULL, BT_CM_BUF,
	    old_rec_serialization, old_rec_serialization_size, NULL) != 0,
	    "failed to update daemon job record to simulate "
	    "a completed job engine job: %s", strerror(errno));

	delete djob_record;

	/* Now finish the task. */
	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;
	daemon_job_manager::finish_task(&cgc_task, arg.reason_, sbt_ops,
	    objects_to_delete, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Since this test uses an operation that supports only a single job,
	 * we don't update the daemon job configuration to decrement the number
	 * of active jobs even though this job might now be complete.
	 * Therefore, there should be 1 SBT operation that updates the daemon
	 * job record.
	 */
	fail_if (sbt_ops.size() != 1, "expected 1 SBT operations, not %zu",
	    sbt_ops.size());

	/*
	 * The first (and only) operation is the update of the daemon job
	 * record, so use it to populate a daemon job record object in order to
	 * verify the operation.
	 */
	daemon_job_record new_record;
	new_record.set_serialization(sbt_ops[0].bulk_entry.entry_buf,
	    sbt_ops[0].bulk_entry.entry_size, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (new_record.get_state() != arg.new_djob_state_,
	    "job state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(arg.new_djob_state_),
	    djob_op_job_task_state_fmt(new_record.get_state()));

	/* Create the expected set of stats and verify. */
	daemon_job_stats expected_stats;

	/* First, the task we finished... */
	expected_stats.increment_num_members();
	switch (arg.reason_) {
	case CPOOL_TASK_SUCCESS:
		expected_stats.increment_num_succeeded();
		break;
	case CPOOL_TASK_PAUSED:
	case CPOOL_TASK_RETRYABLE_ERROR:
		expected_stats.increment_num_pending();
		break;
	case CPOOL_TASK_CANCELLED:
		expected_stats.increment_num_cancelled();
		break;
	case CPOOL_TASK_NON_RETRYABLE_ERROR:
		expected_stats.increment_num_failed();
		break;
	case CPOOL_TASK_STOPPED:
		break;
	}

	/* ... and next, the in-progress task that may have been added. */
	if (arg.add_one_inprogress_task_) {
		expected_stats.increment_num_members();
		expected_stats.increment_num_inprogress();
	}

	fail_if (*(new_record.get_stats()) != expected_stats,
	    "stats mismatch (exp: %{} act: %{})",
	    cpool_djob_stats_fmt(&expected_stats),
	    cpool_djob_stats_fmt(new_record.get_stats()));
}

/*
 * Operation: single job
 * Job Engine State: final
 * Other Incomplete Tasks: no
 * Task Finish Reason: success
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_01)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_FINISHED;
	arg.add_one_inprogress_task_ = false;
	arg.reason_ = CPOOL_TASK_SUCCESS;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: final
 * Other Incomplete Tasks: no
 * Task Finish Reason: retryable error
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_02)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_FINISHED;
	arg.add_one_inprogress_task_ = false;
	arg.reason_ = CPOOL_TASK_RETRYABLE_ERROR;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: final
 * Other Incomplete Tasks: no
 * Task Finish Reason: nonretryable error
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_03)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_FINISHED;
	arg.add_one_inprogress_task_ = false;
	arg.reason_ = CPOOL_TASK_NON_RETRYABLE_ERROR;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: final
 * Other Incomplete Tasks: no
 * Task Finish Reason: task paused
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_04)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_FINISHED;
	arg.add_one_inprogress_task_ = false;
	arg.reason_ = CPOOL_TASK_PAUSED;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: final
 * Other Incomplete Tasks: no
 * Task Finish Reason: task cancelled
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_05)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_FINISHED;
	arg.add_one_inprogress_task_ = false;
	arg.reason_ = CPOOL_TASK_CANCELLED;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: final
 * Other Incomplete Tasks: yes
 * Task Finish Reason: success
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_06)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_FINISHED;
	arg.add_one_inprogress_task_ = true;
	arg.reason_ = CPOOL_TASK_SUCCESS;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: final
 * Other Incomplete Tasks: yes
 * Task Finish Reason: retryable error
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_07)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_FINISHED;
	arg.add_one_inprogress_task_ = true;
	arg.reason_ = CPOOL_TASK_RETRYABLE_ERROR;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: final
 * Other Incomplete Tasks: yes
 * Task Finish Reason: nonretryable error
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_08)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_FINISHED;
	arg.add_one_inprogress_task_ = true;
	arg.reason_ = CPOOL_TASK_NON_RETRYABLE_ERROR;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: final
 * Other Incomplete Tasks: yes
 * Task Finish Reason: task paused
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_09)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_FINISHED;
	arg.add_one_inprogress_task_ = true;
	arg.reason_ = CPOOL_TASK_PAUSED;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: final
 * Other Incomplete Tasks: yes
 * Task Finish Reason: task cancelled
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_10)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_FINISHED;
	arg.add_one_inprogress_task_ = true;
	arg.reason_ = CPOOL_TASK_CANCELLED;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: not final
 * Other Incomplete Tasks: no
 * Task Finish Reason: success
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_11)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_RUNNING;
	arg.add_one_inprogress_task_ = false;
	arg.reason_ = CPOOL_TASK_SUCCESS;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: not final
 * Other Incomplete Tasks: no
 * Task Finish Reason: retryable error
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_12)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_RUNNING;
	arg.add_one_inprogress_task_ = false;
	arg.reason_ = CPOOL_TASK_RETRYABLE_ERROR;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: not final
 * Other Incomplete Tasks: no
 * Task Finish Reason: nonretryable error
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_13)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_RUNNING;
	arg.add_one_inprogress_task_ = false;
	arg.reason_ = CPOOL_TASK_NON_RETRYABLE_ERROR;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: not final
 * Other Incomplete Tasks: no
 * Task Finish Reason: task paused
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_14)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_RUNNING;
	arg.add_one_inprogress_task_ = false;
	arg.reason_ = CPOOL_TASK_PAUSED;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: not final
 * Other Incomplete Tasks: no
 * Task Finish Reason: task cancelled
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_15)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_RUNNING;
	arg.add_one_inprogress_task_ = false;
	arg.reason_ = CPOOL_TASK_CANCELLED;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: not final
 * Other Incomplete Tasks: yes
 * Task Finish Reason: success
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_16)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_RUNNING;
	arg.add_one_inprogress_task_ = true;
	arg.reason_ = CPOOL_TASK_SUCCESS;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: not final
 * Other Incomplete Tasks: yes
 * Task Finish Reason: retryable error
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_17)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_RUNNING;
	arg.add_one_inprogress_task_ = true;
	arg.reason_ = CPOOL_TASK_RETRYABLE_ERROR;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: not final
 * Other Incomplete Tasks: yes
 * Task Finish Reason: nonretryable error
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_18)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_RUNNING;
	arg.add_one_inprogress_task_ = true;
	arg.reason_ = CPOOL_TASK_NON_RETRYABLE_ERROR;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: not final
 * Other Incomplete Tasks: yes
 * Task Finish Reason: task paused
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_19)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_RUNNING;
	arg.add_one_inprogress_task_ = true;
	arg.reason_ = CPOOL_TASK_PAUSED;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

/*
 * Operation: single job
 * Job Engine State: not final
 * Other Incomplete Tasks: yes
 * Task Finish Reason: task cancelled
 * Expected Resultant Job State: running
 */
TEST(test_finish_task_single_20)
{
	struct test_finish_task_single_job_operation_helper_arg arg;
	arg.new_je_state_ = STATE_RUNNING;
	arg.add_one_inprogress_task_ = true;
	arg.reason_ = CPOOL_TASK_CANCELLED;
	arg.new_djob_state_ = DJOB_OJT_RUNNING;

	test_finish_task_single_job_operation_helper(arg);
}

#define FINISH_TASK_TEST_NUM_JOBS 2
struct test_finish_task_multi_job_operation_helper_arg {
	/* INPUT */

	/* the new job engine state */
	enum jobstat_state new_je_state_[FINISH_TASK_TEST_NUM_JOBS];
	/* does this job have other unfinished tasks? */
	bool add_one_inprogress_task_[FINISH_TASK_TEST_NUM_JOBS];
	/* why was this task finished? */
	task_process_completion_reason reason_;

	/* OUTPUT */

	/* the expected state of the daemon job */
	djob_op_job_task_state new_djob_state_[FINISH_TASK_TEST_NUM_JOBS];
};

static void
test_finish_task_multi_job_operation_helper(
    struct test_finish_task_multi_job_operation_helper_arg &arg)
{
	struct isi_error *error = NULL;

	archive_task ar_task;
	ar_task.set_lin(23456);
	ar_task.set_snapid(65432);

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Simulate the daemon job management part of adding a task to a job.
	 */
	djob_id_t djob_ids[FINISH_TASK_TEST_NUM_JOBS];
	for (int i = 0; i < FINISH_TASK_TEST_NUM_JOBS; ++i) {
		djob_ids[i] = daemon_job_manager::start_job_no_job_engine(
		    ar_task.get_op_type(), &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));
	}

	/* (These steps would have been performed by the client_task_map.) */
	ar_task.set_location(TASK_LOC_PENDING);

	for (int i = 0; i < FINISH_TASK_TEST_NUM_JOBS; ++i) {
		ar_task.add_daemon_job(djob_ids[i], &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));

		std::vector<struct hsbt_bulk_entry> sbt_ops;
		scoped_object_list objects_to_delete;
		daemon_job_manager::add_task(&ar_task, NULL, djob_ids[i],
		    sbt_ops, objects_to_delete, &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));

		hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));
	}

	/*
	 * Now simulate the task being started.  (This next step would have
	 * been peformed by the server_task_map.)
	 */
	ar_task.set_location(TASK_LOC_INPROGRESS);

	{
		std::vector<struct hsbt_bulk_entry> sbt_ops;
		scoped_object_list objects_to_delete;
		daemon_job_manager::start_task(&ar_task, sbt_ops,
		    objects_to_delete, &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));

		hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}

	/*
	 * Before finishing the task, update the job record to reflect the new
	 * job engine job state and to add any other (bogus) tasks to this job.
	 */
	for (int i = 0; i < FINISH_TASK_TEST_NUM_JOBS; ++i) {
		daemon_job_record *djob_record =
		    daemon_job_manager::get_daemon_job_record(djob_ids[i],
		    &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));
		fail_if (djob_record == NULL);

		djob_record->save_serialization();

		djob_record->set_job_engine_job_id(100 + i);
		djob_record->set_job_engine_state(arg.new_je_state_[i]);

		if (arg.add_one_inprogress_task_[i]) {
			djob_record->increment_num_members();
			djob_record->increment_num_inprogress();
		}

		const void *old_rec_serialization, *new_rec_serialization;
		size_t old_rec_serialization_size, new_rec_serialization_size;

		djob_record->get_saved_serialization(old_rec_serialization,
		    old_rec_serialization_size);
		djob_record->get_serialization(new_rec_serialization,
		    new_rec_serialization_size);

		btree_key_t djob_record_key = djob_record->get_key();

		int djob_record_sbt_fd = cpool_fd_store::getInstance().\
		    get_djobs_sbt_fd(false, &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));

		fail_if (ifs_sbt_cond_mod_entry(djob_record_sbt_fd,
		    &djob_record_key, new_rec_serialization,
		    new_rec_serialization_size, NULL, BT_CM_BUF,
		    old_rec_serialization, old_rec_serialization_size,
		    NULL) != 0,
		    "failed to update daemon job record to simulate "
		    "a completed job engine job (i = %d): %s",
		    i, strerror(errno));

		delete djob_record;
	}

	/* Now finish the task. */
	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;
	daemon_job_manager::finish_task(&ar_task, arg.reason_, sbt_ops,
	    objects_to_delete, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Since this test uses an operation that supports multiple jobs, the
	 * daemon job configuration should be updated if a job is now complete.
	 */
	int num_complete_jobs = 0;
	for (int i = 0; i < FINISH_TASK_TEST_NUM_JOBS; ++i) {
		if (arg.new_djob_state_[i] == DJOB_OJT_COMPLETED ||
		    arg.new_djob_state_[i] == DJOB_OJT_ERROR)
			++num_complete_jobs;
	}

	size_t expected_num_ops = FINISH_TASK_TEST_NUM_JOBS +
	    (num_complete_jobs == 0 ? 0 : 1);
	fail_if (sbt_ops.size() != expected_num_ops,
	    "expected %zu SBT ops, not %zu)",
	    expected_num_ops, sbt_ops.size());

	/*
	 * If the daemon job configuration is changed, it will be the last SBT
	 * operation in the vector.  Verify the changes to the daemon job
	 * records.
	 */
	for (int i = 0; i < FINISH_TASK_TEST_NUM_JOBS; ++i) {
		daemon_job_record new_record;
		new_record.set_serialization(sbt_ops[i].bulk_entry.entry_buf,
		    sbt_ops[i].bulk_entry.entry_size, &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));

		fail_if (new_record.get_state() != arg.new_djob_state_[i],
		    "job state mismatch (exp: %{} act: %{})",
		    djob_op_job_task_state_fmt(arg.new_djob_state_[i]),
		    djob_op_job_task_state_fmt(new_record.get_state()));

		/* Create the expected set of stats and verify. */
		daemon_job_stats expected_stats;

		/* First, the task we finished... */
		expected_stats.increment_num_members();
		switch (arg.reason_) {
		case CPOOL_TASK_SUCCESS:
			expected_stats.increment_num_succeeded();
			break;
		case CPOOL_TASK_PAUSED:
		case CPOOL_TASK_RETRYABLE_ERROR:
			expected_stats.increment_num_pending();
			break;
		case CPOOL_TASK_CANCELLED:
			expected_stats.increment_num_cancelled();
			break;
		case CPOOL_TASK_NON_RETRYABLE_ERROR:
			expected_stats.increment_num_failed();
			break;
		case CPOOL_TASK_STOPPED:
			break;
		}

		/* ... and next, the in-progress task that may have been added. */
		if (arg.add_one_inprogress_task_[i]) {
			expected_stats.increment_num_members();
			expected_stats.increment_num_inprogress();
		}

		fail_if (*(new_record.get_stats()) != expected_stats,
		    "stats mismatch (exp: %{} act: %{})",
		    cpool_djob_stats_fmt(&expected_stats),
		    cpool_djob_stats_fmt(new_record.get_stats()));
	}

	/*
	 * Verify the daemon job configuration change, if it exists. The
	 * expected number of active jobs is the number of jobs we started with
	 * (FINISH_TASK_TEST_NUM_JOBS) minus the number of jobs that should be
	 * completed by finishing the task plus 4 (to account for the
	 * single-job operation jobs).
	 */
	if (num_complete_jobs > 0) {
		struct hsbt_bulk_entry *last_entry =
		    sbt_ops.data() + (sbt_ops.size() - 1);

		daemon_job_configuration djob_config;
		djob_config.set_serialization(last_entry->bulk_entry.entry_buf,
		    last_entry->bulk_entry.entry_size, &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));

		int num_expected_active_jobs =
		    FINISH_TASK_TEST_NUM_JOBS - num_complete_jobs + 4;
		fail_if (djob_config.get_num_active_jobs() !=
		    num_expected_active_jobs,
		    "num active jobs mismatch (exp: %d act: %d)",
		    num_expected_active_jobs,
		    djob_config.get_num_active_jobs());
	}

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: success
 * Expected Resultant Job State: complete, complete
 */
TEST(test_finish_task_multi_01)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_SUCCESS;
	arg.new_djob_state_[0] = DJOB_OJT_COMPLETED;
	arg.new_djob_state_[1] = DJOB_OJT_COMPLETED;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: retryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_02)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: nonretryable error
 * Expected Resultant Job State: error, error
 */
TEST(test_finish_task_multi_03)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_NON_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_ERROR;
	arg.new_djob_state_[1] = DJOB_OJT_ERROR;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: paused
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_04)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_PAUSED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: cancelled
 * Expected Resultant Job State: complete, complete
 */
TEST(test_finish_task_multi_05)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_CANCELLED;
	arg.new_djob_state_[0] = DJOB_OJT_COMPLETED;
	arg.new_djob_state_[1] = DJOB_OJT_COMPLETED;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: success
 * Expected Resultant Job State: running, complete
 */
TEST(test_finish_task_multi_06)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_SUCCESS;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_COMPLETED;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: retryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_07)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: nonretryable error
 * Expected Resultant Job State: running, error
 */
TEST(test_finish_task_multi_08)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_NON_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_ERROR;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: paused
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_09)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_PAUSED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: cancelled
 * Expected Resultant Job State: running, complete
 */
TEST(test_finish_task_multi_10)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_CANCELLED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_COMPLETED;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: success
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_11)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_SUCCESS;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: retryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_12)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: nonretryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_13)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_NON_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: paused
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_14)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_PAUSED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: cancelled
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_15)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_FINISHED;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_CANCELLED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: success
 * Expected Resultant Job State: complete, running
 */
TEST(test_finish_task_multi_16)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_SUCCESS;
	arg.new_djob_state_[0] = DJOB_OJT_COMPLETED;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: retryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_17)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: nonretryable error
 * Expected Resultant Job State: error, running
 */
TEST(test_finish_task_multi_18)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_NON_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_ERROR;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: paused
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_19)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_PAUSED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: cancelled
 * Expected Resultant Job State: complete, running
 */
TEST(test_finish_task_multi_20)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_CANCELLED;
	arg.new_djob_state_[0] = DJOB_OJT_COMPLETED;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: success
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_21)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_SUCCESS;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: retryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_22)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: nonretryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_23)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_NON_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: paused
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_24)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_PAUSED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: cancelled
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_25)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_CANCELLED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: success
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_26)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_SUCCESS;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: retryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_27)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: nonretryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_28)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_NON_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: paused
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_29)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_PAUSED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: final, not final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: cancelled
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_30)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_FINISHED;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_CANCELLED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: success
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_31)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_SUCCESS;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: retryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_32)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: nonretryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_33)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_NON_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: paused
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_34)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_PAUSED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: false, false
 * Task Finish Reason: cancelled
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_35)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = false;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_CANCELLED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: success
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_36)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_SUCCESS;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: retryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_37)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: nonretryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_38)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_NON_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: paused
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_39)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_PAUSED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: true, false
 * Task Finish Reason: cancelled
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_40)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = false;
	arg.reason_ = CPOOL_TASK_CANCELLED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: success
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_41)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_SUCCESS;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: retryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_42)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: nonretryable error
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_43)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_NON_RETRYABLE_ERROR;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: paused
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_44)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_PAUSED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

/*
 * Operation: multi job
 * Job Engine State: not final, not final
 * Other Incomplete Tasks: true, true
 * Task Finish Reason: cancelled
 * Expected Resultant Job State: running, running
 */
TEST(test_finish_task_multi_45)
{
	struct test_finish_task_multi_job_operation_helper_arg arg;
	arg.new_je_state_[0] = STATE_RUNNING;
	arg.new_je_state_[1] = STATE_RUNNING;
	arg.add_one_inprogress_task_[0] = true;
	arg.add_one_inprogress_task_[1] = true;
	arg.reason_ = CPOOL_TASK_CANCELLED;
	arg.new_djob_state_[0] = DJOB_OJT_RUNNING;
	arg.new_djob_state_[1] = DJOB_OJT_RUNNING;

	test_finish_task_multi_job_operation_helper(arg);
}

static void
daemon_job_existence_checker(djob_id_t djob_id, bool should_find)
{
	ASSERT(djob_id != DJOB_RID_INVALID);

	struct isi_error *error = NULL;

	/* Try to retrieve the daemon job record. */
	daemon_job_record *djob_rec =
	    daemon_job_manager::get_daemon_job_record(djob_id, &error);
	if (should_find) {
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
		fail_if (djob_rec == NULL);

		delete djob_rec;
	}
	else {
		fail_if (error == NULL, "expected ENOENT");
		fail_if (!isi_system_error_is_a(error, ENOENT),
		    "expected ENOENT: %#{}", isi_error_fmt(error));
		fail_if (djob_rec != NULL);

		isi_error_free(error);
		error = NULL;
	}

	/* Try to open the daemon job member SBT. */
	int djob_member_sbt_fd = open_member_sbt(djob_id, false, &error);
	if (should_find) {
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
		fail_if (djob_member_sbt_fd < 0, "djob_member_sbt_fd: %d",
		    djob_member_sbt_fd);

		close_sbt(djob_member_sbt_fd);
	}
	else {
		fail_if (error == NULL, "expected ENOENT");
		fail_if (!isi_system_error_is_a(error, ENOENT),
		    "expected ENOENT: %#{}", isi_error_fmt(error));

		isi_error_free(error);
	}
}

/*
 * Remove the job of an operation supporting only a single job.
 */
TEST(test_remove_job__single_job)
{
	struct isi_error *error = NULL;

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	const cpool_operation_type *single_job_op_type = OT_CLOUD_GC;
	fail_if (single_job_op_type->is_multi_job(),
	    "invalid operation (%{}), test requires a single job operation",
	    task_type_fmt(single_job_op_type->get_type()));

	djob_id_t single_job_id = single_job_op_type->get_single_job_id();
	daemon_job_record *djob_record =
	    daemon_job_manager::get_daemon_job_record(single_job_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_record == NULL);

	daemon_job_manager::remove_job(djob_record, &error);
	fail_if (error == NULL, "expected EINVAL");
	fail_if (!isi_system_error_is_a(error, EINVAL),
	    "expected EINVAL: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	delete djob_record;

	daemon_job_existence_checker(single_job_id, true);
}

/*
 * Remove a not-yet-finished job of an operation supporting multiple jobs.
 */
TEST(test_remove_job__unfinished_multi_job)
{
	struct isi_error *error = NULL;

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	const cpool_operation_type *op_type = OT_RECALL;
	fail_if (!op_type->is_multi_job(),
	    "invalid operation (%{}), test requires a multi job operation",
	    task_type_fmt(op_type->get_type()));

	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	daemon_job_record *djob_record =
	    get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_record == NULL);

	djob_record->set_job_engine_job_id(134);
	djob_record->set_job_engine_state(STATE_RUNNING);

	daemon_job_manager::remove_job(djob_record, &error);
	fail_if (error == NULL, "expected EINVAL");
	fail_if (!isi_system_error_is_a(error, EINVAL),
	    "expected EINVAL: %#{}", isi_error_fmt(error));

	isi_error_free(error);

	daemon_job_existence_checker(djob_id, true);

	delete djob_record;

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * Remove a finished job of an operation supporting multiple jobs.
 */
TEST(test_remove_job__finished_multi_job)
{
	struct isi_error *error = NULL;

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	const cpool_operation_type *op_type = OT_ARCHIVE;
	fail_if (!op_type->is_multi_job(),
	    "invalid operation (%{}), test requires a multi job operation",
	    task_type_fmt(op_type->get_type()));

	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Modify the daemon job record to mark it complete. */
	daemon_job_record *djob_record =
	    daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_record == NULL);

	djob_record->save_serialization();

	djob_record->set_state(DJOB_OJT_COMPLETED);

	const void *old_rec_serialization, *new_rec_serialization;
	size_t old_rec_serialization_size, new_rec_serialization_size;

	djob_record->get_saved_serialization(old_rec_serialization,
	    old_rec_serialization_size);
	djob_record->get_serialization(new_rec_serialization,
	    new_rec_serialization_size);

	btree_key_t djob_record_key = djob_record->get_key();

	int djob_record_sbt_fd = cpool_fd_store::getInstance().\
	    get_djobs_sbt_fd(false, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (ifs_sbt_cond_mod_entry(djob_record_sbt_fd,
	    &djob_record_key, new_rec_serialization,
	    new_rec_serialization_size, NULL, BT_CM_BUF,
	    old_rec_serialization, old_rec_serialization_size,
	    NULL) != 0,
	    "failed to update daemon job record to simulate "
	    "a completed job: %s", strerror(errno));

	delete djob_record;
	djob_record =
	    daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_record == NULL);

	daemon_job_manager::remove_job(djob_record, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	delete djob_record;

	daemon_job_existence_checker(djob_id, false);

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

TEST(test_cleanup_jobs)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_RECALL;
	cpool_test_fail_point ufp("cpool__djob_cleanup__force_je_job_finished");

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	int djob_rec_sbt_fd = cpool_fd_store::getInstance().\
	    get_djobs_sbt_fd(false, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Create some daemon jobs.  Some of these will have their state and
	 * completion time altered:
	 *
	 * [0] : not finished (JobEngine job)
	 * [1] : not finished (unresolved tasks)
	 * [2] : finished, too soon
	 * [3] : finished, old enough, JE job STATE_FINISHED
	 * [4] : finished, too soon
	 * [5] : cancelled, old enough, JE job STATE_NULL
	 * [6] : finished, old enough
	 * [7] : finished, too soon
	 * [8] : finished, old enough
	 * [9] : paused
	 * [10]: cancelled, too soon
	 * [11]: completed with error, old enough
	 * [12]: completed with error, too soon
	 */
	djob_id_t djob_ids[13];
	for (int i = 0; i < sizeof(djob_ids) / sizeof(djob_ids[0]); ++i) {
		djob_ids[i] =
		    daemon_job_manager::start_job_no_job_engine(op_type,
		    &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));

		daemon_job_record *djob_rec_non_ut =
		    get_daemon_job_record(djob_ids[i], &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));

		/*
		 * We can safely cast from parent to child class since the only
		 * difference between the two classes is method visibility.
		 */
		daemon_job_record_ut *djob_rec =
		    static_cast<daemon_job_record_ut *>(djob_rec_non_ut);
		fail_if (djob_rec == NULL);

		djob_rec->save_serialization();

		switch (i) {
		case 0:
			djob_rec->set_job_engine_job_id(12);
			djob_rec->set_job_engine_state(STATE_RUNNING);
			break;
		case 1:
			djob_rec->set_num_members(12);
			djob_rec->set_num_succeeded(11);
			break;
		case 2:
		case 4:
		case 7:
			djob_rec->set_num_members(134);
			djob_rec->set_num_succeeded(134);
			djob_rec->set_completion_time(1000);
			break;
		case 3:
			djob_rec->set_job_engine_job_id(103);
			djob_rec->set_job_engine_state(STATE_FINISHED);
			// No break here intended
		case 6:
		case 8:
			djob_rec->set_num_members(43);
			djob_rec->set_num_succeeded(43);
			djob_rec->set_completion_time(100);
			break;
		case 5:
			djob_rec->set_job_engine_job_id(105);
			djob_rec->set_state(DJOB_OJT_CANCELLED);
			djob_rec->set_completion_time(100);
			break;
		case 9:
			djob_rec->set_state(DJOB_OJT_PAUSED);
			break;
		case 10:
			djob_rec->set_state(DJOB_OJT_CANCELLED);
			djob_rec->set_completion_time(1000);
			break;
		case 11:
			djob_rec->set_state(DJOB_OJT_ERROR);
			djob_rec->set_completion_time(100);
			break;
		case 12:
			djob_rec->set_state(DJOB_OJT_ERROR);
			djob_rec->set_completion_time(1000);
			break;
		default:
			fail ("unhandled index (%d) -- fix this test", i);
		}

		btree_key_t djob_rec_key = djob_rec->get_key();

		const void *old_rec_serialization, *new_rec_serialization;
		size_t old_rec_serialization_size, new_rec_serialization_size;
		djob_rec->get_saved_serialization(old_rec_serialization,
		    old_rec_serialization_size);
		djob_rec->get_serialization(new_rec_serialization,
		    new_rec_serialization_size);

		fail_if (ifs_sbt_cond_mod_entry(djob_rec_sbt_fd, &djob_rec_key,
		    new_rec_serialization, new_rec_serialization_size, NULL,
		    BT_CM_BUF, old_rec_serialization,
		    old_rec_serialization_size, NULL) != 0,
		    "unexpected error (i = %d): %s", i, strerror(errno));

		delete djob_rec;
	}

	/*
	 * Cleanup jobs older than 500 over successive calls.  This should
	 * remove [3], [6], [8], and [11], and possibly [5], depending on
	 * how we mock the state of its associated JE job.
	 *
	 * For the first call, use a failpoint to simulate that the JE
	 * job associated with [5] is still running (and therefore [5]
	 * isn't currently eligible for cleanup).
	 * In order to make sure we consider (but skip over) [5],
	 * let cleanup_old_jobs remove 2 jobs, which should be [3] and [6].
	 */
	ufp.set("return(0)");

	int num_jobs_removed =
	    daemon_job_manager::cleanup_old_jobs(2, 500, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (num_jobs_removed != 2,
	    "num_jobs_removed mismatch (exp: 2 act: %d)", num_jobs_removed);

	/* Verify each daemon job exists/does not exist as expected. */
	for (int i = 0; i < sizeof(djob_ids) / sizeof(djob_ids[0]); ++i) {
		switch (i) {
		case 0:
		case 1:
		case 2:
		case 4:
		case 5:
		case 7:
		case 8:
		case 9:
		case 10:
		case 11:
		case 12:
			daemon_job_existence_checker(djob_ids[i], true);
			break;
		case 3:
		case 6:
			daemon_job_existence_checker(djob_ids[i], false);
			break;
		default:
			fail ("unhandled index (%d) -- fix this test", i);
		}
	}

	/*
	 * For this final call, cleanup all remaining jobs older than 500.
	 * In order to remove [5], we'll need to modify our failpoint to
	 * simulate that the JE job associated with [5] is considered finished.
	 * Set the upper limit of jobs to remove at 10, but since we've already
	 * removed [3] and [6] above, we only expect 3 jobs to be removed: [5],
	 * [8], and [11].
	 */
	ufp.set("return(1)");

	num_jobs_removed =
	    daemon_job_manager::cleanup_old_jobs(10, 500, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (num_jobs_removed != 3,
	    "num_jobs_removed mismatch (exp: 3 act: %d)", num_jobs_removed);

	/* Verify each daemon job exists/does not exist as expected. */
	for (int i = 0; i < sizeof(djob_ids) / sizeof(djob_ids[0]); ++i) {
		switch (i) {
		case 0:
		case 1:
		case 2:
		case 4:
		case 7:
		case 9:
		case 10:
		case 12:
			daemon_job_existence_checker(djob_ids[i], true);
			break;
		case 3:
		case 5:
		case 6:
		case 8:
		case 11:
			daemon_job_existence_checker(djob_ids[i], false);
			break;
		default:
			fail ("unhandled index (%d) -- fix this test", i);
		}
	}

	ufp.set("off");

	/*
	 * Cleanup any remaining jobs older than 500 - although there shouldn't
	 * be any.
	 */
	num_jobs_removed =
	    daemon_job_manager::cleanup_old_jobs(10, 500, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (num_jobs_removed != 0,
	    "num_jobs_removed mismatch (exp: 0 act: %d)", num_jobs_removed);

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

TEST(test_query_job_for_members)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;

	at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Create a job and populate it with tasks.  Start and possibly finish
	 * some of these tasks to represent different task states.
	 */

	djob_id_t djob_id = start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	const size_t num_tasks = 7;
	archive_task created_tasks[num_tasks];

	char filename_buf[512];
	std::list<std::string> nlist; /* list of filenames */

	uint32_t policy_id = 11111;

	for (size_t i = 0; i < num_tasks; ++i) {
		int fd;
		struct stat stat_buf;
		/* Add mix of long and short names */
		snprintf(filename_buf, sizeof filename_buf - 1, short_file_fmt, i);
		nlist.insert(nlist.end(), filename_buf);

		fd = open(filename_buf, O_RDONLY | O_CREAT, 0400);
		fail_if (fd == -1, "failed to open %s", filename_buf);

		fail_if (stat(filename_buf, &stat_buf) != 0,
		    "failed to stat %s: %s", filename_buf, strerror(errno));

		fail_if (close(fd) != 0, "failed to close %s: %s",
		    filename_buf, strerror(errno));

		created_tasks[i].set_lin(stat_buf.st_ino);
		created_tasks[i].set_snapid(HEAD_SNAPID);
		created_tasks[i].set_policy_id(policy_id);

		created_tasks[i].set_priority(0);
		created_tasks[i].set_location(TASK_LOC_PENDING);
		created_tasks[i].add_daemon_job(djob_id, &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));

		std::vector<struct hsbt_bulk_entry> sbt_ops;
		scoped_object_list objects_to_delete;
		daemon_job_manager::add_task(&(created_tasks[i]), NULL,
		    djob_id, sbt_ops, objects_to_delete, &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));

		hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));
	}

	/*
	 * All tasks have been created and added to the job.  Start all but the
	 * first.
	 */
	for (size_t i = 1; i < num_tasks; ++i) {
		created_tasks[i].set_location(TASK_LOC_INPROGRESS);

		std::vector<struct hsbt_bulk_entry> sbt_ops;
		scoped_object_list objects_to_delete;
		daemon_job_manager::start_task(&(created_tasks[i]), sbt_ops,
		    objects_to_delete, &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));

		hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));
	}

	/*
	 * Finish all tasks but the first two with one of the following
	 * reasons.
	 */
	task_process_completion_reason reasons[] = {
		CPOOL_TASK_SUCCESS,
		CPOOL_TASK_PAUSED,
		CPOOL_TASK_CANCELLED,
		CPOOL_TASK_NON_RETRYABLE_ERROR,
		CPOOL_TASK_RETRYABLE_ERROR
	};

	for (size_t i = 2; i < num_tasks; ++i) {
		std::vector<struct hsbt_bulk_entry> sbt_ops;
		scoped_object_list objects_to_delete;
		daemon_job_manager::finish_task(&(created_tasks[i]),
		    reasons[i - 2], sbt_ops, objects_to_delete, &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));

		hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));
	}

	/* Setup is complete, so query this job for its members. */
	const cpool_operation_type *op_type_returned = NULL;
	op_type_returned =
	    daemon_job_manager::get_operation_type_for_job_id(djob_id, &error);
	fail_if (error != NULL, "unexpected error getting operation type: %#{}",
	    isi_error_fmt(error));
	fail_if (op_type_returned == NULL);
	fail_if (op_type_returned != op_type,
	    "op_type mismatch (exp: %{} act: %{})",
	    task_type_fmt(op_type->get_type()),
	    task_type_fmt(op_type_returned->get_type()));

	resume_key start_key;

	/* get first half of entries with an empty start_key*/
	std::vector<daemon_job_member *> list1;
	daemon_job_manager::query_job_for_members(djob_id, 0, start_key,
	    num_tasks/2 , list1, op_type_returned, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (start_key.empty());

	/* get the rest entries with start_key returned from prior call */
	std::vector<daemon_job_member *> list2;
	daemon_job_manager::query_job_for_members(djob_id, list1.size(),
	    start_key, 100 , list2, op_type_returned, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (!start_key.empty(),
	    "Start key must be empty after reading all entries");

	/* merge list1 and list2 into members */
	std::vector<daemon_job_member *> members;
	members.insert(members.end(), list1.begin(), list1.end());
	members.insert(members.end(), list2.begin(), list2.end());

	fail_if (members.size() != num_tasks,
	    "num_tasks mismatch (exp: %zu act: %zu)",
	    num_tasks, members.size());

	std::vector<daemon_job_member *>::iterator iter = members.begin();
	/*
	* There is no guarntee of getting filenames in the same sequence
	* of insertion so doing linear search in the list.
	*/
	for (size_t i = 0; iter != members.end(); ++iter, ++i) {
		const char *filename = (*iter)->get_name();
		bool match = false;

		std::list<std::string>::iterator nit;
		for (nit = nlist.begin(); nit != nlist.end(); nit++) {
			if ((*nit) == filename) {
				nlist.remove(*nit);
				match = true;
				break;
			}
		}
		fail_if(!match, "Filename not found %s", filename);

		delete *iter;
	}
	fail_if(!nlist.empty(), "Query missed %d entries", nlist.size());

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}
