#include <unistd.h>

#include <check.h>

#include <isi_util/check_helper.h>
#include <isi_migrate/migr/isirep.h>
#include "siq_target.h"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);
SUITE_DEFINE_FOR_FILE(siq_target, .suite_setup = suite_setup,
    .test_setup = test_setup, .test_teardown = test_teardown);

#define TARG_ID_5_0		"src_cluster.policy_name./ifs/data/targ_test"
#define TARG_ID			"1234567890abcdef1234567890abcdef"
#define TARG_DIR		"/ifs/data/targ_test"
#define TARG_NAME		"target-cluster"
#define TARG_SRC_CLU_NAME	"source-cluster"
#define TARG_SRC_CLU_ID		"ABCDEFG"
#define TARG_LAST_SRC_COORD_IP	"127.0.0.1"
#define TARG_STATUS		SIQ_JS_SUCCESS
#define TARG_READ_ONLY		true
#define TARG_MONITOR_NODE_ID	1	
#define TARG_MONITOR_PID	1234	
#define TARG_CANCEL_STATE	CANCEL_DONE	

#define TARG_COMPOSITE_JOB_VER_LOCAL		0xa5a5c3c3
#define TARG_COMPOSITE_JOB_VER_COMMON		0x3c3c5a5a
#define TARG_PREV_COMPOSITE_JOB_VER_LOCAL	0x96966969
#define TARG_PREV_COMPOSITE_JOB_VER_COMMON	0xf0f00f0f


TEST_FIXTURE(suite_setup)
{
	struct isi_error *error = NULL;

	/* Fool dmalloc checks by pre-initializing */
	xmlInitParser();

	if (siq_target_record_exists(TARG_ID)) {
		siq_target_delete_record(TARG_ID, true, false, &error);
		fail_if_error(error);
	}

	if (siq_target_record_exists(TARG_ID_5_0)) {
		siq_target_delete_record(TARG_ID_5_0, true, false, &error);
		fail_if_error(error);
	}
}

TEST_FIXTURE(test_setup)
{
	struct isi_error *error = NULL;

	if (siq_target_record_exists(TARG_ID)) {
		siq_target_delete_record(TARG_ID, true, false, &error);
		fail_if_error(error);
	}

	if (siq_target_record_exists(TARG_ID_5_0)) {
		siq_target_delete_record(TARG_ID_5_0, true, false, &error);
		fail_if_error(error);
	}
}

TEST_FIXTURE(test_teardown)
{
}

static void
create_record(char *pid)
{
	struct target_record *record = NULL;
	struct isi_error *error = NULL;

	record = calloc(1, sizeof(struct target_record));
	record->id = strdup(pid);
	record->target_dir = strdup(TARG_DIR);
	record->policy_name = strdup(TARG_NAME);
	record->src_cluster_name = strdup(TARG_SRC_CLU_NAME);
	record->src_cluster_id = strdup(TARG_SRC_CLU_ID);
	record->last_src_coord_ip = strdup(TARG_LAST_SRC_COORD_IP);
	record->job_status = TARG_STATUS;
	record->read_only = TARG_READ_ONLY;
	record->monitor_node_id = TARG_MONITOR_NODE_ID;
	record->monitor_pid = TARG_MONITOR_PID;
	record->cancel_state = TARG_CANCEL_STATE;

	record->composite_job_ver.local = TARG_COMPOSITE_JOB_VER_LOCAL;
	record->composite_job_ver.common = TARG_COMPOSITE_JOB_VER_COMMON;
	record->prev_composite_job_ver.local =
	    TARG_PREV_COMPOSITE_JOB_VER_LOCAL;
	record->prev_composite_job_ver.common =
	    TARG_PREV_COMPOSITE_JOB_VER_COMMON;

	siq_target_record_write(record, &error);
	fail_if_error(error);
	fail_unless(siq_target_record_exists(pid));

	siq_target_record_free(record);
	record = NULL;
}

static void
validate_id(char *pid)
{
	fail_unless(pid != NULL);
	fail_unless(strcmp(pid, TARG_ID) == 0 ||
	    strcmp(pid, TARG_ID_5_0) == 0);
}

static void
validate_record(struct target_record *record, char *pid)
{
	validate_id(pid);
	fail_unless(strcmp(record->id, pid) == 0);
	fail_unless(strcmp(record->target_dir, TARG_DIR) == 0);
	fail_unless(strcmp(record->policy_name, TARG_NAME) == 0);
	fail_unless(strcmp(record->src_cluster_name, TARG_SRC_CLU_NAME) == 0);
	fail_unless(strcmp(record->src_cluster_id, TARG_SRC_CLU_ID) == 0);
	fail_unless(strcmp(record->last_src_coord_ip, TARG_LAST_SRC_COORD_IP) == 0);
	fail_unless(record->job_status == TARG_STATUS);
	fail_unless(record->read_only == TARG_READ_ONLY);
	fail_unless(record->monitor_node_id == TARG_MONITOR_NODE_ID);
	fail_unless(record->monitor_pid == TARG_MONITOR_PID);
	fail_unless(record->cancel_state == TARG_CANCEL_STATE);

	fail_unless(record->composite_job_ver.local ==
	    TARG_COMPOSITE_JOB_VER_LOCAL);
	fail_unless(record->composite_job_ver.common ==
	    TARG_COMPOSITE_JOB_VER_COMMON);
	fail_unless(record->prev_composite_job_ver.local ==
	    TARG_PREV_COMPOSITE_JOB_VER_LOCAL);
	fail_unless(record->prev_composite_job_ver.common ==
	    TARG_PREV_COMPOSITE_JOB_VER_COMMON);

}

TEST(simple_storage_check, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct target_record *record;

	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(!record);
	fail_unless_error(error);
	isi_error_free(error);
	error = NULL;

	create_record(TARG_ID);

	/*
	 * Now make sure the entry got written.
	 */
	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(record != NULL);
	fail_if_error(error);

	validate_record(record, TARG_ID);

	siq_target_record_free(record);
}

TEST(simple_storage_check_5_0, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct target_record *record;

	record = siq_target_record_read(TARG_ID_5_0, &error);
	fail_unless(!record);
	fail_unless_error(error);
	isi_error_free(error);
	error = NULL;

	create_record(TARG_ID_5_0);

	/*
	 * Now make sure the entry got written.
	 */
	record = siq_target_record_read(TARG_ID_5_0, &error);
	fail_unless(record != NULL);
	fail_if_error(error);

	validate_record(record, TARG_ID_5_0);

	siq_target_record_free(record);
}

TEST(find_record_by_id, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct target_records *records = NULL;
	struct target_record *record = NULL;

	record = siq_target_record_read(TARG_ID_5_0, &error);
	fail_unless(!record);
	fail_unless_error(error);
	isi_error_free(error);
	error = NULL;

	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(!record);
	fail_unless_error(error);
	isi_error_free(error);
	error = NULL;

	create_record(TARG_ID);

	create_record(TARG_ID_5_0);

	/*
	 * Now make sure the entries got written.
	 */
	siq_target_records_read(&records, &error);
	fail_unless(records != NULL);
	fail_if_error(error);

	record = siq_target_record_find_by_id(records, TARG_ID);
	fail_unless(record != NULL);
	validate_record(record, TARG_ID);

	record = siq_target_record_find_by_id(records, TARG_ID_5_0);
	fail_unless(record != NULL);
	validate_record(record, TARG_ID_5_0);

	siq_target_records_free(records);
}

TEST(siq_target_record_find_by_id_null_check, .attrs="overnight") {
    struct target_record *trec = NULL;
    struct target_records *records = NULL;
    struct isi_error *error = NULL;

    create_record(TARG_ID_5_0);
    siq_target_records_read(&records, &error);

    fail_if_error(error);
    fail_unless(records != NULL);

    free(records->record->id);
    records->record->id = NULL;

    trec = siq_target_record_find_by_id(records, TARG_ID);
    fail_unless(trec == NULL);

    siq_target_records_free(records);
}

TEST(set_target_states_check, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct target_record *record;

	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(!record);
	fail_unless_error(error);
	isi_error_free(error);
	error = NULL;

	create_record(TARG_ID);

	/*
	 * Now make sure the entry got written.
	 */
	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(record != NULL);
	fail_if_error(error);

	validate_record(record, TARG_ID);

	siq_target_record_free(record);
	record = NULL;

	siq_target_states_set(TARG_ID, CHANGE_JOB_STATE | CHANGE_CANCEL_STATE,
	    SIQ_JS_SUCCESS, CANCEL_DONE, false, false, &error);
	fail_if_error(error);

	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(record != NULL);
	fail_if_error(error);

	fail_unless(record->job_status == SIQ_JS_SUCCESS);
	fail_unless(record->cancel_state == CANCEL_DONE);

	fail_unless(record->composite_job_ver.local == 0);
	fail_unless(record->composite_job_ver.common == 0);
	fail_unless(record->prev_composite_job_ver.local ==
	    TARG_COMPOSITE_JOB_VER_LOCAL);
	fail_unless(record->prev_composite_job_ver.common ==
	    TARG_COMPOSITE_JOB_VER_COMMON);

	siq_target_record_free(record);
}

TEST(cancel_state_get_check, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct target_record *record;
	enum siq_target_cancel_state cs = CANCEL_INVALID;

	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(!record);
	fail_unless_error(error);
	isi_error_free(error);
	error = NULL;

	create_record(TARG_ID);

	/*
	 * Now make sure the entry got written.
	 */
	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(record != NULL);
	fail_if_error(error);

	validate_record(record, TARG_ID);

	siq_target_record_free(record);
	record = NULL;

	cs = siq_target_cancel_state_get(TARG_ID, false, &error);
	fail_if_error(error);
	fail_unless(cs == TARG_CANCEL_STATE);
}

TEST(cancel_request_error_check_1, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct target_record *record;
	enum siq_target_cancel_resp cr = -1;

	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(!record);
	fail_unless_error(error);
	isi_error_free(error);
	error = NULL;

	create_record(TARG_ID);

	/*
	 * Now make sure the entry got written.
	 */
	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(record != NULL);
	fail_if_error(error);

	validate_record(record, TARG_ID);

	/* check the cant_cancel error path  */
	record->cant_cancel = true;
	siq_target_record_write(record, &error);
	fail_if_error(error);

	cr = siq_target_cancel_request(TARG_ID, &error);
	fail_unless_error(error);
	fail_unless(cr == CLIENT_CANCEL_ERROR);

	isi_error_free(error);
	siq_target_record_free(record);
}

TEST(cancel_request_error_check_2, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct target_record *record;
	enum siq_target_cancel_resp cr = -1;

	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(!record);
	fail_unless_error(error);
	isi_error_free(error);
	error = NULL;

	create_record(TARG_ID);

	/*
	 * Now make sure the entry got written.
	 */
	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(record != NULL);
	fail_if_error(error);

	validate_record(record, TARG_ID);

	/* check the not currently running error path */
	record->cant_cancel = false;
	record->job_status = SIQ_JS_SUCCESS;
	siq_target_record_write(record, &error);
	fail_if_error(error);

	cr = siq_target_cancel_request(TARG_ID, &error);
	fail_unless_error(error);
	fail_unless(cr == CLIENT_CANCEL_NOT_RUNNING);

	isi_error_free(error);
	siq_target_record_free(record);
}

TEST(cancel_request_error_check_3, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct target_record *record;
	enum siq_target_cancel_resp cr = -1;

	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(!record);
	fail_unless_error(error);
	isi_error_free(error);
	error = NULL;

	create_record(TARG_ID);

	/*
	 * Now make sure the entry got written.
	 */
	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(record != NULL);
	fail_if_error(error);

	validate_record(record, TARG_ID);

	/* check the already pending request error path */
	record->cant_cancel = false;
	record->job_status = SIQ_JS_RUNNING;
	record->cancel_state = CANCEL_REQUESTED;
	siq_target_record_write(record, &error);
	fail_if_error(error);

	cr = siq_target_cancel_request(TARG_ID, &error);
	fail_unless_error(error);
	fail_unless(cr == CLIENT_CANCEL_ALREADY_IN_PROGRESS);

	isi_error_free(error);
	siq_target_record_free(record);
}

TEST(cancel_request_error_check_4, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct target_record *record;
	enum siq_target_cancel_resp cr = -1;

	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(!record);
	fail_unless_error(error);
	isi_error_free(error);
	error = NULL;

	create_record(TARG_ID);

	/*
	 * Now make sure the entry got written.
	 */
	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(record != NULL);
	fail_if_error(error);

	validate_record(record, TARG_ID);

	/* check the unexpected cancel state error path */
	record->cant_cancel = false;
	record->job_status = SIQ_JS_RUNNING;
	record->cancel_state = CANCEL_INVALID;
	siq_target_record_write(record, &error);
	fail_if_error(error);

	cr = siq_target_cancel_request(TARG_ID, &error);
	fail_unless_error(error);
	fail_unless(cr == CLIENT_CANCEL_ERROR);

	isi_error_free(error);
	siq_target_record_free(record);
}

TEST(cancel_request_success_check, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct target_record *record;
	enum siq_target_cancel_resp cr = -1;

	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(!record);
	fail_unless_error(error);
	isi_error_free(error);
	error = NULL;

	create_record(TARG_ID);

	/*
	 * Now make sure the entry got written.
	 */
	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(record != NULL);
	fail_if_error(error);

	validate_record(record, TARG_ID);

	/* check the successful path */
	record->cant_cancel = false;
	record->job_status = SIQ_JS_RUNNING;
	record->cancel_state = CANCEL_WAITING_FOR_REQUEST;
	siq_target_record_write(record, &error);
	fail_if_error(error);
	siq_target_record_free(record);
	record = NULL;

	cr = siq_target_cancel_request(TARG_ID, &error);
	fail_if_error(error);
	fail_unless(cr == CLIENT_CANCEL_SUCCESS);

	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(record != NULL);
	fail_if_error(error);
	fail_unless(record->cancel_state == CANCEL_REQUESTED);

	siq_target_record_free(record);
}

TEST(delete_record_check, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct target_record *record;

	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(!record);
	fail_unless_error(error);
	isi_error_free(error);
	error = NULL;

	create_record(TARG_ID);

	/*
	 * Now make sure the entry got written.
	 */
	record = siq_target_record_read(TARG_ID, &error);
	fail_unless(record != NULL);
	fail_if_error(error);

	validate_record(record, TARG_ID);

	siq_target_record_free(record);

	siq_target_delete_record(TARG_ID, true, false, &error);
	fail_if_error(error);
	fail_unless(siq_target_record_exists(TARG_ID) == false);
}
