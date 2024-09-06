#include <unistd.h>

#include <check.h>

#include <isi_util/check_helper.h>
#include <isi_migrate/migr/isirep.h>
#include "siq_source.h"

#define FAKE_ID "1234567890abcdef1234567890abcdef"
#define SIQ_SOURCE_RECORD_FILE SIQ_SOURCE_RECORDS_DIR "/" FAKE_ID ".xml" 
#define SIQ_SOURCE_RECORD_LOCK SIQ_SOURCE_RECORDS_LOCK_DIR "/." FAKE_ID ".lock" 

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);
SUITE_DEFINE_FOR_FILE(siq_source, .suite_setup = suite_setup,
    .test_setup = test_setup, .test_teardown = test_teardown);

TEST_FIXTURE(suite_setup)
{
	/* Fool dmalloc checks by pre-initializing. */
	xmlInitParser();
}

TEST_FIXTURE(test_setup)
{
	bool found = false;
	struct isi_error *error = NULL;

	found = siq_source_has_record(FAKE_ID);
	fail_unless(!found);

	siq_source_create(FAKE_ID, &error);
	fail_if_error(error);

	found = siq_source_has_record(FAKE_ID);
	fail_unless(found);


}

TEST_FIXTURE(test_teardown)
{
	struct isi_error *error = NULL;
	siq_source_delete(FAKE_ID, &error);
	fail_if_error(error);
}

static void
save_and_free_rec(struct siq_source_record *srec)
{
	struct isi_error *error = NULL;
	siq_source_record_save(srec, &error);
	fail_if_error(error);
	siq_source_record_free(srec);
}

TEST(simple_storage_check, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct siq_source_record *srec = NULL;
	ifs_snapid_t snap;

	/*
	 * Now make sure the entry got written.
	 */
	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_set_new_snap(srec, 321);
	save_and_free_rec(srec);
	srec = NULL;

	/*
	 * Now make sure the entry got written.
	 */
	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_get_new(srec, &snap);
	fail_unless(snap == 321);

	siq_source_record_free(srec);
	srec = NULL;
}

TEST(concurrent_writes, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct siq_source_record *srec1, *srec2;

	/* 
	 * Concurrently load two copies
	 */
	srec1 = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	srec2 = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	/* 
	 * The first copy will win the race
	 */
	siq_source_set_new_snap(srec1, 3847);
	
	save_and_free_rec(srec1);
	srec1 = NULL;

	/*
	 * And now the second copy should lose
	 */
	siq_source_set_pending_job(srec2, SIQ_JOB_RUN);

	siq_source_record_save(srec2, &error);
	fail_unless_error_is_a(error, SIQ_SOURCE_STALE_ERROR_CLASS);

	siq_source_record_free(srec2);
}

TEST(ensure_proper_locking_with_open, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct siq_source_record *srec;
	int lock_fd;

	/* Open the record with O_EXLOCK */
	srec = siq_source_record_open(FAKE_ID, &error);
	fail_if_error(error);

	/* Try to lock the record's lock file - should fail */
	lock_fd = open(SIQ_SOURCE_RECORD_LOCK, O_RDONLY | O_SHLOCK | O_NONBLOCK);
	fail_unless(lock_fd == -1, "%s", strerror(errno));
	fail_unless(errno == EWOULDBLOCK);

	siq_source_record_unlock(srec);

	/* Try again - should succeed */
	lock_fd = open(SIQ_SOURCE_RECORD_LOCK, O_RDONLY | O_SHLOCK | O_NONBLOCK);
	fail_unless(lock_fd != -1);

	close(lock_fd);
	siq_source_record_free(srec);
}

#define TEST_RECORD_FIELD(name, set_code)				\
static void post_check_##name(struct siq_source_record *, char *);	\
TEST(name, .attrs="overnight")						\
{									\
	char *id = FAKE_ID;						\
	struct isi_error *error = NULL;					\
	struct siq_source_record *srec;					\
	srec = siq_source_record_load(id, &error);			\
	fail_if_error(error);						\
	set_code;							\
	save_and_free_rec(srec);					\
	srec = siq_source_record_load(id, &error);			\
	fail_if_error(error);						\
	post_check_##name(srec, id);					\
	save_and_free_rec(srec);					\
}									\
static void post_check_##name(struct siq_source_record *srec,		\
    char *id)

TEST_RECORD_FIELD(new_snap, siq_source_set_new_snap(srec, 321))
{
	ifs_snapid_t value;
	siq_source_get_new(srec, &value);
	fail_unless(value == 321);
}

TEST_RECORD_FIELD(simple_job_action, 
    siq_source_set_pending_job(srec, SIQ_JOB_RUN))
{
	enum siq_job_action job;
	siq_source_get_pending_job(srec, &job);
	fail_unless(job == SIQ_JOB_RUN);
}

TEST_RECORD_FIELD(simple_job_owner, siq_source_take_ownership(srec))
{
	fail_unless(siq_source_has_owner(srec));
	siq_source_release_ownership(srec);
	fail_unless(!siq_source_has_owner(srec));
}

TEST_RECORD_FIELD(simple_job_delay_start,
    siq_source_set_job_delay_start(srec, 120))
{
	time_t delay_start;
	siq_source_get_job_delay_start(srec, &delay_start);
	fail_unless(delay_start == 120);
}

TEST(job_delay_clear, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct siq_source_record *srec;
	time_t delay_start;

	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_set_job_delay_start(srec, 180);
	save_and_free_rec(srec);
	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_get_job_delay_start(srec, &delay_start);
	fail_unless(delay_start == 180);

	siq_source_clear_job_delay_start(srec);
	save_and_free_rec(srec);
	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_get_job_delay_start(srec, &delay_start);
	fail_unless(delay_start == 0);
	save_and_free_rec(srec);
}

TEST_RECORD_FIELD(simple_last_known_good_time,
    siq_source_set_last_known_good_time(srec, 99))
{
	time_t lkg;
	siq_source_get_last_known_good_time(srec, &lkg);
	fail_unless(lkg == 99);
}

TEST(composite_job_ver, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct siq_job_version ver_in = { 0xa5a5c3c3, 0x3c3c5a5a }, ver_out;
	struct siq_source_record *srec;

	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_set_composite_job_ver(srec, &ver_in);
	save_and_free_rec(srec);
	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_get_composite_job_ver(srec, &ver_out);
	fail_unless(memcmp(&ver_in, &ver_out, sizeof(ver_in)) == 0);

	siq_source_clear_composite_job_ver(srec);
	save_and_free_rec(srec);
	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_get_composite_job_ver(srec, &ver_out);
	fail_unless(ver_out.local == 0 && ver_out.common == 0);
	save_and_free_rec(srec);
}

TEST(prev_composite_job_ver, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct siq_job_version ver_in = { 0x96966969, 0xf0f00f0f }, ver_out;
	struct siq_source_record *srec;

	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_set_prev_composite_job_ver(srec, &ver_in);
	save_and_free_rec(srec);
	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_get_prev_composite_job_ver(srec, &ver_out);
	fail_unless(memcmp(&ver_in, &ver_out, sizeof(ver_in)) == 0);
	save_and_free_rec(srec);
}

TEST(restore_composite_job_ver, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct siq_job_version ver_in = { 0xa5a5c3c3, 0x3c3c5a5a }, ver_out;
	struct siq_source_record *srec;

	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_set_restore_composite_job_ver(srec, &ver_in);
	save_and_free_rec(srec);
	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_get_restore_composite_job_ver(srec, &ver_out);
	fail_unless(memcmp(&ver_in, &ver_out, sizeof(ver_in)) == 0);

	siq_source_clear_restore_composite_job_ver(srec);
	save_and_free_rec(srec);
	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_get_restore_composite_job_ver(srec, &ver_out);
	fail_unless(ver_out.local == 0 && ver_out.common == 0);
	save_and_free_rec(srec);
}

TEST(restore_prev_composite_job_ver, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct siq_job_version ver_in = { 0x96966969, 0xf0f00f0f }, ver_out;
	struct siq_source_record *srec;

	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_set_restore_prev_composite_job_ver(srec, &ver_in);
	save_and_free_rec(srec);
	srec = siq_source_record_load(FAKE_ID, &error);
	fail_if_error(error);

	siq_source_get_restore_prev_composite_job_ver(srec, &ver_out);
	fail_unless(memcmp(&ver_in, &ver_out, sizeof(ver_in)) == 0);
	save_and_free_rec(srec);
}
