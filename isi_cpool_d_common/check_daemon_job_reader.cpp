#include <tuple>
#include <sstream>
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
// #include <isi_cloud_common/sbt.h>
#include <isi_cpool_config/cpool_config.h>
#include "scoped_object_list.h"
#include "task.h"
#include "unit_test_helpers.h"

#define CPOOL_DJM_PRIVATE
#include "daemon_job_manager_private.h"
#undef CPOOL_DJM_PRIVATE

#include <isi_cpool_d_common/sbt_reader.h>
#include <isi_job/jobstatus_gcfg.h>
#include <isi_ilog/ilog.h>
#include <chrono>
#include <isi_cpool_d_common/daemon_job_reader.h>
#include <isi_cpool_d_common/operation_type.h>
#include "daemon_job_manager.h"
#include <isi_cpool_config/scoped_cfg.h>

using namespace isi_cloud;
using namespace isi_cloud::daemon_job_manager;

const int SUCCESS = 0;
const int INVALID_FD = -1;

/**
 * Generic SBT to use for testing
 */
const char *SBT_NAME = "isi_cpool_d.daemon_reader_test";

/**
 * Using existing code to create daemon_job_config_record and to
 * retrieve the locked daemon_job_record. Therefore need to use the
 * standard jobs sbt file in these cases
 */
const char *JOBS_SBT = "isi_cpool_d.cpool_daemon_jobs_file";

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);
TEST_FIXTURE(teardown_test);

SUITE_DEFINE_FOR_FILE(check_daemon_job_reader,
					  .mem_check = CK_MEM_LEAKS,
					  .suite_setup = setup_suite,
					  .suite_teardown = teardown_suite,
					  .test_setup = setup_test,
					  .test_teardown = teardown_test,
					  .timeout = 0,
					  .fixture_timeout = 1200);

//////////////////////////////////////////////////////////////////////////
/**
 * This is an extension of the daemon_job_record. The completion time is
 * not modifiable in the base class. This derivation(派生类) allows test times to
 * be set.
 */
class test_daemon_job_record : public daemon_job_record
{
public:
	test_daemon_job_record()
	{
	}

	~test_daemon_job_record() {}

	void set_completed_on(time_t when)
	{
		set_completion_time(when);
	}

private:
};

//////////////////////////////////////////////////////////////////////////
/**
 * The sbt_reader does not support SBT record creation. This class extends
 * the base class so that SBT records can be added and retrieved in bulk.
 */
class sbt_reader_ex : public sbt_reader
{
public:
	sbt_reader_ex()
	{
	}

	sbt_reader_ex(int fd)
		: sbt_reader(fd)
	{
	}

	~sbt_reader_ex()
	{
	}

	/**
	 * Stores a record into the SBT
	 *
	 * @param   const btree_key_t*	Record's lookup key
	 * @param   const void*		Pointer to the record to add
	 * @param   size_t		Size of the new record
	 * @param   const struct btree_flags* Applicable flags
	 * @returns int			0 if successfully added
	 */
	int add_entry(const btree_key_t *key,
				  const void *entry_buf, size_t entry_size,
				  const struct btree_flags *entry_flags)
	{
		btree_key_t *not_const_key = (btree_key_t *)key;
		struct btree_flags *not_const_entry_flags =
			(struct btree_flags *)entry_flags;

		return ifs_sbt_add_entry(
			m_fd, not_const_key, entry_buf, entry_size, not_const_entry_flags);
	}

	/**
	 * Retrieves a collection of entries from the SBT
	 *
	 * @param   btree_key_t*	Key to start with
	 * @param   btree_key_t*	Key to resume with
	 * @param   sbt_entries&	Retrieved entries
	 * @param   struct isi_error**	Tracks issues
	 */
	void get_entries(
		btree_key_t *key, btree_key_t *next_key,
		sbt_entries &entries, struct isi_error **error_out)
	{
		struct isi_error *error = NULL;

		sbt_reader::get_entries(key, next_key, entries, &error);

		isi_error_handle(error, error_out);
	}

protected:
private:
};

//////////////////////////////////////////////////////////////////////////
////sbt_entry: carry the contents of a daemon_job_record
test_daemon_job_record *create_daemon_job(int ID, time_t finished_on)
{
	/**
	 * SBT_Entry contains a pointer to an sbt_entry that carries the
	 * contents of a daemon_job_record.
	 */
	test_daemon_job_record *job = new test_daemon_job_record;

	job->set_daemon_job_id(ID);
	job->set_state(DJOB_OJT_COMPLETED);
	job->set_completed_on(finished_on);
	job->set_operation_type(&cpool_operation_type::CP_OT_ARCHIVE);

	std::stringstream ss;
	ss << "job #" << ID;
	job->set_description(ss.str().c_str());

	return job;
}

/**
 * Used to create an SBT with a set number of daemon_job_records.
 * - This is used for both sbt_reader and daemon_job_reader tests.
 *
 * @param   std::string		Name of the SBT to use
 * @param   int			Number to create
 * @param   bool		True to include a daemon_job_configuation_record
 * @param   struct isi_error**	Tracks issues
 * @param   int			Offset to add to the job ID
 * @returns int
 */
int create_jobs_sbt(const char *sbt_name, int num_desired, bool createdjc,
					struct isi_error **error_out, int offset = 0)
{
	struct isi_error *error = NULL;

	/**
	 * Create the SBT file
	 */
	bool create = true;

	int fd = open_sbt(sbt_name, create, &error);
	fail_if(error, "Failed to open SBT, %s : %#{}",
			sbt_name, isi_error_fmt(error));
	fail_if(fd == INVALID_FD, "Unable to create SBT file");

	/**
	 * Populate with the rest of the records
	 */
	sbt_entries jobs;
	sbt_reader_ex reader(fd);
	int qty_created = 0;

	/**
	 * Create the specified number of jobs.
	 *
	 * Will create completed jobs with a completion date that is
	 * days before now. Will set the completed time to NOW - 48 hours.
	 */
	int completed_secondsbefore = 48 * 3600;

	if (createdjc)
	{
		/**
		 * The first record in the jobs SBT is the job_configuration
		 * record. Create the configuration first, then populate the the
		 * SBT with the desired number of records.
		 */
		create_daemon_job_config(&error);
		fail_if(error, "Unable to create daemon job config: %#{}",
				isi_error_fmt(error));
	}

	for (int i = 0; i < num_desired; i++)
	{
		time_t now;
		time(&now);

		/**
		 * time_t is a number of seconds. Reduce this value by n hours,
		 * n hours * 3600 seconds/hour
		 */
		time_t when = now - completed_secondsbefore; ////finish on time 完成的时间

		int jobID = i + 1 + offset;
		btree_key_t key = {{jobID}};

		test_daemon_job_record *pJob = create_daemon_job(jobID, when); ////创建一个djob,并且设置完成的时间

		const void *pBuff = NULL;
		size_t num_bytes = 0;

		pJob->get_serialization(pBuff, num_bytes); /////内部会malloc空间,保存字节流

		int added = reader.add_entry(&key, pBuff, num_bytes, NULL);

		delete pJob;
		pJob = NULL;

		fail_if(SUCCESS != added, "Failed to add entry #%d", i);

		qty_created++;
	}

	close_sbt(fd);

	return qty_created;
}
////return sbt's fd  jjz test
int create_sbt(const char *sbt_name, bool create)
{
	struct isi_error *error = NULL;
	int fd = open_sbt(sbt_name, create, &error);
	fail_if(fd == INVALID_FD, "Unable to create SBT file");
	return fd;
}
void create_djob_record(bool createdjc, int offset = 0) ////jjz test
{
	struct isi_error *error = NULL;
	int fd = open_sbt(SBT_NAME, false, &error);
	sbt_entries jobs;
	sbt_reader_ex reader(fd);
	/// create the specified number of jobs.
	time_t now;
	time(&now);
	time_t when = now - 24 * 3600;
	int jobID = 10;
	for (int i = 10; i < 150; i++)
	{
		jobID = i;
		btree_key_t key = {{jobID}};
		test_daemon_job_record *pJob = create_daemon_job(jobID, when + i);
		const void *pBuff = NULL;
		size_t num_bytes = 0;
		pJob->get_serialization(pBuff, num_bytes);
		int added = reader.add_entry(&key, pBuff, num_bytes, NULL);
		delete pJob;
		pJob = NULL;
		fail_if(SUCCESS != added, "Failed to add entry");
	}
	// btree_key_t key = {{jobID}};
	// test_daemon_job_record *pJob = create_daemon_job(jobID, when);
	// const void *pBuff = NULL;
	// size_t num_bytes = 0;
	// pJob->get_serialization(pBuff, num_bytes);
	// int added = reader.add_entry(&key, pBuff, num_bytes, NULL);
	// delete pJob;
	// pJob = NULL;
	// fail_if(SUCCESS != added, "Failed to add entry");
	close_sbt(fd);
}
void visit_sbt_entry() ////jjz test
{
	struct isi_error *error = NULL;
	bool create = false;
	int fd = open_sbt(SBT_NAME, create, &error);
	fail_if(fd == INVALID_FD, "Unable open sbt file");

	sbt_entries jobs;
	sbt_reader_ex reader(fd);
	btree_key_t key = {{0}};
	int total_retrieved = 0;
	// daemon_job_record *pdjob = NULL;
	do
	{
		static int cnt = 0;
		reader.get_entries(&key, &key, jobs, &error); /* code */
		fail_if(error, "Problems retrieving entries");
		total_retrieved += jobs.size();
		printf("\nretrieve %lu entries\n", jobs.size());
		for (auto it : jobs)
		{
			/////how to convert sbt_entry to the instance of djob_record ???
			daemon_job_record rec;
			rec.set_serialization(it->buf, it->size_out, &error);
			printf("\ndjob_id:%d\n", rec.get_daemon_job_id());
		}
		printf("\ncnt:%d\n", cnt++);
	} while (jobs.size() > 0);
	printf("\njobs num:%d\n", total_retrieved);
	close_sbt(fd);
}
void remove_sbt_entry() /// jjz test
{
	struct isi_error *error = NULL;
	bool create = false;
	int fd = open_sbt(SBT_NAME, create, &error);
	fail_if(fd == INVALID_FD, "Unable open sbt file");

	sbt_entries jobs;
	sbt_reader_ex reader(fd);
	btree_key_t key = {{0}};
	int total_retrieved = 0;
	do
	{
		reader.get_entries(&key, &key, jobs, &error); /* code */
		fail_if(error, "Problems retrieving entries");
		total_retrieved += jobs.size();

		for (auto entry : jobs)
		{
			btree_key_t jobkey = {{entry->key.keys[0]}};
			if (ifs_sbt_cond_remove_entry(fd, &jobkey, BT_CM_BUF, entry->buf, entry->size_out, NULL) != 0)
			{
				printf("\nifs_sbt_cond_remove_entry failed\n");
			}
		}
	} while (jobs.size() > 0);
	printf("\njobs num:%d\n", total_retrieved);
	close_sbt(fd);
}
/**
 * Used to verify that all of the sbt_entries in an SBT are present
 *
 * @param   std::string		Name of the SBT to use
 * @returns int 		Number of records gathered
 */
int count_jobs_in_sbt(const char *sbt_name)
{
	struct isi_error *error = NULL;

	/**
	 * Open the SBT
	 */
	bool create = false;

	int fd = open_sbt(sbt_name, create, &error);
	fail_if(error, "Failed to open SBT, %s : %#{}",
			sbt_name, isi_error_fmt(error));
	fail_if(fd == INVALID_FD,
			"Unable to create SBT file");

	/**
	 * Create the SBT reader/writer
	 */
	sbt_entries jobs;
	sbt_reader_ex reader(fd);

	btree_key_t key = {{0}};
	int total_retrieved = 0;

	do
	{
		reader.get_entries(&key, &key, jobs, &error);
		fail_if(error, "Problems retrieving entries. Error: %#{}",
				isi_error_fmt(error));

		total_retrieved += jobs.size();

	} while (jobs.size() > 0);

	close_sbt(fd);

	return total_retrieved;
}

//////////////////////////////////////////////////////////////////////////
TEST_FIXTURE(setup_suite)
{
	struct ilog_app_init init = {
		"check_daemon_job_reader",			 // full app name
		"cpool_d_common",					 // component
		"",									 // job (opt)
		"check_daemon_job_reader",			 // syslog program
		IL_TRACE_PLUS,						 // default level
		false,								 // use syslog
		false,								 // use stderr
		false /*true*/,						 // log_thread_id
		LOG_DAEMON,							 // syslog facility
		"/root/check_daemon_job_reader.log", // log file
		IL_ERR_PLUS,						 // syslog_threshold
		NULL,								 // tags
	};

	/**
	 * Determine whether the debug and trace statement should be directed
	 * to ILOG. The log will be activate if the environment variable,
	 * CHECK_USE_ILOG is defined. One way to do this is to define the
	 * variable and launch the test in the same command. For example,
	 *	 CHECK_USE_ILOG=true make check
	 */
	// char *ilogenv = getenv("CHECK_USE_ILOG");

	// if (ilogenv != NULL)
	//{
	ilog_init(&init, false, true);
	ilog(IL_NOTICE, "ilog initialized\n");
	//}

	/* to prevent false positive memory leaks */
	isi_cloud_object_id object_id;

	init_failpoints_ut();

	enable_cpool_d(false);
}

TEST_FIXTURE(teardown_suite)
{
	//
	// Make sure test SBTs are removed
	//
	// delete_sbt(JOBS_SBT, NULL);
	// delete_sbt(SBT_NAME, NULL);

	// restart isi_cpool_d
	enable_cpool_d(true);
}

TEST_FIXTURE(setup_test)
{
	//
	// Make sure test SBTs are removed
	//
	// delete_sbt(JOBS_SBT, NULL);
	// delete_sbt(SBT_NAME, NULL);

	struct isi_error *error = NULL;

	// /**
	//  * This SBT is not deleted by delete_all_sbts()
	//  */
	// delete_sbt(SBT_NAME, &error);
	// fail_if(error != NULL, "failed to delete SBT (%s): %#{}",
	// 		SBT_NAME, isi_error_fmt(error));

	/* Sets up sandboxed gconfig environment */
	cpool_unit_test_setup(&error);
	fail_if(error != NULL, "Error calling cpool_unit_test_setup: %#{}",
			isi_error_fmt(error));
}

TEST_FIXTURE(teardown_test)
{
	struct isi_error *error = NULL;
	cpool_unit_test_cleanup(&error);
	fail_if(error != NULL, "Error calling cpool_unit_test_cleanup: %#{}",
			isi_error_fmt(error));
}
TEST(test_my_smoke)
{
	printf("\n%s called\n", __func__);
}
TEST(test_create_sbt) // jjz test
{
	bool create = false;
	int fd = create_sbt(SBT_NAME, create);
	printf("\nfd:%d\n", fd); /////fd: 6
}
TEST(test_add_djob_record)
{
	// int fd = 6;
	bool createdjc = true;
	create_djob_record(createdjc);
}
TEST(test_sbt_entry_traverse)
{
	visit_sbt_entry();
}
TEST(test_sbt_reader_remove) // jjz test: traverse the sbt entry in SBT
{
	remove_sbt_entry();
}
//////////////////////////////////////////////////////////////////////////
/**
 * Verify that the sbt_reader can iterate through an SBT. In this case an
 * SBT full of daemon_job_records is created. The sbt_reader is pointed at
 * the SBT and the code then iterates from beginning to end.
 */
TEST(test_sbt_reader_1)
{
	struct isi_error *error = NULL;

	const int REQUESTED = 100;

	int created = create_jobs_sbt(SBT_NAME, REQUESTED, false, &error);
	fail_if(error, "Unable to createjobs: %#{}", isi_error_fmt(error));

	fail_if(REQUESTED != created, "Failed to create test records (%d/%d)",
			created, REQUESTED);

	int fd = open_sbt(SBT_NAME, false, &error);
	fail_if(error, "Failed to open SBT, %s : %#{}",
			SBT_NAME, isi_error_fmt(error));

	fail_if(fd == INVALID_FD, "Unable to create SBT file");

	/**
	 * Create an sbt reader. Verify each record in the SBT is found.
	 */
	sbt_reader reader(fd);

	int id = 1;
	int returned_id = -1;

	const sbt_entry *pentry = reader.get_first(&error);
	fail_if(error, "Failed to get first entry : %#{}", isi_error_fmt(error));
	fail_if(NULL == pentry, "get_first(), an entry should have been returned");

	while (NULL != pentry)
	{
		fail_if(pentry->key.keys[0] != id, "Should have found the key %d", id); ////////pentry->key.keys[0]就是djob_id  转成btree_key_t: btree_key_t btk = {{pentry->key.keys[0]}}

		returned_id = pentry->key.keys[0];

		pentry = reader.get_next(&error);
		fail_if(error, "Failed to get entry %d: %#{}",
				id + 1, isi_error_fmt(error));

		id++;
	}
	fail_if(REQUESTED != returned_id, "Last entry ID should be %d, was %d",
			REQUESTED, returned_id);

	/**
	 * verify that get_first() returns to the begining after a get_next()
	 * - get_first() -> 1
	 * - get_next()  -> 2
	 * - get_first() -> 1
	 */
	pentry = reader.get_first(&error);
	fail_if(error, "Failed to get first entry : %#{}", isi_error_fmt(error));
	fail_if(NULL == pentry, "An entry should have been returned");
	fail_if(pentry->key.keys[0] != 1, "Should have found the key %d", 1);

	pentry = reader.get_next(&error);
	fail_if(error, "Failed to get entry %d: %#{}", 2, isi_error_fmt(error));
	fail_if(NULL == pentry, "An entry should have been returned");
	fail_if(pentry->key.keys[0] != 2, "Should have found the key %d", 2);

	pentry = reader.get_first(&error);
	fail_if(error, "Failed to get first entry : %#{}", isi_error_fmt(error));
	fail_if(NULL == pentry, "An entry should have been returned");
	fail_if(pentry->key.keys[0] != 1, "Should have found the key %d", 1);

	close(fd);
}

/*
 * Test sbt_reader with filtering.
 */
static bool
lower_key_mod_N(const struct sbt_entry *ent, void *const divisor_ptr,
				struct isi_error **error_out);

static bool
filter_with_error(const struct sbt_entry *ent, void *const divisor_ptr,
				  struct isi_error **error_out);

static void
check_sbt_entry(const struct sbt_entry *sbt_ent, const btree_key_t &key,
				struct isi_error **error_out);

TEST(sbt_reader_with_filtering)
{
	struct isi_error *error = NULL;
	const char *sbt_name = "sbt_reader_with_filtering.ut";
	int sbt_fd = -1;
	sbt_reader sbt_rdr;
	int num_entries = 0;
	btree_key_t key = {{0, 0}};
	int filter_divisor = 0;
	const struct sbt_entry *sbt_ent = NULL;

	/* Cleanup from any previous failed test. */
	delete_sbt(sbt_name, &error);
	fail_if(error != NULL, "failed to delete %s: %#{}",
			sbt_name, isi_error_fmt(error));

	/* Create the SBT, then populate. */
	sbt_fd = open_sbt(sbt_name, true, &error);
	fail_if(error != NULL, "failed to create %s: %#{}",
			sbt_name, isi_error_fmt(error));
	fail_if(sbt_fd < 0, "invalid file descriptor (%d)", sbt_fd);

	sbt_rdr.set_sbt(sbt_fd);

	num_entries = 10000;
	for (int i = 0; i < num_entries; ++i)
	{
		/* { i, i * i } --> i */
		key = (btree_key_t){{i, i * i}};
		fail_if(ifs_sbt_add_entry(sbt_fd, &key, &key,
								  sizeof key.keys[0], NULL) != 0,
				"failed to add entry (key: %{}): %s (%d)",
				btree_key_fmt(&key), strerror(errno), errno);
	}

	/*
	 * Set the filter on the SBT reader instance, then start retrieving
	 * entries.
	 */
	filter_divisor = 2;
	sbt_rdr.set_filter(lower_key_mod_N, &filter_divisor);

	/* First should be { 0, 0 }. */
	sbt_ent = sbt_rdr.get_first(&error);
	fail_if(error != NULL, "failed to get entry: %#{}",
			isi_error_fmt(error));
	check_sbt_entry(sbt_ent, (btree_key_t){{0, 0}}, &error);
	fail_if(error != NULL, "%#{}", isi_error_fmt(error));

	/* Next should be { 2, 4 }. */
	sbt_ent = sbt_rdr.get_next(&error);
	fail_if(error != NULL, "failed to get entry: %#{}",
			isi_error_fmt(error));
	check_sbt_entry(sbt_ent, (btree_key_t){{2, 4}}, &error);
	fail_if(error != NULL, "%#{}", isi_error_fmt(error));

	/* Next should be { 4, 16 }. */
	sbt_ent = sbt_rdr.get_next(&error);
	fail_if(error != NULL, "failed to get entry: %#{}",
			isi_error_fmt(error));
	check_sbt_entry(sbt_ent, (btree_key_t){{4, 16}}, &error);
	fail_if(error != NULL, "%#{}", isi_error_fmt(error));

	/* Next should be { 6, 36 }. */
	sbt_ent = sbt_rdr.get_next(&error);
	fail_if(error != NULL, "failed to get entry: %#{}",
			isi_error_fmt(error));
	check_sbt_entry(sbt_ent, (btree_key_t){{6, 36}}, &error);
	fail_if(error != NULL, "%#{}", isi_error_fmt(error));

	/* First should still be { 0, 0 }. */
	sbt_ent = sbt_rdr.get_first(&error);
	fail_if(error != NULL, "failed to get entry: %#{}",
			isi_error_fmt(error));
	check_sbt_entry(sbt_ent, (btree_key_t){{0, 0}}, &error);
	fail_if(error != NULL, "%#{}", isi_error_fmt(error));

	/* Next should be { 2, 4 }. */
	sbt_ent = sbt_rdr.get_next(&error);
	fail_if(error != NULL, "failed to get entry: %#{}",
			isi_error_fmt(error));
	check_sbt_entry(sbt_ent, (btree_key_t){{2, 4}}, &error);
	fail_if(error != NULL, "%#{}", isi_error_fmt(error));

	/*
	 * Check all remaining entries.  Starting with the next expected entry,
	 * { 4, 16 }, should get all entries with an even key[0] value.
	 */
	for (int i = 4; i < num_entries; i += 2)
	{
		key = (btree_key_t){{i, i * i}};
		sbt_ent = sbt_rdr.get_next(&error);
		fail_if(error != NULL, "failed to get entry (i: %d): %#{}",
				i, isi_error_fmt(error));
		check_sbt_entry(sbt_ent, key, &error);
		fail_if(error != NULL, "(i: %d) %#{}",
				i, isi_error_fmt(error));
	}

	/*
	 * Having exhausted all entries, this last attempt should return NULL.
	 * (Note: using fail_if for checking the sbt_ent pointer value led to
	 * a crash, presumably because short-circuiting didn't occur.)
	 */
	sbt_ent = sbt_rdr.get_next(&error);
	fail_if(error != NULL, "failed to get entry: %#{}",
			isi_error_fmt(error));
	if (sbt_ent != NULL)
	{
		fail("expected not to receive an SBT entry "
			 "(key: %{} size: %u value: %{})",
			 btree_key_fmt(&sbt_ent->key), sbt_ent->size_out,
			 array_byte_fmt((uint8_t *)sbt_ent->buf,
							sbt_ent->size_out));
	}

	/*
	 * Get first again, then change the filter to one that returns an
	 * error.
	 */
	sbt_ent = sbt_rdr.get_first(&error);
	fail_if(error != NULL, "failed to get entry: %#{}",
			isi_error_fmt(error));
	check_sbt_entry(sbt_ent, (btree_key_t){{0, 0}}, &error);
	fail_if(error != NULL, "%#{}", isi_error_fmt(error));

	sbt_rdr.set_filter(filter_with_error, NULL);

	sbt_ent = sbt_rdr.get_next(&error);
	fail_if(error == NULL || !isi_system_error_is_a(error, EDOOFUS),
			"expected EDOOFUS: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	/* Cleanup. */
	close(sbt_fd);
	sbt_fd = -1;

	delete_sbt(sbt_name, &error);
	fail_if(error != NULL, "failed to delete %s: %#{}",
			sbt_name, isi_error_fmt(error));
}

/*
 * Filter function that returns true if lower bits of SBT key is evenly divided
 * by a supplied value.
 */
static bool
lower_key_mod_N(const struct sbt_entry *ent, void *const divisor_ptr,
				struct isi_error **error_out)
{
	int *const divisor = static_cast<int *const>(divisor_ptr);
	ASSERT(divisor != NULL);

	return (ent->key.keys[1] % *divisor == 0);
}

static bool
filter_with_error(const struct sbt_entry *ent, void *const divisor_ptr,
				  struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ON_SYSERR_GOTO(out, EDOOFUS, error,
				   "filter error");

out:
	isi_error_handle(error, error_out);

	return false;
}

/*
 * An sbt_entry check function, specifically for sbt_reader_with_filtering.
 */
static void
check_sbt_entry(const struct sbt_entry *sbt_ent, const btree_key_t &key,
				struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (sbt_ent == NULL)
	{
		ON_SYSERR_GOTO(out, EDOOFUS, error,
					   "expected an SBT entry");
	}

	if (sbt_ent->key.keys[0] != key.keys[0] ||
		sbt_ent->key.keys[1] != key.keys[1])
	{
		ON_SYSERR_GOTO(out, EDOOFUS, error,
					   "key mismatch: expected %{}, not %{}",
					   btree_key_fmt(&key), btree_key_fmt(&sbt_ent->key));
	}

	if (sbt_ent->size_out != sizeof(key.keys[0]))
	{
		ON_SYSERR_GOTO(out, EDOOFUS, error,
					   "entry size mismatch: expected %zu, not %u",
					   sizeof(key.keys[0]), sbt_ent->size_out);
	}

	if (*((uint64_t *)sbt_ent->buf) != key.keys[0])
	{
		ON_SYSERR_GOTO(out, EDOOFUS, error,
					   "entry mismatch: expected %llx, not %{}",
					   key.keys[0],
					   array_byte_fmt((uint8_t *)sbt_ent->buf, sbt_ent->size_out));
	}

out:
	isi_error_handle(error, error_out);
}

//////////////////////////////////////////////////////////////////////////
/**
 * Verifies that test tool can populate an SBT
 */
TEST(test_verify_sbt_population_technique)
{
	struct isi_error *error = NULL;

	int qty_desired = 100;

	/**
	 * Create jobs. sbt_reader tests, daemon_job_configuration_record
	 * is not required
	 */
	int created = create_jobs_sbt(SBT_NAME, qty_desired, false, &error);
	fail_if(error, "Unable to createjobs: %#{}", isi_error_fmt(error));

	fail_if(qty_desired != created,
			"Failed to create the desired number of jobs (%d/%d).",
			created, qty_desired);

	int qty = count_jobs_in_sbt(SBT_NAME);

	fail_if(qty != created,
			"Did not count all of the recorded jobs. Wrote %d, Read %d",
			created, qty);
}

/**
 * daemon_job_reader filtration functions
 */
bool cleanup_filter(const daemon_job_record *candidate, const void *criteria)
{
	/**
	 * The criteria for cleanup is that the job has the properties specified
	 * in the IF statement and that it completed before the specified
	 * date/time. The criteria parameter is this date.
	 */
	time_t older_than = *((time_t *)criteria);
	int DJOB_RECORD_NOT_YET_COMPLETED = 0;

	return (
		candidate->get_operation_type()->is_multi_job() && candidate->is_finished() && candidate->get_completion_time() != DJOB_RECORD_NOT_YET_COMPLETED && candidate->get_completion_time() < older_than);
}

bool even_job_id_filter(const daemon_job_record *candidate, const void *criteria)
{
	/**
	 * Accepts only jobs with an even job_id. Criteria parameter is
	 * not used
	 */
	return (candidate->get_daemon_job_id() % 2 == 0);
}

//////////////////////////////////////////////////////////////////////////
//
// Use get_first/next() to retrieve all 10 job records
//
TEST(test_daemon_job_reader_1)
{
	struct isi_error *error = NULL;

	const int REQUESTED = 10;

	int created = create_jobs_sbt(JOBS_SBT, REQUESTED, true, &error);
	fail_if(error, "Unable to createjobs: %#{}", isi_error_fmt(error));

	fail_if(REQUESTED != created, "Failed to create test records (%d/%d)",
			created, REQUESTED);

	int fd = open_sbt(JOBS_SBT, false, &error);
	fail_if(error, "Failed to open SBT, %s : %#{}",
			JOBS_SBT, isi_error_fmt(error));

	fail_if(fd == INVALID_FD, "Unable to create SBT file");

	/**
	 * Create a job reader and set the acceptance time to now. Since the
	 * jobs are created with a finished time of two hours previous, all
	 * should be found.
	 */
	time_t now;
	now = time(&now);

	daemon_job_reader reader;

	reader.open(fd, false); // No need for exclusivity
	reader.set_post_deserialization_filter(cleanup_filter, &now);

	int id = 1;
	int returned_id = -1;

	daemon_job_record *pjob = reader.get_first_match(&error);
	fail_if(error, "Failed to get first job : %#{}", isi_error_fmt(error));
	fail_if(NULL == pjob, "At least one job should have matched");

	while (NULL != pjob)
	{
		fail_if(pjob->get_daemon_job_id() != id, "Should have found the job %d", id);

		returned_id = pjob->get_daemon_job_id();

		/**
		 * Need to unlock and release memory before moving on to
		 * the next
		 */
		unlock_daemon_job_record(pjob->get_daemon_job_id(), &error);
		delete pjob;
		pjob = NULL;
		fail_if(error, "Unable to unlock job: %#{}", isi_error_fmt(error));

		pjob = reader.get_next_match(&error);
		fail_if(error, "Failed to get job %d: %#{}",
				id + 1, isi_error_fmt(error));
		id++;
	}
	fail_if(REQUESTED != returned_id, "Last found ID should be %d, was %d",
			REQUESTED, returned_id);
}

//
// Use get_first_match() to retrieve the first job that was completed 49 hours
// ago. All jobs were created finished 48 hours ago. In this test, one call to
// get_first_match() will traverse the entire SBT. There are no jobs present
// that meet the 49 hour requirement.
//
TEST(test_daemon_job_reader_2)
{
	struct isi_error *error = NULL;

	const int REQUESTED = 100;
	int created = create_jobs_sbt(JOBS_SBT, REQUESTED, true, &error);
	fail_if(error, "Unable to create daemon_config_record: %#{}",
			isi_error_fmt(error));

	fail_if(REQUESTED != created, "Failed to create test records (%d/%d)",
			created, REQUESTED);

	int fd = open_sbt(JOBS_SBT, false, &error);
	fail_if(error, "Failed to open SBT, %s : %#{}",
			JOBS_SBT, isi_error_fmt(error));
	fail_if(fd == INVALID_FD, "Unable to create SBT file");

	/**
	 * Create a job reader and set the acceptance time to 49 hours ago.
	 * Since the jobs are created with a finished time of 48 hours
	 * previous, no jobs should be found.
	 */
	time_t before;
	before = time(&before);

	//
	// Set the time to 49 hours previous
	//
	before -= 49 * 3600;

	daemon_job_reader reader;

	reader.open(fd, false); // No need for exclusivity
	reader.set_post_deserialization_filter(cleanup_filter, &before);

	daemon_job_record *pjob = reader.get_first_match(&error);
	fail_if(error, "Problem occurred searching for nonexistent job: %#{}",
			isi_error_fmt(error));

	fail_if(NULL != pjob, "No matching jobs should have been found");
}

//
// Use get_first/next_match() to fetch all of the jobs with an even job number.
// 100 jobs, so 50 should be found. This test verifies that matching jobs
// can be found using both get_first_match() and get_next_match().
//
TEST(test_daemon_job_reader_multiple_selection)
{
	struct isi_error *error = NULL;

	const int REQUESTED = 100;
	int created = create_jobs_sbt(JOBS_SBT, REQUESTED, true, &error);
	fail_if(error, "Unable to create daemon_config_record: %#{}",
			isi_error_fmt(error));

	fail_if(REQUESTED != created, "Failed to create test records (%d/%d)",
			created, REQUESTED);
	int fd = open_sbt(JOBS_SBT, false, &error);
	fail_if(error, "Failed to open SBT, %s : %#{}",
			JOBS_SBT, isi_error_fmt(error));
	fail_if(fd == INVALID_FD, "Unable to create jobs SBT file");

	/**
	 * Create a job reader and set the acceptance function include only
	 * those jobs with an even job_id. No criteria information is needed.
	 */
	daemon_job_reader reader;

	reader.open(fd, false); // No need for exclusivity
	reader.set_post_deserialization_filter(even_job_id_filter, NULL);

	int id = 2;
	int returned_id = -1;

	daemon_job_record *pjob = reader.get_first_match(&error);
	fail_if(error, "Failed to get first job : %#{}", isi_error_fmt(error));
	fail_if(NULL == pjob, "At least one job should have matched");

	while (NULL != pjob)
	{
		fail_if(pjob->get_daemon_job_id() != id, "Should have found the job %d", id);

		returned_id = pjob->get_daemon_job_id();

		/**
		 * Need to unlock and release memory before moving on to
		 * the next
		 */
		unlock_daemon_job_record(pjob->get_daemon_job_id(), &error);
		delete pjob;
		pjob = NULL;
		fail_if(NULL != error, "Unable to unlock job: %#{}", isi_error_fmt(error));

		pjob = reader.get_next_match(&error);
		fail_if(error, "Failed to get job %d: %#{}",
				id + 2, isi_error_fmt(error));
		id += 2;
	}
	fail_if(REQUESTED != returned_id, "Last found ID should be %d, was %d",
			REQUESTED, returned_id);

	close(fd);
}

//
// Use get_first() to retrieve job #75. About 73 jobs are retrieved in the
// first collection so get_first() will need to fetch twice.
//
TEST(test_sbt_reader_multiple_sbts)
{
	struct isi_error *error = NULL;

	const char *sbt_name50 = "isi_cpool_d.cpool_daemon_jobs_50";
	const char *sbt_name1050 = "isi_cpool_d.cpool_daemon_jobs_1050";

	delete_sbt(sbt_name50, &error);
	fail_if(error != NULL,
			"sbt50:failed to delete SBT (%s): %#{}",
			sbt_name50, isi_error_fmt(error));

	delete_sbt(sbt_name1050, &error);
	fail_if(error != NULL,
			"sbt1050:failed to delete SBT (%s): %#{}",
			sbt_name1050, isi_error_fmt(error));

	const int REQUESTED = 50;
	const int OFFSET = 1000;

	int created = create_jobs_sbt(sbt_name50, REQUESTED, false, &error);
	fail_if(error, "sbt50: Unable to create daemon_config_record: %#{}",
			isi_error_fmt(error));

	fail_if(REQUESTED != created, "sbt50: Failed to create test records");

	int fd50 = open_sbt(sbt_name50, false, &error);
	fail_if(error, "Failed to open SBT, %s : %#{}",
			sbt_name50, isi_error_fmt(error));
	fail_if(fd50 == INVALID_FD, "sbt50: Unable to create SBT file");

	created = create_jobs_sbt(sbt_name1050, REQUESTED, false, &error, OFFSET);
	fail_if(error, "Failed to open SBT, %s : %#{}",
			sbt_name1050, isi_error_fmt(error));
	fail_if(error, "sbt1050: Unable to create daemon_config_record: %#{}",
			isi_error_fmt(error));

	fail_if(REQUESTED != created, "sbt1050: Failed to create test records");

	int fd1050 = open_sbt(sbt_name1050, false, &error);
	fail_if(error, "Failed to open SBT, %s : %#{}",
			sbt_name50, isi_error_fmt(error));
	fail_if(fd1050 == INVALID_FD, "sbt1050: Unable to create SBT50 file");

	/**
	 * Create a job reader and set the acceptance to be job ID 75. This
	 * is an ID that will not be found in the first collection.
	 * get_first() will need to fetch twice to find this.
	 */
	sbt_reader reader(fd50);

	const sbt_entry *pentry = reader.get_first(&error);

	fail_if(error, "sbt50: error getting the first record: %#{}",
			isi_error_fmt(error));
	fail_if(NULL == pentry, "sbt50: At least one job should have matched");

	/**
	 * Read a portion of the file, 33 entries
	 */
	int id = 1;

	while (pentry && (id < 33))
	{
		fail_if(pentry->key.keys[0] != id, "sbt50: Should have found ID %d", id);

		pentry = reader.get_next(&error);
		fail_if(error, "sbt50: error getting the next record: %#{}",
				isi_error_fmt(error));
		id++;
	}

	/**
	 * Swap the SBT with the one with job IDs starting at 1001
	 */
	reader.set_sbt(fd1050);

	/**
	 * The swap only reset the bookkeeping such that the next get first/
	 * next will query the new SBT. The entry pointer currently is
	 * pointing to the 33rd entry and should have an ID of 33.
	 */
	fail_if(pentry->key.keys[0] != id, "sbt50: ID should be %d is: %d",
			id, pentry->key.keys[0]);

	/**
	 * Most people might/would call get_first() next, but to prove the
	 * reset() worked, will call get_next() instead.
	 */
	id = 1001;
	pentry = reader.get_next(&error);
	fail_if(error, "sbt1050: error getting the next record: %#{}",
			isi_error_fmt(error));

	/**
	 * Read a portion of the file, up to record 1033
	 */
	while (pentry && (id < 1033))
	{
		fail_if(pentry->key.keys[0] != id, "sbt1050: Should have found ID %d", id);

		pentry = reader.get_next(&error);
		fail_if(error, "sbt1050: error getting the next record: %#{}",
				isi_error_fmt(error));
		id++;
	}

	/**
	 * Now back to sbt50
	 * Swap the SBT with the one with job IDs starting at 1
	 */
	reader.set_sbt(fd50);

	/**
	 * The swap only reset the bookkeeping such that the next get first/
	 * next will query the new SBT. The entry pointer currently is
	 * pointing to the 33rd entry and should have an ID of 33.
	 */
	fail_if(pentry->key.keys[0] != id, "sbt1050: ID should be %d is: %d",
			id, pentry->key.keys[0]);

	/**
	 * Most people might/would call get_first() next, but to prove the
	 * reset() worked, will call get_next() instead.
	 */
	id = 1;
	pentry = reader.get_next(&error);
	fail_if(error, "sbt50b: error getting the next record: %#{}",
			isi_error_fmt(error));

	/**
	 * Read a portion of the file, up to record 1033
	 */
	while (pentry && (id < 33))
	{
		fail_if(pentry->key.keys[0] != id, "sbt50b: Should have found ID %d", id);

		pentry = reader.get_next(&error);
		fail_if(error, "sbt50b: error getting the next record: %#{}",
				isi_error_fmt(error));
		id++;
	}

	close(fd50);
	close(fd1050);

	//
	// Remove the test SBTs
	//
	delete_sbt(sbt_name50, &error);
	fail_if(error != NULL, "failed to delete SBT (%s): %#{}",
			sbt_name50, isi_error_fmt(error));

	delete_sbt(sbt_name1050, &error);
	fail_if(error != NULL, "failed to delete SBT (%s): %#{}",
			sbt_name1050, isi_error_fmt(error));
}
