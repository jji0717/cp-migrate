#include <signal.h>

#include <ifs/ifs_syscalls.h>
#include <ifs/pq/pq.h>
#include <isi_cpool_cbm/isi_cbm_file.h>
#include <isi_cpool_cbm/isi_cbm_mapper.h>
#include <isi_cpool_config/cpool_d_config.h>
#include <isi_cpool_config/scoped_cfg.h>
#include <isi_cpool_d_api/cpool_api.h>
#include <isi_cpool_d_common/hsbt.h>
#include <isi_cpool_d_common/scoped_object_list.h>
#include <isi_ilog/ilog.h>
#include <isi_util/isi_assert.h>
#include <isi_util/isi_error.h>

#include "kdq_scanning_thread_pool.h"

#define NUM_ENTRIES_TO_READ		10
#define ENTRY_SIZE			512

/*
 * Failpoints
 */
#define UFP_SKIP_KDQ_ENTRY_DELETE \
	UFAIL_POINT_CODE(kdq_stp__process__skip_kdq_delete, {		\
		goto out;						\
	});

kdq_scanning_thread_pool::kdq_scanning_thread_pool()
{
}

kdq_scanning_thread_pool::~kdq_scanning_thread_pool()
{
	shutdown();
}

void
kdq_scanning_thread_pool::start_threads(int num_threads)
{
	for (int i = 0; i < num_threads; ++i)
		add_thread(NULL, scan_kdq, this);
}

void *
kdq_scanning_thread_pool::scan_kdq(void *arg)
{
	ASSERT(arg != NULL);

	struct isi_error *error = NULL;
	int pq_fd = -1;
	float sleep_interval;
	uint32_t num_entries_read;
	struct pq_bulk_query_entry entries[NUM_ENTRIES_TO_READ];
	char data_buf[NUM_ENTRIES_TO_READ * ENTRY_SIZE];

	kdq_scanning_thread_pool *tp =
	    static_cast<kdq_scanning_thread_pool *>(arg);
	ASSERT(tp != NULL);

	/*
	 * Block SIGTERM and SIGHUP in this thread as they are handled
	 * elsewhere.
	 */
	sigset_t s;
	sigemptyset(&s);
	sigaddset(&s, SIGTERM);
	sigaddset(&s, SIGHUP);
	pthread_sigmask(SIG_BLOCK, &s, NULL);

	while (!tp->is_shutdown_pending()) {
		/*
		 * The PQ won't exist until the (very) first time a stubbed
		 * file is deleted.
		 */
		if (pq_fd == -1) {
			if (pq_open(PQ_QUEUE_ID_CPOOL_NOTIFY, &pq_fd,
			    0) != 0) {
				if (errno != ENOENT)
					ilog(IL_ERR,
					    "failed to open pq (%s): %d %s",
					    PQ_QUEUE_ID_CPOOL_NOTIFY, errno,
					    strerror(errno));

				pq_fd = -1;
				sleep_interval =
				    tp->get_sleep_interval(false);
				goto sleep;
			}
		}

		/* Query for some entries. */
		tp->read_entries_from_queue(pq_fd, NUM_ENTRIES_TO_READ,
		    num_entries_read, entries, data_buf, &error);

		if (error != NULL) {

			/* Bad file descriptor?  Release before continuing */
			if (isi_system_error_is_a(error, EBADF)) {

				/*
				 * pq_close will probably fail (because of EBADF
				 * above), but call to be safe with whatever
				 * else pq implementation does
				 */
				pq_close(pq_fd, 0);

				pq_fd = -1;
			}
			/*
			 * We failed in our attempt to read entries from the
			 * kernel queue, so sleep for a short period of time
			 * before the next attempt.
			 */
			ilog(IL_ERR, "failed to read entries from queue: %#{}",
			    isi_error_fmt(error));
			isi_error_free(error);
			error = NULL;

			sleep_interval = tp->get_sleep_interval(true);
		}
		else if (num_entries_read > 0) {
			/*
			 * We've read some number of entries, so process them
			 * individually.  Since there could be more entries to
			 * process, sleep for a short period of time before the
			 * next query.
			 */
			for (uint32_t i = 0; i < num_entries_read; ++i) {
				tp->process_queue_entry(&(entries[i]), pq_fd,
				    &error);
				if (error != NULL) {
					/*
					 * If more than one thread read the
					 * same entry, all but one of them will
					 * fail with ENOENT as only one of them
					 * will "win" the bulk_op race.  Lower
					 * the log level to prevent log spam.
					 */
					int log_level =
					    isi_system_error_is_a(error,
					    ENOENT) ? IL_DEBUG : IL_ERR;

					ilog(log_level,
					    "failed to process "
					    "kernel delete queue entry "
					    "(key: %{}): %#{}",
					    btree_key_fmt(
						&(entries[i].qe_out)),
					    isi_error_fmt(error));

					isi_error_free(error);
					error = NULL;
				}
			}

			sleep_interval = tp->get_sleep_interval(true);
		}
		else {
			/*
			 * No entries in the kernel queue, so sleep for a long
			 * period of time.
			 */
			sleep_interval = tp->get_sleep_interval(false);
		}

 sleep:
		tp->sleep_for(sleep_interval);
	}

	if (pq_fd != -1)
		if (pq_close(pq_fd, 0) != 0)
			ilog(IL_NOTICE, "failed to close PQ (%s): %d %s",
			    PQ_QUEUE_ID_CPOOL_NOTIFY, errno, strerror(errno));

	pthread_exit(NULL);
}

void
kdq_scanning_thread_pool::read_entries_from_queue(int q_fd,
    uint32_t num_desired, uint32_t &num_read,
    struct pq_bulk_query_entry *entries, char *data_buf,
    struct isi_error **error_out) const
{
	ASSERT(q_fd >= 0, "q_fd: %d", q_fd);
	ASSERT(num_desired > 0, "num_desired: %u", num_desired);
	ASSERT(num_desired <= NUM_ENTRIES_TO_READ,
	    "num_desired: %u", num_desired);
	ASSERT(entries != NULL);
	ASSERT(data_buf != NULL);

	struct isi_error *error = NULL;
	btree_key_t pq_key = {{ 0, 0 }};

	/*
	 * Prepare the buffers into which the queue entries will be read.  Each
	 * entry in entries points to a separate buffer for the queue entry
	 * data.
	 */
	memset(entries, 0, num_desired * sizeof(struct pq_bulk_query_entry));
	memset(data_buf, 0, num_desired * ENTRY_SIZE);

	for (uint32_t i = 0; i < num_desired; ++i) {
		entries[i].data = &(data_buf[i * ENTRY_SIZE]);
		entries[i].data_size = ENTRY_SIZE;
	}

	if (ifs_pq_bulk_query(q_fd, &pq_key, num_desired, entries,
	    &num_read, 0) != 0) {
		ASSERT(errno != ENOSPC,
		    "specified entry size (%d) is insufficient",
		    ENTRY_SIZE);

		if (errno == ENOENT)
			num_read = 0;
		else
			error = isi_system_error_new(errno,
			    "failed to retrieve entries");

		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

void
kdq_scanning_thread_pool::process_queue_entry(
    const struct pq_bulk_query_entry *entry, int q_fd,
    struct isi_error **error_out) const
{
	ASSERT(entry != NULL);
	ASSERT(q_fd >= 0, "q_fd: %d", q_fd);

	struct isi_error *error = NULL;

	ifs_lin_t lin;
	ifs_snapid_t snapid;
	isi_cfm_mapinfo mapinfo;
	struct timeval date_of_death;
	btree_key_t kdq_entry_key = entry->qe_out;

	/* Translate the entry we read from the kernel queue. */
	isi_cbm_get_gcinfo_from_pq(entry->data, entry->data_returned, lin,
	    snapid, mapinfo, &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to translate kernel queue entry");

	/* Calculate the date of death for this stub file. */
	date_of_death = isi_cbm_get_cloud_data_retention_time(lin, snapid,
	    mapinfo, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/*
	 * Add the local GC entry to isi_cpool_d before removing from the
	 * kernel delete queue.  EEXIST is possible in the scenario where the
	 * addition occurs without the deletion (such as isi_cpool_d crash).
	 */
	stub_file_delete(lin, snapid, mapinfo.get_object_id(), date_of_death,
	    &error);
	if (error != NULL && isi_system_error_is_a(error, EEXIST)) {
		ilog(IL_DEBUG, "ignoring stub_file_delete error for %{}: %#{}",
		    lin_snapid_fmt(lin, snapid), isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;
	}
	ON_ISI_ERROR_GOTO(out, error);

	UFP_SKIP_KDQ_ENTRY_DELETE

	if (ifs_pq_remove_check(q_fd, &kdq_entry_key,
	    const_cast<void *>(entry->data), entry->data_returned,
	    PQR_CHECK_NONE, 0, NULL) != 0) {
		error = isi_system_error_new(errno,
		    "failed to remove entry (%{}) from kernel delete queue "
		    "(key: %{})", lin_snapid_fmt(lin, snapid),
		    btree_key_fmt(&kdq_entry_key));
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

float
kdq_scanning_thread_pool::get_sleep_interval(bool get_short_interval) const
{
	struct isi_error *error = NULL;
	const struct cpool_d_index_scan_thread_intervals
	    *sleep_intervals = NULL;

	/*
	 * Set a default interval of 30 seconds to be used if there is an error
	 * reading the configuration from gconfig.
	 */
	float ret_interval = 30.0;

	{
		scoped_cfg_reader reader;
		const cpool_d_config_settings *config_settings =
		    reader.get_cfg(&error);
		ON_ISI_ERROR_GOTO(out, error);

		sleep_intervals = config_settings->get_kdq_intervals();
		ret_interval =
		    get_short_interval ?
		    sleep_intervals->with_skipped_locked_entries :
		    sleep_intervals->without_skipped_locked_entries;
	}

 out:
	if (error != NULL) {
		/* ilog doesn't handle floats */
		char float_str[64];
		snprintf(float_str, sizeof float_str, "%f", ret_interval);

		ilog(IL_ERR,
		    "failed to get sleep interval, "
		    "using default of %s seconds: %#{}",
		    float_str, isi_error_fmt(error));
		isi_error_free(error);
	}

	return ret_interval;
}
