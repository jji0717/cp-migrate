#include <isi_cpool_d_common/daemon_job_reader.h>
#include <isi_cpool_d_common/daemon_job_configuration.h>

using namespace isi_cloud;
using namespace daemon_job_manager;

daemon_job_reader::daemon_job_reader()
	: m_exclusive_mode(false), m_pfilter(NULL), m_criteria(NULL)
{
}

daemon_job_reader::~daemon_job_reader()
{
}

void daemon_job_reader::open(int src, bool exclusive_mode)
{
	m_sbt_reader.set_sbt(src);
	m_exclusive_mode = exclusive_mode;
}

daemon_job_record *daemon_job_reader::get_candidate(
	const sbt_entry *pentry, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	daemon_job_record *pjob = NULL;
	bool found = false;

	//
	// Deserialize the job and evaluate
	//   'rec' is used to convert the raw sbt_entry into a
	//   daemon_job_record. The job is evaluated and if a match, it is
	//   then retrieved from the SBT as a locked job.
	//
	daemon_job_record rec;

	rec.set_serialization(pentry->buf, pentry->size_out, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/**
	 * If there is a filter function, use it to determine
	 * whether the record is a candidate. If no function,
	 * then the record is a candidate.
	 */
	found = found_match(&rec);

	if (found)
	{
		/**
		 * Lock and retrieve the current record.
		 * There is a remote possibility that the
		 * record state may have changed. Retest
		 * and should it fail(如果它失败了), find another.
		 */
		pjob = get_locked_record(pentry->key.keys[0], &error);
		ON_ISI_ERROR_GOTO(out, error);

		if (pjob)
		{
			if (!found_match(pjob))
			{
				/**
				 * Is not a candidate
				 */
				unlock_daemon_job_record(pjob->get_daemon_job_id(), &error);
				delete pjob;
				pjob = NULL;
				ON_ISI_ERROR_GOTO(out, error);
			}
		}
	}

out:
	isi_error_handle(error, error_out);
	return pjob;
}

bool daemon_job_reader::found_match(const daemon_job_record *rec) const
{
	//
	// All entries are 'found' if no filter function was provided
	//
	bool found = true;

	if (m_pfilter)
	{
		found = m_pfilter(rec, m_criteria);
	}

	return found;
}

daemon_job_record *daemon_job_reader::get_first_match(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	daemon_job_record *candidate = NULL;

	/**
	 * Return to the beginning of the SBT and fetch the very first matching
	 * record. Subsequent calls may not return the same record as the
	 * first time for the first match may have been removed or modified.
	 */
	const sbt_entry *pentry = m_sbt_reader.get_first(&error);
	ON_ISI_ERROR_GOTO(out, error);

	candidate = get_next_match(pentry, &error);

out:
	isi_error_handle(error, error_out);
	return candidate;
}

size_t daemon_job_reader::get_entry_num()
{
	return m_sbt_reader.get_num();
}

daemon_job_record *daemon_job_reader::get_next_match(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	daemon_job_record *candidate = NULL;
	const sbt_entry *pentry = m_sbt_reader.get_next(&error);
	ON_ISI_ERROR_GOTO(out, error);

	candidate = get_next_match(pentry, &error);

out:
	isi_error_handle(error, error_out);
	return candidate;
}

daemon_job_record *daemon_job_reader::get_locked_record(
	int job_id, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	/**
	 * - Get the record
	 * - lock it
	 * - Get it again
	 * - Return it
	 */

	daemon_job_record *p_djob_record = NULL;

	lock_daemon_job_record(job_id, true, m_exclusive_mode, &error);
	ON_ISI_ERROR_GOTO(out, error);

	p_djob_record = get_daemon_job_record(job_id, &error);

	if (error != NULL)
	{
		if (isi_system_error_is_a(error, ENOENT))
		{
			/*
			 * It's OK if the daemon job record doesn't
			 * exist; unlock the daemon job record and free
			 * the error before continuing.
			 */
			isi_error_free(error);
			error = NULL;
		}

		unlock_daemon_job_record(job_id, NULL);
		ON_ISI_ERROR_GOTO(out, error);
	}

out:
	isi_error_handle(error, error_out);
	return p_djob_record;
}

daemon_job_record *daemon_job_reader::get_next_match(const sbt_entry *&pentry,
													 struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	daemon_job_record *candidate = NULL;

	/**
	 * The first record in the jobs SBT is the job config record. Verify
	 * the job ID is that of the config and then get the next record.
	 */
	btree_key_t DJOB_CONFIG_KEY =
		daemon_job_record::get_key(
			daemon_job_configuration::get_daemon_job_id());

	while (NULL != pentry)
	{
		if ((pentry->key.keys[0] == DJOB_CONFIG_KEY.keys[0]) && (pentry->key.keys[1] == DJOB_CONFIG_KEY.keys[1]))
		{
			/**
			 * This is the configuration job. Start with the
			 * next record.
			 */
			pentry = m_sbt_reader.get_next(&error);
			ON_ISI_ERROR_GOTO(out, error);
			continue;
		}
		candidate = get_candidate(pentry, &error);
		ON_ISI_ERROR_GOTO(out, error);

		if (candidate)
		{
			break;
		}
		else
		{
			pentry = m_sbt_reader.get_next(&error);
			ON_ISI_ERROR_GOTO(out, error);
		}
	};

out:
	isi_error_handle(error, error_out);
	return candidate;
}
