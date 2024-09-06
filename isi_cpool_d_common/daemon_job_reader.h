#pragma once

#include <memory>

#include <isi_cpool_d_common/sbt_reader.h>
#include <isi_cpool_d_common/daemon_job_record.h>
#include <isi_cpool_d_common/daemon_job_manager.h>
#include <isi_util/isi_error.h>

/**
 * The daemon_job_reader uses functions of this type to determine which
 * daemon_job_records are desired.
 *
 * @param *p_job_filter		The pointer to the filter function
 * @param const daemon_job_record*	Pointer to the job record to evaluate
 * @param const void*		Pointer to opaque user data that the function
 *				will convert and use to evaluate the job
 * @returns bool		True if desired
 */
typedef bool (*p_job_filter)(const daemon_job_record *candidate,
							 const void *criteria);

/**
 * This class is used to read the daemon_job_record SBT. The class uses the
 * generic sbt_reader class to obtain a block of sbt records. These
 * records are then converted to daemon_job_records, evaluated and returned
 * to the caller via the get_first() and get_next() methods. Which records
 * are of interest is determined by the provided filter function.
 */
class daemon_job_reader
{
public:
	daemon_job_reader();
	~daemon_job_reader();

	/**
	 * Associates an already opened daemon job SBT with this reader.
	 * - This function cannot fail
	 *
	 * @param   int		File descriptor to the SBT source
	 * @param   bool	True, use exclusive job locks
	 */
	void open(int src, bool exclusive_mode);

	/*
	 * Filter entries prior to deserialization.
	 */
	void set_pre_deserialization_filter(sbt_filter_cb_t sbt_filter,
										void *sbt_filter_ctx)
	{
		m_sbt_reader.set_filter(sbt_filter, sbt_filter_ctx);
	}

	/**
	 * Assign a selection function that will be used to find possible
	 * job records. The criteria parameter should be used to pass the
	 * acceptance properties. By default, the search filter is not set
	 * and all jobs are returned.
	 *
	 * @param   p_job_filter	Pointer to acceptance function
	 * @param   const void*		Pointer to acceptance criteria
	 */
	void set_post_deserialization_filter(p_job_filter pfilter,
										 const void *criteria)
	{
		m_pfilter = pfilter;
		m_criteria = criteria;
	}

	/**
	 * Returns the very first job matching record in the SBT. The returned
	 * job is locked, live and ready for use.
	 * - Please note the caller is responsible for unlocking and
	 *   deallocating the job pointer.
	 *
	 * @param   struct isi_error*	Tracks retrieval problems
	 * @returns daemon_job_record*	Pointer to the job record or
	 *				NULL if no match found.
	 */
	daemon_job_record *get_first_match(struct isi_error **error_out);

	size_t get_entry_num(); /////return the size of m_sbt_reader.m_entries.size()

	/**
	 * Returns the next matching job record in the SBT. The returned job
	 * is locked, live and ready for use.
	 * - Please note the caller is responsible for unlocking and
	 *   deallocating the job pointer.
	 *
	 * @param   struct isi_error*	Tracks retrieval problems
	 * @returns daemon_job_record*	Pointer to the job record or
	 *				NULL if no match found.
	 */
	daemon_job_record *get_next_match(struct isi_error **error_out);

private:
	/**
	 * Retrieves a locked pointer to the job with the specified job
	 * identifier.
	 *
	 * @param   int			Job ID of desired record
	 * @param   struct isi_error**	Tracks issues obtaining the record
	 * @returns daemon_job_record*	A pointer to the locked record or
	 *				NULL if no match found.
	 */
	daemon_job_record *get_locked_record(int job_id, struct isi_error **error_out);

	/**
	 * Using the selection filter, determines whether the current SBT
	 * entry is a match.
	 *
	 * @param   const sbt_entry*	Raw sbt_entry pointer to a job
	 * @param   struct isi_error**	Tracks evaluation issues
	 * @returns daemon_job_record*	Pointer to the candidate
	 */
	daemon_job_record *get_candidate(
		const sbt_entry *pentry, struct isi_error **error_out);

	/**
	 * Determines whether the specified record is a match.
	 * - Consolidates the existence and absence of the filtration
	 *   function.
	 *
	 * @param   const daemon_job_record*	Record to evaluate
	 * @returns bool			True, is a candidate
	 */
	bool found_match(const daemon_job_record *rec) const;

	/**
	 * With the selection function evaluates each record in the SBT
	 * against the acceptance criteria and returns a pointer to the next
	 * matching record.
	 *
	 * @param   sbt_entry*&		Candidate SBT entry to start the
	 *				search with
	 * @param   struct isi_error**	Tracks search issues
	 * @returns daemon_job_record*	Pointer to next matching record
	 */
	daemon_job_record *get_next_match(const sbt_entry *&pentry,
									  struct isi_error **error_out);

	bool m_exclusive_mode;	 // Used with job locking
	sbt_reader m_sbt_reader; // Reads jobs from the SBT

	//
	// Filtration parameters
	//
	p_job_filter m_pfilter; // Search filter function
	const void *m_criteria; // Pointer to acceptance criteria
};
