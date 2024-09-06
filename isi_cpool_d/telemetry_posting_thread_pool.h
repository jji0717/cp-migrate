#ifndef __ISI_CPOOL_D_TELEMETRY_POSTING_THREADPOOL_H__
#define __ISI_CPOOL_D_TELEMETRY_POSTING_THREADPOOL_H__

#include <isi_cpool_config/cpool_d_config.h>
#include <isi_cpool_stats/cpool_stat_manager.h>

#include "thread_pool.h"
#include "report_writer.h"

class telemetry_posting_thread_pool : public thread_pool
{
protected:
	static void *start_thread(void *arg);

	/**
	 * @return the time at which the next event should occur
	 */
	time_t get_next_event_time(struct isi_error **error_out) const;

	/**
	 */
	void process_event(time_t event_time, struct isi_error **error_out);

	void post_telemetry(struct cpool_config_context *context,
	    const cpool_d_config_settings *config_settings,
	    struct isi_error **error_out);

	/* Default/copy construction and assignment are prohibited. */
	telemetry_posting_thread_pool();
	telemetry_posting_thread_pool(const telemetry_posting_thread_pool &);
	telemetry_posting_thread_pool &operator=(
	    const telemetry_posting_thread_pool &);

	/**
	 * Writes given statistical data to a report with the given filename
	 */
	static void write_stats_data_to_report(
	    std::vector<cpool_stat_time_record*> &stats,
	    const char *base_filename,
	    const struct cpool_config_context *context,
	    struct isi_error **error_out);

	/**
	 * Converts given timestamp to human readable string
	 */
	static const char * write_report_timestamp_to_string(time_t timestamp,
	    char *buffer, const unsigned int buffer_size);

	/**
	 * Writes the current date/time as a human readable string
	 */
	static void write_report_date(report_writer *writer,
	    struct isi_error **error_out);

	/**
	 * Writes the notes to the report.
	 */
	static void write_report_notes(report_writer *writer,
	     struct isi_error **error_out);

	/**
	 * Writes relevant account metadata to report
	 */
	static void write_report_account(report_writer *writer,
	    struct cpool_account *acct,
	    const struct cpool_config_context *context,
	    struct isi_error **error_out);

	/**
	 * Writes all accounts' metadata to report
	 */
	static void write_report_accounts(report_writer *writer,
	    const struct cpool_config_context *context,
	    struct isi_error **error_out);

	/**
	 * Writes relevant customer information to report
	 */
	static void write_report_customer_info(report_writer *writer,
	    struct isi_error **error_out);

	static void write_report_customer_license_info(report_writer *writer,
	    struct isi_error **error_out);

	static void write_report_version_info(report_writer *writer,
	    struct isi_error **error_out);

	static void write_report_node_info(report_writer *writer,
	    struct isi_error **error_out);

	/**
	 * Writes the body of the report (delegates to other write_report_*
	 * functions
	 */
	static void write_report_body(report_writer *writer,
	    std::vector<cpool_stat_time_record*> &stats,
	    struct isi_error **error_out);

	/**
	 * Writes the given account data record to report
	 */
	static void write_report_account_record(report_writer *writer,
	    cpool_stat_time_record *rec, struct isi_error **error_out);


public:
	telemetry_posting_thread_pool(int num_threads);
	~telemetry_posting_thread_pool();
};

#endif // __ISI_CPOOL_D_TELEMETRY_POSTING_THREADPOOL_H__
