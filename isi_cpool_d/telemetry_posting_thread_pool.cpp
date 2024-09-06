#include <signal.h>

#include <isi_config/array.h>
#include <isi_cpool_cbm/isi_cbm_id.h>
#include <isi_cpool_cbm/isi_cbm_scoped_ppi.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_config/cpool_config.h>
#include <isi_cpool_config/cpool_d_config.h>
#include <isi_cpool_config/cpool_gcfg.h>
#include <isi_cpool_config/scoped_cfg.h>
#include <isi_cpool_d_common/cpool_d_debug.h>
#include <isi_hal/hal.h>
#include <isi_ilog/ilog.h>
#include <isi_version/isi_version.h>
#include <isi_util/isi_assert.h>
#include <isi_util/isi_printf.h>
#include <isi_util_cpp/scoped_clean.h>

#include "report_writer.h"
#include "scheduler.h"
#include "telemetry_posting_thread_pool.h"
#include "thread_mastership.h"

#define MASTER_FILE_NAME "telemetry_posting_master.txt"
#define MASTER_LOCKFILE_NAME MASTER_FILE_NAME ".lock"

#define REPORT_TAG_ROOT "telemetry"
#define REPORT_TAG_REPORT_DATE "date"
#define REPORT_TAG_REPORT_NOTES "notes"
#define REPORT_TAG_REPORT_NOTE "note"
#define REPORT_TAG_REPORT_DATE_TS "timestamp"
#define REPORT_TAG_CUST_INFO "customer-info"
#define REPORT_TAG_BODY "data"
#define REPORT_TAG_ACCT_STAT_CONTAINER "record"
#define REPORT_TAG_ACCT_KEY "account-key"
#define REPORT_TAG_ACCT_USAGE "usage"
#define REPORT_TAG_TIMESTAMP "timestamp"
#define REPORT_TAG_GMT_DATE "date-gmt"
#define REPORT_TAG_ACCOUNT_CONTAINER "accounts"
#define REPORT_TAG_ACCOUNT "account"
#define REPORT_TAG_ACCT_ATTRIB_KEY "key"
#define REPORT_TAG_ACCT_ATTRIB_TYPE "type"
#define REPORT_TAG_ACCT_ATTRIB_NAME "name"
#define REPORT_TAG_ACCT_POOL_NAME "pool"
#define REPORT_VALUE_EMPTY_POOL ""
#define REPORT_LABEL_ACCT_TYPE_RAN "ran"
#define REPORT_LABEL_ACCT_TYPE_AZURE "azure"
#define REPORT_LABEL_ACCT_TYPE_S3 "s3"
#define REPORT_LABEL_ACCT_TYPE_ECS "ecs"
#define REPORT_LABEL_ACCT_TYPE_ECS2 "virtustream"
#define REPORT_LABEL_ACCT_TYPE_GOOGLE_XML "google-xml"
#define REPORT_TAG_LIC_INFO "license_info"
#define REPORT_TAG_VERSION_INFO "onefs_version"
#define REPORT_TAG_RELEASE "release"
#define REPORT_TAG_VERSION "version"
#define REPORT_TAG_OS_TYPE "type"
#define REPORT_TAG_BUILD "build"
#define REPORT_TAG_REVISION "revision"
#define REPORT_TAG_LOCAL_DEVID "local_devid"
#define REPORT_TAG_LOCAL_LNN "local_lnn"
#define REPORT_TAG_LOCAL_SERIAL "local_serial"
#define REPORT_TAG_CLUSTER_NAME "name"
#define REPORT_TAG_CLUSTER_GUID "guid"
#define REPORT_TAG_DEVICES "devices"
#define REPORT_TAG_DEV_GUID "guid"
#define REPORT_TAG_DEV_ID "devid"
#define REPORT_TAG_DEV_LNN "lnn"

#define LICENSING_FILE "/etc/ifs/licensing/CloudPools"

char note_1[2048] = "Linking of S3 accounts is supported by Amazon for "
		      "billing purposes. Accounts usage data reported by "
		      "Cloudpools will not be accurate if the S3 accounts "
		      "are linked. Currently there are no other impacts in "
		      "Cloudpools due to linking S3 accounts. ";


telemetry_posting_thread_pool::telemetry_posting_thread_pool(int num_threads)
{
	ASSERT(num_threads == 1,
	    "only 1 thread is supported (not %d)", num_threads);

	for (int i = 0; i < num_threads; ++i)
		add_thread(NULL, start_thread, this);
}

telemetry_posting_thread_pool::~telemetry_posting_thread_pool()
{
	shutdown();
}

void * ///////在bin/isi_cpool_d/main.cpp中telemetry_posting_thread_pool就启动一个线程 
telemetry_posting_thread_pool::start_thread(void *arg)
{
	ASSERT(arg != NULL);

	telemetry_posting_thread_pool *tp =
	    static_cast<telemetry_posting_thread_pool *>(arg);
	ASSERT(tp != NULL);

	const int seconds_between_failed_parse_attempts = 60;

	/*
	 * Block SIGTERM and SIGHUP in this thread as they are handled by the
	 * isi_cpool_d main thread (and not here).
	 */
	sigset_t s;
	sigemptyset(&s);
	sigaddset(&s, SIGTERM);
	sigaddset(&s, SIGHUP);
	sigaddset(&s, SIGUSR1);
	pthread_sigmask(SIG_BLOCK, &s, NULL);

	tp->wait_for_mastership(MASTER_LOCKFILE_NAME, MASTER_FILE_NAME,
	    ACQUIRE_MASTERSHIP_SLEEP_INTERVAL);

	while (!tp->is_shutdown_pending()) {
		struct isi_error *error = NULL;

		struct timeval next_event_tm = {
			.tv_sec = tp->get_next_event_time(&error),
			.tv_usec = 0
		};

		if (error != NULL) {
			ilog(IL_ERR,
			    "error determining next "
			    "telemetry posting event time: %#{}",
			    isi_error_fmt(error));

			tp->sleep_for(seconds_between_failed_parse_attempts);

			isi_error_free(error);
			error = NULL;
		}
		/////jjz  tp->sleep_for(300)
		else if (tp->sleep_for(300)) {           /////////tp->sleep_until(next_event_tm)
			tp->process_event(next_event_tm.tv_sec, &error);
			if (error != NULL) {
				ilog(IL_ERR,
				    "error processing "
				    "telemetry posting event: %#{}",
				    isi_error_fmt(error));
				isi_error_free(error);
				error = NULL;
			}
		}
	}

	ilog(IL_NOTICE, "thread exiting due to shutdown");

	pthread_exit(NULL);
}

time_t
telemetry_posting_thread_pool::get_next_event_time(
    struct isi_error **error_out) const
{
	struct isi_error *error = NULL;
	time_t next_posting_time = 0;

	const cpool_d_config_settings *config_settings = NULL;
	const struct cpool_d_thread_schedule_info *posting_info = NULL;
	time_t starting_from = time(NULL);
	scheduler scheduler;

	scoped_cfg_reader reader;
	config_settings = reader.get_cfg(&error);
	ON_ISI_ERROR_GOTO(out, error);

	posting_info = config_settings->get_usage_posting_schedule_info();

	next_posting_time =
	    scheduler.get_next_time(posting_info->schedule_info,
	    starting_from, &error);
	ON_ISI_ERROR_GOTO(out, error);
 out:
	isi_error_handle(error, error_out);

	return next_posting_time;
}

void
telemetry_posting_thread_pool::write_stats_data_to_report(
    std::vector<cpool_stat_time_record*> &stats/*stat,acct_key,timestamp*/, const char *base_filename,
    const struct cpool_config_context *context, struct isi_error **error_out)
{
	ASSERT(base_filename != NULL);

	struct isi_error *error = NULL;
	report_writer *writer = NULL;

	const unsigned int buf_size = strlen(base_filename) * 4 + 1;
	char *filename_buf = (char*) calloc (1, buf_size);
	time_t now = time(NULL);
	struct tm *local = localtime(&now);

	strftime(filename_buf, buf_size - 1, base_filename, local);

	writer = report_writer::create_report_writer(filename_buf, &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->start_member(REPORT_TAG_ROOT, &error);
	ON_ISI_ERROR_GOTO(out, error);

	write_report_date(writer, &error);
	ON_ISI_ERROR_GOTO(out, error);

	write_report_notes(writer, &error);
	ON_ISI_ERROR_GOTO(out, error);

	write_report_customer_info(writer, &error);
	ON_ISI_ERROR_GOTO(out, error);

	write_report_accounts(writer, context, &error);
	ON_ISI_ERROR_GOTO(out, error);

	write_report_body(writer, stats, &error);
	ON_ISI_ERROR_GOTO(out, error);

	// End root
	writer->end_member(&error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	if (writer != NULL)
		delete writer;

	isi_error_handle(error, error_out);

}

const char *
telemetry_posting_thread_pool::write_report_timestamp_to_string(
    time_t timestamp, char *buffer, const unsigned int buffer_size)
{
	ASSERT(buffer != NULL);
	/*
	 * Per asctime_r man page - buffer must be at least 26 characters long
	 */
	ASSERT(buffer_size >= 26);

	struct tm timeinfo;
	gmtime_r(&timestamp, &timeinfo);
	asctime_r(&timeinfo, buffer);

	return buffer;
}

void
telemetry_posting_thread_pool::write_report_date(report_writer *writer,
    struct isi_error **error_out)
{
	ASSERT(writer != NULL);

	struct isi_error *error = NULL;
	char time_buffer[80];
	const time_t cur_time = time(NULL);

	writer->write_member(REPORT_TAG_REPORT_DATE,
	    write_report_timestamp_to_string(cur_time, time_buffer,
	    sizeof(time_buffer)), &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_member(REPORT_TAG_REPORT_DATE_TS, cur_time, &error);
	ON_ISI_ERROR_GOTO(out, error);
out:
	isi_error_handle(error, error_out);
}

void
telemetry_posting_thread_pool::write_report_notes(report_writer *writer,
    struct isi_error **error_out)
{
	ASSERT(writer != NULL);

	struct isi_error *error = NULL;

	writer->start_member(REPORT_TAG_REPORT_NOTES, &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_member(REPORT_TAG_REPORT_NOTE, note_1,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->end_member(&error);

out:
	isi_error_handle(error, error_out);
}


void
telemetry_posting_thread_pool::write_report_account(report_writer *writer,
    struct cpool_account *acct, const struct cpool_config_context *context,
    struct isi_error **error_out)
{
	ASSERT(writer != NULL);
	ASSERT(acct != NULL);
	ASSERT(context != NULL);

	struct isi_error *error = NULL;
	struct cpool_provider_instance *referencing_pool = NULL;

	writer->start_member(REPORT_TAG_ACCOUNT, &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_attribute(REPORT_TAG_ACCT_ATTRIB_KEY,
	    cpool_account_get_local_key(acct), &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_attribute(REPORT_TAG_ACCT_ATTRIB_NAME,
	    cpool_account_get_name(acct), &error);
	ON_ISI_ERROR_GOTO(out, error);

	switch(cpool_account_get_type(acct)) {
		case CPT_RAN:
			writer->write_attribute(REPORT_TAG_ACCT_ATTRIB_TYPE,
			    REPORT_LABEL_ACCT_TYPE_RAN, &error);
			break;

		case CPT_AZURE:
			writer->write_attribute(REPORT_TAG_ACCT_ATTRIB_TYPE,
			    REPORT_LABEL_ACCT_TYPE_AZURE, &error);
			break;

		case CPT_AWS:
			writer->write_attribute(REPORT_TAG_ACCT_ATTRIB_TYPE,
			    REPORT_LABEL_ACCT_TYPE_S3, &error);
			break;

		case CPT_ECS:
			writer->write_attribute(REPORT_TAG_ACCT_ATTRIB_TYPE,
			    REPORT_LABEL_ACCT_TYPE_ECS, &error);
			break;

		case CPT_ECS2:
			writer->write_attribute(REPORT_TAG_ACCT_ATTRIB_TYPE,
			    REPORT_LABEL_ACCT_TYPE_ECS2, &error);
			break;

		case CPT_GOOGLE_XML:
			writer->write_attribute(REPORT_TAG_ACCT_ATTRIB_TYPE,
			    REPORT_LABEL_ACCT_TYPE_GOOGLE_XML, &error);
			break;

		default:
			// Unknown type!
			ASSERT(0);
	}
	ON_ISI_ERROR_GOTO(out, error);

	referencing_pool = cpool_account_get_referencing_provider(context,
	    acct);
	if (referencing_pool != NULL)
		writer->write_attribute(REPORT_TAG_ACCT_POOL_NAME,
			referencing_pool->provider_instance_name, &error);

	else
		writer->write_attribute(REPORT_TAG_ACCT_POOL_NAME,
		    REPORT_VALUE_EMPTY_POOL, &error);

	ON_ISI_ERROR_GOTO(out, error);

	// End account
	writer->end_member(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
telemetry_posting_thread_pool::write_report_accounts(report_writer *writer,
    const struct cpool_config_context *context, struct isi_error **error_out)
{
	ASSERT(writer != NULL);
	ASSERT(context != NULL);

	struct isi_error *error = NULL;

	writer->start_member(REPORT_TAG_ACCOUNT_CONTAINER, &error);
	ON_ISI_ERROR_GOTO(out, error);

	for (struct cpool_account *acct = cpool_account_get_iterator(context);
	    acct != NULL; acct = cpool_account_get_next(context, acct)) {

		write_report_account(writer, acct, context, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	// End account container
	writer->end_member(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
telemetry_posting_thread_pool::write_report_customer_license_info(
    report_writer *writer, struct isi_error **error_out)
{
	ASSERT(writer != NULL);

	struct isi_error *error = NULL;
	FILE *f = NULL;
	const unsigned int buffer_size = 2048;
	char license_buffer[buffer_size];
	char encoded_buffer[buffer_size];
	int bytes_read = 0;


	/* Read licensing file */
	f = fopen(LICENSING_FILE, "r");
	if (f == NULL) {
		error = isi_system_error_new(errno,
		    "Could not open licensing file");
		ON_ISI_ERROR_GOTO(out, error);
	}

	bytes_read = fread(license_buffer, 1, buffer_size - 1, f);
	if (bytes_read <= 0 && ferror(f)) {
		error = isi_system_error_new(errno,
		    "Could not read licensing file");
		ON_ISI_ERROR_GOTO(out, error);
	}

	license_buffer[bytes_read] = 0;

	/* File read, encode newlines */
	report_writer::encode_string(license_buffer, encoded_buffer);

	writer->write_member(REPORT_TAG_LIC_INFO, encoded_buffer, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	if (f != NULL)
		fclose(f);

	isi_error_handle(error, error_out);
}

void
telemetry_posting_thread_pool::write_report_version_info(report_writer *writer,
    struct isi_error **error_out)
{
	ASSERT(writer != NULL);

	struct isi_error *error = NULL;

	writer->start_member(REPORT_TAG_VERSION_INFO, &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_member(REPORT_TAG_RELEASE, get_kern_osrelease(), &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_member(REPORT_TAG_VERSION, get_kern_version(), &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_member(REPORT_TAG_OS_TYPE, get_kern_ostype(), &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_member(REPORT_TAG_BUILD, get_kern_osbuild(), &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_member(REPORT_TAG_REVISION, get_kern_osrevision(),
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	// End onefs_verison
	writer->end_member(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
telemetry_posting_thread_pool::write_report_node_info(report_writer *writer,
    struct isi_error **error_out)
{
	ASSERT(writer != NULL);

	struct isi_error *error = NULL;
	struct arr_config *ac = NULL;
	struct arr_device *dev = NULL;
	ifs_devid_t devid = 0;
	char buf[256];
	void **dppv;

	scoped_ptr_clean<arr_config> arr_config_clean(ac, arr_config_free);

	// Load cluster info
	ac = arr_config_load(&error);
	ON_ISI_ERROR_GOTO(out, error);

	devid = arr_config_get_local_devid(ac);
	writer->write_member(REPORT_TAG_LOCAL_DEVID, devid, &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_member(REPORT_TAG_LOCAL_LNN,
	    arr_config_get_lnn(ac, devid), &error);
	ON_ISI_ERROR_GOTO(out, error);

	hal_get_chassis_serial_number(0, buf, sizeof buf);
	writer->write_member(REPORT_TAG_LOCAL_SERIAL, buf, &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_member(REPORT_TAG_CLUSTER_NAME, arr_config_get_name(ac),
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	isi_snprintf(buf, sizeof buf, "%{}",
	    arr_guid_fmt(arr_config_get_guid(ac)));
	writer->write_member(REPORT_TAG_CLUSTER_GUID, buf, &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->start_member(REPORT_TAG_DEVICES, &error);
	ON_ISI_ERROR_GOTO(out, error);

	ARR_DEVICE_FOREACH(dppv, dev, arr_config_get_devices(ac)) {

		isi_snprintf(buf, sizeof buf, "%{}",
		    arr_guid_fmt(arr_device_get_guid(dev)));

		writer->write_member(REPORT_TAG_DEV_GUID, buf, &error);
		ON_ISI_ERROR_GOTO(out, error);

		writer->write_member(REPORT_TAG_DEV_ID,
		    arr_device_get_devid(dev), &error);
		ON_ISI_ERROR_GOTO(out, error);

		writer->write_member(REPORT_TAG_DEV_LNN,
		    arr_device_get_lnn(dev), &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	// End devices
	writer->end_member(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
telemetry_posting_thread_pool::write_report_customer_info(
    report_writer *writer, struct isi_error **error_out)
{
	ASSERT(writer != NULL);

	struct isi_error *error = NULL;

	writer->start_member(REPORT_TAG_CUST_INFO, &error);
	ON_ISI_ERROR_GOTO(out, error);

	write_report_customer_license_info(writer, &error);
	ON_ISI_ERROR_GOTO(out, error);

	write_report_version_info(writer, &error);
	ON_ISI_ERROR_GOTO(out, error);

	write_report_node_info(writer, &error);
	ON_ISI_ERROR_GOTO(out, error);

	// End customer info
	writer->end_member(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
telemetry_posting_thread_pool::write_report_body(report_writer *writer,
    std::vector<cpool_stat_time_record*> &stats, struct isi_error **error_out)
{
	ASSERT(writer != NULL);

	struct isi_error *error = NULL;

	std::vector<cpool_stat_time_record*>::iterator it;

	writer->start_member(REPORT_TAG_BODY, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/* Iterate through all data and dump it as we go */
	for (it = stats.begin(); it != stats.end(); ++it) {

		write_report_account_record(writer, (*it), &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	// End body
	writer->end_member(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
telemetry_posting_thread_pool::write_report_account_record(
    report_writer *writer, cpool_stat_time_record *rec,
    struct isi_error **error_out)
{
	ASSERT(writer != NULL);
	ASSERT(rec != NULL);

	struct isi_error *error = NULL;

	const cpool_stat &cur_stat = rec->get_stats();
	const cpool_stat_account_key &key = rec->get_key();
	const time_t timestamp = rec->get_timestamp();
	char time_buffer[80];

	writer->start_member(REPORT_TAG_ACCT_STAT_CONTAINER, &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_member(REPORT_TAG_ACCT_KEY, key.get_account_key(),
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_member(REPORT_TAG_ACCT_USAGE, cur_stat.get_total_usage(),
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_member(REPORT_TAG_GMT_DATE,
	    write_report_timestamp_to_string(timestamp, time_buffer,
	    sizeof(time_buffer)), &error);
	ON_ISI_ERROR_GOTO(out, error);

	writer->write_member(REPORT_TAG_TIMESTAMP, timestamp, &error);
	ON_ISI_ERROR_GOTO(out, error);

	// End account stat container
	writer->end_member(&error);
	ON_ISI_ERROR_GOTO(out, error);
out:
	isi_error_handle(error, error_out);
}

void
telemetry_posting_thread_pool::post_telemetry(
    struct cpool_config_context *context,
    const cpool_d_config_settings *config_settings,
    struct isi_error **error_out)
{
	ASSERT(context != NULL);
	ASSERT(config_settings != NULL);

	struct isi_error *error = NULL;

	std::vector<cpool_stat_time_record*> stats;
	std::vector<account_key_t> account_keys;
	cpool_stat_manager *mgr = NULL;
	const char *filename = cpool_settings_get_telemetry_reporting_file(
	    context);

	const time_t now = time(NULL);
	unsigned int seconds_to_go_back = 60*60*24*31;/////31天
	const struct cpool_d_thread_schedule_info *posting_info =
	    config_settings->get_usage_posting_schedule_info();
	const unsigned int seconds_since_last_post =
	    now - posting_info->last_run_time;       //////31天前

	if (filename == NULL || filename[0] == 0) {
		ON_SYSERR_GOTO(out, EINVAL, error,
		    "No telemetry filename found.  Cannot process report");
	}

	/* We want to query for at least a month's data */
	if (seconds_since_last_post > seconds_to_go_back)    /////查询一个月的时间
		seconds_to_go_back = seconds_since_last_post;

	mgr = cpool_stat_manager::get_new_instance(&error);
	ON_ISI_ERROR_GOTO(out, error);

	for (struct cpool_account *acct = cpool_account_get_iterator(context);
	    acct != NULL; acct = cpool_account_get_next(context, acct))

		account_keys.push_back(cpool_account_get_local_key(acct));

	mgr->get_all_data_for_accounts(stats/*stat, acct_key, timestamp*/, seconds_to_go_back/*31天之内的数据*/, /////////获取每一个account,对应的stats(cpool_stat)
	    &account_keys, &error);
	ON_ISI_ERROR_GOTO(out, error);

	write_stats_data_to_report(stats, filename, context, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	if (mgr != NULL)
		delete mgr;

	isi_error_handle(error, error_out);
}

void
telemetry_posting_thread_pool::process_event(time_t event_time,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct cpool_config_context *context = NULL;

	cpool_d_config_settings config_settings;
	scoped_cfg_writer writer;

	config_settings.reload(&error);
	ON_ISI_ERROR_GOTO(out, error);

	context = cpool_context_open(&error);
	ON_ISI_ERROR_GOTO(out, error);

	post_telemetry(context, &config_settings, &error);
	ON_ISI_ERROR_GOTO(out, error);

	config_settings.update_last_usage_posting_time();

	config_settings.commit(&error);
	ON_ISI_ERROR_GOTO(out, error);

	/* Since we just wrote to config, make sure that it is refreshed */
	writer.reset_cfg(&error);
	ON_ISI_ERROR_GOTO(out, error);
 out:

	if (context != NULL)
		cpool_context_close(context);

	isi_error_handle(error, error_out);
}
