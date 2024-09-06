#include <isi_util/isi_assert.h>
#include <isi_util/isi_error.h>
#include <isi_util/isi_format.h>
#include <isi_cloud_common/isi_cpool_version.h>

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "daemon_job_stats.h"

static void
pack(void **dst, const void *src, int size)
{
	ASSERT(dst != NULL);
	ASSERT(*dst != NULL);
	ASSERT(src != NULL);
	ASSERT(size >= 0);

	memcpy(*dst, src, size);
	*(char **)dst += size;
}

static void
unpack(void **dst, void *src, int size)
{
	ASSERT(dst != NULL);
	ASSERT(*dst != NULL);
	ASSERT(src != NULL);
	ASSERT(size >= 0);

	memcpy(src, *dst, size);
	*(char **)dst += size;
}

static void
stats_fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args,
    union fmt_conv_arg arg)
{
	const daemon_job_stats *stats =
	    static_cast<const daemon_job_stats *>(arg.ptr);
	ASSERT(stats != NULL);

	fmt_print(fmt, "{");
	fmt_print(fmt, "version: %d, ", stats->get_version());
	fmt_print(fmt, "num_members: %d, ", stats->get_num_members());
	fmt_print(fmt, "num_succeeded: %d, ", stats->get_num_succeeded());
	fmt_print(fmt, "num_failed: %d, ", stats->get_num_failed());
	fmt_print(fmt, "num_cancelled: %d, ", stats->get_num_cancelled());
	fmt_print(fmt, "num_pending: %d, ", stats->get_num_pending());
	fmt_print(fmt, "num_inprogress: %d", stats->get_num_inprogress());
	fmt_print(fmt, "}");
}

struct fmt_conv_ctx
cpool_djob_stats_fmt(const daemon_job_stats *stats)
{
	struct fmt_conv_ctx ctx = { stats_fmt_conv, { ptr: stats } };
	return ctx;
}

daemon_job_stats::daemon_job_stats()
	: version_(CPOOL_DAEMON_JOB_STATS_VERSION), num_members_(0)
	, num_succeeded_(0), num_failed_(0), num_cancelled_(0)
	, num_pending_(0), num_inprogress_(0), serialization_up_to_date_(false)
	, serialization_(NULL), serialization_size_(0)
{
}

daemon_job_stats::~daemon_job_stats()
{
	free(serialization_);
}

daemon_job_stats &
daemon_job_stats::operator=(const daemon_job_stats &rhs)
{
	ASSERT(version_ == rhs.version_);

	num_members_ = rhs.num_members_;
	num_succeeded_ = rhs.num_succeeded_;
	num_failed_ = rhs.num_failed_;
	num_cancelled_ = rhs.num_cancelled_;
	num_pending_ = rhs.num_pending_;
	num_inprogress_ = rhs.num_inprogress_;

	serialization_up_to_date_ = false;

	return *this;
}

bool
daemon_job_stats::operator==(const daemon_job_stats &rhs) const
{
	return !operator!=(rhs);
}

bool
daemon_job_stats::operator!=(const daemon_job_stats &rhs) const
{
	return (
	    (get_version() != rhs.get_version()) ||
	    (get_num_members() != rhs.get_num_members()) ||
	    (get_num_succeeded() != rhs.get_num_succeeded()) ||
	    (get_num_failed() != rhs.get_num_failed()) ||
	    (get_num_cancelled() != rhs.get_num_cancelled()) ||
	    (get_num_pending() != rhs.get_num_pending()) ||
	    (get_num_inprogress() != rhs.get_num_inprogress())
	    );
}

int
daemon_job_stats::get_version(void) const
{
	return version_;
}

int
daemon_job_stats::get_num_members(void) const
{
	return num_members_;
}

void
daemon_job_stats::increment_num_members(void)
{
	++num_members_;
	serialization_up_to_date_ = false;
}

void
daemon_job_stats::decrement_num_members(void)
{
	--num_members_;
	ASSERT(num_members_ >= 0);

	serialization_up_to_date_ = false;
}

int
daemon_job_stats::get_num_succeeded(void) const
{
	return num_succeeded_;
}

void
daemon_job_stats::increment_num_succeeded(void)
{
	++num_succeeded_;
	serialization_up_to_date_ = false;
}

int
daemon_job_stats::get_num_failed(void) const
{
	return num_failed_;
}

void
daemon_job_stats::increment_num_failed(void)
{
	++num_failed_;
	serialization_up_to_date_ = false;
}

int
daemon_job_stats::get_num_cancelled(void) const
{
	return num_cancelled_;
}

void
daemon_job_stats::increment_num_cancelled(void)
{
	++num_cancelled_;
	serialization_up_to_date_ = false;
}

int
daemon_job_stats::get_num_pending(void) const
{
	return num_pending_;
}

void
daemon_job_stats::increment_num_pending(void)
{
	++num_pending_;
	serialization_up_to_date_ = false;
}

void
daemon_job_stats::decrement_num_pending(void)
{
	--num_pending_;
	ASSERT(num_pending_ >= 0);

	serialization_up_to_date_ = false;
}

int
daemon_job_stats::get_num_inprogress(void) const
{
	return num_inprogress_;
}

void
daemon_job_stats::increment_num_inprogress(void)
{
	++num_inprogress_;
	serialization_up_to_date_ = false;
}

void
daemon_job_stats::decrement_num_inprogress(void)
{
	--num_inprogress_;
	ASSERT(num_inprogress_ >= 0);

	serialization_up_to_date_ = false;
}

size_t
daemon_job_stats::get_packed_size(void) const
{
	return sizeof version_ +
	    sizeof num_members_ +
	    sizeof num_succeeded_ +
	    sizeof num_failed_ +
	    sizeof num_cancelled_ +
	    sizeof num_pending_ +
	    sizeof num_inprogress_;
}

void
daemon_job_stats::pack(void **stream) const
{
	::pack(stream, &version_, sizeof version_);
	::pack(stream, &num_members_, sizeof num_members_);
	::pack(stream, &num_succeeded_, sizeof num_succeeded_);
	::pack(stream, &num_failed_, sizeof num_failed_);
	::pack(stream, &num_cancelled_, sizeof num_cancelled_);
	::pack(stream, &num_pending_, sizeof num_pending_);
	::pack(stream, &num_inprogress_, sizeof num_inprogress_);
}

void
daemon_job_stats::unpack(void **stream, isi_error **error_out)
{
	isi_error *error = NULL;
	::unpack(stream, &version_, sizeof version_);
	if (CPOOL_DAEMON_JOB_STATS_VERSION != get_version()) {
		error = isi_system_error_new(EPROGMISMATCH,
		    "Daemon Job Stats. Version mismatch. "
		    "expected version %d found version %d",
		    CPOOL_DAEMON_JOB_STATS_VERSION, get_version());
		goto out;
	}

	::unpack(stream, &num_members_, sizeof num_members_);
	::unpack(stream, &num_succeeded_, sizeof num_succeeded_);
	::unpack(stream, &num_failed_, sizeof num_failed_);
	::unpack(stream, &num_cancelled_, sizeof num_cancelled_);
	::unpack(stream, &num_pending_, sizeof num_pending_);
	::unpack(stream, &num_inprogress_, sizeof num_inprogress_);

 out:
	isi_error_handle(error, error_out);
}

void
daemon_job_stats::get_serialization(const void *&serialization,
    size_t &serialization_size) const
{
	if (!serialization_up_to_date_)
		serialize();

	serialization = serialization_;
	serialization_size = serialization_size_;
}

void
daemon_job_stats::set_serialization(const void *serialization,
    size_t serialization_size, struct isi_error **error_out)
{
	ASSERT(serialization != NULL);

	struct isi_error *error = NULL;

	free(serialization_);

	serialization_size_ = serialization_size;
	serialization_ = malloc(serialization_size_);
	if (serialization_ == NULL) {
		error = isi_system_error_new(ENOMEM, "malloc failed");
		goto out;
	}

	memcpy(serialization_, serialization, serialization_size_);

	deserialize(&error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	if (error != NULL) {
		free(serialization_);
		serialization_ = NULL;

		serialization_size_ = 0;
	}

	isi_error_handle(error, error_out);
}

void
daemon_job_stats::serialize(void) const
{
	free(serialization_);

	serialization_size_ = get_packed_size();

	serialization_ = malloc(serialization_size_);
	ASSERT(serialization_ != NULL);

	void *temp = serialization_;
	pack(&temp);

	serialization_up_to_date_ = true;
}

void
daemon_job_stats::deserialize(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	size_t correct_serialized_size = 0;
	void *temp = serialization_;

	unpack(&temp, &error);
	if (error != NULL)
		goto out;

	correct_serialized_size = get_packed_size();
	if (serialization_size_ != correct_serialized_size) {
		error = isi_system_error_new(EINVAL,
		    "received size (%zu) does not match expected size (%zu)",
		    serialization_size_, correct_serialized_size);
		goto out;
	}

	serialization_up_to_date_ = true;

 out:
	isi_error_handle(error, error_out);
}
