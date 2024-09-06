#include <string.h>

#include <algorithm>

#include <isi_ilog/ilog.h>
#include <isi_util_cpp/isi_exception.h>
#include <isi_util_cpp/scoped_lock.h>

#include "api_error.h"
#include "license_info.h"

namespace {

const char *const LIC_EXPIRED_FMT =
    "The evaluation license key for %s has expired. Please "
    "contact your Isilon account team.";

const char *const LIC_UNLICENSED_FMT =
    "The %s application is not currently installed. "
    "Please contact your Isilon account team for more "
    "information on evaluating and purchasing %s.";

const char *const LIC_EXPIRED_REASON_FMT =
    "Failed: %s.\nReason: The evaluation license key for %s has expired. Please "
    "contact your Isilon account team.";

const char *const LIC_UNLICENSED_REASON_FMT =
    "Failed: %s.\nReason: The %s application is not currently installed. "
    "Please contact your Isilon account team for more "
    "information on evaluating and purchasing %s.";

}

license_info license_info::s_instance;

license_info::license_info()
	: mtx_(PTHREAD_MUTEX_INITIALIZER), last_update_(0), cache_time_(DEFAULT_CACHE_TIME)
{ }

license_info::~license_info()
{
}

isi_licensing_status
license_info::status(const char *name)
{
	return isi_licensing_module_status(name);
}

const char**
license_info::get()
{
	return isi_licensing_get_licenses();
}

struct isi_licensing_module
license_info::find(const char *name)
{
	return isi_licensing_get_module(name);
}

void
license_info::check(const char *name)
{
	return;
	// switch (status(name)) {
	// case ISI_LICENSING_LICENSED:
	// 	return;

	// case ISI_LICENSING_EXPIRED:
	// 	throw api_exception(AEC_FORBIDDEN, LIC_EXPIRED_FMT, name);
	// 	break;

	// case ISI_LICENSING_UNLICENSED:
	// 	throw api_exception(AEC_FORBIDDEN, LIC_UNLICENSED_FMT, name,
	// 	    name);
	// 	break;
	// }
}

void
license_info::check(const char *name, const char *field, const char *reason)
{
	switch (status(name)) {
	case ISI_LICENSING_LICENSED:
		return;

	case ISI_LICENSING_EXPIRED:
		if (field == NULL) {
			throw api_exception(AEC_FORBIDDEN, LIC_EXPIRED_REASON_FMT,
					    reason, name);
		} else {
			throw api_exception(field, AEC_FORBIDDEN, LIC_EXPIRED_REASON_FMT,
					    reason, name);
		}
		break;

	case ISI_LICENSING_UNLICENSED:
		if (field == NULL) {
			throw api_exception(AEC_FORBIDDEN, LIC_UNLICENSED_REASON_FMT, reason, name, name);
		} else {
			throw api_exception(field, AEC_FORBIDDEN, LIC_UNLICENSED_REASON_FMT, reason, name, name);
		}
		break;
	}
}

time_t
license_info::cache_time(time_t new_cache_time)
{
	scoped_lock lock(mtx_);
	std::swap(new_cache_time, cache_time_);
	return new_cache_time;
}

time_t
license_info::get_time()
{
	struct timespec now_ts;
	clock_gettime(CLOCK_MONOTONIC, &now_ts);
	return now_ts.tv_sec;
}
