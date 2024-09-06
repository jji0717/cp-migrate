#include <unistd.h>

#include <errno.h>
#include <pwd.h>
#include <sys/event.h>
#include <sys/isi_ntoken.h>
#include <sys/proc.h>
#include <sys/time.h>
#include <sys/types.h>

#include <isi_cpool_cbm/isi_cbm_access.h>
#include <isi_ilog/ilog.h>
#include <isi_util_cpp/safe_strerror.h>

/**
 * Check to see if stub access has been enabled for the current process
 */
bool
isi_cbm_is_stub_access_enabled(struct isi_error **error_out)
{
	struct procoptions po = PROCOPTIONS_INIT;
	struct isi_error *error = NULL;
	bool enabled = false;

	if (getprocoptions(&po)) {
		error = isi_system_error_new(errno,
		    "Could not get current process options from kernel");
		goto out;
	}

	enabled = ((po.po_isi_flag_on & PI_CPOOL_ALLOW_ACCESS_MASK) ==
	    PI_CPOOL_ALLOW_ACCESS_MASK);

 out:
	if (error)
		isi_error_handle(error, error_out);

	return enabled;
}

/*
 * Toggle access to the stub files.
 */
static void
isi_cbm_set_stub_access(bool setoption, struct isi_error **error_out)
{
	struct procoptions po = PROCOPTIONS_INIT;
	struct isi_error *error = NULL;

	if (getprocoptions(&po)) {
		error = isi_system_error_new(errno,
		    "Could not get current process options from kernel");
		goto out;
	}

	if (setoption)
		po.po_isi_flag_on |= PI_CPOOL_ALLOW_ACCESS_MASK;
	else
		po.po_isi_flag_off |= PI_CPOOL_ALLOW_ACCESS_MASK;

	if (setprocoptions(&po) < 0) {
		error = isi_system_error_new(errno,
		    "Failed to enable access to stubbed files");
		goto out;
	}
 out:
	if (error) {
		ilog(IL_ERR,
		    "Failed to enable access to stubbed files: %s (%d)",
		    strerror(errno), errno);
		isi_error_handle(error, error_out);
	}

}

void
isi_cbm_enable_stub_access(struct isi_error **error_out)
{
	isi_cbm_set_stub_access(true, error_out);
}

void
isi_cbm_disable_stub_access(struct isi_error **error_out)
{
	isi_cbm_set_stub_access(false, error_out);
}

/*
 * set/clear the per-process no-atime flag
 */
static void
isi_cbm_set_no_atime(bool setoption, struct isi_error **error_out)
{
	struct procoptions po = PROCOPTIONS_INIT;
	struct isi_error *error = NULL;

	if (getprocoptions(&po)) {
		error = isi_system_error_new(errno,
		    "Could not get current process options from kernel");
		goto out;
	}

	if (setoption)
		po.po_isi_flag_on |= PI_NO_ATIME_UPDATES;
	else
		po.po_isi_flag_off |= PI_NO_ATIME_UPDATES;

	if (setprocoptions(&po) < 0) {
		error = isi_system_error_new(errno,
		    "Failed to %s atime update to stubbed files",
		    (setoption ? "disable" : "enable"));
		goto out;
	}
 out:
	if (error) {
		ilog(IL_ERR,
		    "Failed to set atime update to stubbed files: %s (%d)",
		    strerror(errno), errno);
		isi_error_handle(error, error_out);
	}

}

void
isi_cbm_disable_atime_updates(struct isi_error **error_out)
{
	isi_cbm_set_no_atime(true, error_out);
}

void
isi_cbm_enable_atime_updates(struct isi_error **error_out)
{
	isi_cbm_set_no_atime(false, error_out);
}
