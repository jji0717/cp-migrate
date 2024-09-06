#include <isi_util/isi_error.h>

#include "isi_cpool_cbm.h"
#include "isi_cbm_access.h"


/* Goal:  Allow single-threaded apps to be forkable.
 *
 * This implementation relies on stub access not *implictly* inheriting across
 * children processes on creation (in the kernel).
 *
 * It also requires special considerations for initializations (that will be
 * added in the future) and that use static/global values.  This since
 * static variables retain their value across a fork. So such initializations
 * should provide/invoke cleanup routines for static. (C++ impls should use
 * phoneix singletons).  Or be unaffected by such statics being pre-set in the
 * new context.
 *
 * Discarded idea:
 * Getting the pid of the caller and comparing it against a stored
 * pid.  That *almost* works.  Where it fails is when pids are recycled:
 * consider P1, forking P2, P1 dying, and after a long time P2 forking P3.
 * Then, say, if P1 has called isi_cbm_init(), and P3 has the same pid as
 * P1, the comparison would succeed and P3 init would fail.
 */

void
isi_cbm_init(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool enabled;

	ASSERT(error_out);

	enabled = isi_cbm_is_stub_access_enabled(&error);
	if (error)
		goto out;
	ASSERT(!enabled);

	/*
	 * Placeholder: Perform other initializations here
	 */

	/*
	 * Clean out any [old] config that comes to us from the parent process.
	 * This allows forked-but-not-exec'ed child process to read the
	 * latest config on first use. 
	 */
	isi_cbm_unload_conifg();

	/*
	 * Last initialization
	 */
	isi_cbm_enable_stub_access(&error);
	if (error)
		goto out;

 out:
	isi_error_handle(error, error_out);
}

void
isi_cbm_destroy(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool enabled;

	ASSERT(error_out);

	enabled = isi_cbm_is_stub_access_enabled(&error);
	if (error)
		goto out;
	ASSERT(enabled);

	/*
	 * Placeholder: Perform other de-initializations here
	 */

	/*
	 * Clean out any [new] config that we may have allocated in
	 * this scope.
	 */
	isi_cbm_unload_conifg();

	/*
	 * Last de-initialization
	 */
	isi_cbm_disable_stub_access(&error);
	if (error)
		goto out;

 out:
	isi_error_handle(error, error_out);
}
