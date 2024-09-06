#include <isi_cpool_config/cpool_config.h>
#include <isi_cpool_config/cpool_initializer.h>
#include <isi_cpool_config/gconfig_ufp.h>

#include "thread_mastership.h"
#include "auth_capability_refresh_thread_pool.h"

#define MASTER_FILE_NAME "auth_cap_refresh_master.txt"
#define MASTER_LOCKFILE_NAME MASTER_FILE_NAME ".lock"

#define MAX_GCONFIG_ATTEMPTS 5

void
auth_capability_refresh_thread::do_action_impl(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	struct cpool_config_context *context = NULL;

	for (int i = 0; i < MAX_GCONFIG_ATTEMPTS; ++i) {

		context = cpool_context_open(&error);
		ON_ISI_ERROR_GOTO(out, error);

		cpool_provider_auth_cap_reset_cache(context);

		rebuild_cap_records(context, &error);
		ON_ISI_ERROR_GOTO(out, error);

		cpool_context_commit(context, &error);

		UFP_TRIGGER_GCONFIG_CONFLICT(cpool_d_auth_cap_gconfig_conflict,
		    error)

		if (error == NULL)
			break;

		if (isi_error_is_a(error, GCI_RECOVERABLE_COMMIT_ERROR_CLASS) &&
		    i < MAX_GCONFIG_ATTEMPTS - 1) {

			isi_error_add_context(error,
			    "Error committing auth cap to gconfig");

			ilog(IL_DEBUG, "Conflict on attempt %d while writing "
			    "to gconfig: %#{}", i, isi_error_fmt(error));

			isi_error_free(error);
			error = NULL;

			cpool_context_close(context);
			context = NULL;
		}
		ON_ISI_ERROR_GOTO(out, error);

	}
out:
	if (context != NULL)
		cpool_context_close(context);

	isi_error_handle(error, error_out);
}

void
auth_capability_refresh_thread::do_thread_startup_initialization()
{
	/* Only need one node per cluster to do this */
	wait_for_mastership(MASTER_LOCKFILE_NAME, MASTER_FILE_NAME,
	    ACQUIRE_MASTERSHIP_SLEEP_INTERVAL);
}

void
auth_capability_refresh_thread::rebuild_cap_records(
    struct cpool_config_context *context, struct isi_error **error_out)
{
	ASSERT(context != NULL, "Expected non-NULL context");

	struct isi_error *error = NULL;

	// For each account, rebuild the capability cache
	struct cpool_account *it = cpool_account_get_iterator(context);
	for (; it != NULL; it = cpool_account_get_next(context, it)) {

		uint64_t acct_auth_cap = 0;

		cpool_initializer::build_provider_auth_cap_for_account(context,
		    it, &acct_auth_cap, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

out:
	isi_error_handle(error, error_out);
}
