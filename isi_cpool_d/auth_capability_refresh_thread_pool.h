#ifndef __ISI_CPOOL_D_AUTH_CAPABILITY_REFRESH_THREADPOOL_H__
#define __ISI_CPOOL_D_AUTH_CAPABILITY_REFRESH_THREADPOOL_H__

#include "stats_base_thread_pool.h"

const int AUTH_CAP_REFRESH_DEFAULT_SLEEP = 300;

// TODO:: Rename base class - it's much more generic than stats.  Will take that
// as a refactoring task for the future
class auth_capability_refresh_thread : public stats_base_thread_pool
{
protected:
	/* See comments in base class */
	float get_default_sleep_interval() const
	{
		return AUTH_CAP_REFRESH_DEFAULT_SLEEP;
	};

	/* See comments in base class */
	float get_sleep_interval_from_config_settings(
	    const cpool_d_config_settings &config_settings) const
	{
		return config_settings.
		    get_auth_capability_refresh_interval()->interval;
	};

	void do_action_impl(struct isi_error **error_out);

	void do_thread_startup_initialization();

	void rebuild_cap_records(struct cpool_config_context *context,
	    struct isi_error **error_out);

	/* Copy, and assignment constructions aren't allowed. */
	auth_capability_refresh_thread(const auth_capability_refresh_thread&);
	auth_capability_refresh_thread operator=(const auth_capability_refresh_thread&);

public:
	auth_capability_refresh_thread() {};
	~auth_capability_refresh_thread() {};
};

#endif // __ISI_CPOOL_D_AUTH_CAPABILITY_REFRESH_THREADPOOL_H__
