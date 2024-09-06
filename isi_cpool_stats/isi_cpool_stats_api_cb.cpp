#include "isi_cpool_stats.h"
#include "isi_cpool_stats_api_cb.h"
#include "cpool_stat_manager.h"
#include <isi_ilog/ilog.h>

void update_stats_operations(void *in_cb_ctx, const cloud_api_data &data)
{
	cpool_callback_ctx *cb_ctx = (cpool_callback_ctx *) in_cb_ctx;
	if (cb_ctx == NULL) {
		return;
	}

	uint32_t account_local_key = cb_ctx->account_key;
	cloud_operation op = cb_ctx->op;
	cpool_stat s;
	switch (op) {
	case CLOUD_PUT:
	case CLOUD_UPDATE:
		s.set_num_puts(1);
		s.set_bytes_out(data.out_bytes);
		break;
	case CLOUD_GET:
		s.set_num_gets(1);
		s.set_bytes_in(data.in_bytes);
		break;
	case CLOUD_DELETE:
		s.set_num_deletes(1);
		break;
	default:
		break;


	}

	struct isi_error *error = NULL;
	cpool_stat_manager *mgr = cpool_stat_manager::get_new_instance(&error);

	/*
	 * We'd better either have a NULL error or a NULL stat_manager
	 * but not both
	 */
	ASSERT((error == NULL) != (mgr == NULL));
	ON_ISI_ERROR_GOTO(out, error);

	ASSERT(mgr != NULL);

	mgr->add_to_stats(account_local_key/*account*/, s, &error);
	ON_ISI_ERROR_GOTO(out, error);
out:

	/*
	 * Since this is a callback, we can't really handle an error here.  The
	 * stat functionality isn't critical to operation anyway, so we suppress
	 * the error but log it with IL_ERR
	 */
	if (error != NULL) {
		ilog(IL_ERR, "Failed to update stats in memory: %#{}",
		    isi_error_fmt(error));
		isi_error_free(error);
	}

	if (mgr != NULL)
		delete mgr;
}
