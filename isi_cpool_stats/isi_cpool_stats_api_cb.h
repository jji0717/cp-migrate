#ifndef __ISI_CPOOL_STATS_API_CB_H__
#define __ISI_CPOOL_STATS_API_CB_H__
#include <isi_cloud_api/isi_cloud_api_callback.h>

typedef enum {
	CLOUD_PUT	= 1,
	CLOUD_GET 	= 2,
	CLOUD_UPDATE 	= 3,
	CLOUD_DELETE 	= 4,
	CLOUD_NONE 	= 5,
	CLOUD_LAST_OP 	= 6,
} cloud_operation;

typedef struct {
	uint32_t account_key;
	cloud_operation op;
} cpool_callback_ctx;

void update_stats_operations(void *cb_ctx, const cloud_api_data &data);

#endif // __ISI_CPOOL_STATS_API_CB_H__

