#include "isi_cbm_test_util_ads.h"
#include "isi_cbm_file.h"

struct isi_error *
cbm_test_precreate_cacheinfo(uint64_t lin)
{
	struct isi_error *error = NULL;
	struct isi_cbm_file *file_obj = NULL;

	file_obj = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	if (error)
		goto out;
		
	isi_cbm_file_open_and_or_init_cache(file_obj, false, false, &error);

 out:
	return error;
}

