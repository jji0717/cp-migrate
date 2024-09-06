#include "oapi.h"

#include <string>

#include <isi_object/iobj_licensing.h>
#include <isi_object/ostore.h>
#include <isi_util_cpp/isi_exception.h>
#include <isi_ilog/ilog.h>
#include "oapi_config.h"
#include "oapi_gcfg.h"
#include "oapi/oapi_store.h"
#include "oapi_mapper.h"


/*static*/
uint64_t oapi_config::max_sort_dir_sz_ = 100000;

// Do the application initialization here.
bool
init_oapi(const oapi_cfg_root &cfg)
{
	// initialize the license early:
	iobj_set_license_ID(ISI_LICENSING_OBJECT);

	ostore_set_system_dirs(cfg.system_dirs);

	oapi_config::notify_config_change(cfg);
	return true;
}

void
notify_config_change(const oapi_cfg_root &cfg)
{
	oapi_config::notify_config_change(cfg);
}
