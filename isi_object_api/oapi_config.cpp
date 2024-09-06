#include "oapi_config.h"

#include "oapi_gcfg.h"

void
oapi_config::notify_config_change(const oapi_cfg_root &cfg)
{
	set_max_sort_dir_sz(cfg.max_sort_dir_sz);
}

void
oapi_config::set_max_sort_dir_sz(uint64_t sz)
{
	max_sort_dir_sz_ = sz;
}

uint64_t
oapi_config::get_max_sort_dir_sz()
{
	return max_sort_dir_sz_;
}
