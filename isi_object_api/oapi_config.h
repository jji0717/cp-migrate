#ifndef __OAPI_CONFIG_H__
#define __OAPI_CONFIG_H__

#include <sys/types.h>

struct oapi_cfg_root;

class oapi_config {
public:
	static void notify_config_change(const oapi_cfg_root &cfg);

	static uint64_t get_max_sort_dir_sz();
private:
	static uint64_t max_sort_dir_sz_;
	static void set_max_sort_dir_sz(uint64_t sz);
};

#endif //__OAPI_CONFIG_H__
