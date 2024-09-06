#ifndef __OAPI_H__
#define __OAPI_H__

#include <sys/types.h>

struct oapi_cfg_root;
/**
 * Initialize the application.
 * @return true on success false on failure.
 */
bool init_oapi(const oapi_cfg_root &cfg);

/**
 * Notification of the configuration change.
 */
void notify_config_change(const oapi_cfg_root &cfg);

#endif //__OAPI_H__
