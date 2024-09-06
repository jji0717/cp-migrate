/*
 * Copyright (c) 2013
 *	Isilon Systems, LLC.  All rights reserved.
 */

#ifndef __NS_OBJECT_PROTECTION_H_
#define __NS_OBJECT_PROTECTION_H_

#include <ifs/ifs_syscalls.h>
#include <isi_rest_server/request.h>

/* protection setting related definitions. */
#define OHQ_PROTECTION			"protection"

#define ISI_NODE_POOL_NAME		"node_pool_name"

#define ISI_NODE_POOL_NAME_ANY		"ANY"

#define HDR_NODE_POOL_NAME  		"X_ISI_IFS_NODE_POOL_NAME"


 //added definitions
#define ISI_PROTECTION_POLICY	"policy"
#define ISI_PROTECTION_IS_ENFORCED	"is_enforced"
#define ISI_PROTECTION_LEVEL	"level"
#define ISI_PROTECTION_COALESCED	"coalesced"
#define ISI_PROTECTION_ENDURANT	 "endurant"
#define ISI_PROTECTION_PERFORMANCE	"performance"
#define ISI_PROTECTION_MANUALLY_MANAGE_ACCESS	"manually_manage_access"
#define ISI_PROTECTION_MANUALLY_MANAGE_PROTECTION	"manually_manage_protection"
#define ISI_PROTECTION_DISK_POOL_POLICY_ID	 "disk_pool_policy_id"
#define ISI_PROTECTION_DATA_DISK_POOL_ID	"data_disk_pool_id"
#define ISI_PROTECTION_METADATA_DISK_POOL_ID	"metadata_disk_pool_id"
#define ISI_PROTECTION_SSD_STRATEGY	"ssd_strategy"

inline bool
is_get_put_protection(const request &input)
{
	return input.get_main_arg() == OHQ_PROTECTION;
}

static inline bool
ifs_ssd_strategy_parse(const char *s, enum ifs_ssd_strategy *state)
{
	return fmt_scan_full_string(s, strlen(s), "%{}",
	    ssd_strategy_scan_fmt(state)) == 1;
}


class oapi_genobj;
class response;

class ns_object_protection {
public:
	/**
	 * Get the protection arguments based on headers from request
	 *
	 * @param[IN] input - a request object
	 *
	 * @return pointer pointing to newly allocated data of
	 * type iobj_set_protection_args
	 */
	static struct iobj_set_protection_args *get_obj_set_protection_args(
	    const request &input);



	/**
	 *Helper routies for protection get/put requests.
	 */
	 static void execute_get(const request &input, response &output);
	 static void execute_put(const request &input, response &output);

};

#endif  //__NS_OBJECT_PROTECTION_H_
