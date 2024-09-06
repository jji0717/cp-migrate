/*
 * Copyright (c) 2013
 *	Isilon Systems, LLC.  All rights reserved.
 */

#ifndef __OBJECT_WORM_H_
#define __OBJECT_WORM_H_

#include <isi_rest_server/request.h>
#include <isi_rest_server/api_utils.h>

/* protection setting related definitions. */
#define OHQ_WORM                              "worm"
#define OHQ_FOLLOW_SYMLINKS                   "follow_symlinks"

#define ISI_WORM_COMMITTED                    "worm_committed"
#define ISI_WORM_RETENTION_DATE               "worm_retention_date"
#define ISI_WORM_RETENTION_DATE_VAL           "worm_retention_date_val"
#define ISI_WORM_OVERRIDE_RETENTION_DATE      "worm_override_retention_date"
#define ISI_WORM_OVERRIDE_RETENTION_DATE_VAL  "worm_override_retention_date_val"
#define ISI_WORM_ROOT_PATH                    "domain_path"
#define ISI_WORM_DOMAIN_ID                    "domain_id"
#define ISI_WORM_AUTOCOMMIT_OFFSET            "autocommit_delay"
#define ISI_WORM_CTIME                        "worm_ctime"
#define ISI_WORM_ANCESTORS                    "worm_ancestor_list"

#define ISI_COMMIT_TO_WORM                    "commit_to_worm"

class response;

inline bool
is_worm_request(const request &input)
{
	std::string s = "";
	return (find_args(s, input.get_arg_map(), OHQ_WORM) == 1);
}

/**
 * Collection of helper routine(s) for handling WORM related request
 */
class ns_object_worm {

public:

	/**
	 * These helper routines processes WORM get/put requests.
	 */
	static void execute_get(const request &input, response &output);
	static void execute_put(const request &input, response &output);

};

#endif  //__OBJECT_WORM_H_
