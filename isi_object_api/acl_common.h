/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	acl_common.h __ lchampakesan. 
 */

#ifndef __ACL_COMMON_H__
#define __ACL_COMMON_H__

#include <errno.h>
#include <isi_ilog/ilog.h>
#include <isi_rest_server/request.h>
#include "oapi/oapi_genobj.h"
#include "oapi/oapi_acl.h"

#include <isi_json/json_helpers.h>
#include "handler_common.h"

/* ACL related definitions. */
#define OHQ_ACL		        "acl"
#define OHQ_ACL_SIZE            sizeof (OHQ_ACL) - 1

#define ACCESSTYPE         "accesstype"
#define ACCESSRIGHTS       "accessrights"
#define INHERIT_FLAGS      "inherit_flags"
#define AUTHORITATIVE      "authoritative"
#define ACL                "acl"
#define MODE               "mode"
#define OWNER              "owner"
#define USER               "user"
#define GROUP              "group"
#define WELLKNOWN          "wellknown"
#define UNKNOWN            "Unknown"
#define TRUSTEE            "trustee"
#define ALLOW              "allow"
#define DENY               "deny"
#define READ               "Read"
#define WRITE              "Write"
#define READ_ACL           "Read ACL"
#define WRITE_ACL          "Write ACL"
#define FULLACCESS         "Full access"

/* 
 * Defined in the order of minimum access rights to 
 * maximum access rights 
*/

/* Owner has read acl/write acl and read/read attributes access */
#define PRIVATE_READ       "private_read"    
/* Owner has full access. */
#define PRIVATE            "private"       
/* Owner has full access. Everyone has read and read attributes access. */
#define PUBLIC_READ        "public_read"
/* Owner has full access. Everyone has read/write and read/write attributes access. */
#define PUBLIC_READ_WRITE  "public_read_write" 
/* Everyone has all access (including read acl/write acl access). */
#define PUBLIC             "public"

extern const std::string HDR_ACCESS_CONTROL;
extern const std::string HHDR_ACCESS_CONTROL;

/**
 * This function checks the input request argument.
 * If the input request is ?acl then return true else 
 * return false.
 */

inline bool
is_get_put_acl(const request &input)
{
	const std::string &main_arg = input.get_main_arg();

	return main_arg == OHQ_ACL;
}


/**
 * Validate and populate oapi_acl object and acl modification type
 * based on @j_input
 *
 * @param[IN]     j_input  json input with ace entries.
 * @param[IN]     oacl     acl object.
 * @param[INOUT]  acl_mod_type modification type
 *
 * @throws api_exception if input is invalid.
 */
void validate_input_acl(const Json::Value &j_input, oapi_acl &oacl,
    enum ifs_acl_mod_type *acl_mod_type);

/**
 * Update the owner/group for security descriptor @sd_new
 *
 * If owner/group is specified in @j_input and is different than
 * the one in @sd, then it will be updated to @sd_new; otherwise,
 * no change to @sd_new
 *
 * @pre @sd_new's owner or group fields must be NULL.
 * the resource elements of @sd is not constructed in one chunk.
 *
 * @param[IN]     j_input  json input with owner and group persona.
 * @param[INOUT]  sd       security descriptor to update.
 * @param[INOUT]  sd_new   where owner and group updated to
 *
 * @throws api_exception if input is invalid.
 */
void update_sd_ownership(const Json::Value &j_input,
    const struct ifs_security_descriptor *sd,
    struct ifs_security_descriptor *sd_new);

/**
 * Get the owner user id, group id and mode from unix permission json string
 *
 * @param[IN] j_input json input body.
 * @param[OUT] uid owner user id if the owner is defined.
 * @param[OUT] gid owner group id if the group is defined.
 * @param[OUT] mode absolute mode number.
 * @throws api_exception if invalid.
 *
 * If owner user is not defined, its value will be set to -1. Same to group id
 * and mode.
 */
void get_unix_permission_from_json(const Json::Value &j_input,
    uid_t &uid, gid_t &gid, mode_t &mode);

#endif  //__ACL_COMMON_H__
