/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	acl_common.cpp __ lchampakesan. 
 */

#include "acl_common.h"

#include <algorithm>
#include <string>

#include <isi_rest_server/api_error.h>
#include <isi_rest_server/response.h>

#include <isi_object/iobj_ostore_protocol.h>
#include <isi_acl/isi_acl_util.h>
#include <isi_acl/isi_sd.h>
#include <isi_object/ostore.h>
#include <isi_auth_cpp/connection.h>
#include <isi_auth_cpp/auth_persona.h>
#include <isi_util_cpp/user_text.h>

#include "common_obj_handler.h"

namespace {

/**
 * This function will retrieve the op value entered by the user  
 * for updating acl's. The op can be add/replace/delete for action update
 * and op is optional for action replace.
 * If action is update and op is not specified, then the op defaults to 
 * add.
 * @param j_input json input body for put acl.
 * @param string action to be done on the acl.
 * @param string op to be done on the acl.
 * @throws api_exception if invalid.
 */
void 
validate_op_for_action(const Json::Value &j_input_ace_entry, 
    std::string &action, std::string &op)
{
	try {
		json_to_str(op, j_input_ace_entry, OP, false);
	}
	catch (isi_exception &exp) {
		throw api_exception(exp.detach(), AEC_BAD_REQUEST, 
		    "Invalid op value.");
	}

	if (!op.empty()) {
	    std::transform(op.begin(), op.end(), op.begin(), ::tolower);
	}
	
	// If op is specified for action update, it should be either
	// update or delete. If op is empty, it is defaulted to OP_ADD.
	if ((action == ACTION_UPDATE) && (!op.empty()) && 
	    (op != OP_ADD) && (op != OP_DELETE) && (op != OP_REPLACE)) {
		throw api_exception(AEC_BAD_REQUEST, 
		    "Invalid op for action update %s. For update action, "
		    "the op should be add or delete or replace or not "
		    "specified (defaulted to add).", op.c_str());
	}

	// If action is update and op is empty, default op to add access rights.
	if ((action == ACTION_UPDATE) && (op.empty())) {
		op = OP_ADD;
	}
}


/**
 * Retrieve the user entered access rights and inheritance flags.
 * For action update, access rights or inheritance flags must be 
 * specified. For action update, op delete, the access rights/
 * inheritance flags is optional. If access rights/inherited flags 
 * are specified, then those access rights/inheritance flags are deleted. 
 * If access rights/inheritance flags are not specified, then the whole 
 * ace corresponding to the user and access type is deleted. 
 * For action replace, the access rights/inheritance flags are optional. 
 * If access rights/inheritance flags are specified, the those 
 * access rights/inheritance flags are set after removing existing 
 * access rights/inheritance flags. If access rights/inheritance flags 
 * are not specified, then the existing access rights/inheritance flags 
 * are removed.
 *
 * @param j_input_ace_entry json input body with the ace entries.
 * @param action action to do with the input ace entries.
 * @param op op can be add/replace/delete for action update and op is not 
 *           necessary for action replace.
 * @param ifs_ace_mod mod struct to store the input ace entries input by 
 *          the user.
 * @throws api_exception if invalid.
 */
void 
validate_access_rights_inheritance_flags(const Json::Value 
    &j_input_ace_entry, std::string action, std::string op, 
    struct ifs_ace_mod *mod)
{

	// Validate the access rights. Access rights are specified in 
	// the Json array.
	// Access rights or inheritance flags must be specified for 
	// action update, op update.
	// Access rights and inheritance flags are optional for action update, 
	// op delete and action replace.
	const Json::Value &acc_rts_array = j_input_ace_entry[ACCESSRIGHTS];
	const Json::Value &inheritflg_array = j_input_ace_entry[INHERIT_FLAGS];
	Json::Value::const_iterator it;
	
	std::string amsk_str, iflg_str, mask_str;
	struct ifs_ace ace = {};
	int i = 0;
	
	if (!acc_rts_array.isNull() && !acc_rts_array.isArray()) {
		throw api_exception(AEC_BAD_REQUEST, 
		    "Access rights must be a JSON Array.");
	}
	
	if ((action == ACTION_UPDATE) && 
	    ((op == OP_ADD) || (op == OP_REPLACE))&& 
	    ((acc_rts_array.isNull()) && (inheritflg_array.isNull()))) {
		throw api_exception(AEC_BAD_REQUEST, 
		    "Access rights or inherit flags must be specified when "
		    "action is update and op is add or replace.");
	}

	// If the access rights and inheritance flags are not specified 
	// then the full ace should be deleted.
	if ((action == ACTION_UPDATE) && (op == OP_DELETE) && 
	    acc_rts_array.isNull() && inheritflg_array.isNull()) {
		return;
	}

	for (it = acc_rts_array.begin(); it != acc_rts_array.end(); ++it) {
		const Json::Value &acc_rts = *it;
		
		if (!acc_rts.isString()) {
			throw api_exception(AEC_BAD_REQUEST, 
			    "access rights is not a valid string %s.", 
			    acc_rts.toStyledString().c_str());
		}

		if (!amsk_str.empty()) {
			amsk_str.append(",");
		}
		amsk_str.append(acc_rts.asString());
	}

	for (it = inheritflg_array.begin(); it != inheritflg_array.end(); 
	     ++it) {
		const Json::Value &flg = *it;

		if (!flg.isString()) {
			throw api_exception(AEC_BAD_REQUEST, 
			    "flags is not a valid string %s.", 
			    flg.toStyledString().c_str());
		}

		if (!iflg_str.empty()) {
			iflg_str.append(",");
		}
		iflg_str.append(flg.asString());
	}

	if (!amsk_str.empty()) {
            std::transform(amsk_str.begin(), amsk_str.end(), 
		amsk_str.begin(), ::tolower);
	}

	if (!iflg_str.empty()) {
    	    std::transform(iflg_str.begin(), iflg_str.end(), 
		iflg_str.begin(), ::tolower);
	}

	mask_str.append(amsk_str);
	
	if (!iflg_str.empty() && (!mask_str.empty())) {
		mask_str.append(",");
		mask_str.append(iflg_str);
	}

	if (!mask_str.empty()) {
		struct fmt FMT_INIT_CLEAN(fmt_mask);
		// Convert the access rights string to enum ifs_ace_rights.
		fmt_scan_init(&fmt_mask, mask_str.c_str(), 
		    strlen(mask_str.c_str()));
		i = fmt_scan(&fmt_mask, "%{}", access_mask_scan_fmt(&ace));
		
		if (i != 1) {
			throw api_exception(AEC_BAD_REQUEST, 
			    "Invalid access rights/inheritance flags "
			    "specified %s.", fmt_string(&fmt_mask));
			return;
		}
	}

	mod->access_mask = ace.access_mask;
	mod->flags = ace.flags;
}


/**
 * Validate ace.
 * This routine is called twice. First time it is called to validate and 
 * calculate the number of ace's in the acl. This routine goes through 
 * each ace input in the acl and validates the input. The ace list 
 * passed will be null and size zero. 
 * The second time, this routine is invoked to populate the ace list 
 * for the calculated number of ace's passed as input.
 * The acl format is as follows,
 * Trustee is json object with 3 entities, id, name, type of user.
 *	"acl":
 *	[
 *	        {
 *	                "trustee":
 *                      { 
 *                          "id":<"id">,
 *                          "name":<"name">,
 *                          "type":<"type">
 *                      }   
 *			"accesstype":"<access type>",
 *			"accessrights":"<access rights>",
 *			"op":"<operation>"
 *		},
 *      ]
 *
 *
 * @param j_input json input body for put acl.
 * @param oacl oacl object.
 * @param action action string input by the user.
 * @throws api_exception if invalid.
 */

void 
validate_ace(const Json::Value &j_input, oapi_acl &oacl, 
    std::string action)
{
	Json::Value::const_iterator it_ace;
	auth::lsa_connection connection;
	ifs_ace_mod *ace_list_temp;

	// Get the ace array.
	const Json::Value &ace_array = j_input[OHQ_ACL];
	
	// If there is no ace, return here
	if (!ace_array.size())
		return;

	// Allocate the memory for ace_mods.
	oacl.allocate_ace_mods(ace_array.size());
	ace_list_temp = oacl.get_ace_mods();

	// Iterate over the ace array for validation and update the 
	// valid values into the ace array.
	for (it_ace = ace_array.begin(); it_ace != ace_array.end(); ++it_ace) {

		const Json::Value &ace_entry = *it_ace;
		std::string access_type;
		std::string op;
		const Json::Value &trustee_json = ace_entry["trustee"];

		// Check if the trustee is a json object.
		if (!trustee_json.isObject()) {
			// we only support JSON object
			throw api_exception(AEC_BAD_REQUEST,
			    "Invalid trustee - Should be a JSON object, "
                            "please refer to the API documentation.");
		}
		
		// Get the trustee information. 
		try {
			auth::persona trustee =
			    auth::json_to_persona(trustee_json, connection);

			ace_list_temp->trustee = persona_dup(trustee.get());
		} catch (...) {
			throw api_exception(AEC_BAD_REQUEST,
			    "Invalid trustee");
		}

		if (!ace_entry[ACCESSTYPE].isString()) {
			throw api_exception(AEC_BAD_REQUEST, 
			    "access type is not a valid string %s.", 
			    (ace_entry[ACCESSTYPE]).toStyledString().c_str());
		}

		// Validate the access type.
		access_type = ace_entry[ACCESSTYPE].asString();
				
		if (access_type == ALLOW) {
			ace_list_temp->type = IFS_ACE_TYPE_ACCESS_ALLOWED;
		}
		else if (access_type == DENY) {
			ace_list_temp->type = IFS_ACE_TYPE_ACCESS_DENIED;
		}
		else {
			throw api_exception(AEC_BAD_REQUEST,
			    "%s must be 'allow' or 'deny', not '%s'.",
			    ACCESSTYPE, access_type.c_str());
		}
		
		// Validate the op.
		validate_op_for_action(ace_entry, action, op);

		validate_access_rights_inheritance_flags (ace_entry, 
		    action, op, ace_list_temp);
		
		// Update the ace_entry.
		if (action == ACTION_UPDATE) {
			if (op == OP_ADD) {
				ace_list_temp->mod_type = IFS_ACE_MOD_ADD;
			}				
			else if (op == OP_DELETE) {
				// If access rights are specified, then 
				// those specific access rights are deleted.
				if ((ace_list_temp->access_mask) || 
				    ace_list_temp->flags) {
					ace_list_temp->mod_type = 
					    IFS_ACE_MOD_DEL;
				}
				// If no access rights are specified, 
				// the whole ace is deleted if the trustee 
				// and the access type match.
				else {
					ace_list_temp->mod_type = IFS_ACE_DEL;
				}
			}
				
			else if (op == OP_REPLACE) {
				ace_list_temp->mod_type = IFS_ACE_MOD_REPLACE;
			}
		}
		else if (action == ACTION_REPLACE) {
			/* Do nothing. Just form the ace array. */
		}
		
		// Set the mod position to zero. This is not 
		// useful except for insert and remove operations, 
		// which is not used yet.
		ace_list_temp->mod_position = 0;
		
		// Set the flags information.
		ace_list_temp->ifs_flags = IFS_INTERNAL_ACE_FLAG_KNOWN_MASK;
		ace_list_temp++;

	} // iterator - ace array.
}

}


/*
	The put input json format is in the following notation.

	{
	        "owner":
		{
		    "id":"<id>",
		    "name":"<name>",
		    "type":"<type>"
		},
		"group":
		{
		    "id":"<id>",
		    "name":"<name>",
		    "type":"<type>"
		},
		"authoritative":"mode" | "acl",
		"mode": <"POSIX octal number">,

		"action":"<action_value>",
		"acl":
		[
		    {
                        "trustee":
			{
			    "id":"<id>",
			    "name":"<name>",
			    "type":"<type>"
			},
			"accesstype":"<access type>",
			"accessrights":"<access rights>",
			"op":"<operation>"
		    },
                ]
	}	


	Notes:

	authoritative: can be "acl" or "mode"; 	mandatory field.
	  "acl" - indicating the operation to be performed on true ACL setting.
		  The mode field will be ignored in this case if present.
	  "mode"- indicating the operation to be performed in POSIX way.
	          If the target resource has no true ACL setting, it will
	          remain so. The action and acl fileds are ignored always
	          in this case.
	   The end result of the operation may vary, depending on the Global
	   ACL Policy. Please refer to OneFS administrator guide for a clear
	   understanding how mode and ACL interacting with each other.

	action : Can take the value, update or replace.
	                  update adds new access rights or modifies or 
			  deletes specified access rights or replaces 
			  the specified ace. 
			  replace does replace of the entire acl with the 
			  contents in the json body.

			  If no action_value is mentioned, 
			  the default is replace.

	acl             : Json array of Access control elements (ACE).

	trustee         : Is a Json object that has id, name and type of 
	                  the trustee.

	accesstype      : Can either be 'allow' or 'deny' permissions.
	
	accessrights    : Can take the access rights values.
			  
        op              : Can take the value add or replace or delete if 
	                  action is update.
                          update: Adds or replaces or delete access rights 
			  in the ace.
			    
			  For action value update, if no op is mentioned, 
			  the default op is add.
			 
			  There is no op value for the action replace. 
			  op value is ignored. 

*/

/**
 * Validate the input acl.
 * @param j_input json input body for put acl.
 * @param oacl acl object.
 * @param acl_mod_type the mod type can be update or replace.
 * @throws api_exception if invalid.
 */
void
validate_input_acl(const Json::Value &j_input,
    oapi_acl &oacl,
    enum ifs_acl_mod_type *acl_mod_type)
{

	std::string action;

	if (!j_input.isObject()) {
		// Support only Json Object.
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid JSON acl, please refer to the API documentation.");
	}

	// Check the action_value.
	json_to_str(action, j_input, ACTION, false);

	if (action.size() && action != ACTION_REPLACE &&
	    action != ACTION_UPDATE) {
		throw api_exception(AEC_BAD_REQUEST, "Invalid action '%s'.",
		    action.c_str());
	}

	if (!action.size()) {
		action = ACTION_REPLACE;
	}

	std::transform(action.begin(), action.end(), action.begin(), 
	    ::tolower);

	// Validate the acl array.
	validate_ace(j_input, oacl, action);

	if (action == ACTION_REPLACE) {
		*acl_mod_type = IFS_ACL_REPLACE;		
	}
	else {
		*acl_mod_type = IFS_ACL_MOD;
	}

	return;
}

/*
 * see above for json input format
 */
void
update_sd_ownership(const Json::Value &j_input,
    const struct ifs_security_descriptor *sd,
    struct ifs_security_descriptor *sd_new)
{
	ASSERT(sd && sd_new && !sd_new->owner && !sd_new->group);

	auth::lsa_connection connection;
	struct persona *p = NULL;

	p = sd->owner;
	const Json::Value &owner_j = j_input[OWNER];
	if (owner_j.isObject() && !owner_j.isNull()) {
		auth::persona owner_p = auth::json_to_persona(owner_j,
		    connection, OWNER);
		// if not same
		if (!p || !persona_equal(p, owner_p.get()))
			sd_new->owner = persona_dup(owner_p.get());
	}

	p = sd->group;
	const Json::Value &group_j = j_input[GROUP];
	if (group_j.isObject() && !group_j.isNull()) {
		auth::persona group_p = auth::json_to_persona(group_j,
		    connection, GROUP);
		// if not same
		if (!p || !persona_equal(p, group_p.get()))
			sd_new->group = persona_dup(group_p.get());
	}
}

/*
 * see header
 */
void
get_unix_permission_from_json(const Json::Value &j_input,
    uid_t &uid, gid_t &gid, mode_t &mode)
{
	uid_t uid_ = (uid_t) -1;
	gid_t gid_ = (gid_t) -1;
	mode_t mode_ = (mode_t) -1;

	auth::lsa_connection connection;
	const Json::Value &owner_j = j_input[OWNER];
	const Json::Value &group_j = j_input[GROUP];
	try {
		if (owner_j.isObject() && !owner_j.isNull()) {
			auth::persona owner_p = auth::json_to_persona(owner_j,
			    connection, OWNER);
			uid_ = owner_p.query_uid(connection);
		}
	} catch (...) {
		throw api_exception(OWNER, AEC_BAD_REQUEST,
				    get_user_text(RAN_PROPERTY_PERMISSIONS_INVALID_INPUT),
				    (owner_j["name"].asString()).c_str(),
				    OWNER);
	}

	try {
		if (group_j.isObject() && !group_j.isNull()) {
			auth::persona group_p = auth::json_to_persona(group_j,
			    connection, GROUP);
			gid_ = group_p.query_gid(connection);
		}
	} catch (...) {
		throw api_exception(GROUP, AEC_BAD_REQUEST,
				    get_user_text(RAN_PROPERTY_PERMISSIONS_INVALID_INPUT),
				    (group_j["name"].asString()).c_str(),
				    GROUP);
	}

	// Check the mode value.
	std::string s;
	json_to_str(s, j_input, ISI_ACC_MODE, false);
	if (!s.empty())
		mode_ = common_obj_handler::get_create_mode(s);

	uid = uid_;
	gid = gid_;
	mode = mode_;
}
