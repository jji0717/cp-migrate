/*
 * Copyright (c) 2013
 *	Isilon Systems, LLC.  All rights reserved.
 */

#include "ns_object_protection.h"

#include <ifs/bam/bam_pctl.h>
#include <ifs/ifs_types.h>
#include <ifs/ifs_syscalls.h>
#include <ifs/util/protection.h>

#include <isi_json/json_helpers.h>
#include <isi_rest_server/request.h>
#include <isi_rest_server/response.h>
#include <isi_util_cpp/scoped_clean.h>

#include "handler_common.h"
#include <sstream>
#include "ns_handler_provider.h"
#include "common_obj_handler.h"

namespace {

/**
 * diskpool related supporting data structure
 */
struct disk_pool_info {
	std::string name;
	ifs_disk_pool_id_t id;
	uint8_t type;
};


/**
 * Read DiskPool database into a vector
 * @PARAM[INOUT] pools - vector to hold the DiskPool entries
 * @returns isi_error if reading error occurs
 */
struct isi_error *
read_diskpool_db(std::vector<struct disk_pool_info> &pools)
{
	struct isi_error *error = 0;
	int ret_code = 0;
	ifs_disk_pool_db_cookie_t db_cookie;
	unsigned int i, num_entries = 32; /* will grow as needed */

	std::vector<ifs_disk_pool_id_t> ids;
	std::vector<struct ifs_disk_pool_db_entry_configuration> confs;

	bool include_reserved = false;
	do {
		ids.reserve(num_entries);
		confs.reserve(num_entries);

		/* Read the pools. */
		ret_code = ifs_disk_pool_read_db(&db_cookie, num_entries,
		    &num_entries, &ids[0], NULL, NULL, &confs[0],
		    NULL, 0, NULL, NULL, 0, NULL, NULL,
		    include_reserved);
	} while (ret_code && errno == ENOSPC);

	if (ret_code) {
		error = isi_system_error_new(errno,
		    "failed to read disk pool database");
	}
	for (i = 0; i < num_entries; i++) {
		struct disk_pool_info pool;
		pool.id = ids[i];
		pool.name = confs[i].name;
		pool.type = confs[i].type;
		pools.push_back(pool);
	}
	return error;
}

/**
 * Convert node pool name to id
 * Returns false if @pname is not a valid node pool name
 */
bool
convert_nodepool_name_to_id(const std::string &pname,
    ifs_disk_pool_policy_id_t &pid)
{
	std::vector<struct disk_pool_info> pools;
	struct isi_error *error = read_diskpool_db(pools);
	if (error) {
		if (isi_system_error_is_a(error, EPERM))
			throw api_exception(error, AEC_FORBIDDEN,
			    "node pool operation requires root user access.");
		else
			throw api_exception(error, AEC_SYSTEM_INTERNAL_ERROR,
			    "failed to read disk pool database");
	}
	std::vector<struct disk_pool_info>::const_iterator itr;
	for (itr = pools.begin(); itr != pools.end(); ++itr) {
		if (itr->name == pname && itr->type == DISK_POOL_TYPE_GROUP) {
			pid = itr->id;
			break;
		}
	}
	return itr != pools.end() ? true : false;
}

}

/*
 * Get the protection arguments based on headers from request
 */
struct iobj_set_protection_args *
ns_object_protection::get_obj_set_protection_args(const request &input)
{
	std::string node_pool;

	input.get_header(HDR_NODE_POOL_NAME, node_pool);

	if (node_pool.empty())
		return NULL;

	struct iobj_set_protection_args *se_ptr =
	    genobj_set_protection_args_create_and_init();
	scoped_ptr_clean<struct iobj_set_protection_args> se(se_ptr,
	    &genobj_set_protection_args_release);

	ASSERT(!node_pool.empty());

	ifs_disk_pool_policy_id_t pid = -1;

	if (node_pool != ISI_NODE_POOL_NAME_ANY &&
	    !convert_nodepool_name_to_id(node_pool, pid))
		throw api_exception(AEC_BAD_REQUEST,
		    "invalid node pool");

	se_ptr->node_pool_id = pid;

	return se.release();
}


void ns_object_protection::execute_get(const request &input, response &output)
{

	ns_handler_provider provider(input, output, IFS_RTS_STD_ALL);
	oapi_genobj &genobj = provider.get_obj();

	Json::Value &attrs = output.get_json();
	output.set_status(ASC_OK);



	isi_error *error = 0;

	struct pctl2_get_expattr_args ge;

	pctl2_get_expattr_args_init(&ge);

	genobj.get_protection_attr(&ge, error);

	api_exception::throw_on_error(error);

	attrs[ISI_PROTECTION_POLICY] =
	    get_protection_policy_str(ge.ge_file_protection_policy);

	attrs[ISI_PROTECTION_LEVEL] =
	    get_protection_level_str(ge.ge_file_protection_policy);

	attrs[ISI_PROTECTION_COALESCED] = ge.ge_coalescing_on;

	attrs[ISI_PROTECTION_ENDURANT] = ge.ge_coalescing_ec;

	attrs[ISI_PROTECTION_MANUALLY_MANAGE_ACCESS] =
	    ge.ge_manually_manage_access;

	attrs[ISI_PROTECTION_MANUALLY_MANAGE_PROTECTION] =
	    ge.ge_manually_manage_protection;

	attrs[ISI_PROTECTION_DISK_POOL_POLICY_ID] = ge.ge_disk_pool_policy_id;

	attrs[ISI_PROTECTION_DATA_DISK_POOL_ID] = ge.ge_data_disk_pool_id;

	attrs[ISI_PROTECTION_METADATA_DISK_POOL_ID] =
	    ge.ge_metadata_disk_pool_id;

	attrs[ISI_PROTECTION_IS_ENFORCED] = !(ge.ge_needs_repair ||
	    ge.ge_needs_reprotect);
	attrs[ISI_PROTECTION_PERFORMANCE] = ge.ge_access_pattern;

	attrs[ISI_PROTECTION_SSD_STRATEGY] =
	    get_ssd_strategy_str(ge.ge_ssd_strategy);

	return;

}



void ns_object_protection::execute_put(const request &input, response &output)
{
	ns_handler_provider provider(input, output, IFS_RTS_STD_ALL);
	oapi_genobj &genobj = provider.get_obj();

	const Json::Value &attrs = input.get_json();

	pctl2_set_expattr_args se = PCTL2_SET_EXPATTR_ARGS_INITIALIZER;

	struct protection_policy policy = {0};
	const Json::Value *val;


	output.set_status(ASC_OK);

	val = &attrs[ISI_PROTECTION_DISK_POOL_POLICY_ID];
	if (!val->isNull()) {

		if (val->isIntegral())
			se.se_disk_pool_policy_id = val->asInt();

		if (!val->isIntegral() ||
		    !disk_pool_policy_id_valid(se.se_disk_pool_policy_id))
			output.add_error(AEC_BAD_REQUEST, "Invalid value",
			    ISI_PROTECTION_DISK_POOL_POLICY_ID);
	}

	val = &attrs[ISI_PROTECTION_POLICY];
	if (!val->isNull()) {
		if (val->isString()) {
			const char*str = val->asCString();
			if (1 == fmt_scan_full_string(str, strlen(str),
				"%{}", protection_policy_scan_fmt(&policy)))
				if (protection_policy_is_valid(policy))
					se.se_file_protection_policy =&policy;
		}
		if (se.se_file_protection_policy == NULL)
			output.add_error(AEC_BAD_REQUEST, "Invalid value",
			    ISI_PROTECTION_POLICY);
	}

	val = &attrs[ISI_PROTECTION_SSD_STRATEGY];
	if (!val->isNull()) {
		if (!val->isString() || !ifs_ssd_strategy_parse(
		    val->asCString(), &se.se_ssd_strategy))
			output.add_error(AEC_BAD_REQUEST, "Invalid value",
			    ISI_PROTECTION_SSD_STRATEGY);
	}

	val = &attrs[ISI_PROTECTION_COALESCED];
	if (!val->isNull()) {
		if (val->isBool())
			se.se_coalescing = val->asBool();
		else
			output.add_error(AEC_BAD_REQUEST, "Invalid value",
			    ISI_PROTECTION_COALESCED);
	}

	val = &attrs[ISI_PROTECTION_PERFORMANCE];
	if (!val->isNull()) {
		if (val->isIntegral())
			se.se_access_pattern = val->asInt();

		if (!val->isIntegral() || !access_pattern_valid(
			(ifs_access_pattern_t)se.se_access_pattern))
			output.add_error(AEC_BAD_REQUEST, "Invalid value",
			    ISI_PROTECTION_PERFORMANCE);
	}


	val = &attrs[ISI_PROTECTION_MANUALLY_MANAGE_ACCESS];
	if (!val->isNull()) {
		if (val->isBool())
			se.se_manually_manage_access = val->asBool();
		else
			output.add_error(AEC_BAD_REQUEST, "Invalid value",
			    ISI_PROTECTION_MANUALLY_MANAGE_ACCESS);
	}

	val = &attrs[ISI_PROTECTION_MANUALLY_MANAGE_PROTECTION];
	if (!val->isNull()) {
		if (val->isBool())
			se.se_manually_manage_protection = val->asBool();
		else
			output.add_error(AEC_BAD_REQUEST, "Invalid value",
			    ISI_PROTECTION_MANUALLY_MANAGE_PROTECTION);
	}

	bool blocked_prot_changes = false;
	bool blocked_access_changes = false;

	se.se_blocked_prot_changes = &blocked_prot_changes;
	se.se_blocked_access_changes = &blocked_access_changes;

	isi_error *error = NULL;

	if (!output.has_error())
		genobj.set_protection_attr(&se, error);

	if (blocked_prot_changes||blocked_access_changes){
		/* TODO: should we return some sort of
		 * notification (not an error) that changes were blocked? */
	}


	api_exception::throw_on_error(error);

}

