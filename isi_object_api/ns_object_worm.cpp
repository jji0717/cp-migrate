/*
 * Copyright (c) 2013
 *	Isilon Systems, LLC.  All rights reserved.
 */

#include "ns_object_worm.h"

#include <isi_domain/dom.h>
#include <isi_domain/dom_util.h>
#include <isi_json/json_helpers.h>

#include <isi_object/common.h>

#include <isi_rest_server/response.h>

#include "handler_common.h"
#include "ns_handler_provider.h"
#include "oapi/oapi_genobj.h"
#include "oapi/oapi_object.h"

static const char *TIME_FMT = "%Y-%m-%d %H:%M:%S GMT";

static const char *F_NOT_SUPP =
    "The target has no associated SmartLock Root Directories.";

static const char *D_NOT_SUPP =
    "WORM operation for directory not supported yet.";

namespace {

/**
 * Throw api_exception on WORM error
 */
void
throw_on_error(isi_error *error, const char *what)
{
	if (!error)
		return;

	if (isi_system_error_is_a(error, EOPNOTSUPP)) {
			throw api_exception(error, AEC_FORBIDDEN,
			    F_NOT_SUPP);
	} else if (isi_system_error_is_a(error, EPERM)) {
		throw api_exception(error, AEC_FORBIDDEN,
		    "operation requires root user access.");
	} else if (isi_system_error_is_a(error, EROFS)) {
		// this occurs when setting retention date to an earlier date
		throw api_exception(error, AEC_FORBIDDEN,
		    "The target is on a read-only filesystem or the "
		    "retention date is set earlier than current retention "
		    "date on the file.");
	}

	oapi_error_handler().throw_on_error(error, what);
}

}

/*
 * This helper routine processes WORM get request
 */
void
ns_object_worm::execute_get(const request &input, response &output)
{
	ns_handler_provider provider(input, output, IFS_RTS_STD_READ);
	oapi_genobj &genobj = provider.get_obj();

	Json::Value &attrs = output.get_json();
	output.set_status(ASC_OK);
	isi_error *error = 0;

	object_worm_attr worm_attr = {};
	object_worm_domain worm_dom = {};
	object_worm_domain *worm_ancs = NULL;
	size_t num_ancs = 0;

	genobj.get_worm_attr(worm_attr, worm_dom, worm_ancs, num_ancs,
	    error);
	if (error)
		::throw_on_error(error, "get WORM attributes");
	std::string domain_root = "" ;
	/*
	JSON Syntax in success response for GET:
	{
	"domain_id": <integer>,
	"domain_path": <string>,
	"worm_committed":<boolean>,
	"worm_retention_date":<"YYYY-MM-DD hh:mm:ss GMT">,
	"worm_override_retention_date":<"YYYY-MM-DD hh:mm:ss GMT">|null,
	"worm_retention_date_val":<seconds from the Epoch>
	"worm_override_retention_date_val":<seconds from the Epoch> | null
	}
	*/
	attrs = Json::objectValue;
	if (worm_dom.domain_root != NULL) {
		struct tm gmt;
		const unsigned int buf_sz = 64;
		char gmt_s[buf_sz]; // enough for the date time

		domain_root = std::string(worm_dom.domain_root);
		free(worm_dom.domain_root);

		// Get domain root path from lin, add domain_id to output,
		// for both files and directories
		attrs[ISI_WORM_ROOT_PATH] = std::string("/ifs/") +
		    domain_root;
		attrs[ISI_WORM_DOMAIN_ID] = worm_dom.domain_id;

		if (!error && !worm_attr.w_is_valid)
			throw api_exception(AEC_FORBIDDEN, F_NOT_SUPP);
		::throw_on_error(error, "get WORM attributes");

		if (genobj.get_object_type() == OOT_OBJECT) {
			attrs[ISI_WORM_COMMITTED] =
			    worm_attr.w_attr.w_committed != 0;
			attrs[ISI_WORM_RETENTION_DATE] = Json::Value::null;
			attrs[ISI_WORM_OVERRIDE_RETENTION_DATE] =
			    Json::Value::null;
			attrs[ISI_WORM_RETENTION_DATE_VAL] =
			    Json::Value::null;
			attrs[ISI_WORM_OVERRIDE_RETENTION_DATE_VAL] =
			    Json::Value::null;

			time_t tr = worm_attr.w_attr.w_retention_date;
			if (tr > 0) {
				gmtime_r(&tr, &gmt);
				strftime(gmt_s, buf_sz, TIME_FMT, &gmt);
				attrs[ISI_WORM_RETENTION_DATE] = gmt_s;
				attrs[ISI_WORM_RETENTION_DATE_VAL] = tr;
			}

			time_t tor = worm_dom.dom_entry.d_override_retention;
			if (tor > tr) {
				gmtime_r(&tor, &gmt);
				strftime(gmt_s, buf_sz, TIME_FMT, &gmt);
				attrs[ISI_WORM_OVERRIDE_RETENTION_DATE] =
				    gmt_s;
				attrs[ISI_WORM_OVERRIDE_RETENTION_DATE_VAL] =
				    tor;
			}

			if (worm_dom.dom_entry.d_flags & DOM_COMPLIANCE) {
				attrs[ISI_WORM_CTIME] =
				    worm_attr.w_attr.w_ctime.tv_sec;

			} else {
				struct igenobj_handle *igobj_hdl =
				    genobj.get_handle();
				int fd = igobj_hdl->fd;
				int ret = 0;
				struct stat st;
				ret = fstat(fd, &st);

				if( ret != 0 ) {
					throw api_exception(AEC_BAD_REQUEST,
					    "File Error: %d\n", errno);
				}

				attrs[ISI_WORM_CTIME] = st.st_ctime;
			}

			time_t aco = worm_dom.dom_entry.d_autocommit_offset;
			if (!worm_attr.w_attr.w_committed && aco) {
				attrs[ISI_WORM_AUTOCOMMIT_OFFSET] = aco;
			} else {
				attrs[ISI_WORM_AUTOCOMMIT_OFFSET] =
				    Json::Value::null;
			}
		}
	}

	if ((genobj.get_object_type() == OOT_OBJECT ||
	    genobj.get_object_type() == OOT_CONTAINER) && worm_ancs != NULL &&
	    num_ancs > 0) {
		attrs[ISI_WORM_ANCESTORS] = Json::arrayValue;
		for (size_t i = 0; i < num_ancs; i++) {
			Json::Value anc(Json::objectValue);
			anc[ISI_WORM_ROOT_PATH] =
			    std::string(worm_ancs[i].domain_root);
			anc[ISI_WORM_DOMAIN_ID] =
			    worm_ancs[i].domain_id;
			attrs[ISI_WORM_ANCESTORS].append(anc);
		}
	}

	//XXXDPL Freeing this structure is scary and may require a separate
	// function if done in more than one place!
	if (worm_ancs != NULL && num_ancs > 0) {
		for (size_t i = 0; i < num_ancs; i++) {
			free(worm_ancs[i].domain_root);
		}
		free(worm_ancs);
	}
}

/*
 * This helper routine processes WORM put request
 */
void
ns_object_worm::execute_put(const request &input, response &output)
{
	ns_handler_provider provider(input, output, IFS_RTS_STD_READ);
	oapi_genobj &genobj = provider.get_obj();
	if (genobj.get_object_type() != OOT_OBJECT)
		throw api_exception(AEC_FORBIDDEN, D_NOT_SUPP);

	output.set_status(ASC_OK);

	oapi_object &obj = dynamic_cast<oapi_object &>(genobj);
	isi_error *error = 0;

	/*
	JSON Syntax in the request payload for PUT:
	{
	"worm_retention_date":<"YYYY-MM-DD hh:mm:ss GMT">
	"commit_to_worm":<Boolean>
	}
	*/

	// -1 indicates retention date is not set
	object_worm_set_arg set_arg = {-1, false};
	bool is_set = false;
	const Json::Value &attrs = input.get_json();

	if (!attrs.isObject()) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid JSON input.");
	}

	const Json::Value &j_reten = attrs[ISI_WORM_RETENTION_DATE];

	if (!j_reten.isNull()) {
		const char *remain = NULL;
		std::string retention;
		time_t tr = -1;
		struct tm tm_v = {};

		if (j_reten.isString())
			retention = j_reten.asString();
		if (!retention.empty()) {
			remain = strptime(retention.c_str(),
			    TIME_FMT, &tm_v);
		}
		// allow only extra space in the date string
		if (remain && retention.find_first_not_of(" ",
		    remain - retention.c_str()) == std::string::npos) {
			// require explicit GMT in time string
			size_t off = retention.find("GMT");
			if (off != std::string::npos)
				tr = timegm(&tm_v);//seconds from Epoch
		}

		if (!remain || tr == -1) {
			throw api_exception(AEC_BAD_REQUEST,
			    "date out of range or invalid");
		}

		set_arg.w_retention_date = tr;
		is_set = true;
	}

	const Json::Value &j_commit = attrs[ISI_COMMIT_TO_WORM];
	if (!j_commit.isNull()) {
		if (!j_commit.isBool())
			throw api_exception(AEC_BAD_REQUEST,
			    "invalid %s", ISI_COMMIT_TO_WORM);
		set_arg.w_commit = j_commit.asBool();
		is_set = true;
	}

	if (!is_set)
		return;

	obj.set_worm_attr(set_arg, error);
	::throw_on_error(error, "set WORM retention date or commit");
}

