/*
 * common_obj_handler.cpp
 *
 *  Created on: Mar 10, 2012
 *      Author: lijun
 */

#include "common_obj_handler.h"

#include <ifs/bam/bam_pctl.h>
#include <ifs/ifs_types.h>
#include <ifs/ifs_syscalls.h>

#include <algorithm>
#include <sstream>
#include <vector>

extern "C" {
#include <isi_acl/sd_acl.h>  // for cleanup sd
}
#include <isi_acl/isi_sd.h>
#include <isi_auth_cpp/auth_persona.h>
#include <isi_ilog/ilog.h>
#include <isi_json/json_helpers.h>
#include <isi_persona/persona_util.h>
#include <isi_rest_server/api_headers.h>
#include <isi_rest_server/request.h>
#include <isi_rest_server/response.h>
#include <isi_util_cpp/http_util.h>
#include <isi_util/isi_printf.h>
#include <isi_util_cpp/scoped_clean.h>
#include <boost/tokenizer.hpp>

#include "acl_common.h"
#include "handler_common.h"
#include "bucket_handler.h"
#include "object_query.h"
#include "store_provider.h"

const std::string HDR_ACCESS_CONTROL("X_ISI_IFS_ACCESS_CONTROL");
const std::string HHDR_ACCESS_CONTROL("x-isi-ifs-access-control");

namespace {

// A special class to manage a temp security descriptor which
// supports safely releasing memory due to an issue:
//
//   ifs_modify_fd allocates sd in one chunk, only needs free once in whole.
//   however, it also allocates a copy of replacement dcal, which need to be
//   be freed separately. There is no existing utility can safely do this.
//
//   Ideally, ifs_modify_fd should allocates sd with separate memory for each
//   member data so that we can cleanly control lifecycle of each member when
//   replacement occurs.
//
class scoped_sd {
public:
	/**
	 * Constructor
	 * @PARAM[IN] sd - security descriptor
	 * @PARAM[IN] release_og - whether to release owner and group as separate
	 *                         memory chunk
	 * @PARAM[IN] release_dacl - whether to release dacl as separate
	 *                         memory chunk
	 */
	scoped_sd(struct ifs_security_descriptor *sd, bool release_og,
	    bool release_dacl);
	~scoped_sd();

private:
	struct ifs_security_descriptor *sd_;
	bool release_og_;
	bool release_dacl_;
};

scoped_sd::scoped_sd(struct ifs_security_descriptor *sd, bool release_og,
    bool release_dacl)
    : sd_(sd), release_og_(release_og), release_dacl_(release_dacl)
{
}

scoped_sd::~scoped_sd()
{
	if (release_dacl_ && sd_->dacl)
		free(sd_->dacl);

	if (release_og_) {
		sd_->dacl = NULL;
		sd_->sacl = NULL;
		cleanup_sd(sd_);
	}

	free(sd_);
}


/**
 * Check if the given attribute header is an isi extended attribute
 * @param key - the user defined attribute name
 * @return - true if it the attribute begins with X_ISI_META_
 *         - false otherwise
 */
bool
is_isi_attribute_hdr(const std::string &key)
{
	return key.size() >= sizeof(X_ISI_META_) &&
	    !key.compare(0, sizeof(X_ISI_META_)-1, X_ISI_META_);
}


/**
 * check if the attribute is system defined:
 * We have the following system defined attribute:
 * content-encoding
 * content-type
 */
bool is_sys_attr(const std::string &key)
{
	if (key == ATTR_CONTENT_TYPE ||
	    key == ATTR_CONTENT_ENCODING)
		return true;

	return false;
}

/**
 * Convert the X_ISI_IFS_ATTR_ to X-ISI-IFS-ATTR-
 * return true if the attribute can be displayed
 * via HTTP header.
 * Refer to apache function http2env in util_script.c,
 * for cgi compatibility, apache converts the header to those
 * acceptable in environment variables.
 * It changes everything not alphanumeric to _ and then
 * converts the string to upper case.
 */
bool
isi_attribute_to_header(const std::string &key, std::string &header)
{

	bool can_show = true;
	size_t len = key.size();
	const char *data = key.c_str();
	for (size_t ii = 0; ii < len; ++ii) {

		unsigned char ch = data[ii];
		if (isupper(ch) || isdigit(ch) || ch == '_') {
			continue;
		}
		can_show = false;
		break;
	}

	if (can_show) {
		header = HHDR_X_ISI_META_ + key;
	}

	return can_show;
}

/**
 * Set the ISI_OWNER in the JSon value from the uid
 */
void set_user_name_from_uid(Json::Value &objectVal, uid_t uid)
{
	std::string user_name;
	query_util::get_user_name_from_uid(user_name, uid);

	objectVal[ATTR_NAME] = ISI_OWNER;
	objectVal[ATTR_VALUE] = user_name;
}

/*
 * Convert number in string to unsigned long type
 *
 * if extra non-digit exists in the string, returns false.
 */
bool
convert_to_ulong(const char *str, unsigned long &ret_val)
{
	unsigned long val;
	char *ep;

	errno = 0;
	val = strtoul(str, &ep, 10);
	if (errno || *ep != '\0')
		return false;

	ret_val = val;
	return true;
}

/**
 * Set the ISI_GROUP in the JSon value from the gid
 */
void set_group_name_from_gid(Json::Value &objectVal, gid_t gid)
{
	std::string group_name;
	query_util::get_group_name_from_gid(group_name, gid);

	objectVal[ATTR_NAME] = ISI_GROUP;
	objectVal[ATTR_VALUE] = group_name;
}

/**
 * Utility to add a timed type attribute:
 */
void add_time_value(const char *attr_name, const time_t tim, Json::Value &attr,
    Json::Value &attrs)
{
	attr[ATTR_NAME] = attr_name;
	std::string mod_time;
	convert_time_to_http_date(tim,
	    mod_time);
	attr[ATTR_VALUE] = mod_time;
	attrs.append(attr);
}

/**
 * Load system attributes
 * @param[IN] ctx - the callback context
 * @param[OUT] objectVal - the object value JSON object
 * @param[IN] info - object information received from lower level
 */
void set_sys_attrs(const attribute_manager &attr_mgr,
    const char *name,
    const struct stat *pstat,
    Json::Value &attrs)
{
	Json::Value attr(Json::objectValue);

	if (attr_mgr.is_valid_attr(ISI_HIDDEN)) {
		attr[ATTR_NAME] = ISI_HIDDEN;
		attr[ATTR_VALUE] = query_util::is_hidden(name);
		attrs.append(attr);
	}

	if (pstat == NULL)
		return;

	if (attr_mgr.is_valid_attr(ISI_SIZE)) {
		attr[ATTR_NAME] = ISI_SIZE;
		attr[ATTR_VALUE] = pstat->st_size;
		attrs.append(attr);
	}

	if (attr_mgr.is_valid_attr(ISI_BLK_SIZE)) {
		attr[ATTR_NAME] = ISI_BLK_SIZE;
		attr[ATTR_VALUE] = pstat->st_blksize;
		attrs.append(attr);

	}

	if (attr_mgr.is_valid_attr(ISI_BLOCKS)) {
		attr[ATTR_NAME] = ISI_BLOCKS;
		attr[ATTR_VALUE] = pstat->st_blocks;
		attrs.append(attr);
	}

	if (attr_mgr.is_valid_attr(ISI_MTIME))
		add_time_value(ISI_MTIME, pstat->st_mtime, attr,
		    attrs);

	if (attr_mgr.is_valid_attr(ISI_CTIME))
		add_time_value(ISI_CTIME, pstat->st_ctime, attr,
		    attrs);

	if (attr_mgr.is_valid_attr(ISI_ATIME))
		add_time_value(ISI_ATIME, pstat->st_atime, attr,
		    attrs);

	if (attr_mgr.is_valid_attr(ISI_BTIME))
		add_time_value(ISI_BTIME, pstat->st_birthtime,
		    attr, attrs);

	if (attr_mgr.is_valid_attr(ISI_MTIME_VAL)) {
		attr[ATTR_NAME] = ISI_MTIME_VAL;
		attr[ATTR_VALUE] = pstat->st_mtime;
		attrs.append(attr);
	}

	if (attr_mgr.is_valid_attr(ISI_CTIME_VAL)) {
		attr[ATTR_NAME] = ISI_CTIME_VAL;
		attr[ATTR_VALUE] = pstat->st_ctime;
		attrs.append(attr);
	}

	if (attr_mgr.is_valid_attr(ISI_ATIME_VAL)) {
		attr[ATTR_NAME] = ISI_ATIME_VAL;
		attr[ATTR_VALUE] = pstat->st_atime;
		attrs.append(attr);
	}

	if (attr_mgr.is_valid_attr(ISI_BTIME_VAL)) {
		attr[ATTR_NAME] = ISI_BTIME_VAL;
		attr[ATTR_VALUE] = pstat->st_birthtime;
		attrs.append(attr);
	}

	if (attr_mgr.is_valid_attr(ISI_OWNER)) {
		set_user_name_from_uid(attr, pstat->st_uid);
		attrs.append(attr);
	}

	if (attr_mgr.is_valid_attr(ISI_GROUP)) {
		set_group_name_from_gid(attr, pstat->st_gid);
		attrs.append(attr);
	}

	if (attr_mgr.is_valid_attr(ISI_UID)) {
		attr[ATTR_NAME] = ISI_UID;
		attr[ATTR_VALUE] = pstat->st_uid;
		attrs.append(attr);
	}

	if (attr_mgr.is_valid_attr(ISI_GID)) {
		attr[ATTR_NAME] = ISI_GID;
		attr[ATTR_VALUE] = pstat->st_gid;
		attrs.append(attr);
	}

	if (attr_mgr.is_valid_attr(ISI_ID)) {
		attr[ATTR_NAME] = ISI_ID;
		attr[ATTR_VALUE] = pstat->st_ino;
		attrs.append(attr);
	}

	if (attr_mgr.is_valid_attr(ISI_NLINK)) {
		attr[ATTR_NAME] = ISI_NLINK;
		attr[ATTR_VALUE] = pstat->st_nlink;;
		attrs.append(attr);
	}

	if (attr_mgr.is_valid_attr(ISI_OBJECT_TYPE)) {
		const char *obj_type = NULL;
		if (pstat->st_mode & S_IFDIR)
			obj_type = TT_CONTAINER.c_str();
		else if (pstat->st_mode & S_IFREG)
			obj_type = TT_OBJECT.c_str();
		else
			obj_type = UNKNOWN_OTYPE;

		attr[ATTR_NAME] = ISI_OBJECT_TYPE;
		attr[ATTR_VALUE] = obj_type;
		attrs.append(attr);
	}
	if (attr_mgr.is_valid_attr(ISI_STUB)) {
		attr[ATTR_NAME] = ISI_STUB;
		if ((pstat->st_flags & SF_FILE_STUBBED) != 0)
			attr[ATTR_VALUE] = true;
		else
			attr[ATTR_VALUE] = false;
		attrs.append(attr);
	}


	if (attr_mgr.is_valid_attr(ISI_ACC_MODE)) {
		attr[ATTR_NAME] = ISI_ACC_MODE;
		attr[ATTR_VALUE] = query_util::get_mode_octal_str(pstat);
		attrs.append(attr);
	}
}

/**
 * Set the extended attributes
 */
void
set_exended_attrs(const std::map<std::string, std::string> &attr_map,
    Json::Value &attrs)
{
	Json::Value attr(Json::objectValue);
	std::map<std::string, std::string>::const_iterator it;
	for (it = attr_map.begin(); it != attr_map.end(); ++it) {

		attr[ATTR_NAME] = it->first;
		attr[ATTR_VALUE] = it->second;
		attr[ATTR_NS] = USER;
		attrs.append(attr);
	}
}

/**
 * Validate input attributes
 * @param input input request
 * @param j_input json input body for put attributes
 * @param action[OUT] action
 * @throws api_exception if invalid
 */
void validate_input_attrs(const request &input,
    const Json::Value &j_input,
    std::string &action)
{

	if (!j_input.isObject()) {
		// we only support JSON object
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid JSON attributes, please refer to the API"
		    " documentation");
	}

	// The object is in the following notation:
	// {
	//   "action" : "update|replace",
	//   "attrs" : [{"name":"key_name", "value":"key_value",
	//               "op":"update|delete"}]
	// }

	json_to_str(action, j_input, ACTION, false);

	if (action.size() && action != ACTION_REPLACE &&
	    action != ACTION_UPDATE) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid action '%s'", action.c_str());
	}

	const Json::Value &attrs = j_input[ATTR_ATTRS];


	if (attrs.isNull() || !attrs.isArray()) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Attributes must be a JSON Array");
	}

	Json::Value::const_iterator it;

	std::string attr_name;
	std::string attr_op;
	for (it = attrs.begin(); it != attrs.end(); ++it) {
		const Json::Value &attr = *it;

		if (!attr.isObject()) {
			throw api_exception(AEC_BAD_REQUEST,
			    "The element in the 'attrs' array must be a "
			    "JSON object.");
		}

		json_to_str(attr_name, attr, ATTR_NAME, false);

		if (attr_name.empty()) {
			throw api_exception(AEC_BAD_REQUEST,
			    "Attribute name is missing or empty");
		}

		json_to_str(attr_op, attr, OP, false);

		if (!attr_op.empty() && attr_op != OP_UPDATE &&
		    attr_op != OP_DELETE) {
			throw api_exception(AEC_BAD_REQUEST,
			    "Invalid operations '%s'",
			    attr_op.c_str());
		}
	}
}

}

/*
 * If it is special recognized case, handle it; otherwise pass it to generic
 * handler
 */
void
set_access_error_handler::throw_on_error(isi_error *error)
{
	if (!error)
		return;

	if (isi_system_error_is_a(error, EFTYPE)) {
		// non-root user can get this error
		throw api_exception(error, AEC_FORBIDDEN,
			    "Unable to set specified ACL or mode.");
	} else {
		oapi_error_handler().throw_on_error(error,
		    "set ACL or mode.");
	}
}

/**
 * Get x-isi-ifs-attr- attributes from the request headers
 * @param[OUT] xattrs - the key-value map
 */
void
common_obj_handler::get_isi_attributes_to_map(
    const api_header_map &hdr_map,
    std::map<std::string, std::string> &xattrs)
{
	for (api_header_map::const_iterator it = hdr_map.begin();
	    it != hdr_map.end(); ++it) {
		const std::string &key = it->first;
		if (is_isi_attribute_hdr(key)) {
			// trim the meta prefix
			std::string k2 = key.substr(sizeof(X_ISI_META_) - 1);
			const std::string &val = it->second;
			xattrs[k2] = val;
		}
	}
}

/*
 * Get the attribute list based on headers from request
 * @param[IN] input  a request object
 * @return attribute list
 */
struct iobj_object_attrs *
common_obj_handler::get_obj_attr_list(const request &input)
{
	std::map<std::string, std::string> xattrs;
	struct isi_error *error = NULL;
	// Get headers in the whole request
	get_isi_attributes_to_map(
	    input.get_header_map(), xattrs);
	if (xattrs.size() <= 0)
		return NULL;
	struct iobj_object_attrs *attr_list =
	    genobj_attr_list_create(xattrs.size());
	std::map<std::string, std::string>::iterator it;
	for (it = xattrs.begin(); it != xattrs.end(); ++it) {
		const std::string &key = it->first;
		const std::string &val = it->second;
		genobj_attr_list_append(attr_list, key.c_str(),
		    val.data(), val.size(), &error);
		ASSERT(!error); // don't expect error
	}
	return attr_list;
}

/*
 * Get the octal mode number from input
 */
mode_t
common_obj_handler::get_create_mode(const std::string &acc_ctrl)
{
	mode_t mode;

	const char *p = acc_ctrl.c_str();
	char *ep = NULL;
	long perml = -1;

	// Check if the x-isi-ifs-access-control is not digit,
	// then check for pre-defined acl.
	if (*p && isdigit((unsigned char) *p)) {
		perml = strtol(p, &ep, 8);
	}

	if (perml < 0 || *ep || perml & ~(STANDARD_BITS|S_ISTXT)) {
		throw api_exception(AEC_BAD_REQUEST,
		    "'%s' is not a valid octal mode number.", p);
	}
	mode = (mode_t) perml;
	return mode;
}


/*
 * Get the pre-defined acl from the input.
 */
enum ostore_acl_mode 
common_obj_handler::get_predefined_acl(const request &input,
    const oapi_store &store)
{
	std::string acl_str;
	enum ostore_acl_mode acl_mode = OSTORE_ACL_NONE;
	enum OSTORE_TYPE store_type = store.get_store_type();

	if (store_type == OST_NAMESPACE) {
		if (input.get_header(HDR_ACCESS_CONTROL, acl_str)) {
			
			// Check if the access control specified is 
			// a numeric value.
			if (isdigit((unsigned char) *acl_str.c_str())) {
				return acl_mode;
			}

			std::transform(acl_str.begin(), acl_str.end(), 
			    acl_str.begin(), ::tolower);

			if (acl_str != PRIVATE_READ &&
			    acl_str != PRIVATE && 
			    acl_str != PUBLIC_READ &&
			    acl_str != PUBLIC_READ_WRITE && 
			    acl_str != PUBLIC) {
				throw api_exception(AEC_BAD_REQUEST,
				    "'%s' '%s' is invalid. Should be "
				    "one of the valid values "
				    "'%s' or '%s' or '%s' "
				    "or '%s' or '%s' ",
				    HHDR_ACCESS_CONTROL.c_str(), 
				    acl_str.c_str(), PRIVATE_READ, 
				    PRIVATE, PUBLIC_READ, 
				    PUBLIC_READ_WRITE, PUBLIC);
			}
		}
		else {
			/*
			 * If the pre-defined acl is not specified by 
			 * the user, use user input mode. If user 
			 * didnt specify the mode, use default mode.
			 */
			return acl_mode;
		}

		if (acl_str == PRIVATE_READ) {
			acl_mode = NS_PRIVATE_READ;
		}
		else if (acl_str == PRIVATE) {
			acl_mode = NS_PRIVATE;
		}
		else if (acl_str == PUBLIC_READ) {
			acl_mode = NS_PUBLIC_READ;
		}
		else if (acl_str == PUBLIC_READ_WRITE) {
			acl_mode = NS_PUBLIC_READ_WRITE;
		}
		else if (acl_str == PUBLIC) {
			acl_mode = NS_PUBLIC;
		}
	}
	
	/* Ostore headers need to be handled when ostore functionality is 
	 * implemented.
	 */
	
	return acl_mode;
}

/*
 *
 */
void
common_obj_handler::get_access_control(const request &input,
    const oapi_store &store, enum ostore_acl_mode &acl_mode,
    mode_t &mode, bool throw_if_not_found)
{
	std::string acc_ctrl;
	if (input.get_header(HDR_ACCESS_CONTROL, acc_ctrl)) {
		/* Check if any pre-defined acl is provided by the user. */
		acl_mode = get_predefined_acl(input, store);

		/* If no pre-defined acl is specified,
		 * check if mode is specified.
		 * Acl if specified is considered over mode.
		 */
		if (acl_mode == OSTORE_ACL_NONE) {
			mode = get_create_mode(acc_ctrl);
		}
	} else if (throw_if_not_found) {
		throw api_exception(AEC_BAD_REQUEST,
		    "%s is not set in the request.",
		    HHDR_ACCESS_CONTROL.c_str());
	}
}

/**
 * Utility function to set extended attributes in the
 * output http headers
 */
void
common_obj_handler::set_attribute_headers(oapi_genobj &object, response &output)
{
	std::map<std::string, std::string> attr_map;
	isi_error *error = 0;
	object.list_attrs(attr_map, error);
	oapi_error_handler().throw_on_error(error, "get object attributes");

	std::map<std::string, std::string>::const_iterator it;
	std::string header;

	int missing_cnt = 0;
	for (it = attr_map.begin(); it != attr_map.end(); ++it) {

		const std::string  &attr = it->first;
		const std::string &val = it->second;

		if (is_sys_attr(attr))
			continue;

		if (!isi_attribute_to_header(attr, header)) {
			++missing_cnt;
			continue;
		}

		if (val.size() == 0) {
			++missing_cnt;
			continue;
		}

		ilog(IL_DEBUG, "set attr %s to %s len=%zu",
		    attr.c_str(), val.c_str(), val.length());

		output.get_header_map()[header] = val;
	}

	if (missing_cnt > 0) {
		std::stringstream strm;
		strm << missing_cnt;
		output.get_header_map()[HHDR_X_ISI_MISSING_META_] = strm.str();
	}
}

/**
 * Set the target type
 */
void
common_obj_handler::set_target_type_header(oapi_genobj &object,
    response &output)
{
	if (object.get_object_type() == OOT_OBJECT)
		output.get_header_map()[HHDR_TARGET_TYPE] = TT_OBJECT;
	else if (object.get_object_type() == OOT_CONTAINER)
		output.get_header_map()[HHDR_TARGET_TYPE] = TT_CONTAINER;

}

/**
 * SET CONTENT-TYPE header in the output
 */
void
common_obj_handler::set_content_type_header(oapi_genobj &object, response &output)
{
}

/**
 * SET CONTENT-ENCODING header in the output
 */
void
common_obj_handler::set_content_encoding_header(oapi_genobj &object,
    const request &input, response &output)
{

}

void 
common_obj_handler::set_basic_system_attributes(oapi_genobj &object, 
    const request &input, 
    response &output, oapi_genobj::object_sys_attr &sys_attr, 
    bool head_method) 
{
	// get system attributes
	isi_error *error = 0;
	struct fmt FMT_INIT_CLEAN(etag_fmt);

	object.get_sys_attr(sys_attr, error);
	oapi_error_handler().throw_on_error(error,
	    "get object attribute");

	std::string mod_time;
	if (!convert_time_to_http_date(sys_attr.attr_.st_mtime, mod_time)) {
		throw isi_exception(isi_system_error_new(errno,
		    "Error in converting mod time "
		    "%zu to GMT date", sys_attr.attr_.st_mtime));
	}
	output.get_header_map()[api_header::LAST_MODIFIED] = mod_time;

	// Etag: lin-snapid-objrev
	fmt_print(&etag_fmt, "\"%lu-%lu-%lu\"", sys_attr.attr_.st_ino,
	    sys_attr.attr_.st_snapid, sys_attr.objrev);
	output.get_header_map()[api_header::ETAG] = fmt_string(&etag_fmt);

	output.get_header_map()[HHDR_ACCESS_CONTROL] =
	    query_util::get_mode_octal_str(&sys_attr.attr_);

	if (head_method) {
		
		std::stringstream tstrm;
		tstrm << sys_attr.attr_.st_size;
		
		output.get_header_map()[api_header::CONTENT_LENGTH] = 
		    tstrm.str();
		ilog(IL_DEBUG, "Content-Length:%llu", sys_attr.attr_.st_size);
	}
}

/**
 * Set CONTENT-TYPE, CONTENT-ENCODING and extended attributes
 * @throw isi_exception or api_exception if it fails
 */
void
common_obj_handler::set_common_headers(oapi_genobj &object,
    const request &input,
    response &output,
    oapi_genobj::object_sys_attr &sys_attr,
    bool head_method)
{
	// set the target type
	set_target_type_header(object, output);

	// set content type
	set_content_type_header(object, output);

	// set content encoding
	set_content_encoding_header(object, input, output);

	// set the extended attributes:
	set_attribute_headers(object, output);

	set_basic_system_attributes(object, input, output, sys_attr, 
	    head_method);
}

void
common_obj_handler::get_head_info(oapi_genobj &object, const request &input,
    response &output)
{
	oapi_genobj::object_sys_attr attr = {{0}};
	bool head_method = true;

	set_common_headers(object, input, output, attr, head_method);

	handle_conditional_get(object.get_name(), attr, input, output);
}

/**
 * Handle any conditional get headers
 * Refer to http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.25
 * @return true if to continue the rest of the handling, false otherwise
 */
bool
common_obj_handler::handle_conditional_get(
    const std::string & obj_name,
    const oapi_genobj::object_sys_attr &attr,
    const request &input, response &output)
{
	std::string unmod_since;
	if (attr.attr_.st_mtime &&
	    input.get_header(HDR_IF_UNMODIFIED_SINCE, unmod_since)) {
		time_t mt = 0;
		if (convert_http_date_to_time(unmod_since, mt) &&
		    attr.attr_.st_mtime > mt  ) {
			throw api_exception(AEC_PRE_CONDITION_FAILED,
			    "object %s modified since %s",
			    obj_name.c_str(), unmod_since.c_str());
		}
	}

	std::string mod_since;
	if (attr.attr_.st_mtime &&
	    input.get_header(HDR_IF_MODIFIED_SINCE, mod_since)) {
		time_t mt = 0;
		timeval now = {0};
		gettimeofday(&now, NULL);
		if (convert_http_date_to_time(mod_since, mt) &&
		    attr.attr_.st_mtime < mt  &&
		    mt <= now.tv_sec) {
			throw api_exception(AEC_NOT_MODIFIED,
			    "object %s unmodified since %s",
			    obj_name.c_str(), mod_since.c_str());
		}
	}

	// Etag: lin-snapid-objrev
	std::string etag;
	typedef boost::char_separator<char> c_separator;
	typedef boost::tokenizer<c_separator> tokenizer;

	if (input.get_header(HDR_IF_NONE_MATCH, etag)) {
		struct fmt FMT_INIT_CLEAN(actual_etag);
		tokenizer tok(etag, c_separator(", "));

		fmt_print(&actual_etag, "\"%lu-%lu-%lu\"", attr.attr_.st_ino,
		    attr.attr_.st_snapid, attr.objrev);

		if (etag.compare(0, 2, "W/") == 0)
			throw api_exception(AEC_BAD_REQUEST,
			    "Weak comparison is not supported");

		if (etag == "*")
			throw api_exception(AEC_PRE_CONDITION_FAILED,
			    "Operation for %s not performed",
			    obj_name.c_str());

		for (tokenizer::iterator itr = tok.begin();
		    itr != tok.end(); ++itr) {
			if (*itr == fmt_string(&actual_etag)) {
				throw api_exception(AEC_PRE_CONDITION_FAILED,
				    "Operation for %s is not performed",
				    obj_name.c_str());
			}
		}
	}

	if (input.get_header(HDR_IF_MATCH, etag)) {
		struct fmt FMT_INIT_CLEAN(actual_etag);
		bool match = false;

		fmt_print(&actual_etag, "\"%lu-%lu-%lu\"", attr.attr_.st_ino,
		    attr.attr_.st_snapid, attr.objrev);

		if (etag.compare(0, 2, "W/") == 0) {
			throw api_exception(AEC_BAD_REQUEST,
			    "Weak comparison is not supported");
		}
		else if (etag == "*")
			match = true;
		else {
			tokenizer tok(etag, c_separator(", "));

			for (tokenizer::iterator itr = tok.begin();
			    itr != tok.end(); ++itr) {
				if (*itr == fmt_string(&actual_etag)) {
					match = true;
					break;
				}
			}
		}

		if (!match) {
			// If-Match: standard doesn't specify which error to
			// return
			throw api_exception(AEC_PRE_CONDITION_FAILED,
			    "Object %s etag match failed", obj_name.c_str());
		}
	}

	return true;
}

/**
 * Get the metadata from the object opened.
 */
void
common_obj_handler::get_attributes(oapi_genobj &object, const request &input,
    response &output)
{
	isi_error *error = 0;
	std::map<std::string, std::string> attr_map;
	object.list_attrs(attr_map, error);

	if (error && isi_error_is_a(error,
	    OSTORE_UNSUPPORTED_OBJECT_TYPE_ERROR_CLASS)) {
		// just skip for now; ideally, we would like to indicate
		// user attributes not supported on the object
		isi_error_free(error);
		error = 0;
	}
	oapi_error_handler().throw_on_error(error,
	    "list attributes on object");

	Json::Value &j_output = output.get_json();

	Json::Value &attrs = j_output[ATTR_ATTRS];

	std::map<std::string, std::string>::const_iterator it;
	Json::Value attr(Json::objectValue);

	it = attr_map.find(ATTR_CONTENT_TYPE);
	if (it != attr_map.end()) {
		attr[ATTR_NAME] = ISI_CONTENT_TYPE;
		attr[ATTR_VALUE] = it->second;
		attrs.append(attr);
		attr_map.erase(ATTR_CONTENT_TYPE);
	}

	oapi_genobj::object_sys_attr sys_attr = {{0}};
	object.get_sys_attr(sys_attr, error);
	oapi_error_handler().throw_on_error(error, "get system attributes");

	const attribute_manager &attr_mgr =
	    store_provider::get_attribute_mgr(input);

	set_sys_attrs(attr_mgr, object.get_name(), &sys_attr.attr_, attrs);

	set_exended_attrs(attr_map, attrs);
}

/**
 * Set the metadata from the object opened.
 */
void
common_obj_handler::put_attributes(oapi_genobj &object, const request &input,
    response &output)
{
	output.set_status(ASC_OK);

	const Json::Value &j_input = input.get_json();

	ilog(IL_DEBUG, "val=%s", j_input.toStyledString().c_str());

	std::string action;
	validate_input_attrs(input, j_input, action);

	bool replace = (action == ACTION_REPLACE);

	isi_error *error = 0;

	const Json::Value &attrs = j_input[ATTR_ATTRS];

	if (replace) {

		std::map<std::string, std::string> attr_map;
		object.list_attrs(attr_map, error);
		oapi_error_handler().throw_on_error(error,
		    "list attributes on object");

		// remove the existing keys.
		std::map<std::string, std::string>::const_iterator it;
		std::string attr_name;
		for (it = attr_map.begin(); it != attr_map.end(); ++it) {
			attr_name = it->first;
			// we do not allow deleting CONTENT_TYPE attributes

			if (attr_name == ATTR_CONTENT_TYPE ||
			    attr_name == ISI_CONTENT_TYPE) {
				continue;
			}
			object.delete_attr(attr_name.c_str(), error);
			oapi_error_handler().throw_on_error(error,
			    "delete attributes on object");
		}
	}

	Json::Value::const_iterator it;
	std::string attr_name;
	std::string attr_val;
	std::string attr_op;
	std::string attr_ns;

	for (it = attrs.begin(); it != attrs.end(); ++it) {
		const Json::Value &attr = *it;

		bool del = false;

		json_to_str(attr_name, attr, ATTR_NAME, false);
		if (attr_name.empty()) {
			throw api_exception(AEC_BAD_REQUEST,
			    "Attribute name cannot be empty");
		}

		json_to_str(attr_ns, attr, ATTR_NS, false);

		if (!attr_ns.empty() && attr_ns != ATTR_NS_SYSTEM &&
		    attr_ns != ATTR_NS_USER) {
			throw api_exception(AEC_BAD_REQUEST,
			    "Invalid attribute namespace: %s", attr_ns.c_str());
		}

		if ((attr_ns.empty() || attr_ns == ATTR_NS_SYSTEM) &&
		    attr_name != ISI_CONTENT_TYPE) {
			// we only support change content_type attribute
			continue; // other attributes will be ignored
		}

		json_to_str(attr_val, attr, ATTR_VALUE, false);

		if (!replace) {
			// updating, check operation
			json_to_str(attr_op, attr, OP, false);

			if (attr_op == OP_DELETE)
				del = true;
		}

		if (del) {
			if (attr_name == ISI_CONTENT_TYPE) {
				continue;
			}
			ilog(IL_DEBUG, "Deleting attribute %s",
			    attr_name.c_str());
			object.delete_attr(attr_name.c_str(), error);
			oapi_error_handler().throw_on_error(error,
			    "delete attribute on object");
			continue;
		}

		// update case:
		ilog(IL_DEBUG, "Setting attribute %s=%s",
		    attr_name.c_str(), attr_val.c_str());

		const char *name = attr_name.c_str();
		if (attr_name == ISI_CONTENT_TYPE) {
			if(check_content_type_invalid_chars(attr_val)) {
				throw api_exception(AEC_BAD_REQUEST,
				    "Invalid characters in attribute "
				    "content_type: %s", attr_val.c_str());
			}
			name = ATTR_CONTENT_TYPE;
		}

		object.set_attr(name, (void*)attr_val.data(),
		    attr_val.size(), error);
		oapi_error_handler().throw_on_error(error,
		    "set attribute on object");
	}
}

/*
 * Get Access Control List (ACL) via JSON message body
 * This routine is common to get the ACL of 
 * 1. Object store Buckets or Objects. 
 * 2. Namespace Directories or Files.
 */
void
common_obj_handler::get_acl(oapi_genobj &genobject, 
    enum ifs_security_info sec_info,
    ifs_security_descriptor **sd_out)
{
	oapi_genobj &genobj = genobject;
	isi_error *error = NULL;

	genobj.get_acl(sec_info, sd_out, error);

	oapi_error_handler().throw_on_error(error, "Get acl.");
	return;
}

/*
 * Set Access Control List (ACL) via JSON message body
 * This routine is common to set the ACL of 
 * 1. Object store Buckets or Objects.
 * 2. Namespace Directories or Files.
 */

void 
common_obj_handler::set_acl(const request &input, oapi_genobj &gen_object)
{
	isi_error *error = NULL;
	oapi_genobj &genobj = gen_object;

	const Json::Value &j_input = input.get_json();
	ilog (IL_DEBUG, "val=%s", j_input.toStyledString().c_str());

	if (!j_input.isObject()) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid JSON input.");
	}

	std::string auth;
	json_to_str(auth, j_input, AUTHORITATIVE, false);
	if (auth == MODE) {
		mode_t mode = (mode_t) -1;
		uid_t uid = (uid_t) -1;
		gid_t gid = (gid_t) -1;
		get_unix_permission_from_json(j_input, uid, gid, mode);
		genobj.set_access_control(OSTORE_ACL_NONE, mode,
		    uid, gid, error);
		set_access_error_handler::throw_on_error(error);
		return;
	}

	if (auth != ACL) {
		throw api_exception(AEC_BAD_REQUEST,
		    "missing or invalid %s", AUTHORITATIVE);
	}

	oapi_acl oacl;
	enum ifs_acl_mod_type acl_mod_type = IFS_ACL_REPLACE;
	enum ifs_security_info sec_info = IFS_SEC_INFO_ACCESS;
	struct ifs_security_descriptor *sd = NULL;
	bool free_og = true, free_dacl = true;

	// Validate the input argument.
	validate_input_acl(j_input, oacl, &acl_mod_type);

	// This routine will modify the security descriptor to include 
	// the modifications requested.
	// actually, this just modifies ACEs
	genobj.modify_sd(oacl.get_ace_mods(), oacl.get_ace_mods_cnt(),
	    acl_mod_type, sec_info, &sd, error);

	if (error) {
		oapi_error_handler().throw_on_error(error, 
		    "modify security descriptor");
	}
	ASSERT(sd);
	scoped_sd sd_tmp(sd, !free_og, free_dacl); // ensure to free memory

	/* Get scoped sd to hold ownership */
	struct ifs_security_descriptor *sd_new = NULL;
	sd_new = (struct ifs_security_descriptor *)
            malloc(sizeof(struct ifs_security_descriptor));
        ASSERT(sd_new);
	scoped_sd ssd_tmp(sd_new, free_og, !free_dacl);// ensure to free memory

	*sd_new = *sd;
	sd_new->owner = NULL;
	sd_new->group = NULL;
	try {
		update_sd_ownership(j_input, sd, sd_new);
	} catch (...) {
		throw api_exception(AEC_BAD_REQUEST,
		    "invalid owner or group");
	}

	unsigned int sec_info_i = sec_info;
	// remove ownership flag if not set
	if (!sd_new->owner)
		sec_info_i &= ~((unsigned int) IFS_SEC_INFO_OWNER);
	if (!sd_new->group)
		sec_info_i &= ~((unsigned int) IFS_SEC_INFO_GROUP);

	sec_info = static_cast<ifs_security_info> (sec_info_i);

	// Now set the security descriptor. 
	genobj.set_acl(sd_new, sec_info, error);

	if (error)
		oapi_error_handler().throw_on_error(error, "set acl");

}



std::string 
get_protection_level_str(struct protection_policy &level)
{
	switch(level.type) {
		case PROTECTION_TYPE_MIRRORING:
			return "PROTECTION_TYPE_MIRRORING";
		case PROTECTION_TYPE_FEC:
			return "PROTECTION_TYPE_FEC";
		default:
			return "PROTECTION_TYPE_DEFAULT";
	}
}

std::string
get_protection_policy_str(struct protection_policy &policy)
{
	char policy_to_return[25];

	isi_snprintf(policy_to_return, 25, "%{}", protection_policy_nowidth_fmt(policy));
	return std::string(policy_to_return);
}

std::string
get_ssd_strategy_str(enum ifs_ssd_strategy strategy)
{


	struct fmt FMT_INIT_CLEAN(fmt_inst);

	fmt_print(&fmt_inst, "%{}", ssd_strategy_fmt(strategy));


	return std::string(fmt_string(&fmt_inst));
}

/*
 * Check content type characters for invalid characters in security
 */
struct check_content_type_invalid_chars_util : std::unary_function<char, bool> {
	bool operator()(char c) {
		unsigned char uc=reinterpret_cast<unsigned char&>(c);
		return (uc < ' ' && uc != '\t') || uc == '\x7f';
	}
};
bool common_obj_handler::check_content_type_invalid_chars(
    const std::string& content_type_value) {
	return std::any_of(content_type_value.cbegin(),
	    content_type_value.cend(),
	    check_content_type_invalid_chars_util());
}

