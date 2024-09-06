#include "ns_object_handler.h"

#include <sys/isi_privilege.h>

#include <memory>
#include <uuid.h>
#include <vector>
#include <stdio.h>

#include <isi_json/json_helpers.h>
#include <isi_rest_server/api_utils.h>
#include <isi_rest_server/license_info.h>
#include <isi_rest_server/scoped_settcred.h>
#include <isi_util_cpp/http_util.h>
#include <isi_util_cpp/uri_normalize.h>
#include <isi_util_cpp/fmt_helpers.h>
#include <isi_auth_cpp/connection.h>
#include <isi_auth_cpp/auth_persona.h>
#include <isi_object/iobj_ostore_protocol.h>
#include <isi_acl/isi_sd.h>
#include <isi_acl/isi_acl_util.h>
#include <isi_util_cpp/scoped_clean.h>

#include "bucket_handler.h"
#include "handler_common.h"
#include "oapi/oapi_bucket.h"
#include "oapi/oapi_genobj.h"
#include "oapi/oapi_genobj_opener.h"
#include "object_handler.h"
#include "object_query.h"
#include "query_handler.h"
#include "store_provider.h"
#include "acl_common.h"
#include "ns_handler_provider.h"
#include "ns_object_protection.h"
#include "ns_object_worm.h"
#include "oapi_snapshot.h"

#define ATTR_NS_PATH		"path"
#define PARAM_NS_MERGE		"merge"
#define PARAM_NS_CONTINUE	"continue"

const std::string HDR_COPY_SOURCE("X_ISI_IFS_COPY_SOURCE");
const std::string HDR_SET_LOCATION("X_ISI_IFS_SET_LOCATION");
const std::string HDR_TARGET_TYPE("X_ISI_IFS_TARGET_TYPE");
const std::string HDR_MODE_MASK("X_ISI_IFS_MODE_MASK");

const std::string HHDR_COPY_SOURCE("x-isi-ifs-copy-source");
const std::string HHDR_TARGET_TYPE("x-isi-ifs-target-type");
const std::string HHDR_SET_LOCATION("x-isi-ifs-set-location");
const std::string HHDR_MODE_MASK("x-isi-ifs-mode-mask");


namespace {

/**
 * copy operation context for callback
 */
class ns_obj_copy_context {
public:
	ns_obj_copy_context(const request &input, response &output,
	    struct ostore_copy_param &cp_param);
	~ns_obj_copy_context();

	const request &input_;
	response &output_;
	struct ostore_copy_param &cp_param_;

	int count_;
};

ns_obj_copy_context::ns_obj_copy_context(const request &input,
    response &output, struct ostore_copy_param &cp_param)
    : input_(input), output_(output), cp_param_(cp_param),
    count_(0)
{
	cp_param_.base_param.caller_context = this;
}

ns_obj_copy_context::~ns_obj_copy_context()
{

}

/**
 * Get the namespace store name and path
 * @param[IN] uri   decoded uri without leading '/'
 * @param[OUT] sn   store name specified in the uri
 * @param[OUT] path object path specified in the uri
 */
void
parse_uri(const std::string &uri,
    std::string &sn,
    std::string &path)
{
	size_t pos = uri.find_first_of('/', 0);

	sn = uri.substr(0, pos);

	if (pos != std::string::npos)
		path = uri.substr(pos + 1);
}

/**
 * Get the namespace store name and path
 * @param[IN] uri   decoded uri without leading '/'
 * @param[OUT] sn   store name specified in the uri
 * @param[OUT] path object path specified in the uri
 * @param[OUT] bucket bucket path specified in the uri
 */
void
parse_uri(const std::string &uri,
    std::string &sn,
    std::string &path,
    std::string &bucket)
{
	size_t pos = uri.find_first_of('/', 0);
	size_t l_pos = uri.find_last_of('/');

	sn = uri.substr(0, pos);

	if (pos != std::string::npos) {
		path = uri.substr(pos + 1);
		if (pos != l_pos) {
			bucket = uri.substr(pos + 1, l_pos - pos - 1);
		} else {
			bucket = "";
		}
	}
}

/**
 * Parse header for the namespace store name and path
 *
 * @param[IN] uri    string with format of /namespace/<store-name>/<path>
 * @param[IN] header the header name for this uri
 * @param[OUT] sn   store name specified in the uri
 * @param[OUT] path object path specified in the uri
 * @param[OUT] bucket bucket path specified in the uri
 *
 * @throw api_exception if uri format is invalid
 */
void
parse_header_uri(const std::string &uri, const std::string &header,
    std::string &sn, std::string &path, std::string &bucket)
{
	bool good_fmt = false;
	if (uri.compare(0, sizeof(NS_NAMESPACE), NS_NAMESPACE "/") == 0) {
		parse_uri(uri.substr(sizeof(NS_NAMESPACE)), sn, path, bucket);
		if (!sn.empty() && !path.empty())
			good_fmt = true;
	}
	if (!good_fmt) {
		throw api_exception(AEC_BAD_REQUEST,
		    "%s '%s' is invalid.",
		    header.c_str(), uri.c_str());
	}
}

/*
 * Routine to get and parse the mode mask from input for copy flags.
 * @param[IN] target type string.
 * @param[OUT] copy flags mode mask related fields
 *
 * @throw api_exception if mode mask is invalid
 */
inline void
parse_mode_mask_for_copy(const request &input, struct fsu_copy_flags &cp_flags)
{
	std::string mm_str;
	if (!input.get_header(HDR_MODE_MASK, mm_str))
		mm_str = "default";

	if (mm_str == "default")
		cp_flags.mask = S_IWGRP | S_IWOTH;
	else if (mm_str == "preserve")
		cp_flags.pflag = true;
	else {
		const char *p = mm_str.c_str();
		char *ep = NULL;
		long perml = -1;

		// Check if it is digit,
		if (*p && isdigit((unsigned char) *p)) {
			perml = strtol(p, &ep, 8);
		}

		if (perml < 0 || *ep || perml & ~(STANDARD_BITS|S_ISTXT)) {
			throw api_exception(AEC_BAD_REQUEST,
			    "%s '%s' is an invalid octal number.",
			    HHDR_MODE_MASK.c_str(), p);
		}

		cp_flags.mask = perml & 0777;
	}
}

enum privilege_method {
	PM_INVALID,
	PM_GET_CONTENT,
	PM_GET_ATTR,
	PM_GET_HEAD_INFO
};

/**
 * Method invoker: based on the method invoke the appropriate method function
 */
void
execute_method(privilege_method pm, common_obj_handler &handler,
    oapi_genobj &genobj, const request &input, response &output)
{
	switch (pm) {
	case PM_GET_CONTENT:
		handler.get_content(genobj, input, output);
		break;
	case PM_GET_ATTR:
		handler.get_attributes(genobj, input, output);
		break;
	case PM_GET_HEAD_INFO:
		handler.get_head_info(genobj, input, output);
		break;
	default:
		ASSERT(false, "Invalid method, only GET/HEAD is supported.");
		break;
	}
}

/**
 * Create a namespace store access point
 *
 * @param[IN]  input   the request
 * @param[OUT] output  the response
 * @param[IN]  ns      namespace access point name
 * @throws api_exception if failed
 */

void
create_ns_access_point(const request &input, response &output,
    const std::string &ns)
{
	isi_error  *error = 0;
	oapi_store store;

	// to get access point path from json
	// The path is in the following json format:
	// { "path" : "/ifs/abc" }
	const Json::Value &j_input = input.get_json();
	std::string path;

	if (!j_input.isObject()) {
		// we only support JSON object
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid JSON object, please refer to the API"
		    " documentation.");
	}
	
	json_to_str(path, j_input, ATTR_NS_PATH, false);
	if (path.empty())
		throw api_exception(AEC_BAD_REQUEST,
		    "The json content is invalid.");

	// ifs namespace store created internally.
	if (ns == NS_DEFAULT_STORE) {
		throw api_exception(AEC_FORBIDDEN, "'%s' is system "
		    "namespace store, operation is not allowed.",
		    NS_DEFAULT_STORE);
	}

	store.create_store(OST_NAMESPACE, ns, path.c_str(), error);

	if (error) {
		if (isi_error_is_a(error,
		    OSTORE_PATHNAME_NOT_EXIST_ERROR_CLASS)) {
			throw api_exception(error,AEC_BAD_REQUEST,
			    "The namespace path '%s' does not exist.",
			    path.c_str());
		} else if (isi_error_is_a(error,
		    OSTORE_PATHNAME_NOT_DIR_ERROR_CLASS)) {
			throw api_exception(error,AEC_BAD_REQUEST,
			    "The namespace path '%s' must be a directory.",
			    path.c_str());
		} else if (isi_error_is_a(error,
		    OSTORE_PATHNAME_NOT_ONEFS_ERROR_CLASS)) {
			throw api_exception(error,AEC_BAD_REQUEST,
			    "The namespace path '%s' must be on the "
			    "OneFS file system.", path.c_str());
		}
	}

	// any special error before turn to generic handler?
	oapi_error_handler().throw_on_error(error, "create store");
}

/**
 * writes one copy status to json output
 */
static void
write_one_status(ns_obj_copy_context *ctx, const struct fsu_copy_rpt *rpt_info)
{
	static const std::string src_side("source side");
	static const std::string tgt_side("target side");
	Json::Value objectVal;

	//Keep some field internal
	//objectVal["severity"] =
	//    rpt_info->severity == COPY_RPT_INFO ? "info" : "error";
	objectVal[ISI_SOURCE] = rpt_info->src;
	objectVal[ISI_TARGET] = rpt_info->dest;
	objectVal[ISI_ERROR_SRC] = rpt_info->on_dest ? tgt_side : src_side;
	objectVal[ISI_MESSAGE] = rpt_info->msg;

	std::string obj = objectVal.toStyledString();
	ctx->output_.write_data(obj.c_str(), obj.size());

	++ctx->count_;
}

/**
 * copy back function to receive copy report of error or other msgs
 *
 * @param[IN] context - the context passed down to copy api
 * @param[IN] rpt_info - the report generated from copy operation
 * @return true instructing to continue; false otherwise
 */
bool
ns_obj_copy_callback(void *context, const struct fsu_copy_rpt *rpt_info)
{
	ns_obj_copy_context *ctx = (ns_obj_copy_context *) context;

	try {
		// Do we have other better way to report error?
		// May we just stop copying after certain number of errors ?
		// or write to local temp file and allow user to get it later
		if (ctx->count_ == 0) {
			// we are writing a json text manually:
			ctx->output_.set_content_type(
			    api_content::APPLICATION_JSON);

			// write the array specifier "["
			static const std::string begin("{\"copy_errors\":[");
			ctx->output_.write_data(begin.c_str(), begin.size());
		}
		else {
			ctx->output_.write_data(",", sizeof(",") - 1);
		}

		write_one_status(ctx, rpt_info);
	} catch (isi_exception &exp) {
		ilog(IL_DEBUG, "failed to send copy status to client %s",
		    exp.what());
	}
	return true;
}

/**
 * Routine to handle the create container argument
 */
void
handle_create_container(
    const request     &input,
    int               create_container,
    bool              overwrite,
    oapi_store        &dest_store,
    const std::string &dest_bucket,
    isi_error         **error_out)
{
	isi_error	*error = NULL;
	oapi_bucket	bucket(dest_store);

	//
	// Make sure we can open the bucket, even though we may
	// not want to create it.  There are cases later, i.e.
	// during copy where use report_callback to report errors,
	// where we have no way of returning the specific
	// error, so check it now.
	bucket.open(SH_ACCT, dest_bucket.c_str(),
	    (enum ifs_ace_rights) BUCKET_GEN_WRITE, 0,
	    CF_FLAGS_NONE, error);

	if ((error) &&
	    (create_container != 0) &&
	    isi_error_is_a(error, OSTORE_OBJECT_NOT_FOUND_ERROR_CLASS)) {
		isi_error_free(error);
		error = NULL;
		ns_object_handler::create_bucket_containers(
		    input, bucket,
		    dest_bucket, dest_store,
		    false, overwrite,
		    create_container, &error);
	}
	bucket.close();

	isi_error_handle(error, error_out);
	return;
}

/**
 * Routine to copy
 *
 * @throw exception if failed
 */
void
handle_copy(const request &input, response &output,
    const std::string &src_uri)
{
	isi_error *error = 0;
	oapi_store src_store, dest_store;
	struct ifs_createfile_flags cf_flags = CF_FLAGS_NONE;

	std::string src_sn, src_path, src_bucket;
	parse_header_uri(src_uri, HHDR_COPY_SOURCE, src_sn, src_path, src_bucket);
	normalize_uri(src_path);

	std::string dest_sn, dest_path, dest_bucket;
	ns_object_handler::get_input_params(input, dest_sn,
	    dest_path, dest_bucket);

	store_provider::open_store(src_store, NS_NAMESPACE, src_sn);
	store_provider::open_store(dest_store, NS_NAMESPACE, dest_sn);

	// check store name (current specs v1.0)
	if (src_sn != dest_sn)
		throw api_exception(AEC_BAD_REQUEST,
		    "The source namespace '%s' must be the same as target "
		    "'%s' for the copy operation.",
		    src_sn.c_str(), dest_sn.c_str());

	bool overwrite = false;
	struct fsu_copy_flags cp_flags = {0};

	arg_to_bool arg_tb;
	const request::arg_map &arg_m = input.get_arg_map();
	find_args(overwrite, arg_tb, arg_m, PARAM_OVERWRITE);
	cp_flags.noovw = !overwrite;

        bool clone = false;
	find_args(clone, arg_tb, arg_m, PARAM_CLONE);
	cp_flags.cflag = clone;

	int create_container = 0;
	find_args(create_container, arg_to_generic<int>(), arg_m,
	    PARAM_CREATE_CONTAINER);

	parse_mode_mask_for_copy(input, cp_flags);

	std::string snapname;
	std::auto_ptr<oapi_snapshot> s_ptr;

        if (clone == true) {
		input.get_arg(PARAM_SNAPSHOT_NAME, snapname);
		s_ptr = std::auto_ptr<oapi_snapshot>(new oapi_snapshot(snapname,
			src_sn, src_path, &error));
		src_path.assign(s_ptr->src_path_in_snapshot);
	}

	if (error) {
		if (isi_system_error_is_a(error, EISDIR)) {
			throw api_exception(error, AEC_BAD_REQUEST,
			    "Source path is a directory, cloning of "
			    "directory not supported.");
		} else if (isi_system_error_is_a(error, ENOENT)) {
			throw api_exception(error, AEC_NOT_FOUND,
			    "The source '%s' does not exist.",
			    src_path.c_str());
		} else
			throw api_exception(error, AEC_EXCEPTION);
	}

	handle_create_container(input, create_container, overwrite,
	    dest_store, dest_bucket, &error);

	oapi_error_handler().throw_on_error(error, "Create Container");

	oapi_genobj *genobj = ns_handler_provider::open_genobj(
	    src_store, src_path.c_str(),
	    (enum ifs_ace_rights) (IFS_RTS_FILE_READ_DATA | IFS_RTS_DIR_LIST),
	    cf_flags);
	std::auto_ptr<oapi_genobj> genobj_ptr(genobj); //ensure release res.

	if (genobj->get_object_type() == OOT_CONTAINER) {
		bool cont = false, merge = false, recur = true;
		find_args(merge, arg_tb, arg_m, PARAM_NS_MERGE);
		find_args(cont, arg_tb, arg_m, PARAM_NS_CONTINUE);
		find_args(recur, arg_tb, arg_m, PARAM_RECURSIVE);
		cp_flags.cont = cont;
		cp_flags.merge = merge;
		cp_flags.recur = recur;

		if (recur && cp_flags.cflag)
			throw api_exception(AEC_BAD_REQUEST,
			    "Source path is a directory, cloning of "
			    "directory not supported.");
		if (!recur) {
			throw api_exception(AEC_BAD_REQUEST,
			    "Source path '%s' is a directory.",
			    src_sn.c_str());
		}
	}

	struct ostore_copy_param cp_param;
	cp_param.base_param.cp_flags = cp_flags;
	cp_param.base_param.src = src_path.c_str();
	cp_param.base_param.dest = dest_path.c_str();
	cp_param.base_param.cp_callback = ns_obj_copy_callback;
	cp_param.base_param.caller_context = NULL;
	cp_param.src_ios = src_store.get_handle();
	cp_param.dest_ios = dest_store.get_handle();

	ns_obj_copy_context cp_ctx(input, output, cp_param);

	int rval = oapi_genobj::copy(SH_ACCT, &cp_param, error);


	if (cp_ctx.count_ > 0) {
		// free the error if any to avoid memory leak as write_data
		// can throw in case of network write failure.
		isi_error_free(error);

		output.write_data("]", sizeof("]") - 1);

		std::string success(",\"success\":");
		success.append(rval ? "false" : "true");
		output.write_data(success.c_str(), success.length());

		// write the closing bracket:
		output.write_data("}", sizeof("}") - 1);

		return;
	}

	if (!error)
		return;

	if (isi_error_is_a(error, OSTORE_COPY_TO_SUB_DIR_ERROR_CLASS)) {
		throw api_exception(error, AEC_FORBIDDEN, "Source "
		    "directory is same to or parent of the destination");
	}

	oapi_error_handler().throw_on_error(error, "copy");

}

/**
 * Routine to move
 *
 * @param[IN]  input   the request
 * @param[OUT] output  the response
 * @param[IN]  set_Loc URI specifying where to move
 *
 * @throws api_exception if failed
 */
void
handle_move(const request &input, response &output,
    const std::string &set_loc)
{
	isi_error *error = 0;
	oapi_store src_store, dest_store;

	std::string src_sn, src_path;
	std::string dest_sn, dest_path, dest_bucket;
	parse_header_uri(set_loc, HHDR_SET_LOCATION, dest_sn,
	    dest_path, dest_bucket);
	normalize_uri(dest_path);
	normalize_uri(dest_bucket);

	ns_object_handler::get_input_params(input, src_sn, src_path);

	store_provider::open_store(src_store, NS_NAMESPACE, src_sn);
	store_provider::open_store(dest_store, NS_NAMESPACE, dest_sn);

        // Overwrite
	bool overwrite = false;
        struct fsu_move_flags mv_flags = {0};

        arg_to_bool arg_tb;
	const request::arg_map &arg_m = input.get_arg_map();
	find_args(overwrite, arg_tb, arg_m, PARAM_OVERWRITE);
	mv_flags.ovw = overwrite;

	int create_container = 0;
	find_args(create_container, arg_to_generic<int>(), arg_m,
	    PARAM_CREATE_CONTAINER);

	handle_create_container(input, create_container, overwrite,
	    dest_store, dest_bucket, &error);
	oapi_error_handler().throw_on_error(error, "Create Container");

	output.set_status(ASC_NO_CONTENT); // default
	int rval = oapi_genobj::move(SH_ACCT, src_store.get_handle(),
	    src_path.c_str(), dest_store.get_handle(), dest_path.c_str(),
	    &mv_flags, error);

	if (!rval) {
		ASSERT(!error);
		return;
	}
	ASSERT(error);

	std::string src_uri(NS_NAMESPACE);
	src_uri.append(src_sn + "/" + src_path);
	if (rval == -1) {// error prior to move
		if (isi_system_error_is_a(error, EEXIST)) {
			throw api_exception(error, AEC_FORBIDDEN,
			    "The destination '%s' exists.",
			    set_loc.c_str());
		} else if (isi_system_error_is_a(error, ENOENT)) {
			throw api_exception(error, AEC_NOT_FOUND,
			    "The source '%s' does not exist.",
			    src_uri.c_str());
		} else if (isi_system_error_is_a(error, ENOTDIR)) {
			throw api_exception(error, AEC_NOT_FOUND,
			    "A component of the source path '%s' is not "
			    "a directory.", src_uri.c_str());
		} else if (isi_error_is_a(error,
		    OSTORE_PATHNAME_PROHIBITED_ERROR_CLASS)) {
			throw api_exception(error, AEC_FORBIDDEN,
			    "Access to system directory is prohibited.");
		}
	} else if (rval == -2) { //error from rename syscall
		if (isi_system_error_is_a(error, EINVAL)) {
			throw api_exception(error, AEC_FORBIDDEN,
			    "The source is a parent directory of the "
			    "destination.");
		} else if (isi_system_error_is_a(error, ENOENT)) {
			throw api_exception(error, AEC_NOT_FOUND,
			    "A path prefix of the destination doesn't exist "
			    "or a component of the source path doesn't exist.");
		} else if (isi_system_error_is_a(error, ENOTDIR)) {
			throw api_exception(error, AEC_FORBIDDEN,
			    "A component of the source or destination path "
			    "is not a directory; or the source is a directory "
			    "but the destination is not.");
		} else if (isi_system_error_is_a(error, EISDIR)) {
			throw api_exception(error, AEC_FORBIDDEN,
			    "The source is a not a directory but destination "
			    "is a directory.");
		} else if (isi_system_error_is_a(error, ENOTEMPTY)) {
			throw api_exception(error, AEC_FORBIDDEN,
			    "The destination is a directory and not empty.");
		} else if (isi_system_error_is_a(error, EDOM)) {
			throw api_exception(error, AEC_FORBIDDEN,
			    "The source and target don't have same protection "
			    "domain.");
		}
	}
	oapi_error_handler().throw_on_error(error, "move");
}

/**
 * Create a namespace directory or file
 *
 * @param[IN]  input   the request
 * @param[OUT] output  the response
 * @param[IN]  target  target dir or file to be created
 * @throws api_exception if failed
 */
void
handle_create(const request &input, response &output,
    const std::string &target)
{
	// handle create
	std::string tt;
	ns_object_handler::get_target_type(input, tt);

	if (tt.empty())
		throw api_exception(AEC_ARG_REQUIRED,
		    "'%s' must be set.", HHDR_TARGET_TYPE.c_str());
	else if (tt != TT_CONTAINER && tt != TT_OBJECT)
		throw api_exception(AEC_BAD_REQUEST,
		    "'%s' must be '%s' or '%s'.", HHDR_TARGET_TYPE.c_str(),
		    TT_CONTAINER.c_str(), TT_OBJECT.c_str());

	if (tt == TT_CONTAINER) {
		oapi_store  store;
		bool recursive 	= false;
		bool overwrite 	= true;
		store_provider::open_store(store, input);
		oapi_bucket bucket(store);
		isi_error *error = NULL;

		find_args(recursive, arg_to_bool(), input.get_arg_map(),
		    PARAM_RECURSIVE);

		find_args(overwrite, arg_to_bool(), input.get_arg_map(),
		    PARAM_OVERWRITE);

		ns_object_handler::create_bucket_containers(input, bucket,
		    target, store,
		    recursive, overwrite,
		    0, &error);
		if (error) {
			if (isi_system_error_is_a(error, ENOTDIR)) {
				throw api_exception(error, AEC_BAD_REQUEST,
				    "A component of path '%s' is not a "
				    "directory.", target.c_str());
			} else if (isi_system_error_is_a(error, ENOENT)) {
				throw api_exception(error, AEC_BAD_REQUEST,
				    "A component of path '%s' does not exist.",
				    target.c_str());
			}
			//TBD: more specific error ?
			oapi_error_handler().throw_on_error(error,
			    "create directory as requested");
		}

	} else {
		object_handler handler;
		handler.put_object(input, output);
	}
}

/**
 * Routine to delete a store
 * @throw exception if failed
 */
void
delete_store(const request &input, response &output)
{
	isi_error  *error = 0;
	oapi_store store;

	const std::string &uri = input.get_token("OBJECT");
	size_t pos = uri.find_first_of('/', 0);
	std::string store_name = uri.substr(0, pos);

	// deletion of ifs not allowed.
	if (store_name == NS_DEFAULT_STORE) {
		throw api_exception(AEC_FORBIDDEN, "'%s' is system "
		    "namespace store, operation is not allowed.",
		    NS_DEFAULT_STORE);
	}

	try {
		store_provider::open_store(store, input);
	}
	catch (const api_exception &exp) {
		if (exp.error_code() == AEC_NOT_FOUND)
			return;
		else
			throw;
	}

	store.destroy_store(error);
	oapi_error_handler().throw_on_error(error, "delete store");
}


/* 
 * This function will form the JSON output data for displaying the 
 * acl of the requested entity.
 * The entity can be Namespace directory or file.
 */

void 
construct_acl_output(oapi_genobj &genobj,
    struct ifs_security_descriptor *sd, 
    response &output)
{
	oapi_genobj::object_sys_attr sys_attr;
	struct isi_error *error = 0;
	genobj.get_sys_attr(sys_attr, error);
	oapi_error_handler().throw_on_error(error, "stat object");

	bool has_ntfs_acl_f =
	    ((sys_attr.attr_.st_flags & SF_HASNTFSACL) == SF_HASNTFSACL);

	OSTORE_OBJECT_TYPE obj_type = genobj.get_object_type();

	Json::Value &j_output = output.get_json();
	Json::Value acl(Json::objectValue);
	Json::Value ace_array (Json::arrayValue);
	output.set_status(ASC_OK);
	
	if (has_ntfs_acl_f)
		acl[AUTHORITATIVE] = ACL;
	else
		acl[AUTHORITATIVE] = MODE;


	acl[MODE] = query_util::get_mode_octal_str(&sys_attr.attr_);

	uint i = 0, j = 0;
	std::vector <std::string> access_rights;
	std::vector <std::string> inherit_flags;
	enum ifs_ace_rights_type ace_rights_type;

	const ifs_ace *ace_entry = NULL;

	if (obj_type == OOT_CONTAINER)
		ace_rights_type = IFS_RTS_DIR_TYPE;
	else
		ace_rights_type = IFS_RTS_FILE_TYPE;

	if (sd == NULL)
		return;

	auth::lsa_connection connection;

	// Move the contents of the acl to the json output.
	acl[ISI_OWNER] = auth::persona_to_json(sd->owner, connection);
	acl[ISI_GROUP] = auth::persona_to_json(sd->group, connection);

	if (sd->dacl == NULL)
		goto out;
	
	// Loop through the ace's and construct the json output.
	for (i = 0; i < sd->dacl->num_aces; i++) {
		Json::Value ace (Json::objectValue);
		struct fmt FMT_INIT_CLEAN(fmt_flags);
		struct fmt FMT_INIT_CLEAN(fmt_access_mask);
		
		ace_entry = aclu_get_ace(sd->dacl, i, ace_entry);

		auth::persona trustee(aclu_get_trustee(ace_entry));
		ace[TRUSTEE] = auth::persona_to_json(trustee, connection);

		ace[ACCESSTYPE] = isi_printf_to_str("%{}",
		    ace_type_fmt(ace_entry->type));

		// Store the permission list as array.
		Json::Value access_rights_array (Json::arrayValue);

		// Get the access rights as displayable string.
		fmt_print(&fmt_access_mask, "%{}", 
		    access_mask_fmt(ace_entry->access_mask, ace_rights_type));
 
		access_rights = 
		    request::tokenize(fmt_string(&fmt_access_mask), ",", 
			false);
		
		for (j = 0; j < access_rights.size(); j++) {
			access_rights_array.append(access_rights[j]);
		}
		
		Json::Value inherit_flags_array (Json::arrayValue);
		// Store the inheritance flags as array.
		fmt_print(&fmt_flags, "%{}", ace_flags_fmt(ace_entry->flags));

		inherit_flags =
		    request::tokenize(fmt_string(&fmt_flags), ",", 
			false);
		
		for (j = 0; j < inherit_flags.size(); j++) {
			inherit_flags_array.append(inherit_flags[j]);
		}
		
		ace[ACCESSRIGHTS] = access_rights_array;
		ace[INHERIT_FLAGS] = inherit_flags_array;

		ace_array.append(ace);
 	}

out:
	acl[OHQ_ACL] = ace_array;

	// Update the json output.
	j_output = acl;
}


/**
 * This encapsulates the mechanism of running with ISI_PRIV_PAPI_IFS_TRAVERSE
 * Only HEAD and GET is supported.
 * The privilege is only allowed on listing directories or getting directory
 * or file attributes.
 */
void
execute_with_privilege(const request &input, response &output)
{
	privilege_method pm = PM_INVALID;
	request::method mthd = input.get_method();
	bool run_as_root = false;

	if (mthd == request::GET) {
		if (is_get_put_attr(input))
			pm = PM_GET_ATTR;
		else
			pm = PM_GET_CONTENT;
	}
	else
		pm = PM_GET_HEAD_INFO;

	if (ipriv_check_cred(input.get_user_token(),
	    ISI_PRIV_NS_IFS_ACCESS, ISI_PRIV_FLAG_NONE) == 0 &&
	    ipriv_check_cred(input.get_user_token(),
	    ISI_PRIV_NS_TRAVERSE, ISI_PRIV_FLAG_NONE) == 0)
		run_as_root = true;

	// We run as root only for users with the privilege
	if (run_as_root) {
		scoped_settcred_root
		    handler_cred(input.get_user_token(), run_as_root);

		enum ifs_ace_rights ace_rights_open_flg = 
		    (enum ifs_ace_rights) (IFS_RTS_DIR_LIST | 
			IFS_RTS_DIR_READ_ATTRIBUTES | 
			IFS_RTS_FILE_READ_ATTRIBUTES);
		
		ns_handler_provider provider(input, output,
		    ace_rights_open_flg);
		common_obj_handler &handler = provider.get_handler();

		oapi_genobj &genobj = provider.get_obj();

		// not a directory and not getting metadata,
		// do not run with the privilege:
		if (genobj.get_object_type() == OOT_CONTAINER ||
		    pm == PM_GET_ATTR || pm == PM_GET_HEAD_INFO) {
			execute_method(pm, handler, genobj, input, output);
			return;
		}
		run_as_root = false;
	}

	if (!run_as_root) {
		// If this section is executed, then the http_get
		// is trying to get the contents of the file.
		enum ifs_ace_rights ace_rights_open_flg = 
		    IFS_RTS_FILE_READ_DATA; 
		ns_handler_provider provider(input, output, 
		    ace_rights_open_flg);
		common_obj_handler &handler = provider.get_handler();
		oapi_genobj &genobj = provider.get_obj();
		execute_method(pm,  handler, genobj, input, output);
	}
}

void
put_attributes(const request &input, response &output)
{
	enum ifs_ace_rights ace_rights_open_flg = 
	    (enum ifs_ace_rights) (IFS_RTS_DIR_WRITE_ATTRIBUTES | 
		IFS_RTS_DIR_WRITE_EA |
		IFS_RTS_FILE_READ_ATTRIBUTES  | 
		IFS_RTS_FILE_READ_EA);

	ns_handler_provider provider(input, output, ace_rights_open_flg);
	common_obj_handler &handler = provider.get_handler();
	oapi_genobj &genobj = provider.get_obj();
	handler.put_attributes(genobj, input, output);
}

/*
 * The function set_acl will set the permissions of the entity.
 * The entity can be a directory or file.
 */
void 
set_acl(const request &input, response &output)
{
	ns_handler_provider provider(input, output, IFS_RTS_STD_WRITE_DAC);
	common_obj_handler &handler = provider.get_handler();
	oapi_genobj &genobj = provider.get_obj();
	handler.set_acl(input, genobj);
}

}

ns_object_handler::ns_object_handler()
	: uri_handler()
{
	supported_methods_[request::GET] = true;
	supported_methods_[request::PUT] = true;
	supported_methods_[request::POST] = true;
	supported_methods_[request::DELETE] = true;
	supported_methods_[request::HEAD] = true;
}

void
ns_object_handler::http_get(const request &input, response &output)
{
	bool get_acl = false;

	// Check if the acl is requested.
	get_acl = is_get_put_acl(input);
	if (get_acl) {
		struct ifs_security_descriptor *sd_out = NULL;
		// Invoke the get acl method and return the output.
		// This is common to both directory and files in the 
		// Namespace. The Get ACL returns output if the user
		// who is requesting the data has read privilege to  
		// acl of the directory or file requested.
		ns_handler_provider provider(input, output, 
		    IFS_RTS_STD_READ_CONTROL);
		common_obj_handler &handler = provider.get_handler();
		oapi_genobj &genobj = provider.get_obj();
		
		output.set_status(ASC_OK);
		handler.get_acl(genobj, IFS_SEC_INFO_ACCESS, &sd_out);

		construct_acl_output(genobj, sd_out, output);
	   
		// Free the security descriptor memory.
		if (sd_out) {
			free(sd_out);
		}
		
		return; 
	}  // get_acl.

	if (is_worm_request(input))
		return ns_object_worm::execute_get(input, output);

	if(is_get_put_protection(input))
		return ns_object_protection::execute_get(input, output);


	execute_with_privilege(input, output);
}

void
ns_object_handler::http_put(const request &input, response &output)
{
	std::string sn, target;
	get_input_params(input, sn, target);

	output.set_status(ASC_OK);

	// put attributes:
	if (is_get_put_attr(input))
		return put_attributes(input, output);

	if (is_get_put_acl(input))
		return set_acl(input, output);

	if (is_worm_request(input))
		return ns_object_worm::execute_put(input, output);

	if(is_get_put_protection(input))
		return ns_object_protection::execute_put(input, output);
	
	// when target is empty, creating access point is the only action
	if (target.empty())
		return create_ns_access_point(input, output, sn);

	// copy
	std::string src_uri;
	if (input.get_header(HDR_COPY_SOURCE, src_uri))
		return handle_copy(input, output, src_uri);

	//last for creation
	handle_create(input, output, target);
}

void
ns_object_handler::http_post(const request &input, response &output)
{
	enum ifs_ace_rights ace_rights_open_flg;

	// We only support POST on containers for the following:
	// Creating new objects and query, and move.

	// move
	std::string set_loc;
	if (input.get_header(HDR_SET_LOCATION, set_loc))
		return handle_move(input, output, set_loc);

	if (is_object_query(input)) {
		ace_rights_open_flg = (enum ifs_ace_rights) BUCKET_READ;
	}
	else {
		ace_rights_open_flg = (enum ifs_ace_rights) BUCKET_WRITE;
	}

	ns_handler_provider provider(input, output, ace_rights_open_flg);
	common_obj_handler &handler = provider.get_handler();
	oapi_genobj &genobj = provider.get_obj();
	handler.handle_post(genobj, input, output);
}


void
ns_object_handler::http_delete(const request &input, response &output)
{
	output.set_status(ASC_NO_CONTENT);
	std::string ns, target;
	get_input_params(input, ns, target);

	// when target is empty, no other action except removing access point
	if (target.empty())
		return delete_store(input, output);

	// dir or file removing
	isi_error *error = 0;
	struct ostore_del_flags flags = {0};
	bool recur = false;
	find_args(recur, arg_to_bool(), input.get_arg_map(),
	    PARAM_RECURSIVE);
	//TBD: more support?
	flags.worm = is_worm_request(input);
	flags.recur = recur;
	flags.cont_on_err = true;

	try {
		// If directory or file, the delete acl can be specified 
		// at the current directory or file level 
		// (IFS_RTS_STD_DELETE) or it can be specified at the 
		// parent directory level (IFS_RTS_DIR_DELETE_CHILD).
		ns_handler_provider provider(input, output);
		provider.get_obj().remove(target.c_str(), flags, error);
	}
	catch (const api_exception &exp) {
		if (exp.error_code() == AEC_NOT_FOUND)
			return;
		else
			throw;
	}

	if (error) {
		if (isi_system_error_is_a(error, ENOENT)) {
			isi_error_free(error);
			return;
		}
		/*
		 * The ENOTEMPTY is checked for recursive deletion of 
		 * directories. This scenario is a race condition, 
		 * when one thread creates sub-directories under the 
		 * directory while other threads attempts deletion 
		 * of the directory.
		 */
		if (recur && isi_system_error_is_a(error, ENOTEMPTY) &&
		    flags.cont_on_err) {
			throw api_exception(error, AEC_CONFLICT,
			    "The directory '%s' is not empty.",
			    target.c_str());			
		}
		/* 
		 * This error occurs when deletion of a directory is 
		 * attempted without setting the recursive delete flag 
		 * or cont_on_error flag or both.
		 */
		if (isi_system_error_is_a(error, ENOTEMPTY)) {
			throw api_exception(error, AEC_FORBIDDEN,
			    "The directory '%s' is not empty.",
			    target.c_str());
		}
		/*
		 * The EPERM error code is returned for WORM deletes if
		 * deleting is not enabled for that domain.
		 */
		if (flags.worm) {
			if (isi_system_error_is_a(error, EPERM)) {
				throw api_exception(error, AEC_FORBIDDEN,
				    "Operation not permitted.  "
				    "Please verify that privileged delete "
				    "is enabled.");
			} else if (isi_system_error_is_a(error, EFTYPE)) {
				throw api_exception(error, AEC_BAD_REQUEST,
				    "Inappropriate file type or format.  "
				    "Please verify that the file is "
				    "committed.");
			}
		}

		//TBD: more specific error ?
		oapi_error_handler().throw_on_error(error,
		    "delete directory or file");
	}
}


void
ns_object_handler::http_head(const request &input, response &output)
{
	execute_with_privilege(input, output);
}

bool
ns_object_handler::validate_input(const request &input, response &output)
{
	// no json input
	return true;
}

void
ns_object_handler::get_target_type(const request &input,
    std::string &target_type)
{
	input.get_header(HDR_TARGET_TYPE, target_type);
}

/**
 * Get the namespace store name and path from request
 */
void
ns_object_handler::get_input_params(const request &input,
    std::string &ns,
    std::string &path)
{
	const std::string &uri = input.get_token("OBJECT");
	parse_uri(uri, ns, path);
	if (ns.empty()) {
		throw api_exception(AEC_BAD_REQUEST,
		    "The namespace name is missing from the URI.");
	}
}

/**
 * Get the namespace store name, path and bucket from request
 */
void
ns_object_handler::get_input_params(const request &input,
    std::string &ns,
    std::string &path,
    std::string &bucket)
{
	const std::string &uri = input.get_token("OBJECT");
	parse_uri(uri, ns, path, bucket);
	if (ns.empty()) {
		throw api_exception(AEC_BAD_REQUEST,
		    "The namespace name is missing from the URI.");
	}
}

/**
 * Function responsible for creating containers in the bucket
 * Refactored out from handle_create
 * Can throw std::exception
 */
void
ns_object_handler::create_bucket_containers(
    const request 	&input,
    oapi_bucket 	&bucket,
    const std::string	&bucket_name,
    oapi_store    	&store,
    bool 		recursive,
    bool 		overwrite,
    int			levels,
    struct isi_error	**error_out)
{
	isi_error 	*error 		= NULL;
	enum ostore_acl_mode acl_mode 	= OSTORE_ACL_NONE;
	mode_t 		c_mode 		= DEFAULT_MODE_CONTAINER;

	common_obj_handler::get_access_control(input, store,
	    acl_mode, c_mode, false); // fine if not set

	// protection
	struct iobj_set_protection_args *prot =
	    ns_object_protection::get_obj_set_protection_args(input);
	scoped_ptr_clean<struct iobj_set_protection_args> se(prot,
	    &genobj_set_protection_args_release); // self release

	struct iobj_object_attrs *attrs =
	    common_obj_handler::get_obj_attr_list(input);

	if (levels < 0) {
		/*
		 * The request came in on an object type and is
		 * specifically requesting that we create ALL
		 * missing directories in the bucket path
		 */
		bucket.create(SH_ACCT, bucket_name.c_str(), c_mode,
		    attrs, prot, true, acl_mode, overwrite, error);
	} else  if (recursive || (levels < 2)) {
		/*
		 * The request is either to create ALL directories recursively,
		 * or to create just one level.  In either case we can
		 * accomplish this with a single call.
		 */
		bucket.create(SH_ACCT, bucket_name.c_str(), c_mode,
		    attrs, prot, recursive, acl_mode, overwrite, error);
	} else {
		/*
		 * The request is not recursive, so we do not want to
		 * create directories before the requested level.  Strip off
		 * all but the first level and do a non-recursive create.
		 * If that succeeds do a recursive create for the whole path
		 */
		std::string subpath = bucket_name;
		int indx;
		size_t pos = 0;
		for (indx = 1; indx < levels; indx++) {
			pos = subpath.find_last_of('/');
			if (pos != std::string::npos) {
				subpath = subpath.substr(0, pos);
			} else {
				subpath = bucket_name;
				break;
			}
		}
		if (pos != std::string::npos) {
			bucket.create(SH_ACCT, subpath.c_str(), c_mode,
			    attrs, prot, false, acl_mode, overwrite, error);
			if (error &&
			    isi_error_is_a(error,
			    OSTORE_BUCKET_EXISTS_ERROR_CLASS)) {
				/*
				 * The first level already exists now
				 * continue to the second to nth
				 */
				isi_error_free(error);
				error = NULL;
			}
		}
		if (!error) {
			bucket.close();
			bucket.create(SH_ACCT, bucket_name.c_str(), c_mode,
			    attrs, prot, true, acl_mode, overwrite, error);
		}
	}
	/*
	 * Ignore bucket exists error only when either:
	 * 1. overwrite==true (RAN spec), or
	 * 2. input has option create_container (CloudPools)
	 */
	request::arg_map args = input.get_arg_map();
	if ((overwrite || args.find("create_container") != args.end())
	    && error
	    && isi_error_is_a(error, OSTORE_BUCKET_EXISTS_ERROR_CLASS)) {
		/*
		 * Ignore bucket exists.  We do not come here unless at
		 * 1 directory is missing, but someone may have best us
		 * to the punch
		 */
		isi_error_free(error);
		error = NULL;
	}


	if (attrs) {
		genobj_attribute_list_release(attrs, &error);
	}

	isi_error_handle(error, error_out);
}

