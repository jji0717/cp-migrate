
#include "ns_handler_provider.h"

#include <isi_rest_server/api_utils.h>
#include <isi_util_cpp/user_text.h>

#include "acl_common.h"
#include "bucket_handler.h"
#include "ns_object_handler.h"
#include "ns_object_worm.h"
#include "object_handler.h"
#include "store_provider.h"

#include "oapi/oapi_bucket.h"
#include "oapi/oapi_genobj.h"
#include "oapi/oapi_genobj_opener.h"

#define PARAM_NS_ACCESS         "nsaccess"
#define PARAM_CASE_SENSITIVE    "case-sensitive"

namespace {

/**
 * Common routine to open a generic object without knowing
 * the concrete object type.
 * @return the oapi_genobj on success, on failure, an exception is thrown.
 * @throws exception when open failed.
 */
oapi_genobj *
open_genobj(oapi_store &store, const request &input,
    enum ifs_ace_rights ace_rights_open_flg)
{
	std::string ns;
	std::string target;
	bool nsaccess = false;

	ns_object_handler::get_input_params(input, ns, target);

	// check nsaccess only if path is root of the store
	if (target == "" || target == "/") {
		find_args(nsaccess, arg_to_bool(),
		    input.get_arg_map(), PARAM_NS_ACCESS);
	}

	// If this is a request for symlink worm info, flag it.
	bool follow_symlinks = true;
	if (find_args(follow_symlinks, arg_to_bool(), input.get_arg_map(),
	    OHQ_FOLLOW_SYMLINKS) && follow_symlinks == false) {
		if (!is_worm_request(input)) {
			// Response for symlink general info not currently available,
			// only WORM info. (TODO: General info might be a useful feature.)
			throw api_exception(AEC_BAD_REQUEST,
			    get_user_text(RAN_NOSYMLINKS_NEEDS_WORM),
			    OHQ_FOLLOW_SYMLINKS, OHQ_WORM);
		}
	}

	struct ifs_createfile_flags cf_flags = CF_FLAGS_NONE;
	bool case_sensitive = true;

	find_args(case_sensitive, arg_to_bool(), input.get_arg_map(),
	    PARAM_CASE_SENSITIVE);

	if (case_sensitive)
		cf_flags = cf_flags_or(cf_flags, CF_FLAGS_POSIX);

	if (nsaccess) {
		return ns_handler_provider::open_genobj(store, NULL,
		    ace_rights_open_flg, cf_flags);
	} else {
		return ns_handler_provider::open_genobj(store, target.c_str(),
		    ace_rights_open_flg, cf_flags, follow_symlinks);
	}
}


}


ns_handler_provider::ns_handler_provider(const request &input,
    response &output, enum ifs_ace_rights ace_rights_open_flg)
    : input_(input), output_(output)
    , genobj_(NULL)
{
	store_provider::open_store(store_, input);

	genobj_ = ::open_genobj(store_, input_, ace_rights_open_flg);/////打开一个generic object
}


/* Used for http_delete */
ns_handler_provider::ns_handler_provider(const request &input,
    response &output)
    : input_(input), output_(output)
    , genobj_(NULL)
{
	std::string ns;
	std::string target;

	ns_object_handler::get_input_params(input, ns, target);

	store_provider::open_store(store_, input);
	genobj_ = new oapi_genobj(store_, NULL, target.c_str());
}


ns_handler_provider::~ns_handler_provider()
{
	if (genobj_)
		delete genobj_;
}


/**
 * Get appropriate handler based on the HDR_TARGET_TYPE header and
 * the true object type if existing.
 * @throw api_exception if object type is not supported.
 */
common_obj_handler &
ns_handler_provider::get_handler()
{
	std::string target;
	ns_object_handler::get_target_type(input_, target);
	bool any_or_empty = target.empty() || target == TT_ANY;

	if (genobj_->get_object_type() == OOT_OBJECT &&
	    (any_or_empty || target == TT_OBJECT)) {
		static object_handler handler;
		return handler;
	}
	else if (genobj_->get_object_type() == OOT_CONTAINER &&
	    (any_or_empty || target == TT_CONTAINER)) {
		static bucket_handler handler;
		return handler;
	}
	else {
		throw api_exception(AEC_BAD_REQUEST,
		    "Incompatible object type and the requested object"
		    " type %s for object: %s.",
		    HHDR_TARGET_TYPE.c_str(),
		    genobj_->get_name());
	}
}


/*
 * Open a generic object by given target uri;
 * See header for detail
 */
oapi_genobj *
ns_handler_provider::open_genobj(oapi_store &store, const char *target,
    enum ifs_ace_rights ace_rights_open_flg,
    struct ifs_createfile_flags cf_flags, bool follow_symlinks)
{
	isi_error *error = NULL;
	oapi_genobj_opener opener;
	oapi_genobj *genobj = NULL;

	int flags = 0;
	if (!follow_symlinks) {
		flags |= O_NOFOLLOW;
		flags |= O_OPENLINK;
	}

	genobj = opener.open(store, target,
	    flags, ace_rights_open_flg, cf_flags, error);

	if (error) {
		std::string what("open object '");
		if (target) {
			what.append(target);
			what.append("' in store '");
		} else
			what =  "open store '";
		what.append(store.get_name());
		what.append("'");
		oapi_error_handler().throw_on_error(error, what.c_str());
	}
	return genobj;
}

