
#include "isi_rest_server/api_error.h"
#include "isi_rest_server/response.h"

#include "handler_common.h"
#include <isi_object/iobj_ostore_protocol.h>
#include <isi_object/common.h>
#include <isi_acl/isi_acl_util.h>
#include <isi_acl/isi_sd.h>
#include <isi_object/ostore.h>

#include <algorithm>
#include <string>

namespace {

oapi_version obj_api_version("1.0");

oapi_version namespace_api_version("1.0");

/**
 * Maps isi system error to api_exception
 * @param sys_err  pointer to an isi_system_error object
 * @param what   a message associated with the error
 * @throw api_exception appropriately mapped from the error
 */
void throw_on_system_error(struct isi_system_error *sys_err,
    const char *what)
{
	ASSERT(sys_err);
	struct isi_error *error = (struct isi_error *) sys_err;

	int err_no = isi_system_error_get_error(sys_err);

	switch (err_no) {
	case EACCES:
		throw api_exception(error, AEC_FORBIDDEN,
		    "Unable to %s -- permission denied.", what);
		break;
	case EINVAL:
		throw api_exception(error, AEC_BAD_REQUEST,
		    "Unable to %s -- invalid request.", what);
		break;
	case EPERM:
		throw api_exception(error, AEC_FORBIDDEN,
		    "Unable to %s -- operation not permitted.", what);
		break;
	case EROFS:
		throw api_exception(error, AEC_FORBIDDEN,
		    "Unable to %s on read-only filesystem or "
		    "on a WORM file or directory", what);
		break;
	case ENAMETOOLONG:
		throw api_exception(error, AEC_NAMETOOLONG,
		    "Unable to %s -- name too long.", what);
		break;
	case ENOTDIR:
		throw api_exception(error, AEC_BAD_REQUEST,
		    "Unable to %s -- a component of the path is not a "
		    "directory.", what);
		break;
	case EISDIR:
		throw api_exception(error, AEC_FORBIDDEN,
		    "Unable to %s -- Is a directory.", what);
		break;
	case EBUSY:
		throw api_exception(error, AEC_FORBIDDEN,
		    "Unable to %s -- device busy.", what);
		break;
	case ENOSPC :
		throw api_exception(error, AEC_NO_SPACE,
		    "Unable to %s -- No space left.", what);
		break;
	case EIO:
		throw api_exception(error, AEC_FORBIDDEN,
		    "Unable to %s -- File is archived. Accessing "
		    "archived file using RAN is not supported yet.",
		    what);
		break;
	case ENOENT:
		throw api_exception(error, AEC_NOT_FOUND,
		    "Unable to %s -- %s",
		    what, strerror(err_no));
		break;
	default:
		ilog(IL_ERR, "Failed to %s due to error=%#{}",
		    what, isi_error_fmt(error));
		throw api_exception(error, AEC_SYSTEM_INTERNAL_ERROR,
		    "Unable to %s -- system internal error.", what);
		break;
	}
}
}

void
oapi_error_handler::throw_on_error(isi_error *error, const char *what)
{
	if (!error)
		return;

	if (isi_error_is_a(error, ISI_SYSTEM_ERROR_CLASS)) {
		throw_on_system_error(
		    (struct isi_system_error *) error, what);
	} else if (isi_error_is_a(error,
	    OSTORE_ACCESS_DENIED_ERROR_CLASS)) {
		throw api_exception(error, AEC_FORBIDDEN,
		    "Access denied. %s", what);
	} else if (isi_error_is_a(error,
	    OSTORE_PATHNAME_PROHIBITED_ERROR_CLASS)) {
		throw api_exception(error, AEC_FORBIDDEN,
		    "Access to system directory is prohibited -- %s", what);
	} else if (isi_error_is_a(error,
	    OSTORE_EXCEEDS_LIMIT_ERROR_CLASS)) {
		throw api_exception(error, AEC_LIMIT_EXCEEDED,
		    "Unable to %s -- limit exceeded.", what);
	} else if (isi_error_is_a(error,
	    OSTORE_OBJECT_NOT_FOUND_ERROR_CLASS)) {
		throw api_exception(error, AEC_NOT_FOUND,
		    "Unable to %s -- not found.", what);
	} else if (isi_error_is_a(error,
	    OSTORE_INVALID_LICENSE_ERROR_CLASS)) {
		throw api_exception(error, AEC_INVALID_LICENSE,
		    "Unable to %s -- invalid license.", what);
	} else if (isi_error_is_a(error,
	    OSTORE_UNSUPPORTED_OBJECT_TYPE_ERROR_CLASS)) {
		throw api_exception(error, AEC_FORBIDDEN,
		    "Unable to %s -- unsupported object type.", what);
	} else if (isi_error_is_a(error,
	    OSTORE_BUCKET_EXISTS_ERROR_CLASS)) {
		throw api_exception(error, AEC_FORBIDDEN, 
		    "Unable to %s -- container already exists", what);
	} else if (isi_error_is_a(error,
	    OSTORE_OBJECT_EXISTS_ERROR_CLASS)) {
		throw api_exception(error, AEC_FORBIDDEN,
		    "Unable to %s -- object already exists", what);
	} else {
		ilog(IL_ERR, "Failed to %s due to error=%#{}",
		    what, isi_error_fmt(error));
		throw api_exception(error, AEC_EXCEPTION,
		    "Failed to %s due to an internal error.", what);
	}
}


bool
oapi_version::is_compatible(const oapi_version &version) const
{
	return ver_ == version.ver_;
}

const oapi_version &
oapi_version_mgr::get_object_api_version()
{
	return obj_api_version;
}

const oapi_version &
oapi_version_mgr::get_namespace_api_version()
{
	return namespace_api_version;
}

void
oapi_set_versions_to_response(const std::string &ns, response &output)
{
	Json::Value versions(Json::arrayValue);

	if (ns == NS_NAMESPACE) {
		const oapi_version &ver =
		    oapi_version_mgr::get_namespace_api_version();
		versions.append(ver.to_string());
	} else {
		const oapi_version &ver =
		    oapi_version_mgr::get_object_api_version();
		versions.append(ver.to_string());
	}
	Json::Value &j_output = output.get_json();
	j_output[AVQ_VERSIONS] = versions;
}

bool
json_to_str(std::string &c_var, const Json::Value &j_obj, const char *name,
    bool req)
{
	const Json::Value &j_var = j_obj[name];

	if (j_var.type() == Json::nullValue) {
		if (req)
			throw api_exception(AEC_BAD_REQUEST,
			    "Missing entity attribute '%s'.", name);
		else {
			c_var.clear();
			return false;
		}
	}
	else if (!j_var.isString()) {
		// Remove the \n appended to the invalid value.
		std::string tmp, tmp_out;
		tmp = j_var.toStyledString();
		tmp_out.assign(tmp, 0, tmp.length() - 1);
		throw api_exception(AEC_BAD_REQUEST,
		    "%s is not a valid '%s' string.",
		    tmp_out.c_str(), name);
	}

	::json_to_c(c_var, j_var);
	return true;
}

