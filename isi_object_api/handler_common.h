/**
 * Common handler header file.
 */

#ifndef __HANDLER_COMMON_H__
#define __HANDLER_COMMON_H__

#include <errno.h>
#include <isi_ilog/ilog.h>
#include <isi_json/json_helpers.h>
#include <isi_rest_server/api_error.h>
#include <isi_rest_server/request.h>
#include <isi_util/isi_error.h>

#include "oapi/oapi_genobj.h"

// for storing in object library
#define ATTR_CONTENT_TYPE 	"CONTENT_TYPE"
#define ATTR_CONTENT_ENCODING 	"CONTENT_ENCODING"

// system defined attributes for objects:
#define EATTR_PREFIX	"user."
#define ISI_NAME	"name"
#define ISI_PATH	"path"
#define ISI_MTIME_VAL	"mtime_val"
#define ISI_BTIME_VAL	"btime_val"
#define ISI_ATIME_VAL	"atime_val"
#define ISI_CTIME_VAL	"ctime_val"
#define ISI_MTIME	"last_modified"
#define ISI_BTIME	"create_time"
#define ISI_ATIME	"access_time"
#define ISI_CTIME	"change_time"
#define ISI_STUB	"stub"
#define ISI_SIZE	"size"
#define ISI_BLK_SIZE	"block_size"
#define ISI_BLOCKS	"blocks"
#define ISI_BUCKET	"container"
#define ISI_OWNER	"owner"
#define ISI_GROUP	"group"
#define ISI_UID		"uid"
#define ISI_GID		"gid"
#define ISI_CONTENT_TYPE	"content_type"
#define ISI_OBJECT_TYPE		"type"
#define ISI_SYMBLNK_TYPE	"symbolic_link_type"
#define ISI_CONTAINER_PATH	"container_path"
#define ISI_URL		"url"
#define ISI_ID		"id"
#define ISI_NLINK	"nlink"
#define ISI_HIDDEN	"is_hidden"
#define ISI_ACC_MODE	"mode"

// used in output JSON message for reporting error
#define ISI_SOURCE      "source"
#define ISI_TARGET      "target"
#define ISI_ERROR_SRC   "error_src"
#define ISI_MESSAGE     "message"

#define UNKNOWN_USER	"Unknown User"
#define UNKNOWN_GROUP	"Unknown Group"
#define UNKNOWN_OTYPE	"Unknown Type"

// Object query query parameter.
#define BHQ_QUERY	"query"
#define BHQ_QUERY_SIZE	sizeof (BHQ_QUERY) - 1
#define MAX_QUERY_SIZE	8192

#define NS_NAMESPACE	"/namespace"
#define NS_OBJECT	"/object"

#define PARAM_SNAPSHOT_NAME	"snapshot"
#define PARAM_OVERWRITE 	"overwrite"
#define PARAM_CLONE		"clone"
#define PARAM_RECURSIVE		"recursive"
#define PARAM_CREATE_CONTAINER	"create_container"

#define NS_DEFAULT_STORE  "ifs"

// api version query parameter
#define AVQ_VERSIONS	"versions"

#define OHQ_ATTRIBUTE		"metadata"
#define ATTR_NS			"namespace"
#define ATTR_NAME		"name"
#define ATTR_VALUE		"value"
#define ACTION		        "action"
#define OP			"op"
#define ATTR_ATTRS		"attrs"
#define ACTION_UPDATE	        "update"
#define ACTION_REPLACE	        "replace"
#define OP_UPDATE		"update"
#define OP_DELETE		"delete"
#define OP_ADD                  "add"
#define OP_REPLACE              "replace"
#define ATTR_NS_SYSTEM		"system"
#define ATTR_NS_USER	        "user"

#define OHQ_ATTRIBUTE_SIZE      sizeof (OHQ_ATTRIBUTE) - 1

#define X_ISI_META_			"X_ISI_IFS_ATTR_"
#define HHDR_X_ISI_META_		"x-isi-ifs-attr-"
#define HHDR_X_ISI_REQUEST_ID		"x-isi-request-id"
#define HHDR_X_ISI_MISSING_META_	"x-isi-ifs-missing-attr"

#define X_ISI_IFS_SPEC_VERSION          "X_ISI_IFS_SPEC_VERSION"
#define HHDR_X_ISI_IFS_SPEC_VERSION     "x-isi-ifs-spec-version"

/**
 * Object types:
 */
extern const std::string TT_CONTAINER;
extern const std::string TT_OBJECT;
extern const std::string TT_STUB;
extern const std::string TT_FIFO;
extern const std::string TT_CHR;
extern const std::string TT_BLK;
extern const std::string TT_LNK;
extern const std::string TT_SOCK;
extern const std::string TT_WHT;

extern const std::string TT_ALL;
extern const std::string TT_ANY;
extern const std::string TT_UNKNOWN;

extern const std::string HDR_TARGET_TYPE;
extern const std::string HHDR_TARGET_TYPE;
extern const std::string HDR_ACCESS_CONTROL;
extern const std::string HHDR_ACCESS_CONTROL;
extern const std::string HHDR_SET_LOCATION;
extern const std::string HDR_COPY_SOURCE;
extern const std::string HHDR_COPY_SOURCE;

const mode_t DEFAULT_MODE_CONTAINER = S_IRUSR|S_IWUSR|S_IXUSR;
const mode_t DEFAULT_MODE_OBJECT = S_IRUSR|S_IWUSR;

// shared account
#define SH_ACCT	""

/**
 * Checks if an error is EACCESS
 */
inline bool
access_denied(isi_error *error)
{
	return error && isi_error_is_a(error, ISI_SYSTEM_ERROR_CLASS) &&
	    isi_system_error_is_a(error, EACCES);
}

/**
 * This function checks the input request argument.
 * and check if this is an object query.
 */
inline bool
is_object_query(const request &input)
{
	const std::string &main_arg = input.get_main_arg();

	return main_arg == BHQ_QUERY;
}

/**
 * Get the string value for member named "name" of j_obj into c_var.
 * @param[OUT] c_var: The output string. When it is not found, it is cleared.
 * @param[IN] j_obj: The Json::Value input
 * @param[IN] name: the name of the member attribute
 * @param[IN] req: indicates if the member is required
 * @return true if the member if found, false otherwise
 * @throws api_exception in the case of validation failure.
 */
bool json_to_str(std::string &c_var, const Json::Value &j_obj, const char *name,
    bool req);

/**
 * This function checks if the request is version query.
 */
inline bool
is_version_request(const request &input)
{
	const std::string &main_arg = input.get_main_arg();

	return main_arg == AVQ_VERSIONS;
}


/**
 * This function checks the input request argument.
 * If the input request is ?attribute or ?attribute followed by &<xyz>,
 * e.g. ?attribute&abcd, then return true else return false.
 */

inline bool
is_get_put_attr(const request &input)
{
	const std::string &main_arg = input.get_main_arg();

	return main_arg == OHQ_ATTRIBUTE;
}

/** Common error handling via api_exception*/
struct oapi_error_handler {
	/**
	 * Maps common oapi error to api_exception
	 * @param error  pointer to an isi_error object
	 * @param what   a message associated with the error
	 * @throw api_exception appropriately mapped from the error
	 */
	void throw_on_error(isi_error *error, const char *what);

};

/**
 * oapi protocol version
 */
class oapi_version {
public:
	oapi_version(const std::string &ver) : ver_(ver)
	{}

	bool is_compatible(const oapi_version &version) const;

	const std::string &to_string() const
	{
		return ver_;
	}

private:
	std::string ver_;
};


class oapi_version_mgr {

public:
	static const oapi_version &get_object_api_version();
	static const oapi_version &get_namespace_api_version();
};

class response;
/**
 * set supported versions in response in json format
 *
 * @param[IN]     ns       namespace store type
 * @param[INOUT]  output   response object
 */
void
oapi_set_versions_to_response(const std::string &ns, response &output);

#endif  //__HANDLER_COMMON_H__
