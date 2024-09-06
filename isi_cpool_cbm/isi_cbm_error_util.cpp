#include <sstream>

#include "isi_cbm_error_util.h"
#include "isi_cbm_scoped_ppi.h"
#include "isi_cbm_policyinfo.h"

#include <ifs/bam/bam_pctl.h>
#include <isi_cpool_cbm/isi_cbm_error.h>
#include <isi_ilog/ilog.h>
#include <isi_util/isi_error.h>

using namespace isi_cloud;
//using namespace std;


struct isi_error *
convert_cl_exception_to_cbm_error(isi_cloud::cl_exception &cl_err)
{
	cbm_error_type ec = CBM_CLAPI_UNEXPECTED;
	struct isi_error *ret_error = NULL;
	struct isi_error *exception_error = NULL;

	switch (cl_err.get_error_code()) {
	case CL_COULDNT_RESOLVE_HOST:
	case CL_COULDNT_CONNECT:
		ec = CBM_CLAPI_COULDNT_CONNECT;
		break;
	case CL_PEER_FAILED_VERIFICATION:
	case CL_SSL_CONNECT_ERROR:
		// connection error due to security reason
		// e.g. SSL certificate not OK
		ec = CBM_CLAPI_CONNECT_DENIED;
		break;
	case CL_AUTHENTICATION_FAILED:
		ec = CBM_CLAPI_AUTHENTICATION_FAILED;
		break;
	case CL_REMOTE_ACCESS_DENIED:
	case CL_UNAUTHORIZED:
	case CL_FORBIDDEN:
		ec = CBM_CLAPI_AUTHORIZATION_FAILED;
		break;
	case CL_ACCOUNT_IS_DISABLED:
		ec = CBM_CLAPI_ACCOUNT_IS_DISABLED;
		break;
	case CL_SERVER_BUSY:
		ec = CBM_CLAPI_SERVER_BUSY;
		break;
	case CL_SVR_INTERNAL_ERROR:
		ec = CBM_CLAPI_SERVER_ERROR;
		break;
	case CL_SEND_ERROR:
	case CL_RECV_ERROR:
		// network read/write error
		ec = CBM_CLAPI_NETWORK_RW_FAILED;
		break;

	case CL_LIMIT_EXCEEDED:
		ec = CBM_LIMIT_EXCEEDED;
		break;
	case CL_INVALID_KEY:
		ec = CBM_CLAPI_INVALID_KEY;
		break;
	case CL_OUT_OF_MEMORY:
		ec = CBM_CLAPI_OUT_OF_MEMORY;
		break;
	case CL_OPERATION_TIMEDOUT:
		// client side
	case CL_SVR_OPERATION_TIMED_OUT:
		// server side
		ec = CBM_CLAPI_TIMED_OUT;
		break;
	case CL_ABORTED_BY_CALLBACK:
		// error from callback
		ec = CBM_CLAPI_ABORTED_BY_CALLBACK;
		break;
	case CL_READ_ERROR:
	case CL_WRITE_ERROR:
		// error from local caller (callback)
		ec = CBM_CLAPI_CALLBACK_RW_FAILED;
		break;

	case CL_CONTAINER_ALREADY_EXISTS:
		ec = CBM_CLAPI_CONTAINER_EXISTS;
		break;
	case CL_CONTAINER_BEING_DELETED:
		ec = CBM_CLAPI_CONTAINER_BEING_DELETED;
		break;
	case CL_CONTAINER_NOT_FOUND:
		ec = CBM_CLAPI_CONTAINER_NOT_FOUND;
		break;
	case CL_OBJ_NOT_FOUND:
		ec = CBM_CLAPI_OBJECT_NOT_FOUND;
		break;
	case CL_OBJ_ALREADY_EXISTS:
		ec = CBM_CLAPI_OBJECT_EXISTS;
		break;

	case CL_INVALID_RANGE:
	case CL_OUT_OF_RANGE_INPUT:
	case CL_OUT_OF_RANGE_QUERY_PARAMETER_VALUE:
	case CL_INVALID_PARAMETER:
	case CL_BAD_REQUEST:
	case CL_BAD_CONTENT_ENCODING:
	case CL_NAMETOOLONG:
		// group all under one, we shall not send out bad request
		// unless server side changed or we have a bug to fix
		ec = CBM_CLAPI_BAD_REQUEST;
		break;

	case CL_PARTIAL_FILE:
		ec = CBM_CLAPI_PARTIAL_FILE;
		break;

	case CL_METHOD_NOT_SUPPORTED:
		ec = CBM_NOT_SUPPORTED_ERROR;
		break;

	case CL_MD5_MISMATCH: // we won't use MD5 check in cloud_api
	case CL_CONFLICT:
	case CL_PRE_CONDITION_FAILED:
	case CL_NOT_MODIFIED:
	case CL_NOT_ACCEPTABLE:
	case CL_HTTP_POST_ERROR:
	case CL_PRVD_INTERNAL_ERROR:
	case CL_UPLOAD_FAILED:
	case CL_HTTP_RETURNED_ERROR:
	default:
		// we won't expect these, but could happen if settings in
		// server being altered
		ec = CBM_CLAPI_UNEXPECTED;
		break;
	}

	exception_error = cl_err.detach();

	ret_error = isi_cbm_error_new(ec, "%{}: %#{}",
	    cl_error_code_fmt(cl_err.get_error_code()),
	    isi_error_fmt(exception_error));

	isi_error_free(exception_error);
	exception_error = NULL;

	return ret_error;
}


void 
set_cpool_events_attrs(struct cpool_events_attrs *cev_attrs,
    ifs_lin_t lin, off_t offset, isi_cbm_id account_id, 
    std::string stub_entityname)
{
	cev_attrs->lin = lin;
	cev_attrs->offset = offset;
	cev_attrs->account_id = account_id;
	cev_attrs->stub_entityname = stub_entityname;
	return;
}


typedef struct cbm_event_info{
	uint32_t event_id;
	char const *msg;
	enum celog_severity severity;
} cbm_event_info;

static cbm_event_info const err_to_event[MAX_CBM_ERROR_TYPE] = {
		[CBM_CLAPI_CONNECT_DENIED] = {
			CPOOL_CERTIFICATE_ERROR,
			"CloudPools peer certificate verification failed",
			CS_CRIT
		},
		[CBM_CLAPI_AUTHENTICATION_FAILED] = {
			CPOOL_AUTHENTICATION_FAILED,
			"CloudPools Authentication failed",
			CS_CRIT
		},
		[CBM_CLAPI_AUTHORIZATION_FAILED] = {
			CPOOL_AUTHORIZATION_FAILED,
			"CloudPools Authorization failure",
			CS_CRIT
		},
		[CBM_CLAPI_COULDNT_CONNECT] = {
			CPOOL_NW_FAILED,
			"CloudPools Network connection failed",
			CS_WARN
		},
		[CBM_CLAPI_NETWORK_RW_FAILED] = {
			CPOOL_NW_FAILED,
			"CloudPools Network connection failed",
			CS_WARN
		},
		[CBM_CLAPI_CONTAINER_NOT_FOUND] = {
			CPOOL_BUCKET_NOT_FOUND,
			"CloudPools Bucket not found",
			CS_CRIT
		},
		[CBM_CLAPI_OBJECT_NOT_FOUND] = {
			CPOOL_OBJECT_NOT_FOUND,
			"CloudPools Object not found",
			CS_CRIT
		},
		[CBM_INTEGRITY_FAILURE] = {
			CPOOL_DATA_CORRUPTION_INTEGRITY_ERROR,
			"CloudPools data corruption/integrity error",
			CS_CRIT
		},
		[CBM_CHECKSUM_ERROR] = {
			CPOOL_DATA_CORRUPTION_INTEGRITY_ERROR,
			"CloudPools data corruption/integrity error",
			CS_CRIT
		},
	};

/**
 * This routine sends the cloudpool events based on what event it is.
 * @param cpool_events_attrs Has all the input details required to send event.
 * @param incoming_error : Error for which the event is sent.
 */

void send_cloudpools_event(struct cpool_events_attrs *cev_attrs, 
    struct isi_error *incoming_error, char const *caller)
{
	size_t zero = 0;
	size_t sz = 0;
	struct isi_error *error = NULL;
	enum cbm_error_type_values cbm_error;
	int devid = -1;
	char const *err_str = NULL;
	struct cpool_account *cacct = NULL;
	char cl_ptypename[256] = { 0 };

	if (cev_attrs == NULL) {
		return;
	}

	if (incoming_error == NULL) {
		return;
	}

	cbm_error = isi_cbm_error_get_type(incoming_error);
	ASSERT(cbm_error != (enum cbm_error_type_values) -1);
	scoped_ppi_reader ppi_rdr;

	const struct isi_cfm_policy_provider *cfg = ppi_rdr.get_ppi(&error);
	ON_ISI_ERROR_GOTO(out, error, "failed to get cpool config context");

	if (cev_attrs->lin > 0) {
		char file_entity[PATH_MAX + 1] = { 0 };
		
		if (pctl2_lin_get_path_plus(ROOT_LIN, cev_attrs->lin, HEAD_SNAPID,
			0, 0, ENC_DEFAULT, PATH_MAX + 1, file_entity, &sz, 0,
			NULL, &zero, 0, NULL, NULL,
			PCTL2_LIN_GET_PATH_NO_PERM_CHECK) < 0) {  // bug 220379
			if (errno == ENOENT) {
				error = isi_system_error_new(errno, 
				    "File not found for lin %{}", 
				    lin_fmt(cev_attrs->lin));
				goto out;
			}
		}

		cev_attrs->filename = file_entity;
	}

	// Retrieve the account info using the account class.
	cacct = cpool_account_get_by_id(cfg->cp_context,
	    cev_attrs->account_id.get_id(), &error);
	if (error) {
		isi_error_add_context(error,
		    "Failed to get account for sending CloudPool events for %d",
		    cev_attrs->account_id.get_id());
		goto out;
	}

	get_cl_provider_type_name(cacct, cl_ptypename);

	devid = get_devid(&error);
	if (error) 
		goto out;

	err_str = isi_error_get_message(incoming_error);

	if (err_to_event[cbm_error].event_id) {
		struct fmt linfmt = FMT_INITIALIZER;
		int const max_msg_size = 2048;
		char msg[max_msg_size] = { 0 };
		int ret = 0;

		snprintf(msg, max_msg_size, "%s. caller: '{caller}', provider: '{provider}', "
			"devid: '{devid}', account: '{account}', entitypath: '{entitypath}', "
			"filename: '{filename}', lin: '{lin}', offset: '{offset}', "
			"msg: '{errormessage}'", err_to_event[cbm_error].msg);
		fmt_print(&linfmt, "%{}", lin_fmt(cev_attrs->lin));

		ret = celog_event_create(
			NULL,
			CCF_NONE,
			err_to_event[cbm_error].event_id,
			err_to_event[cbm_error].severity,
			0,
			msg,
			"caller %s, provider %s, devid %d, account %s, entitypath %s, "
				"filename %s, lin %s, offset %ld, msg %s",
			caller,
			cl_ptypename,
			devid,
			cacct->account_name,
			cev_attrs->stub_entityname.c_str(),
			cev_attrs->filename.c_str(),
			fmt_string(&linfmt),
			cev_attrs->offset,
			err_str);
		if (ret) {
			ilog(IL_ERR, "celog event create failed ev-%d, errno-%d",
				err_to_event[cbm_error].event_id, ret);
		}
		fmt_clean(&linfmt);

		ilog(IL_ERR, "isi_cpool_cbm error in %s: %s.  "
			"provider %s, devid %d, account %s, entitypath %s, "
			"filename %s, lin %{}, offset %ld, errormessage %s",
			caller,
			err_to_event[cbm_error].msg,
			cl_ptypename,
			devid,
			cacct->account_name,
			cev_attrs->stub_entityname.c_str(),
			cev_attrs->filename.c_str(),
			lin_fmt(cev_attrs->lin),
			cev_attrs->offset,
			err_str);
	}

out:

	if (error != NULL) {
		// Log it in the syslog.
		ilog(IL_ERR,
		    "CloudPools Event Error: "
		    "Error when sending cloudpools "
		    "event -> %s",
		    isi_error_get_message(error));
		isi_error_free(error);
	}
}


void 
trigger_failpoint_cloudpool_events()
{

	// Authentication failure.
	UFAIL_POINT_CODE(cbm_generate_celog_event_authentication, 
	    {
		    throw cl_exception(CL_AUTHENTICATION_FAILED,
			"Authentication failed. ");
	    });

	// Authorization failure.
	UFAIL_POINT_CODE(cbm_generate_celog_event_auth_remote_acc_denied,
	    {
		    throw cl_exception(CL_REMOTE_ACCESS_DENIED, 
			"Remote access denied. ");
	    });

	UFAIL_POINT_CODE(cbm_generate_celog_event_auth_unauth, 
	    {
		    throw cl_exception(CL_UNAUTHORIZED,
			"Unauthorized. ");
	    });

	UFAIL_POINT_CODE(cbm_generate_celog_event_auth_forbidden,
	    {
		    throw cl_exception(CL_FORBIDDEN,
			"Access forbidden. ");
	    });

	// Container not found.
	UFAIL_POINT_CODE(cbm_generate_celog_event_container_not_fnd,
	    {
		    throw cl_exception(CL_CONTAINER_NOT_FOUND,
			"Container not found. ");
	    });

	// Object not found.
	UFAIL_POINT_CODE(cbm_generate_celog_event_object_not_fnd,
	    {
		    throw cl_exception(CL_OBJ_NOT_FOUND,
			"Object not found. ");
	    });

	// Network failure.
	UFAIL_POINT_CODE(cbm_generate_celog_event_nw_send_failed,
	    {
		    throw cl_exception(CL_SEND_ERROR,
			"N/W send error. ");
	    });

	UFAIL_POINT_CODE(cbm_generate_celog_event_nw_recv_failed,
	    {
		    throw cl_exception(CL_RECV_ERROR,
			"N/W recv error. ");
	    });

	UFAIL_POINT_CODE(cbm_generate_celog_event_connect_failed,
	    {
		    throw cl_exception(CL_COULDNT_CONNECT, 
			"Couldn't connect. ");
	    });


	UFAIL_POINT_CODE(cbm_generate_celog_event_resolve_host_failed, 
	    {
		    throw cl_exception(CL_COULDNT_RESOLVE_HOST,
			"Couldn't resolve host. ");
	    });

	// Integrity and checksum failures.
	UFAIL_POINT_CODE(cbm_generate_celog_event_integrity_failed,
	    {
		    throw isi_exception(isi_cbm_error_new(
				CBM_INTEGRITY_FAILURE, 
				"Integrity failure. "));
	    });

	// Integrity and checksum failures.
	UFAIL_POINT_CODE(cbm_generate_celog_event_checksum_failed,
	    {
		    throw isi_exception(isi_cbm_error_new(
				CBM_CHECKSUM_ERROR,
				"Checksum didn't match. "));
	    });
}

