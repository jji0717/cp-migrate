#include <assert.h>
#include <errno.h>
#include <map>

#include <isi_flexnet/isi_flexnet.h>
#include <isi_gconfig/gconfig.h>
#include <isi_lwconfig/isi_lw_config.h>
#include <isi_priv_db/isi_priv_db.h>
#include <isi_util/isi_error.h>

#include "api_error.h"

#define ERR_ENTRY(symbol, http_sc) add_entry(symbol, #symbol, http_sc);

namespace {

typedef std::map<enum api_error_code, api_error_table::tab_entry> err_map_t;

class api_error_table_impl {

public:
	/** Constructor */
	api_error_table_impl();

	/*
	 * Returns the table entry of given error code
	 */
	const api_error_table::tab_entry &get_entry(
	    enum api_error_code ec) const;

private:

	/** Adds an entry */
	void add_entry(enum api_error_code, const char *err_symbol,
	    enum api_status_code sc);

	err_map_t err_map_;
};

// the error table instance
static const api_error_table_impl err_table_impl_;

/*
 * Constructs the error table
 *
 * All defined error codes are added here.
 */
api_error_table_impl::api_error_table_impl()
{
	ERR_ENTRY(AEC_TRANSIENT, ASC_OK);

	ERR_ENTRY(AEC_BAD_REQUEST, ASC_BAD_REQUEST);

	ERR_ENTRY(AEC_ARG_REQUIRED, ASC_BAD_REQUEST);

	ERR_ENTRY(AEC_ARG_SINGLE_ONLY, ASC_BAD_REQUEST);

	ERR_ENTRY(AEC_UNAUTHORIZED, ASC_UNAUTHORIZED);

	ERR_ENTRY(AEC_FORBIDDEN, ASC_FORBIDDEN);

	ERR_ENTRY(AEC_NOT_FOUND, ASC_NOT_FOUND);

	ERR_ENTRY(AEC_METHOD_NOT_ALLOWED, ASC_METHOD_NOT_ALLOWED);

	ERR_ENTRY(AEC_NOT_ACCEPTABLE, ASC_NOT_ACCEPTABLE);

	ERR_ENTRY(AEC_CONFLICT, ASC_CONFLICT);

	ERR_ENTRY(AEC_PRE_CONDITION_FAILED, ASC_PRE_CONDITION_FAILED);

	ERR_ENTRY(AEC_INVALID_REQUEST_RANGE, ASC_INVALID_REQUEST_RANGE);

	ERR_ENTRY(AEC_NOT_MODIFIED, ASC_NOT_MODIFIED);

	ERR_ENTRY(AEC_LIMIT_EXCEEDED, ASC_FORBIDDEN);

	ERR_ENTRY(AEC_INVALID_LICENSE, ASC_FORBIDDEN);

	ERR_ENTRY(AEC_NAMETOOLONG, ASC_BAD_REQUEST);

	ERR_ENTRY(AEC_NAMEEMPTY, ASC_BAD_REQUEST);

	ERR_ENTRY(AEC_NAMEILLEGAL, ASC_BAD_REQUEST);

	ERR_ENTRY(AEC_NO_SPACE, ASC_BAD_REQUEST);

	ERR_ENTRY(AEC_SERVICE_UNAVAILABLE, ASC_SERVICE_UNAVAILABLE);

	ERR_ENTRY(AEC_SYSTEM_INTERNAL_ERROR, ASC_INTERNAL_SERVER_ERROR);

	ERR_ENTRY(AEC_EXCEPTION, ASC_INTERNAL_SERVER_ERROR);
}

void
api_error_table_impl::add_entry(enum api_error_code ec,
    const char *err_symbol, enum api_status_code sc)
{
	api_error_table::tab_entry &entry = err_map_[ec];
	entry.symbol = err_symbol;
	entry.sc = sc;
}

/*
 * Returns the table entry of given error code
 */
const api_error_table::tab_entry &
api_error_table_impl::get_entry(enum api_error_code ec) const
{
	err_map_t::const_iterator itr = err_map_.find(ec);
	if (itr != err_map_.end())
		return itr->second;

	// anything else
	ASSERT(false, "Error %d not found", ec);
	if (ec < _AEC_TRANSIENT_END)
		return err_map_.find(AEC_TRANSIENT)->second;
	else
		return err_map_.find(AEC_EXCEPTION)->second;
}

}

/*
 * Returns the table entry of given error code
 */
const api_error_table::tab_entry &
api_error_table::get_entry(enum api_error_code ec)
{
	return err_table_impl_.get_entry(ec);
}

api_error_code
api_error_map::translate(const isi_error *error) const
{
	assert(error);

	if (isi_error_is_restartable(error))
		return AEC_RESTART;
	int ec = 0;
	if (isi_error_is_a(error, ISI_SYSTEM_ERROR_CLASS)) {
		ec = isi_system_error_get_error((const isi_system_error *)error);
	} else if (isi_error_is_a(error, LWC_VALIDATION_ERROR_CLASS)) {
		ec = EINVAL;
	}

	api_error_code aec;

	switch (ec) {
	case EINVAL:
	case ENAMETOOLONG:
		aec = AEC_BAD_REQUEST;
		break;
	case ENOENT:
		aec = AEC_NOT_FOUND;
		break;
	case EEXIST:
		aec = AEC_CONFLICT;
		break;
	case 0:
		aec = isi_error_is_a(error, FLX_CONFLICT_ERROR_CLASS) ?
			AEC_CONFLICT : AEC_EXCEPTION;
		break;
	default:
		aec = AEC_EXCEPTION;
		break;
	}

	return aec;
}

api_exception::api_exception(api_error_code ec, const std::string &msg)
	: isi_exception("%s", msg.c_str())
	, ec_(ec), ap_()
{ }

/* note comma operator usage for getting va_list setup */
api_exception::api_exception(const std::string &field, api_error_code ec,
    const char *format, ...)
	: isi_exception(0, format, (va_start(ap_, format), ap_))
	, field_(field)
	, ec_(ec)
{
	va_end(ap_);
}

/* note comma operator usage for getting va_list setup */
api_exception::api_exception(api_error_code ec, const char *format, ...)
	: isi_exception(0, format, (va_start(ap_, format), ap_))
	, ec_(ec)
{
	va_end(ap_);
}

api_exception::api_exception(isi_error *error, api_error_code ec)
	: isi_exception(error), ec_(ec), ap_()
{ }

api_exception::api_exception(const std::string &field, isi_error *err)
	: isi_exception(err), field_(field), ec_(AEC_EXCEPTION), ap_()
{ }

api_exception::api_exception(const std::string &field, api_error_code ec,
    isi_error *err)
	: isi_exception(err), field_(field), ec_(ec), ap_()
{ }

api_exception::api_exception(isi_error *error, api_error_code ec,
    const char *format, ...)
	: isi_exception(error), ec_(ec)
{
	va_start(ap_, format);
	set_msg(format, ap_);
	va_end(ap_);
}

void
api_exception_translate(struct isi_error *error)
{
	api_exception::throw_on_error(error);
}

bool
isi_error_is_restartable(const isi_error *error)
{
	return error &&
	    (isi_error_is_a(error, GCI_RECOVERABLE_COMMIT_ERROR_CLASS) ||
	     isi_error_is_a(error, FLX_STALE_ERROR_CLASS) ||
	     isi_error_is_a(error, IPRIV_COMMIT_ERROR_CLASS));
}

const char *
api_status_code_str(api_status_code code)
{
	switch (code) {
	case ASC_OK:                     return "Ok";
	case ASC_CREATED:                return "Created";
	case ASC_ACCEPTED:               return "Accepted";
	case ASC_NO_CONTENT:             return "No Content";
	case ASC_PARTIAL_CONTENT:        return "Partial Content";
	case ASC_NOT_MODIFIED:           return "Not Modified";
	case ASC_BAD_REQUEST:            return "Bad Request";
	case ASC_UNAUTHORIZED:           return "Unauthorized";
	case ASC_FORBIDDEN:              return "Forbidden";
	case ASC_NOT_FOUND:              return "Not Found";
	case ASC_METHOD_NOT_ALLOWED:     return "Method Not Allowed";
	case ASC_NOT_ACCEPTABLE:         return "Not Acceptable";
	case ASC_CONFLICT:               return "Conflict";
	case ASC_PRE_CONDITION_FAILED:   return "Precondition Failed";
	case ASC_INVALID_REQUEST_RANGE:  return "Requested Range Not Satisfiable";
	case ASC_INTERNAL_SERVER_ERROR:  return "Internal Server Error";
	case ASC_SERVICE_UNAVAILABLE:    return "Service Not Available";
	default:                         return "";
	};
}
