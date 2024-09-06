#ifndef __INC_API_ERROR_H__
#define __INC_API_ERROR_H__

#include <exception>
#include <string>

#include <isi_util/isi_error.h>
#include <isi_util_cpp/isi_exception.h>

/*
 * A mapping to api status code should be defined explicitly in
 * api_error.c : api_error_table_impl() for all api error codes
 */
enum api_error_code {
	/* internal error codes with no tranlsations */
	AEC_RESTART            = 1,

	/* transient error codes treated like ASC_OK */
	AEC_TRANSIENT          = 2000,
	_AEC_TRANSIENT_END     = 3999,

	/* bad request error codes treated like ASC_BAD_REQUEST */
	AEC_BAD_REQUEST        = 4000,
	AEC_ARG_REQUIRED       = 4001,
	AEC_ARG_SINGLE_ONLY    = 4002,
	_AEC_BAD_REQUEST_END   = 4900,

	/* others mapping directly status codes */
	/* please add translation between the two in response::add_error */
	AEC_UNAUTHORIZED       = 4901, // maps to ASC_UNAUTHORIZED
	AEC_FORBIDDEN          = 4903, // maps to ASC_FORBIDDEN
	AEC_NOT_FOUND          = 4904, // maps to ASC_NOT_FOUND
	AEC_METHOD_NOT_ALLOWED = 4905, // maps to ASC_METHOD_NOT_ALLOWED
	AEC_NOT_ACCEPTABLE     = 4906, // maps to ASC_NOT_ACCEPTABLE
	AEC_CONFLICT           = 4909, // maps to ASC_CONFLICT
	AEC_PRE_CONDITION_FAILED  = 4912, // maps to ASC_PRE_CONDITION_FAILED
	AEC_INVALID_REQUEST_RANGE = 4916, // maps to ASC_INVALID_REQUEST_RANGE
	AEC_NOT_MODIFIED          = 4917, // maps to ASC_NOT_MODIFIED
	AEC_LIMIT_EXCEEDED	  = 4918, // maps to ASC_FORBIDDEN
	AEC_INVALID_LICENSE	  = 4919, // maps to ASC_FORBIDDEN
	AEC_NAMETOOLONG		  = 4920, // maps to ASC_BAD_REQUEST
	AEC_NAMEEMPTY		  = 4921, // maps to ASC_BAD_REQUEST
	AEC_NAMEILLEGAL		  = 4922, // maps to ASC_BAD_REQUEST
	AEC_NO_SPACE		  = 4923, // maps to ASC_BAD_REQUEST
	AEC_SERVICE_UNAVAILABLE   = 4924, // maps to ASC_SERVICE_UNAVAILABLE
	AEC_SYSTEM_INTERNAL_ERROR = 4999, // maps to ASC_INTERNAL_SERVER_ERROR
	AEC_EXCEPTION             = 5000, // etc map to ASC_INTERNAL_SERVER_ERROR
};

enum api_status_code {
	ASC_OK				= 200,
	ASC_CREATED			= 201,
	ASC_ACCEPTED			= 202,
	ASC_NO_CONTENT			= 204,
	ASC_PARTIAL_CONTENT		= 206,
	ASC_MULTI_STATUS		= 207,
	ASC_NOT_MODIFIED		= 304,
	ASC_BAD_REQUEST			= 400,
	ASC_UNAUTHORIZED		= 401,
	ASC_FORBIDDEN			= 403,
	ASC_NOT_FOUND			= 404,
	ASC_METHOD_NOT_ALLOWED		= 405,
	ASC_NOT_ACCEPTABLE		= 406,
	ASC_CONFLICT			= 409,
	ASC_PRE_CONDITION_FAILED	= 412,
	ASC_INVALID_REQUEST_RANGE	= 416,
	ASC_INTERNAL_SERVER_ERROR	= 500,
	ASC_SERVICE_UNAVAILABLE		= 503,
};

/**
 * This error table gives access to mapping of error code to its symbol
 * and HTTP status code.
 */
class api_error_table {
public:
	/** table entry structure */
	struct tab_entry {
		const char *symbol;
		enum api_status_code sc;
	};

	/** Returns the table entry of given error code */
	static const tab_entry &get_entry(enum api_error_code ec);
};

/** Return true if @a error indicates a method restart might be warranted */
bool isi_error_is_restartable(const isi_error *error);

/** Return text description of @a asc */
const char *
api_status_code_str(enum api_status_code asc);

/** Default isi_error to api_error mapping class */
struct api_error_map {
	/** Return api error equivalent of @a err */
	api_error_code translate(const isi_error *err) const;
};

/** Return true if status code is a success code, false if a failure code */
inline bool
api_status_is_success(enum api_status_code asc)
{
	if (asc < ASC_BAD_REQUEST)
		return true;
	else
		return false;
}

/** Exception class that encodes api_error_code used in responses */
class api_exception : public isi_exception {
public:
	/** Throw if @a err is non-null using default mapping
	 * class @ref * api_error_map.
	 */
	static inline void throw_on_error(isi_error *err);

	/** Throw if @a err is non-null using pass a relevant @a field
	 * onto exception handler
	 */
	static inline void throw_on_error(const std::string &field, isi_error
	    *err);

	/** Throw if @a err is non-null using mapping class @em.
	 * @param EM should have the same signature as @ref api_error_map
	 */
	template<class EM>
	static inline void throw_on_error(isi_error *err, const EM &em);

	/** Throw if @a err is non-null using mapping class @em.
	 * pass a relevant a @a field onto exception handler
	 * @param EM should have the same signature as @ref api_error_map
	 */
	template<class EM>
	static inline void throw_on_error(const std::string &field,
	    isi_error *err, const EM& em);

	/** Exception encoding @a ec with message @a msg */
	api_exception(api_error_code ec, const std::string &msg);

	/** Exception encoding @a field with an @a err */
	api_exception(const std::string &field, isi_error *err);

	/** Exception encoding @a field with an informative error code
	 * @a iec and error @a err
	 */
	api_exception(const std::string &field, api_error_code ec,
	    isi_error *err);

	/** Exception encoding @a ec with @a @field and formatted message */
	api_exception(const std::string &field, api_error_code ec,
	    const char *format, ...) __fmt_print_like(4, 5);

	/** Exception encoding @a ec with formatted message */
	api_exception(api_error_code ec, const char *format, ...)
	    __fmt_print_like(3, 4);

	/** Exception encoding @a ec wrapping error object @a err */
	api_exception(isi_error *err, api_error_code ec);

	/** Exception encoding @a ec wrapping error object @a err and
	 * formatted message
	 */
	api_exception(isi_error *error, api_error_code ec,
	    const char *format, ...) __fmt_print_like(4, 5);

	~api_exception() throw()
	{ }

	/* default copy and assign */

	/** Return api field */
	inline std::string get_field() const
	{
		return field_;
	}

	/** Return api error code */
	inline api_error_code error_code() const
	{
		return ec_;
	}

	/** Return modifiable reference to error code */
	inline api_error_code& error_code()
	{
		return ec_;
	}

private:
	std::string field_;
	api_error_code	ec_;
	va_list		ap_;
};

void api_exception_translate(struct isi_error *e);

/* XXX - wip*/
class api_error_throw {
public:
	typedef isi_error **isi_error_pp;

	inline api_error_throw();

#if __cplusplus >= 201103L
	inline ~api_error_throw() noexcept(false);
#else
	inline ~api_error_throw();
#endif

	inline operator isi_error_pp();

private:
	isi_error *err_;

	/* uncopyable, unassignable */
	api_error_throw(const api_error_throw &);
	api_error_throw &operator=(const api_error_throw &);
};

/*  _       _ _
 * (_)_ __ | (_)_ __   ___
 * | | '_ \| | | '_ \ / _ \
 * | | | | | | | | | |  __/
 * |_|_| |_|_|_|_| |_|\___|
 */

void
api_exception::throw_on_error(isi_error *error)
{
	if (error) {
		api_error_code ec = api_error_map().translate(error);
		throw api_exception(error, ec);
	}
}

void
api_exception::throw_on_error(const std::string &field, isi_error *error)
{
	if (error) {
		api_error_code ec = api_error_map().translate(error);
		throw api_exception(field, ec, error);
	}
}

template<class EM>
void
api_exception::throw_on_error(isi_error *error, const EM& em)
{
	if (error) {
		api_error_code ec = em.translate(error);
		throw api_exception(error, ec);
	}
}

template<class EM>
void
api_exception::throw_on_error(const std::string &field, isi_error *error,
    const EM& em)
{
	if (error) {
		api_error_code ec = em.translate(error);
		throw api_exception(field, ec, error);
	}
}

api_error_throw::api_error_throw()
	: err_(0)
{ }

api_error_throw::~api_error_throw()
#if __cplusplus >= 201103L
noexcept(false)
#endif
{
	api_exception::throw_on_error(err_);
}

api_error_throw::operator isi_error_pp()
{
	return &err_;
}

#endif
