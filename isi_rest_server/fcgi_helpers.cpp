#include <sys/isi_ntoken.h>
#include <sys/isi_persona.h>

#include <ctype.h>
#include <errno.h>

#include <iomanip>
#include <set>
#include <sstream>
#include <string>

#include <isi_ilog/ilog.h>

#include "api_headers.h"
#include "api_utils.h"
#include "fcgi_helpers.h"
#include "request.h"
#include "response.h"
#include "uri_handler.h"

/** XXX
 * - add construct, reset from fcgi to the request class, will allow
 *   lazy loading of input
 */

namespace {

/** Non-HTTP headers passed in by fcgi. */
const std::string HDR_REQUEST_METHOD = "REQUEST_METHOD";
const std::string HDR_QUERY_STRING   = "QUERY_STRING";
const std::string HDR_PATH_INFO      = "PATH_INFO";
const std::string HDR_REQUEST_URI    = "REQUEST_URI";
const std::string HDR_HOST           = "HOST";
const std::string HDR_HTTPS          = "HTTPS";
const std::string HDR_SCRIPT_NAME    = "SCRIPT_NAME";
const std::string HDR_REMOTE_ADDR    = "REMOTE_ADDR";

/** Prefix for http headers */
const std::string HDR_HDR_PREFIX     = "HTTP_";

const std::string HTTP               = "http://";
const std::string HTTPS              = "https://";

typedef std::set<std::string> no_log_headers;

no_log_headers no_log_fcgi;

/* Initialize lists of headers we shouldn't log */
struct no_log_list_initializer {
	no_log_list_initializer()
	{
		/* The request cookie is password equivalent */
		no_log_fcgi.insert("HTTP_COOKIE");
	}
} s_no_log_list_initializer;

/*
 * Case-insensitive and - == _ compare until we mod apache/fcgi
 * to not modify headers for the environment.
 */
bool
munged_header_matches(const char *h, const std::string &name)
{
	for (unsigned j = 0; j < name.size(); ++j) {
		if (h[j] == '_') {
			if (name[j] != '_'  && name[j] != '-')
				return false;
		} else if (toupper(h[j]) != toupper(name[j]))
			return false;
	}
	return true;
}


/** return true if header @a h is name @a n and populate @a v */
bool
hdr_value(std::string &v, const char *h, const std::string &n)
{
	if (munged_header_matches(h, n) && *(h + n.size()) == '=') {
		v = h + n.size() + 1;
		return true;
	} else
		return false;
}

/** return true if header @a h matches prefix @a p and populate @a k and @a v */
bool
hdr_prefix(std::string &k, std::string &v, const char *h, const std::string &p)
{
	if (strncasecmp(h, p.c_str(), p.size()) == 0) {
		const char *key_p = h + p.size();

		if (const char *value_p = strchr(h + p.size(), '=')) {
			/* XXX - do we need to unmunge these headers */
			k.assign(key_p, value_p - key_p);
			v = ++value_p;
			return true;
		}
	}

	return false;
}

}

/*   __           _     _     _
 *  / _| ___ __ _(_)   (_)___| |_ _ __ ___  __ _ _ __ ___
 * | |_ / __/ _` | |   | / __| __| '__/ _ \/ _` | '_ ` _ \
 * |  _| (_| (_| | |   | \__ \ |_| | |  __/ (_| | | | | | |
 * |_|  \___\__, |_|___|_|___/\__|_|  \___|\__,_|_| |_| |_|
 *          |___/ |_____|
 */

fcgi_istream::fcgi_istream(fcgi_request &f)
	: fcgi_(f)
{ }

size_t
fcgi_istream::read(void *buff, size_t len)
{
	return FCGX_GetStr((char *)buff, len, fcgi_.in);
}

void
fcgi_istream::get_error(std::string &error_info)
{
	int err = FCGX_GetError(fcgi_.in);

	std::stringstream strm;
	strm << "requestId:"  << fcgi_.requestId
	     << ", ipcFd:"    << fcgi_.ipcFd
	     << ", keepConn:" << fcgi_.keepConnection
	     << ", request:"  << &fcgi_
	     << ", err:"      << err;

	error_info = strm.str();
}

/*   __           _              _
 *  / _| ___ __ _(_)    ___  ___| |_ _ __ ___  __ _ _ __ ___
 * | |_ / __/ _` | |   / _ \/ __| __| '__/ _ \/ _` | '_ ` _ \
 * |  _| (_| (_| | |  | (_) \__ \ |_| | |  __/ (_| | | | | | |
 * |_|  \___\__, |_|___\___/|___/\__|_|  \___|\__,_|_| |_| |_|
 *          |___/ |_____|
 */

fcgi_ostream::fcgi_ostream(fcgi_request &f)
	: fcgi_(f)
{ }

void
fcgi_ostream::write_data(const void *buff, size_t len)
{
	if (len != (size_t)FCGX_PutStr((char *)buff, len, fcgi_.out))
		throw ISI_ERRNO_EXCEPTION("Error writing data");
}

void
fcgi_ostream::write_from_fd(int fd, size_t len)
{
	char buf[2 * 8192];

	size_t to_read = len ? len : std::numeric_limits<size_t>::max();

	while (to_read > 0) {
		ssize_t n = read(fd, buf, std::min(sizeof buf, to_read));

		if (n == 0)
			break;
		else if (n < 0) {
			if (errno == EINTR)
				continue;
			throw ISI_ERRNO_EXCEPTION("Error reading file");
		} 

		to_read -= n;

		if (n != FCGX_PutStr(buf, n, fcgi_.out))
			throw ISI_ERRNO_EXCEPTION("Error writing data");
	}

	if (len > 0 && to_read)
		throw isi_exception("Short read, %ld expected, %ld remaining",
		    len, to_read);
}

void
fcgi_ostream::write_header(const response &r)
{
	const char *status_str = api_status_code_str(r.get_status());

	typedef std::map<std::string, std::string>::const_iterator iter;

	const api_header_map &h = r.get_header_map();

	// Status
	if (FCGX_FPrintF(fcgi_.out, "%s: %d %s\r\n",
	    api_header::STATUS.c_str(),
	    r.get_status(), status_str) == -1)
		throw ISI_ERRNO_EXCEPTION("Error writing headers");

	// Content-type
	if (FCGX_FPrintF(fcgi_.out, "%s: %s\r\n",
	    api_header::CONTENT_TYPE.c_str(),
	    r.get_content_type().c_str()) == -1)
		throw ISI_ERRNO_EXCEPTION("Error writing headers");

	for (iter it = h.begin(); it != h.end(); ++it)
		if (FCGX_FPrintF(fcgi_.out, "%s: %s\r\n",
			it->first.c_str(), it->second.c_str()) == -1)
			throw ISI_ERRNO_EXCEPTION("Error writing headers");

	if (FCGX_FPrintF(fcgi_.out, "\r\n") == -1)
		throw ISI_ERRNO_EXCEPTION("Error writing headers");
}

/*
 *   __           _        _      _             _
 *  / _| ___ __ _(_)    __| | ___| |_ __ _  ___| |__
 * | |_ / __/ _` | |   / _` |/ _ \ __/ _` |/ __| '_ \
 * |  _| (_| (_| | |  | (_| |  __/ || (_| | (__| | | |
 * |_|  \___\__, |_|___\__,_|\___|\__\__,_|\___|_| |_|
 *          |___/ |_____|
 */

/*
 * Taken directly form fcgiapp.c
 *
 * <fcgiapp.c>
 */
typedef struct Params {
	FCGX_ParamArray vec;    /* vector of strings */
	int length;         /* number of string vec can hold */
	char **cur;         /* current item in vec; *cur == NULL */
} Params;
typedef Params *ParamsPtr;

static void FreeParams(ParamsPtr *paramsPtrPtr)
{
	ParamsPtr paramsPtr = *paramsPtrPtr;
	char **p;
	if(paramsPtr == NULL) {
		return;
	}
	for (p = paramsPtr->vec; p < paramsPtr->cur; p++) {
		free(*p);
	}
	free(paramsPtr->vec);
	free(paramsPtr);
	*paramsPtrPtr = NULL;
}
/* </fcgiapp.c> */

void
fcgi_request::finish(bool child)
{
	/* Set redirect to nullptr so that on deconstruction FCGX_Free
	 * returns without doing anything */
	redirect_ = nullptr;

	/* If child, close streams */
	if (child)
	{
		FCGX_FClose(err);
		FCGX_FClose(out);
	}

	/* Clean up streams and parameters */
	FCGX_FreeStream(&in);
	FCGX_FreeStream(&out);
	FCGX_FreeStream(&err);
	FreeParams(&paramsPtr);

	/* Close cred fd */
	if (credFd != -1)
	{
		close(credFd);
		credFd = -1;
	}

	/* If child, shutdown ipc stream */
	if (child)
	{
		shutdown(ipcFd, SHUT_RDWR);
	}

	/* Close ipc fd */
	if (ipcFd != -1)
	{
		close(ipcFd);
		ipcFd = -1;
	}
}

/*   __           _                                    _
 *  / _| ___ __ _(_)    _ __ ___  __ _ _   _  ___  ___| |_
 * | |_ / __/ _` | |   | '__/ _ \/ _` | | | |/ _ \/ __| __|
 * |  _| (_| (_| | |   | | |  __/ (_| | |_| |  __/\__ \ |_
 * |_|  \___\__, |_|___|_|  \___|\__, |\__,_|\___||___/\__|
 *          |___/ |_____|           |_|
 */

request *
fcgi_request::api_request()
{
	std::string k;
	std::string v;
	bool https = false;
	uint64_t len = 0;

	/* parameters needed for request construction */
	std::string         base;
	std::string         uri;
	std::string         method;
	std::string         args;
	std::string         script_name;
	std::string         request_uri;
	std::string         remote_addr;
	api_header_map      headers;
	int 		    user_fd;

	/* get user_fd from FCGX_Request */
	user_fd = credFd;

	/* process fcgi headers */
	for (char **e = envp; *e != NULL; ++e) {
		if (!g_release || no_log_fcgi.find(*e) == no_log_fcgi.end())
			ilog(IL_REST_ENV, "env var: %s", *e);

		if (hdr_value(v, *e, HDR_REQUEST_METHOD))
			method = v;
		else if (hdr_value(v, *e, HDR_REQUEST_URI))
			request_uri = v;
		else if (hdr_value(v, *e, api_header::CONTENT_LENGTH))
			len = strtoull(v.c_str(), NULL, 10);
		else if (hdr_value(v, *e, HDR_HTTPS))
			https = v == "on";
		else if (hdr_value(v, *e, HDR_SCRIPT_NAME))
			script_name = v;
		else if (hdr_value(v, *e, api_header::CONTENT_TYPE))
			headers[api_header::CONTENT_TYPE] = v;
		else if (hdr_value(v, *e, api_header::CONTENT_ENCODING))
			headers[api_header::CONTENT_ENCODING] = v;
		else if (hdr_value(v, *e, HDR_REMOTE_ADDR))
			remote_addr = v;
		/* etc. */

		else if (hdr_prefix(k, v, *e, HDR_HDR_PREFIX))
			headers[k] = v;
	}

	std::string::size_type q_pos = request_uri.find('?');
	uri.assign(request_uri, script_name.size(), q_pos - script_name.size());
	if (q_pos != std::string::npos)
		args.assign(request_uri, q_pos + 1, std::string::npos);

	base = (https ? HTTPS : HTTP) + headers[HDR_HOST] + script_name;

	return new request(base, script_name, uri, method, args,
	    headers, len, user_fd, remote_addr, istream_, internal);
}
