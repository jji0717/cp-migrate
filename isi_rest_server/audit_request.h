#ifndef __AUDIT_REQUEST_H__
#define __AUDIT_REQUEST_H__

#include <string>

#include <isi_qpl/qpl.h>
#include <isi_util_cpp/scoped_clean.h>
#include <isi_audit/audit_util.h>

#include "request.h"
#include "response.h"

class audit_request
{
public:
	/** Audit a request object.
	 *
	 * If the request object is either PUT, POST or DELETE and
         * auditing_enabled is true then an audit event is generated
         * and sent to the audit daemon; otherwise this object does
         * nothing.
	 *
	 * @param req[IN] - The request to audit.
	 * @param auditing_enabled[IN] - Whether auditing is enabled.
	 */
	audit_request(const request& req, bool auditing_enabled);

	/** Audit the response associated with the request.
	 *
	 * If auditing was disabled when audit_request was created
	 * or the request was not audited this does nothing.
	 *
	 * @param res[IN] - The response to audit.
	 */
	void
	audit_response(response& res);

private:
	bool audited;
	std::string uuid;
	HandleContext audit_handle;
	scoped_clean<HandleContext> clean_audit;

	/** Generate the UUID for the audit request and response. */
	void
	generate_uuid();

	/** Send an audit payload to the audit daemon.
	 *
	 * @param payload[IN] - JSON formatted payload to audit.
	 */
	void
	log_event(const char *payload);

	audit_request(audit_request const&);
	audit_request& operator=(audit_request const&);
};

#endif // __AUDIT_REQUEST_H__
