#include "ns_svc_handler.h"

#include "handler_common.h"
#include "query_handler.h"

void
ns_svc_handler::http_get(const request &input, response &output)
{
	output.set_status(ASC_OK);
	if (is_version_request(input)) {
		oapi_set_versions_to_response(input.get_namespace(), output);
		return;
	}

	query_handler q_handler;
	q_handler.list_store(OST_NAMESPACE, input, output);
}

void
ns_svc_handler::http_head(const request &input, response &output)
{
    output.set_status(ASC_OK);
}

bool
ns_svc_handler::validate_input(const request &input, response &output)
{
	// no json input
	return true;
}
