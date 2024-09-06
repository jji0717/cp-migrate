#include <uuid.h>
#include "audit_request.h"

static
void
cleanup_audit_handle(HandleContext *handle) {
	if (handle != NULL && *handle != NULL) {
		P_CloseLog(handle);
	}
}

audit_request::audit_request(const request& req, bool auditing_enabled):
	audited(auditing_enabled), audit_handle(NULL),
	clean_audit(audit_handle, cleanup_audit_handle)
{
	if (!audited) {
		return;
	}

	switch (req.get_method()) {
		case request::PUT:
		case request::POST:
		case request::DELETE:
			generate_uuid();
			break;

		default:
			audited = false;
			break;
	}

	if (!audited) {
		return;
	}

	char *payload = req.audit_request();
	scoped_ptr_clean<char> clean_payload(payload, free);

	if (payload != NULL) {
		log_event(payload);
	} else {
		ilog(IL_ERR, "Failed to generate a request audit event");
		audited = false;
	}
}

void
audit_request::audit_response(response& res)
{
	if (!audited) {
		return;
	}

	char *payload = res.audit_response();
	scoped_ptr_clean<char> clean_payload(payload, free);

	if (payload != NULL) {
		log_event(payload);
	}
	else {
		ilog(IL_ERR, "Failed to generate a response audit event");
	}
}

void
audit_request::generate_uuid()
{
	uint32_t uuid_result = -1;
	uuid_t uuid_value;
	char *id = NULL;
	scoped_ptr_clean<char> uuid_result_clean(id, free);

	uuid_create(&uuid_value, &uuid_result);
	if (uuid_result != uuid_s_ok) {
		ilog(IL_ERR, "Failed to generate audit UUID: %u", uuid_result);
		uuid = "???";
		return;
	}

	uuid_to_string(&uuid_value, &id, &uuid_result);
	if (uuid_result != uuid_s_ok) {
		ilog(IL_ERR, "Failed to convert audit UUID to a string: %u",
			uuid_result);
		uuid = "???";
	}
	else {
		uuid = id;
	}
}

void
audit_request::log_event(const char *payload)
{
	int status = QPL_SUCCESS;
	int connect_tries = 0;

	for (int tries = 0; tries < 5; sleep(++tries)) {
		if (audit_handle == NULL) {
			++connect_tries;
			status = P_OpenSharedLog(&audit_handle,
				"config", "PAPI");
			if (status != QPL_SUCCESS) {
				ilog(IL_NOTICE,
					"Error connecting to the audit daemon - try #%d",
					connect_tries);
				continue;
			}
		}

		status = P_LogConfigEvent(audit_handle, uuid.c_str(), payload,
			strlen(payload));
		if (status != QPL_SUCCESS) {
			if (status == QPL_TOO_BIG) {
				ilog(IL_ERR, "Cannot deliver audit event to "
					"audit daemon. Event payload size "
					"(%lu) + format length is greater than"
					" allowed size (%d)", strlen(payload),
					AUDIT_MAX_PAYLOAD_SIZE);
				return;
			}
			ilog(IL_ERR, "Audit event I/O error");
			P_CloseLog(&audit_handle);
			audit_handle = NULL;
			continue;
		}

		break;
	}

	if (status != QPL_SUCCESS) {
		ilog(IL_ERR, "Error cannot deliver audit event to audit daemon");
		audited = false;
	}
}
