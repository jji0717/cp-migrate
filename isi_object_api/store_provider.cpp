#include "store_provider.h"

#include "handler_common.h"
#include "oapi/oapi_store.h"

#include "ns_attr_mgr.h"
#include "ob_attr_mgr.h"

#include <isi_rest_server/api_error.h>

void
store_provider::open_store(oapi_store &store, const std::string &ns,
    const std::string &sn)
{
	isi_error *error = NULL;
	if ( ns == NS_NAMESPACE)
		store.open_store(OST_NAMESPACE, sn, error);
	else if (ns == NS_OBJECT) {
		static const std::string OBJECT_STORE("object_api_v1");

		store.create_store(OST_OBJECT, OBJECT_STORE, "", error);
	}
	else
		throw api_exception(AEC_BAD_REQUEST,
		    "Access to '%s' is not supported by this service.",
		    ns.c_str());

	if (error) {
		std::string what("open the store '");
		what.append(sn);
		what.append("'");
		oapi_error_handler().throw_on_error(error, what.c_str());
	}
}

void
store_provider::open_store(oapi_store &store, const request &input)
{
	const std::string &ns = input.get_namespace();
	std::string store_name;
	if (ns == NS_NAMESPACE) {
		const std::string &uri = input.get_token("OBJECT");
		size_t pos = uri.find_first_of('/', 0);
		store_name = uri.substr(0, pos);
	}
	open_store(store, ns, store_name);
}

const attribute_manager &
store_provider::get_attribute_mgr(const request &input)
{
	const std::string &ns = input.get_namespace();
	if ( ns == NS_NAMESPACE)
		return ns_attr_mgr::get_attr_mgr();
	else if (ns == NS_OBJECT)
		return ob_attr_mgr::get_attr_mgr();

	throw api_exception(AEC_BAD_REQUEST,
	    "Access to '%s' is not supported by this service.",
	    ns.c_str());
}
