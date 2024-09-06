#include <isi_util/isi_assert.h>

#include "oapi_mapper.h"

/* handler headers */
#include "acct_handler.h"
#include "bucket_handler.h"
#include "object_handler.h"
#include "ns_object_handler.h"
#include "ns_svc_handler.h"
#include "handler_common.h"

oapi_uri_mapper::oapi_uri_mapper() :
    uri_mapper(0), object_mapper_(0)
{
	uri_mapper *m = &object_mapper_; // alias

	uri_handler *handler = NULL;
	handler = new acct_handler();
	m->add("", handler);
	handler->populate_supported_methods();
	
	handler = new bucket_handler();
	m->add("/<BUCKET>", handler);
	handler->populate_supported_methods();
		
	handler = new object_handler();
	m->add("/<BUCKET>/<OBJECT+>", handler);
	handler->populate_supported_methods();
	
	m = this; // alias
	handler = new ns_svc_handler();
	m->add("", handler);
	handler->populate_supported_methods();
	
	handler = new ns_object_handler();
	m->add("/<OBJECT+>", handler);
	handler->populate_supported_methods();
}

uri_handler *
oapi_uri_mapper::resolve(request &input, response &output) const
{
	std::string s_ver;
	api_header_map::const_iterator it_ver =
	    input.get_header_map().find(X_ISI_IFS_SPEC_VERSION);
	if (it_ver != input.get_header_map().end())
		s_ver = it_ver->second;

	const std::string &ns = input.get_namespace();
	const oapi_version *ov = 0;
	api_header_map &hmap = output.get_header_map();

	if (ns == NS_OBJECT) {
		ov = &oapi_version_mgr::get_object_api_version();
		hmap[HHDR_X_ISI_IFS_SPEC_VERSION] = ov->to_string();
		if (s_ver.empty() || ov->is_compatible(oapi_version(s_ver)))
			return object_mapper_.resolve(input, output);
	} else if (ns == NS_NAMESPACE) {
		ov = &oapi_version_mgr::get_namespace_api_version();
		hmap[HHDR_X_ISI_IFS_SPEC_VERSION] = ov->to_string();
		if (s_ver.empty() || ov->is_compatible(oapi_version(s_ver)))
			return uri_mapper::resolve(input, output);
	} else
		return NULL;

	throw api_exception(AEC_NOT_ACCEPTABLE,
	    "Request version '%s' is not supported.", s_ver.c_str());	
}


oapi_uri_mapper oapi_mapper;

const std::string &DEF_DOC_ROOT = "/usr/share/oapi/doc";
