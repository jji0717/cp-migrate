#ifndef __NS_SVC_HANDLER_H__
#define __NS_SVC_HANDLER_H__

#include "isi_rest_server/uri_handler.h"

class ns_svc_handler : public uri_handler {
public:
	ns_svc_handler() : uri_handler()
	{
		supported_methods_[request::GET] = true;
		supported_methods_[request::HEAD] = true;
	}

	virtual void http_get(const request &input, response &output);
	virtual void http_head(const request &input, response &output);
	
	virtual bool validate_input(const request &input, response &output);
};

#endif

