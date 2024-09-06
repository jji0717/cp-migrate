#ifndef __ACCT_HANDLER_H__
#define __ACCT_HANDLER_H__

#include "isi_rest_server/uri_handler.h"

class acct_handler : public uri_handler {
public:
	acct_handler() : uri_handler()
	{
		supported_methods_[request::GET] = true;
		supported_methods_[request::POST] = true;
	}

	virtual void http_get(const request &input, response &output);
	virtual void http_post(const request &input, response &output);

	virtual bool validate_input(const request &input, response &output);
	virtual bool validate_output(const request &input, response &output);
};

#endif

