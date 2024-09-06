#ifndef __SOA_HANDLER_H__
#define __SOA_HANDLER_H__

#include <isi_rest_server/uri_handler.h>

class soa_handler : public uri_handler {
public:
	soa_handler(int priv);
	soa_handler(const priv_filter &pf);

	virtual void http_get(const request &input, response &output);
        virtual void http_put(const request &input, response &output);
        virtual void http_post(const request &input, response &output);
        virtual void http_delete(const request &input, response &output);
        virtual void http_head(const request &input, response &output);

	// Different protocols need to override this
	virtual void call_service(const request &input, response &output) = 0;
};

#endif

