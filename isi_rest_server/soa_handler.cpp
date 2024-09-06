#include "soa_handler.h"

#include <isi_rest_client/rest_client.h>
#include <isi_ilog/ilog.h>

// Base SOA Class

soa_handler::soa_handler(int priv) :
    uri_handler(priv_filter(priv).set_run_as(false))
{ }

soa_handler::soa_handler(const priv_filter &pf) :
    uri_handler(pf)
{ }

void
soa_handler::http_get(const request &input, response &output)
{
	call_service(input, output);
}

void
soa_handler::http_put(const request &input, response &output)
{
	call_service(input, output);
}

void
soa_handler::http_post(const request &input, response &output)
{
	call_service(input, output);
}

void
soa_handler::http_delete(const request &input, response &output)
{
	call_service(input, output);
}

void
soa_handler::http_head(const request &input, response &output)
{
	call_service(input, output);
}

