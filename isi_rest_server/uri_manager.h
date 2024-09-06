#ifndef __URI_MANAGER_H__
#define __URI_MANAGER_H__

#include <string>
#include <map>

#include <isi_rest_server/request.h>
#include <isi_rest_server/response.h>
#include <isi_rest_server/uri_handler.h>
#include <isi_rest_server/uri_mapper.h>

class uri_manager {
public:
	uri_manager(const uri_mapper &mapper);

	void execute_request(request &input, response &output, bool validate_schema = true);

private:
	const uri_mapper &mapper_;
};

#endif

