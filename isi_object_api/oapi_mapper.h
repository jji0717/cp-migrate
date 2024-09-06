#ifndef _PAPI_MAPPER_H_
#define _PAPI_MAPPER_H_

#include <isi_rest_server/uri_mapper.h>

class oapi_uri_mapper : public uri_mapper {
public:

        /** Find handler for path.
         *
         * It checks request version in the header map, and throws
         * api_exception if request version is not empty and doesn't
         * match with the version of api.
         *
         * @param input: request object
         * @param output response
         *
         * @return handler or NULL if not found.
         */
        virtual uri_handler *resolve(request &input, response &output) const;

        oapi_uri_mapper();

private:
        uri_mapper object_mapper_;
};

extern oapi_uri_mapper oapi_mapper;

#endif
