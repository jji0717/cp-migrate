#ifndef __FASTCGI_SOA_HANDLER_H__
#define __FASTCGI_SOA_HANDLER_H__

#include "soa_handler.h"

class fastcgi_soa_handler : public soa_handler {
public:
        fastcgi_soa_handler(int priv, const std::string &socket_path);
        fastcgi_soa_handler(const priv_filter &pf,
            const std::string &socket_path);

        virtual void call_service(const request &input, response &output);

private:
        std::string socket_path_;

};

#endif

