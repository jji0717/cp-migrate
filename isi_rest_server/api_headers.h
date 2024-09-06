#ifndef __API_HEADERS_H__
#define __API_HEADERS_H__

#include <map>
#include <string>

typedef std::map<std::string, std::string> api_header_map;

namespace api_header
{

extern const std::string ACCEPT_ENCODING;
extern const std::string ALLOW;
extern const std::string CONTENT_DISPOSITION;
extern const std::string CONTENT_ENCODING;
extern const std::string CONTENT_LENGTH;
extern const std::string CONTENT_RANGE;
extern const std::string CONTENT_TYPE;
extern const std::string IF_MODIFIED_SINCE;
extern const std::string IF_UNMODIFIED_SINCE;
extern const std::string IF_MATCH;
extern const std::string IF_NONE_MATCH;
extern const std::string LAST_MODIFIED;
extern const std::string LOCATION;
extern const std::string ETAG;
extern const std::string STATUS;

}


#endif



