#include "environmental_criteria.h"
#include "api_content.h"
#include "api_error.h"
#include "api_utils.h"

#include <isi_util_cpp/user_text.h>

#include <unistd.h>

void
no_modify_on_manufacturing(const request &req)
{
	switch (req.get_method()){
	case request::GET:
	case request::HEAD:
		break;
	default:
		if (!access("/etc/ifs/isi_manufacturing_only", F_OK)){
			throw api_exception(AEC_BAD_REQUEST, get_user_text(
			    MANUFACTURING_BUILD_EXCEPTION_TEXT));
		}
	}
}
