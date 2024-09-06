#ifndef __GOOGLE_XML_IO_HELPER_H__
#define __GOOGLE_XML_IO_HELPER_H__

#include "s3_io_helper.h"

class google_xml_io_helper : public s3_io_helper {
	public:
		explicit google_xml_io_helper(const isi_cbm_account &acct);
};


#endif //!__GOOGLE_XML_IO_HELPER_H__
