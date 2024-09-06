#include "google_xml_io_helper.h"
#include "isi_cbm_ioh_creator.h"

namespace {

isi_cbm_ioh_base *
create_google_xml_io_helper(const isi_cbm_account &acct)
{
	return new google_xml_io_helper(acct);
}

isi_cbm_ioh_register google_xml_reg(CPT_GOOGLE_XML, create_google_xml_io_helper);
}

google_xml_io_helper::google_xml_io_helper(const isi_cbm_account &acct)
	: s3_io_helper(acct)
{
	ASSERT(acct.type == CPT_GOOGLE_XML,
	    "Only GOOGLE_XML should use google_xml_io_helper (%d requested)",
	    acct.type);
}
