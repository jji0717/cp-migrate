#include <ifs/ifs_types.h>

#include <isi_cloud_api/test_stream.h>
#include <isi_cloud_api/test_common_acct.h>
#include <isi_cpool_cbm/check_hardening.h>

#include "../check_cbm_common.h"
#include "azure_io_helper.h"
#include "isi_cbm_ioh_base.h"
#include "isi_cbm_ioh_creator.h"
#include "ran_io_helper.h"
#include "s3_io_helper.h"
#include "header_ostream.h"
#include <isi_cpool_cbm/isi_cbm_write.h>
#include <isi_cpool_cbm/isi_cbm_mapper.h>
#include <isi_cpool_cbm/isi_cbm_stream.h>
#include <isi_cpool_cbm/isi_cbm_read.h>

#include <check.h>

#define UNIT_TEST_FILE_PREFIX "unittestiohelper"
#define FAIL_IF_EXCEPTION catch (cl_exception &ex) {	\
		fail("%s", ex.what());			\
	}

using namespace isi_cloud;

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(cleanup_suite);

SUITE_DEFINE_FOR_FILE(check_io_helper,
    .mem_check = CK_MEM_LEAKS,
    .dmalloc_opts = NULL,
    .suite_setup = setup_suite,
    .suite_teardown = cleanup_suite,
    .test_setup = NULL,
    .test_teardown = NULL,
    .runner_data = NULL,
    .timeout = 260,
    .fixture_timeout = 1200);

static isi_cbm_account ran_acct;
// static isi_cbm_account azure_acct;
// static isi_cbm_account s3_acct;

static ran_io_helper *ioh_ran = NULL;
// static azure_io_helper *ioh_az = NULL;
// static s3_io_helper *ioh_s3 = NULL;

static ran_io_helper *
create_ran_io_helper()
{
	isi_cbm_ioh_base *ioh1 = isi_cbm_ioh_creator::get_io_helper(
	    CPT_RAN, ran_acct);
	fail_if(ioh1 == NULL, "failed to create io helper for RAN");
	return dynamic_cast<ran_io_helper *>(ioh1);
}

TEST_FIXTURE(setup_suite)
{
#ifdef ENABLE_ILOG_USE
	struct ilog_app_init init =  {
		"check_io_helper",		// full app name
		"cpool",			// component
		"",				// job (opt)
		"check_io_helper",		// syslog program
		IL_TRACE_PLUS|IL_CP_EXT|IL_DETAILS,	// default level
		false,				// use syslog
		true,				// use stderr
		false,				// log_thread_id
		LOG_DAEMON,			// syslog facility
		"/ifs/logs/check_io_helper.log",// log file
		IL_TRACE_PLUS,			// syslog_threshold
		NULL,				//tags
	};

	char *ilogenv = getenv("CHECK_USE_ILOG");
	if (ilogenv != NULL) {
		ilog_init(&init, false, true);
		ilog(IL_NOTICE, "ilog initialized\n");
	}
#endif
	// long string to prevent bogus mem leak report
	std::string a = "mcon-123456789-123456789-123456789-123456789"
	    "-123456789-123456789";
	isi_cloud_object_id aid(10, 10); // avoid bogus mem leak

	// make container different each time to run so azure will not
	// complain about deleted container
	char tstr[20];
	sprintf(tstr, "%ld", time(0));
	std::string prefix = UNIT_TEST_FILE_PREFIX;

	std::string test_url;

	if (test_is_hardened()) {
		if (test_make_url(test_url, "https", 8080, "/namespace/ifs") == 0) {
			ran_acct.url = test_url;
		}
		else {
			fail("Unable to fetch external IP address");
		}
	}
	else {
		ran_acct.url = RAN_TEST_ACCOUNT_URL;
	}

	ran_acct.name = RAN_TEST_ACCOUNT_NAME;
	ran_acct.key = RAN_TEST_ACCOUNT_KEY;
	ran_acct.id = RAN_TEST_ACCOUNT_ID;
	ran_acct.type = CPT_RAN;
	ran_acct.container_cmo = prefix + "-m";
	ran_acct.container_cdo = prefix + "-d";
	ran_acct.chunk_size = 256*1024;
	ran_acct.read_size = 256*1024;
	ran_acct.sparse_resolution = 8*1024;

	ioh_ran = create_ran_io_helper();
	fail_if(ioh_ran == NULL, "failed to create io helper for RAN");

	try {
		ioh_ran->put_container(ran_acct.container_cmo);
		//ioh_ran->put_container(ran_acct.container_cdo);
	} FAIL_IF_EXCEPTION

	CHECK_TRACE("\nTest setup complete\n");
}

//
// This function attempts to delete a cloud container. Should the delete fail,
// the failure is reported in the test output.
//
void
delete_container(
    std::string		acct_type,
    isi_cbm_ioh_base*	pIohBase,
    std::string		Name
    )
{
	try {
		pIohBase->delete_container(Name);
	}
	catch (cl_exception &ex) {
		CHECK_TRACE(
		    "\n**********  cleanup_suite()\n" \
		    "Unable to delete %s account, %s. \nError: %s\n**********\n",
		    acct_type.c_str(), Name.c_str(), ex.what()
		    );
	}
}

TEST_FIXTURE(cleanup_suite)
{
	std::string     RECURSIVE_DELETE("?recursive=true");

	// delete_container(std::string("RAN"), ioh_ran,
	//     ran_acct.container_cmo + RECURSIVE_DELETE);
	// delete_container(std::string("RAN"), ioh_ran,
	//     ran_acct.container_cdo + RECURSIVE_DELETE);

	delete ioh_ran;
}

TEST(smoke_test)
{
	printf("%s called\n", __func__);
}
TEST(validate_name)
{
	CHECK_TRACE("%s: \n", __FUNCTION__);
	fail_if(isi_cbm_ioh_base::validate_container_name(
	    "m-container1-")); // ending with '-'
	fail_if(isi_cbm_ioh_base::validate_container_name(
	    "m-Container1")); //upper case
	fail_if(isi_cbm_ioh_base::validate_container_name(
	    "mcon-123456789-123456789-123456789-123456789"
	    "-123456789-123456789")); // longer than 63
	fail_if(isi_cbm_ioh_base::validate_container_name(
	    "m2")); // shorter than 3
	fail_if(isi_cbm_ioh_base::validate_container_name(
	    "a--bc")); // consecutive '-'
	fail_if(isi_cbm_ioh_base::validate_container_name(
	    "m.ontainer1"));

	fail_unless(isi_cbm_ioh_base::validate_container_name(
	    "m12"));
	fail_unless(isi_cbm_ioh_base::validate_container_name(
	    "mcon-123456789-123456789-123456789-123456789"
	    "-123456789-12345678")); // length = 63
	fail_unless(isi_cbm_ioh_base::validate_container_name(
	    "m-container1"));
}

TEST(test_generate_cloud_name)
{
	ifs_lin_t lin = 0x1234567;
	cl_object_name cloud_name;
	ioh_ran->generate_cloud_name(lin,cloud_name); ////产生object_id(使用guid产生), cdo_container(), cmo_container
	std::string container_path;
	ioh_ran->build_container_path(cloud_name.container_cdo, cloud_name.obj_base_name.to_string(),container_path);
	//printf("\nobject_id:%s cdo:%s cmo:%s",cloud_name.obj_base_name.to_c_string(), cloud_name.container_cmo.c_str(),cloud_name.container_cdo.c_str());
	printf("\ncontainer_path:%s\n",container_path.c_str());
}
TEST(test_basic)
{
	printf("\ntest_basic\n");
}
TEST(generate_cloud_name, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{

	ifs_lin_t lin = 0x1234567;
	cl_object_name cloud_name;

	CHECK_TRACE("%s: \n", __FUNCTION__);
	ioh_ran->generate_cloud_name(lin, cloud_name);
	fail_if(cloud_name.container_cmo != ran_acct.container_cmo,
	    "failed in generate_cloud_name for ran");

	// ioh_az->generate_cloud_name(lin, cloud_name);
	// fail_if(cloud_name.container_cmo != azure_acct.container_cmo,
	//     "failed in generate_cloud_name for azure");

	// ioh_s3->generate_cloud_name(lin, cloud_name);
	// fail_if(cloud_name.container_cmo != s3_acct.container_cmo,
	//     "failed in generate_cloud_name for s3");
}

static void write_and_get_cmo_(isi_cbm_ioh_base *ioh1, bool compress,
    bool checksum, bool lowcase_meta)
{
	str_map_t attr_map;
	size_t  t_sz = 1026;

	// attr name upper case as a convention to retrieve
	std::string attrn1 = "COLOR_W", attrv1 = "white";
	std::string attrn2 = "COLOR_B", attrv2 = "brown";

	CHECK_TRACE("  %s: compress, %d, checksum %d, lowcase: %d\n",
	    __FUNCTION__, compress, checksum, lowcase_meta);

	// if (lowcase_meta) {
	// 	std::transform(
	// 	    attrn1.begin(), attrn1.end(), attrn1.begin(), ::tolower);

	// 	std::transform(
	// 	    attrn2.begin(), attrn2.end(), attrn2.begin(), ::tolower);
	// }

	attr_map[attrn1] = attrv1;
	attr_map[attrn2] = attrv2;

	cl_test_istream strm(t_sz, 'c');
	bool overwrite = true;

	// test write cmo

	cl_object_name cloud_name;
	ioh1->generate_cloud_name(0, cloud_name);
	try {
		ioh1->write_cmo(cloud_name, attr_map,
		    strm.get_size(), strm, overwrite, compress, checksum);
	} FAIL_IF_EXCEPTION

	// test read cmo
	cl_test_ostream ostrm;
	str_map_t attr_map_o;
	try {
		ioh1->get_cmo(cloud_name, compress, checksum, attr_map_o, ostrm);
	} FAIL_IF_EXCEPTION

	const std::string &cont_d = ostrm.get_content();
	std::string cont_u(t_sz, 'c');

	fail_if(cont_d != cont_u,
	    "get_cmo failed %d, %d", cont_d.size(), cont_u.size());
	fail_if(attr_map_o[attrn1] != attrv1, "get_cmo failed");
	fail_if(attr_map_o[attrn2] != attrv2, "get_cmo failed");

	// test read attributes
	attr_map_o.clear();
	ioh1->get_cmo_attrs(cloud_name, attr_map_o);
	fail_if(attr_map_o[attrn1] != attrv1, "get_cmo_attrs failed");
	fail_if(attr_map_o[attrn2] != attrv2, "get_cmo_attrs failed");

	// test delete
	try {
		ioh1->delete_cmo(cloud_name,
		    cloud_name.obj_base_name.get_snapid());
	} FAIL_IF_EXCEPTION

}
TEST(test_cmo_upload_download)
{
	struct isi_cmo_info cmo_info;
	cmo_info.version = 2;
	cmo_info.lin = (ifs_lin_t)1009009;
	struct stat stats;
	stats.st_size = 999;
	cmo_info.stats = &stats;
	isi_cfm_mapinfo mapinfo(ISI_CFM_MAP_TYPE_OBJECTINFO);
	mapinfo.set_filesize(stats.st_size);
	//mapinfo.set_lin(cmo_info.lin); 
	mapinfo.set_chunksize(819202);
	cmo_info.mapinfo = &mapinfo;

	meta_istream strm; /////isi_cbm_stream.cpp .h
	strm.setvers(cmo_info.version);
	strm.setmd("metadata file path");
	strm.setmi(*(cmo_info.mapinfo));
	strm.setst(*(cmo_info.stats));

	/////test write cmo
	str_map_t attr_map;
	cl_object_name cloud_name;
	ioh_ran->generate_cloud_name(0, cloud_name);
	printf("\n%s called cmo:%s cdo:%s\n", __func__, cloud_name.container_cmo.c_str(),
			cloud_name.container_cdo.c_str());
	ioh_ran->write_cmo(cloud_name,attr_map,strm.length(),strm,true,false,false);

	/////test read cmo
	cl_cmo_ostream  pstrm;   ///isi_cbm_read.cpp .h
	ioh_ran->get_cmo(cloud_name,false,false,attr_map,pstrm);
	printf("%s called version:%d meta:%s mapinfp.filesize:%ld mapinfo.chunksize:%ld\n", __func__, 
			pstrm.get_version(),pstrm.get_path().c_str(),pstrm.get_mapinfo().get_filesize(),
			pstrm.get_mapinfo().get_chunksize());




}
TEST(ran_write_and_get_cmo, .mem_check = CK_MEM_DISABLE)
{
	CHECK_TRACE("%s: \n", __FUNCTION__);
	write_and_get_cmo_(ioh_ran, false, false, false);
	//write_and_get_cmo_(ioh_ran, false, true, false); 不考虑 jjz
}
////不用考虑compression jjz
// TEST(ran_write_and_get_cmo_with_compression, .mem_check = CK_MEM_DISABLE)
// {
// 	CHECK_TRACE("%s: \n", __FUNCTION__);
// 	write_and_get_cmo_(ioh_ran, true, false, false);
// 	write_and_get_cmo_(ioh_ran, true, true, false);
// }

static bool compare_spa(
    const char *msg,
    sparse_area_header_t expected,
    sparse_area_header_t actual)
{
	bool match = true;
	int i;

	if ((expected.get_spa_version()       != actual.get_spa_version()) ||
	    (expected.get_sparse_map_length() != actual.get_sparse_map_length())   ||
	    (expected.get_sparse_resolution() != actual.get_sparse_resolution())) {
		match = false;
	} else {
		for (i = 0;
		     i < MIN(SPARSE_MAP_SIZE,
			 actual.sparse_map_length);
		     i++) {
			if (actual.get_sparse_map_elem(i) !=
			    expected.get_sparse_map_elem(i)) {
				match = false;
			}
		}
	}
	if (!match) {
		CHECK_TRACE("    %s Expected SPA: "
		    "version %04d, map length %04d, "
		    "resolution %08d map: 0x",
		    msg, expected.get_spa_version(),
		    expected.get_sparse_map_length(),
		    expected.get_sparse_resolution());
		for (i = 0; i < expected.sparse_map_length; i++) {
			CHECK_TRACE("%02x ", expected.get_sparse_map_elem(i));
		}
		CHECK_TRACE("\n");

		CHECK_TRACE("    %s   Actual SPA: "
		    "version %04d, map length %04d, "
		    "resolution %08d map: 0x",
		    msg, actual.get_spa_version(),
		    actual.get_sparse_map_length(),
		    actual.get_sparse_resolution());
		for (i = 0;
		     i < MIN(SPARSE_MAP_SIZE,
			 actual.sparse_map_length);
		     i++) {
			CHECK_TRACE("%02x ", actual.get_sparse_map_elem(i));
		}
		CHECK_TRACE("\n");

		expected.trace_spa(msg, __FUNCTION__, __LINE__);
		actual.trace_spa(msg, __FUNCTION__, __LINE__);
	}

	return match;
}

static void write_and_get_cdo(isi_cbm_ioh_base *ioh1,
    size_t t_sz, size_t chunk_sz, bool compress, bool checksum,
    bool lowcase_meta, encrypt_ctx_p ectx)
{
	str_map_t attr_map;
	cl_test_ostream ostrm;
	str_map_t attr_map_o;
	isi_tri_bool with_compress(compress);
	isi_tri_bool with_checksum(checksum);
	bool cache_hit;
	sparse_area_header_t expected_spa;
	sparse_area_header_t spa_header;
	checksum_header cs_header;
	checksum_value cached_cs;
	sparse_area_header_t cached_spa;
	int i;
	str_map_t cmo_attr;

	// attr name upper case as a convention to retrieve
	std::string attrn1 = "COLOR_W", attrv1 = "white";
	std::string attrn2 = "COLOR_B", attrv2 = "brown";

	cl_test_istream strm(t_sz, 'd');
	bool overwrite = true;
	bool random_io = false;
	int cdo_index = 1;
	int indx = 0;
	bool match = true;

	CHECK_TRACE("  %s: compress, %d, checksum %d, lowcase: %d\n",
	    __FUNCTION__, compress, checksum, lowcase_meta);

	if (lowcase_meta) {
		std::transform(
		    attrn1.begin(), attrn1.end(), attrn1.begin(), ::tolower);

		std::transform(
		    attrn2.begin(), attrn2.end(), attrn2.begin(), ::tolower);
	}

	attr_map[attrn1] = attrv1;
	attr_map[attrn2] = attrv2;

	if (ioh1->get_account().type == CPT_AZURE)
		random_io = true;

	// test create cdo

	cl_object_name cloud_name;
	ioh1->generate_cloud_name(0, cloud_name);
	isi_cpool_master_cdo master_cdo;
	isi_cpool_master_cdo prev_mcdo;

	// Init the expected spa_header
	//
	expected_spa.set_spa_version(CPOOL_SPA_VERSION);
	expected_spa.set_sparse_map_length(SPARSE_MAP_SIZE);
	expected_spa.sparse_resolution = MIN(chunk_sz, IFS_BSIZE); // set to bsize by hand.
	for (i = 0; i < SPARSE_MAP_SIZE; i++) {
		expected_spa.set_sparse_map_elem(i, (0x40 + i));
	}

	//xxx-yi: does this demonstrate how to correctly use master cdo
	//        and delete cdo?
	ioh1->get_new_master_cdo(0, prev_mcdo, cloud_name, false, false,
	    chunk_sz, MIN(128 * 1024, chunk_sz),
	    (8 * 1024), attr_map, master_cdo, 0);

	// Init the master CDO spa_header info to the expected values
	master_cdo.set_spa_version(expected_spa.get_spa_version());
	master_cdo.set_sparse_map_length(expected_spa.get_sparse_map_length());
	// Make sure we round up to the block size.
	master_cdo.set_sparse_resolution(5, chunk_sz);
	master_cdo.set_sparse_map(
	    expected_spa.get_sparse_map_ptr(), SPARSE_MAP_SIZE);

	try {
		ioh1->start_write_master_cdo(master_cdo);

		ioh1->write_cdo(master_cdo, cdo_index,
		    chunk_sz, strm, overwrite, random_io, compress, checksum, ectx);
		// Make sure the CDO comes back with correct
		// values in the sparse header
		//
		match = compare_spa("Mismatch after write_cdo",
		    expected_spa, master_cdo.get_sparse_area_header());
		fail_if(!match, "SPA index %d mismatch", cdo_index);

		++cdo_index;
		ioh1->write_cdo(master_cdo, cdo_index,
		    chunk_sz, strm, overwrite, random_io, compress, checksum, ectx);
		// Make sure the CDO comes back with correct
		// values in the sparse header
		//
		match = compare_spa("Mismatch after write_cdo",
		    expected_spa, master_cdo.get_sparse_area_header());
		fail_if(!match, "SPA index %d mismatch", cdo_index);

		// for testing, not to commit so I can delete the object easily
		ioh1->end_write_master_cdo(master_cdo, false);

	} FAIL_IF_EXCEPTION;

	// test read cdo
	// and create cmo to set up attribute in advance
	cmo_attr["SIGNATURE"] = "1234";
	strm.reset();
	try {
		ioh1->write_cmo(cloud_name, cmo_attr,
		    strm.get_size(), strm, true, compress, checksum);
	} FAIL_IF_EXCEPTION;

	// Make sure the checksum cache entries do not exist for
	// the CDOs
	//
	for (indx = 1; indx <= cdo_index; indx++) {
		cache_hit = checksum_cache_get(
		    cloud_name.container_cdo,
		    ioh1->get_physical_cdo_name(master_cdo, indx),
		    ioh1->get_cdo_physical_offset(
			master_cdo, indx, compress, checksum),
		    cached_cs, cached_spa);
		if (cache_hit) {
			CHECK_TRACE("  SPA %02d already cached: "
			    "version %04d, map length %04d, "
			    "resolution %08d map: 0x",
			    indx, cached_spa.get_spa_version(),
			    cached_spa.get_sparse_map_length(),
			    cached_spa.get_sparse_resolution());
			for (i = 0;
			     i < MIN(SPARSE_MAP_SIZE,
				 cached_spa.sparse_map_length);
			     i++) {
				CHECK_TRACE("%02x ",
				    cached_spa.get_sparse_map_elem(i));
			}
			CHECK_TRACE("\n");

			cached_spa.trace_spa(
			    "Already Cached SPA", __FUNCTION__, __LINE__);

			fail_if(cache_hit,
			    "Checksum cache entry for index %d already exists",
			    indx);
		}
	}

	// Make sure the CDO header comes back with real values
	// in the sparse header
	//
	for (indx = 1; indx <= cdo_index; indx++) {
		str_map_t attr_map_h;
		int num_subchunk = 1;

		if (!compress) {
			num_subchunk = (chunk_sz + t_sz - 1) / t_sz;
		}

		// Init the spa header to known bad values
		//
		spa_header.set_spa_version(CPOOL_SPA_VERSION + 62);
		spa_header.set_sparse_map_length(SPARSE_MAP_SIZE * 62);
		spa_header.sparse_resolution = (8*62);
		for (i = 0; i < SPARSE_MAP_SIZE; i++) {
			spa_header.set_sparse_map_elem(i, (0x62 + i));
		}

		try {
			ioh1->get_cdo_headers(master_cdo, indx,
			    compress, checksum, cs_header, spa_header);
			spa_header.trace_spa(
			    "SPA after get_cdo_headers",
			    __FUNCTION__, __LINE__);
		} FAIL_IF_EXCEPTION;

		match = compare_spa("Mismatch after read CDO header",
		    expected_spa, spa_header);
		fail_if(!match, "SPA index %d mismatch", indx);
	}


	for (indx = 1; indx <= cdo_index; indx++) {
		// get with attr-to-match argument
		try {
			ioh1->get_cdo(master_cdo, indx,
			    compress, checksum, ectx, NULL,
			    &cmo_attr, attr_map_o, ostrm);
		} FAIL_IF_EXCEPTION;

		// The cache entry should exist for the CDO
		//
		cache_hit = checksum_cache_get(
		    cloud_name.container_cdo,
		    ioh1->get_physical_cdo_name(master_cdo, indx),
		    ioh1->get_cdo_physical_offset(
			master_cdo, indx, compress, checksum),
		    cached_cs, cached_spa);
		if (!cache_hit) {
			CHECK_TRACE(
			    "    Checksum cache entry "
			    "for index %d is missing\n",
			    indx);
			fail_if(!cache_hit,
			    "Checksum cache entry for index %d is missing",
			    indx);
		} else {
			match = compare_spa("Mismatchd cached SPA",
			    expected_spa, cached_spa);
			fail_if(!match, "SPA index %d mismatch", indx);
		}
	}

	const std::string &cont_d = ostrm.get_content();
	std::string cont_u((cdo_index * chunk_sz), 'd');
	if (cont_d != cont_u) {
		CHECK_TRACE("    Content mismatch \n"
			    "      Expected (%ld) %s\n"
			    "      Received (%ld) %s",
		    cont_u.size(), cont_u.c_str(),
		    cont_d.size(), cont_d.c_str());
	}
	fail_if(cont_d != cont_u,
	    "get_cdo failed - content mismatch");
	fail_if(attr_map_o[attrn1] != attrv1,
	    "get_cdo failed - attr1 mismatch %s != %s",
	    attr_map_o[attrn1].c_str(), attrv1.c_str());
	fail_if(attr_map_o[attrn2] != attrv2,
	    "get_cdo failed - attr2 mismatch %s != %s",
	    attr_map_o[attrn2].c_str(), attrv2.c_str());

	try {
		cmo_attr["SIGNATURE"] = "4321";
		ioh1->get_cdo(master_cdo, cdo_index, compress, checksum, ectx, NULL,
		    &cmo_attr, attr_map_o, ostrm);
		fail(true, "not expected to reach here");
	} catch (isi_cbm_exception &exp) {
		fail_if(exp.get_error_code() != CBM_IOH_MISMATCHED_CMO_ATTR,
		    "expect only MISMATCHED_CMO_ATTR exception error");
	}

	// test read attributes
	attr_map_o.clear();
	try {
		ioh1->get_cdo_attrs(cloud_name, cdo_index, attr_map_o);
	} FAIL_IF_EXCEPTION;
	fail_if(attr_map_o[attrn1] != attrv1, "get_cdo_attrs failed");
	fail_if(attr_map_o[attrn2] != attrv2, "get_cdo_attrs failed");

	try {
		if (!master_cdo.is_concrete()) {
			// need to delete for each index
			cdo_index = 1;
			ioh1->delete_cdo(cloud_name, cdo_index, 0);
			ioh1->delete_cdo(cloud_name, ++cdo_index, 0);
		} else {
			ioh1->delete_cdo(cloud_name, 0, 0);
		}

		ioh1->delete_cmo(cloud_name, 0);
	} FAIL_IF_EXCEPTION;

	return;
}

TEST(ran_write_and_get_cdo, .mem_check = CK_MEM_DISABLE)
{
	size_t t_sz = 4 * 1024;
	size_t chunk_sz = 1026; // small size

	CHECK_TRACE("%s: t_sz %lld, chunk_sz %lld\n",
	    __FUNCTION__, t_sz, chunk_sz);
	// no compress
	write_and_get_cdo(ioh_ran, t_sz, chunk_sz, false, false, false, NULL);
	write_and_get_cdo(ioh_ran, t_sz, chunk_sz, false,  true, false, NULL);
	// with compress
	write_and_get_cdo(ioh_ran, t_sz, chunk_sz, true,  false, false, NULL);
	write_and_get_cdo(ioh_ran, t_sz, chunk_sz, true,   true, false, NULL);
}

class short_mem_stream : public isi_cloud::idata_stream {

public:
	short_mem_stream(size_t exp, size_t act);

	virtual size_t read(void *buff, size_t len);
private:
	size_t exp_;
	size_t act_;
	size_t read_len_;
};


short_mem_stream::short_mem_stream(size_t exp, size_t act) :
    exp_(exp), act_(act), read_len_(0)
{

}

size_t
short_mem_stream::read(void *buff, size_t len)
{
	size_t to_read = MIN(len, act_ - read_len_);

	memset(buff, 'A', to_read);
	read_len_ += to_read;

	CHECK_TRACE("  %s: requested: %ld returned: %ld, "
	    "total expected: %ld, total_read: %ld \n",
	    __FUNCTION__, len, to_read, exp_, read_len_);
	return to_read;
}

// simulate the situation where available data is shorter than
// originally requested. We expect an exception here.
TEST(ran_write_short_data, .mem_check = CK_MEM_DISABLE)
{
	str_map_t attr_map;
	cl_object_name cloud_name;
	ioh_ran->generate_cloud_name(0, cloud_name);
	isi_cpool_master_cdo master_cdo;
	isi_cpool_master_cdo prev_mcdo;
	size_t chunk_sz = 1026; // small size
	short_mem_stream strm(chunk_sz, 512);

	CHECK_TRACE("%s: \n", __FUNCTION__);
	ioh_ran->get_new_master_cdo(0, prev_mcdo, cloud_name, false, false,
	    chunk_sz, (128 * 1024), (8 * 1024), attr_map, master_cdo, 0);
	int index = 1;

	// try/catch below intentionally triggers failures, disable cloud_api
	// failure logging
	override_clapi_verbose_logging_enabled(false);

	try {
		ioh_ran->start_write_master_cdo(master_cdo);

		ioh_ran->write_cdo(master_cdo, index,
		    chunk_sz, strm, true, false, false, false,
		    NULL);

		fail("Expected failure CL_ABORTED_BY_CALLBACK not met.");

		ioh_ran->end_write_master_cdo(master_cdo, false);

	} catch (cl_exception &ex) {
		fail_if(ex.get_error_code() != CL_ABORTED_BY_CALLBACK,
		    "Expected failure CL_ABORTED_BY_CALLBACK not met. Got: %s",
		    ex.what());
	}

	revert_clapi_verbose_logging();
}
