#define EGC_SKIP_CHECK_CTIME /* shutoff ctime check for now */
#include <fcntl.h>
#include <stdio.h>

#include <isi_util_cpp/scoped_lock.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include <isi_cloud_api/test_stream.h>
#include <isi_cloud_api/test_common_acct.h>
#include <isi_cpool_config/cpool_config.h>

#include <isi_ilog/ilog.h>

#include "check_cbm_common.h"
#include "isi_cpool_cbm.h"
#include "isi_cbm_file.h"
#include "isi_cbm_mapper.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_error_util.h"
#include "isi_cbm_invalidate.h"
#include "isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h"
#include "isi_cpool_cbm/io_helper/isi_cbm_ioh_creator.h"
#include "isi_cpool_cbm/io_helper/ran_io_helper.h"
#include "isi_cpool_cbm/io_helper/header_ostream.h"
#include "isi_cpool_cbm/isi_cbm_policyinfo.h"
#include "isi_cpool_cbm/isi_cbm_scoped_ppi.h"
#include "isi_cpool_cbm/isi_cbm_account_util.h"

#include <check.h>

#define CP_STRING	CBM_ACCESS_ON "cp -f %s %s" CBM_ACCESS_OFF

#define CHECK_TEMP_FILE 	"/tmp/test_cpool_write.tmp.XXXXXX"

using namespace isi_cloud;

static const int g_size_2M = 2 * 1024 * 1024;
static const char *g_input_file = "/ifs/data/check_cbm_file.unittest";
static cbm_test_file_helper *g_test_fo = NULL;

typedef struct getbuf_cb_ctx {
	ssize_t		size;
	char		*alloc_buf;
	char		*returned_buf;
} getbuf_cb_ctx_t;

static void
test_cbm_file_read_write_(
    struct test_file_info	*finfo,
    const char			*pol_name,
    struct  sparse_file_info	*spf,
    bool			expect_stubwrite_success,
    bool			*test_spa_cache);

void
read_stub_write_temp(
    const char		*tmp_file_name,
    const char		*stub_file_name,
    struct isi_cbm_file	*file_obj,
    off_t		length,
    ssize_t		buf_sz);

void
write_stub_and_compare(
    const char *	stub_file_name,
    struct isi_cbm_file	*file_obj,
    ssize_t		buf_sz);

void toss_your_cookies(void)
{
	throw cl_exception(CL_PARTIAL_FILE, "PreLeak memory from throw");
}

#ifdef ENABLE_ILOG_USE
#define ANNOUNCE_TEST_START \
	ilog(IL_DEBUG, "Entered TEST %s (line %d)",  __FUNCTION__, __LINE__);\
	CHECK_TRACE("\nEntered TEST %s (line %d)\n",  __FUNCTION__, __LINE__);
#else
#define ANNOUNCE_TEST_START \
	CHECK_TRACE("\nEntered TEST %s (line %d)\n",  __FUNCTION__, __LINE__);
#endif

TEST_FIXTURE(suite_setup)
{
#ifdef ENABLE_ILOG_USE
	struct ilog_app_init init =  {
		"check_cbm_file",		// full app name
		"cpool",			// component
		"",				// job (opt)
		"check_cbm_file",		// syslog program
		IL_TRACE_PLUS|IL_DETAILS|IL_CP_EXT,	// default level
		false,				// use syslog
		true,				// use stderr
		false,				// log_thread_id
		LOG_DAEMON,			// syslog facility
		"/ifs/logs/check_cbm_file.log",	// log file
		IL_TRACE_PLUS,			// syslog_threshold
		NULL,				//tags
	};

	char *ilogenv = getenv("CHECK_USE_ILOG");
	if (ilogenv != NULL) {
		ilog_init(&init, false, true);
		ilog(IL_NOTICE, "ilog initialized\n");
	}
#endif
	g_test_fo = new cbm_test_file_helper(g_input_file, g_size_2M,
	    "a few words in many lines go on and on ...\n");

	struct isi_error *error = NULL;

	cpool_regenerate_mek(&error);

	fail_if(error, "failed to generate mek %#{}", isi_error_fmt(error));

	cpool_generate_dek(&error);

	fail_if(error, "failed to generate dek %#{}", isi_error_fmt(error));

	cbm_test_common_cleanup(false, true);
	cbm_test_common_setup(false, true);
	fail_if(!cbm_test_cpool_ufp_init(), "Fail points failed to initiate");
	try {
		// Throw an exception before running any tests so that
		// the mutexes used by throw will be preallocated and
		// in the memory snapshot used to look for leaks.  In the
		// normal case the memory is cleaned up by image rundown
		// but we do not run down between tests here.
		toss_your_cookies();
	}
	catch (...) {
		CHECK_P("threw and caught an exception to preAllocate memory\n");
	}
	CHECK_TRACE("\nTest setup complete\n");
}

TEST_FIXTURE(suite_teardown)
{
	delete g_test_fo;
	cbm_test_common_cleanup(false, true);
}

TEST_FIXTURE(setup_test)
{
	cl_provider_module::init();
	cbm_test_enable_stub_access();
}

TEST_FIXTURE(teardown_test)
{
	cl_provider_module::destroy();

	checksum_cache_cleanup();
}

SUITE_DEFINE_FOR_FILE(check_cbm_file,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 1200,
    .fixture_timeout = 2400);


static bool test_spa_cache_hit(
    struct isi_cbm_file *file_obj, bool compress, bool checksum,
    checksum_value &cached_cs, sparse_area_header_t &cached_spa)
{
	bool			cache_hit 	= false;
	struct isi_error	*error		= NULL;
	scoped_ppi_reader	ppi_reader;
	isi_cfm_mapinfo		&mapinfo	= *(file_obj->mapinfo);
	isi_cbm_ioh_base_sptr	io_helper;
	isi_cbm_ioh_base	*ioh		= NULL;
	cl_object_name		cloud_name;
	isi_cpool_master_cdo	master_cdo;
	isi_cfm_mapentry	map_entry;
	isi_cfm_mapinfo::iterator itr;

	mapinfo.get_containing_map_iterator_for_offset(
	    itr, 0, &error);
	fail_if(error, "failed to get map iterator: %#{}",
	    isi_error_fmt(error));
	fail_if((itr == mapinfo.end()),
	    "cannot find map entry read position 0");
	map_entry = itr->second;

	io_helper = isi_cpool_get_io_helper(map_entry.get_account_id(),
	    &error);
	fail_if(((!io_helper) || (error)), "failed to get io_helper: %#{}",
	    isi_error_fmt(error));
	ioh = io_helper.get();

	master_cdo_from_map_entry(master_cdo, mapinfo, &map_entry);

	ioh->generate_cloud_name(file_obj->lin, cloud_name);

	cache_hit = checksum_cache_get(
	    cloud_name.container_cdo,
	    ioh->get_physical_cdo_name(master_cdo, 1),
	    ioh->get_cdo_physical_offset(
		master_cdo, 1, compress, checksum),
	    cached_cs, cached_spa);

	return cache_hit;
}

void
read_stub_write_temp(
    const char *	tmp_file_name,
    const char *	stub_file_name,
    struct isi_cbm_file	*file_obj,
    off_t 		length,
    ssize_t 		buf_sz)
{
	char 			*buf		= NULL;
	struct isi_error 	*error		= NULL;
	int			fd_tmp		= -1;
	uint64_t		offset		= 0;
	ssize_t			rd_sz		= 1;

	ASSERT(buf_sz > 0);
	ASSERT(length >= 0);
	buf = (char *) malloc(buf_sz);
	ASSERT(buf);

	// read stubbed file content and write to a temp file
	fd_tmp = open(tmp_file_name, O_RDWR | O_TRUNC | O_CREAT, 0600);
	fail_if(fd_tmp < 0, "cannot create temp file for write");

	while (((uint64_t)length > offset) && (rd_sz > 0)) {

		rd_sz = isi_cbm_file_read(
		    file_obj, buf, offset, buf_sz,
		    CO_DEFAULT, &error);
		fail_if(rd_sz < 0, "failed to read CBM file %s",
		    stub_file_name);
		fail_if(error, "cannot read file %s, %#{}",
		    stub_file_name, isi_error_fmt(error));

		if (rd_sz >= 0) {
			if ((buf_sz - rd_sz != 0) &&
			    (length != (off_t)offset + rd_sz)) {
			     fail("failed to read CBM file %s", stub_file_name);
			}
			offset += rd_sz;
			write(fd_tmp, buf, rd_sz);
			//printf("%s\n\n", (char *) buf);
		}
	}
	close(fd_tmp);

	if (buf) {
		free(buf);
	}
}

void
write_stub_and_compare(
    const char *	stub_file_name,
    struct isi_cbm_file	*file_obj,
    ssize_t 		buf_sz)
{
	struct isi_error 	*error		= NULL;
	unsigned char 		*wrt_buf	= NULL;
	unsigned char 		*rd_buf		= NULL;
	ssize_t			indx		= 0;
	ssize_t			wrt_sz		= 0;
	ssize_t			rd_sz		= -1;

	ASSERT(buf_sz > 0);
	wrt_buf = (unsigned char *) malloc(buf_sz);
	ASSERT(wrt_buf);
	rd_buf = (unsigned char *) malloc(buf_sz);
	ASSERT(rd_buf);

	for (indx = 0; indx < buf_sz; indx++) {
		wrt_buf[indx] = indx % 256;
	}

	//
	// Write to the stub
	//
	wrt_sz = isi_cbm_file_write(file_obj, wrt_buf, 0, buf_sz, &error);
	fail_if((wrt_sz - buf_sz) != 0,
	    "failed to write cbm file %s", stub_file_name);

	//
	// Now read it back and compare the buffers
	//
	rd_sz = isi_cbm_file_read(
	    file_obj, rd_buf, 0, buf_sz, CO_DEFAULT, &error);
	fail_if(rd_sz < 0,
	    "failed to read CBM file %s", stub_file_name);
	fail_if(error,
	    "cannot read file %s, %#{}", stub_file_name, isi_error_fmt(error));

	if (buf_sz - rd_sz != 0) {
	     fail("failed to read CBM file %s", stub_file_name);
	}

	for (indx = 0; indx < buf_sz; indx++) {
		if (wrt_buf[indx] != rd_buf[indx]) {
			fail("Comparison failure after stub write: "
			     "indx %lld is 0x%x but should be 0x%x",
			    indx, rd_buf[indx], wrt_buf[indx]);
		}
	}

	if (wrt_buf) {
		free(wrt_buf);
	}
	if (rd_buf) {
		free(rd_buf);
	}
}

static void
test_cbm_file_read_write_(
    struct test_file_info	*finfo,
    const char			*pol_name,
    struct  sparse_file_info	*spf,
    bool			expect_stubwrite_success,
    bool			*test_spa_cache)
{
	const size_t		BUF_SIZE	= 10000;
	struct isi_error	*error		= NULL;
	struct isi_cbm_file	*file_obj	= NULL;
	size_t			buf_sz		= BUF_SIZE;
	ifs_lin_t		lin;
	int			status		= -1;
	struct stat		statbuf;
	struct stat		prebuf;
	struct stat		postbuf;
	char			orig_file[256];

	struct fmt FMT_INIT_CLEAN(str);

	if (spf) {
		snprintf(orig_file, 256, "%s_sparse_src", finfo->output_file);
		CHECK_TRACE("Test Buffered IO\n"
			    "     sparse: %s - %s\n"
			    "     output: %s\n"
			    "     policy: %s\n",
		    orig_file, spf->spf_name, finfo->output_file, pol_name);

		unlink(orig_file);
		unlink(finfo->output_file);
		cbm_test_create_sparse_file(orig_file,
		    spf->filesize, spf->num_segments, spf->segments, NULL);
		cbm_test_create_sparse_file(finfo->output_file,
		    spf->filesize, spf->num_segments, spf->segments, NULL);
	} else {
		strncpy(orig_file, finfo->input_file, 256);
		CHECK_TRACE("Test Buffered IO\n"
			    "     input:  %s\n"
			    "     output: %s\n"
			    "     policy: %s\n",
		    orig_file, finfo->output_file, pol_name);

		fmt_print(&str, CP_STRING, finfo->input_file, finfo->output_file);
		CHECK_P("\n%s\n", fmt_string(&str));
		status = system(fmt_string(&str));
		fail_if(status, "Could not create test file: %s", fmt_string(&str));
	}


	status = stat(finfo->output_file, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    finfo->output_file, strerror(errno), errno);
	lin = statbuf.st_ino;

	error = cbm_test_archive_file_with_pol(lin, pol_name, true);
	if (error) {
		fail_if(!isi_system_error_is_a(error, EALREADY),
		    "test stubbed %s not created, lin %#{} got %#{}",
		    finfo->output_file, lin_fmt(lin),
		    isi_error_fmt(error));

		isi_error_free(error);
		error = NULL;
	}

	// Grab stats post stub for time comparison
	status = stat(finfo->output_file, &prebuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    finfo->output_file, strerror(errno), errno);

	// CBM file open
	file_obj = isi_cbm_file_open(statbuf.st_ino, HEAD_SNAPID, 0, &error);
	fail_if(!file_obj, "cannot open file %s, %#{}", finfo->output_file,
	    isi_error_fmt(error));

	// test read
	buf_sz = statbuf.st_size / 3;
	if (buf_sz < BUF_SIZE) {
		buf_sz = BUF_SIZE;
	}

	read_stub_write_temp(
	    CHECK_TEMP_FILE, finfo->output_file,
	    file_obj, statbuf.st_size, buf_sz);

	fail_if(file_compare(orig_file, CHECK_TEMP_FILE),
	    "stubbed file read result doesn't match original file content");

	if (spf) {
		fail_if(!compare_maps_to_spf(spf, orig_file, true),
		    "Sparse maps do not compare for original file %s",
		    orig_file);
		fail_if(!compare_maps_to_spf(
		    spf, finfo->output_file, expect_stubwrite_success),
		    "Sparse maps do not compare for stub file %s",
		    finfo->output_file);
	}

	status = stat(finfo->output_file, &postbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    finfo->output_file, strerror(errno), errno);

	fail_if((postbuf.st_mtimespec.tv_sec != prebuf.st_mtimespec.tv_sec) ||
	    (postbuf.st_mtimespec.tv_nsec != prebuf.st_mtimespec.tv_nsec),
	    "read should not change mtime of the file %s (%{}) "
	    "before: (%ld,%ld), after: (%ld,%ld)", finfo->output_file,
	    lin_fmt(statbuf.st_ino),
	    prebuf.st_mtimespec.tv_sec, prebuf.st_mtimespec.tv_nsec,
	    postbuf.st_mtimespec.tv_sec, postbuf.st_mtimespec.tv_nsec);

#ifndef EGC_SKIP_CHECK_CTIME
	/* NB: CTIME can change of ADS updates - creating of the cache still */
	fail_if((postbuf.st_ctimespec.tv_sec != prebuf.st_ctimespec.tv_sec) ||
	    (postbuf.st_ctimespec.tv_nsec != prebuf.st_ctimespec.tv_nsec),
	    "read should not change ctime of the file %s (%{}) "
	    "before: (%ld,%ld), after: (%ld,%ld)", finfo->output_file,
	    lin_fmt(statbuf.st_ino),
	    prebuf.st_ctimespec.tv_sec, prebuf.st_ctimespec.tv_nsec,
	    postbuf.st_ctimespec.tv_sec, postbuf.st_ctimespec.tv_nsec);
#endif

	if (expect_stubwrite_success) {
		// test write
		write_stub_and_compare(
		    finfo->output_file, file_obj, buf_sz);
	}

	// Test the state of the spa cache if requested
	if (test_spa_cache) {
		checksum_value		cached_cs;
		sparse_area_header_t	cached_spa;
		bool 			cache_hit = false;
		cache_hit = test_spa_cache_hit(
		    file_obj, false, false, cached_cs, cached_spa);
		if ((cache_hit != *test_spa_cache) && (cache_hit)) {
			CHECK_TRACE("  Unexpected SPA cache found: "
			    "version %04d, map length %04d, "
			    "resolution %08d map: 0x",
			    cached_spa.get_spa_version(),
			    cached_spa.get_sparse_map_length(),
			    cached_spa.get_sparse_resolution());
			for (int i = 0;
			     i < MIN(SPARSE_MAP_SIZE,
				 cached_spa.sparse_map_length);
			     i++) {
				CHECK_TRACE("%02x ",
				    cached_spa.get_sparse_map_elem(i));
			}
			CHECK_TRACE("\n");

			cached_spa.trace_spa(
			    "Unexpected SPA cache found", __FUNCTION__, __LINE__);
		}
		fail_if((cache_hit != *test_spa_cache),
		    "Unexpected SPA cache state: expected %s, actual %s",
		    (*test_spa_cache) ? "entry found" : "entry NOT found",
		    (cache_hit) ? "entry found" : "entry NOT found");
	}

	isi_cbm_file_close(file_obj, &error);

	// remove files
	unlink(finfo->output_file);
	if (spf) {
		unlink(orig_file);
	}
	unlink(CHECK_TEMP_FILE);

}

// ---------- following are RAN tests ---------------------
TEST(test_cbm_file_read_write,  mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct test_file_info test_file = {
	    "/etc/services",
	    "/ifs/data/ran_unit_test.txt",
	    REPLACE_FILE
	};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN, NULL, true, NULL);

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN, &sparse_1M, true, NULL);
}

TEST(test_cbm_file_readahead,  mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct isi_error *error = NULL;
	struct isi_cbm_file *file_ufp1 = NULL;
	cbm_test_file_helper test_file_ufp1(
	    "/ifs/test_queue_cache_ufp1.txt",1024,"");
	struct test_file_info test_file_ufp2 = {
	    "/etc/services",
	    "/ifs/data/ran_unit_test_ra.txt",
	    REPLACE_FILE
	};

	cbm_test_fail_point ufp1("isi_cbm_file_queue_full_cache_fail");

	error = cbm_test_archive_file_with_pol(test_file_ufp1.get_lin(),
	    CBM_TEST_COMMON_POLICY_RAN, false);
	fail_if(error, "failed to archive %s: %#{}",
	    test_file_ufp1.get_filename(), isi_error_fmt(error));

	file_ufp1 = isi_cbm_file_open(
	    test_file_ufp1.get_lin(), HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed cbm open for %s : %#{}",
	    test_file_ufp1.get_filename(), isi_error_fmt(error));
	fail_if(file_ufp1 == NULL);

	ufp1.set("return(1)");

	//test that the failpoint in isi_cbm_file_queue_full_cache works
	isi_cbm_file_queue_full_cache(file_ufp1,&error);
	if (error) {
		fail_if(!isi_system_error_is_a(error,EBUSY),
		    "Expected EBUSY, got: %#{}", isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;
	} else {
		fail("Recieved success when expected EBUSY error");
	}

	isi_cbm_file_close(file_ufp1, &error);
	file_ufp1 = NULL;

	isi_error_free(error);

	// Now test the read ahead with the fail point
	test_cbm_file_read_write_(
	    &test_file_ufp2, CBM_TEST_COMMON_POLICY_RAN_FULL_RA,
	    NULL, true, NULL);

	ufp1.set("off");

}

TEST(test_cbm_file_read_write_0byte_file, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	int ret;
	struct test_file_info test_file = {
	    "/ifs/data/0byte_src",
	    "/ifs/data/ran_unit_test_0byte.txt",
	    REPLACE_FILE
	};

	// create src file
	ret = open(test_file.input_file, O_RDWR|O_CREAT|O_TRUNC, 0666);
	fail_if(ret == -1, "Failed to create %s", test_file.input_file);

	// test
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN, NULL, true, NULL);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN, &sparse_all, true, NULL);

	// remove src file
	unlink(test_file.input_file);
	close(ret);
}

TEST(test_cbm_file_read_write_with_compression,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct test_file_info test_file = {
	    "/etc/services",
	    "/ifs/data/ran_unit_test_compress.txt",
	    REPLACE_FILE
	};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN_C, NULL, true, NULL);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN_C, &sparse_1Ma, true, NULL);
}

TEST(test_cbm_file_read_write_with_encryption,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	ANNOUNCE_TEST_START;
	struct test_file_info test_file = {
	    "/etc/services",
	    "/ifs/data/ran_unit_test_encrypt.txt",
	    REPLACE_FILE
	};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN_E, NULL, true, NULL);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN_E, &sparse_all, true, NULL);
}

TEST(test_cbm_file_read_write_with_compression_encryption,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	ANNOUNCE_TEST_START;
	struct test_file_info test_file = {
	    "/etc/services",
	    "/ifs/data/ran_unit_test_compress_encrypt.txt",
	    REPLACE_FILE
	};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN_BOTH, NULL, true, NULL);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN_BOTH,
	    &sparse_alt_odd, true, NULL);
}

TEST(test_cbm_file_read_write_bigfile,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	bool expect_cache_hit = true;
	struct test_file_info test_file = {
	    g_input_file,
	    "/ifs/data/ran_unit_test.txt-1",
	    REPLACE_FILE};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN,
	    NULL, true, &expect_cache_hit);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN,
	    &sparse_alt_odd_byte_2m, true, &expect_cache_hit);
}

TEST(test_cbm_file_read_write_bigfile_with_compress,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct test_file_info test_file = {
	    g_input_file,
	    "/ifs/data/ran_unit_test_compress.txt-1",
	    REPLACE_FILE};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN_C, NULL, true, NULL);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN_C,
	    &sparse_short, true, NULL);

}

TEST(test_cbm_file_read_write_bigfile_with_encrypt,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct test_file_info test_file = {
	    g_input_file,
	    "/ifs/data/ran_unit_test_encrypt.txt-1",
	    REPLACE_FILE};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN_E, NULL, true, NULL);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN_E,
	    &sparse_alt_odd_byte, true, NULL);
}

TEST(test_cbm_file_read_write_bigfile_with_compress_encrypt,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct test_file_info test_file = {
	    g_input_file,
	    "/ifs/data/ran_unit_test_compress_encrypt.txt-1",
	    REPLACE_FILE};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN_BOTH, NULL, true, NULL);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN_BOTH,
	    &sparse_alt_even_byte, true, NULL);
}

TEST(test_cbm_file_read_write_bigfile_ufp,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	isi_error *error = NULL;
	bool expect_cache_hit = false;
	struct test_file_info test_file = {
	    g_input_file,
	    "/ifs/data/ran_unit_test_ufp.txt-1",
	    REPLACE_FILE};

	cbm_test_fail_point ufp("cbm_ioh_get_retryable_exception");
	cbm_test_fail_point sparse_ufp1("cbm_ioh_get_object_nocacheput");
	cbm_test_fail_point sparse_ufp2("cbm_ioh_get_obj_head_nocacheput");

	ufp.set("return(5)"); // Fail every 5th cloud read

	try {
		test_cbm_file_read_write_(
		    &test_file, CBM_TEST_COMMON_POLICY_RAN, NULL, true, NULL);
	}
	CATCH_CLAPI_EXCEPTION(NULL, error, false)

	// The exception should be caught before here.
	fail_if(error, "Error returned through exception: %#{}.\n",
	    isi_error_fmt(error));

	ufp.set("off");

	//
	// Test a sparse file with the checksum/sparse map
	// cache turned off
	//
	sparse_ufp1.set("return(1)");
	sparse_ufp2.set("return(1)");

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_RAN,
	    &sparse_alt_even_byte_2m, true, &expect_cache_hit);

	sparse_ufp2.set("off");
	sparse_ufp1.set("off");


}

TEST(test_cbm_file_read_write_bigfile_ufp_with_compress,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	isi_error *error = NULL;
	struct test_file_info test_file = {
	    g_input_file,
	    "/ifs/data/ran_unit_test_compress_ufp.txt-1",
	    REPLACE_FILE};

	cbm_test_fail_point ufp("cbm_ioh_get_retryable_exception");
	ufp.set("return(3)"); // Fail every 3rd cloud read

	try {
		test_cbm_file_read_write_(
		    &test_file, CBM_TEST_COMMON_POLICY_RAN_C,
		    NULL, true, NULL);
	}
	CATCH_CLAPI_EXCEPTION(NULL, error, false)

	// The exception should be caught before here.
	fail_if(error, "Error returned through exception: %#{}.\n",
	    isi_error_fmt(error));

	ufp.set("off");
}

TEST(test_cbm_file_read_write_bigfile_ufp_with_encrypt,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	isi_error *error = NULL;
	struct test_file_info test_file = {
	    g_input_file,
	    "/ifs/data/ran_unit_test_encrypt_ufp.txt-1",
	    REPLACE_FILE};

	cbm_test_fail_point ufp("cbm_ioh_get_retryable_exception");
	ufp.set("return(3)"); // Fail every 3rd cloud read

	try {
		test_cbm_file_read_write_(
		    &test_file, CBM_TEST_COMMON_POLICY_RAN_E,
		    NULL, true, NULL);
	}
	CATCH_CLAPI_EXCEPTION(NULL, error, false)

	// The exception should be caught before here.
	fail_if(error, "Error returned through exception: %#{}.\n",
	    isi_error_fmt(error));

	ufp.set("off");
}

TEST(test_cbm_file_read_write_bigfile_ufp_with_compress_encrypt,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	isi_error *error = NULL;
	struct test_file_info test_file = {
	    g_input_file,
	    "/ifs/data/ran_unit_test_compress_encrypt_ufp.txt-1",
	    REPLACE_FILE};

	cbm_test_fail_point ufp("cbm_ioh_get_retryable_exception");
	ufp.set("return(3)");  // Fail every 3rd cloud read

	try {
		test_cbm_file_read_write_(
		    &test_file, CBM_TEST_COMMON_POLICY_RAN_BOTH,
		    NULL, true, NULL);
	}
	CATCH_CLAPI_EXCEPTION(NULL, error, false)

	// The exception should be caught before here.
	fail_if(error, "Error returned through exception: %#{}.\n",
	    isi_error_fmt(error));

	ufp.set("off");
}

// ----------------- following are azure tests -------------------
TEST(test_cbm_file_read_write_azure,  mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct test_file_info test_file = {
	    "/etc/services",
	    "/ifs/data/ran_unit_test_azure.txt",
	    REPLACE_FILE
	};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ, NULL, true, NULL);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ,
	    &sparse_alt_even_byte, true, NULL);
}

TEST(test_cbm_file_read_write_0byte_file_azure, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	int ret;
	struct test_file_info test_file = {
	    "/ifs/data/0byte_src",
	    "/ifs/data/ran_unit_test_0byte_azure.txt",
	    REPLACE_FILE
	};

	// create src file
	ret = open(test_file.input_file, O_RDWR|O_CREAT|O_TRUNC, 0666);
	fail_if(ret == -1, "Failed to create %s", test_file.input_file);

	// test
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ, NULL, true, NULL);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ, &sparse_none, true, NULL);

	// remove src file
	unlink(test_file.input_file);
	close(ret);
}

TEST(test_cbm_file_read_write_with_compression_azure,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct test_file_info test_file = {
	    "/etc/services",
	    "/ifs/data/ran_unit_test_compress_azure.txt",
	    REPLACE_FILE
	};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ_C, NULL, true, NULL);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ_C,
	    &sparse_alt_odd_byte, true, NULL);
}

TEST(test_cbm_file_read_write_with_encryption_azure,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	ANNOUNCE_TEST_START;
	struct test_file_info test_file = {
	    "/etc/services",
	    "/ifs/data/ran_unit_test_encrypt_azure.txt",
	    REPLACE_FILE
	};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ_E, NULL, true, NULL);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ_E, &sparse_1M, true, NULL);
}

TEST(test_cbm_file_read_write_with_compression_encryption_azure,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	ANNOUNCE_TEST_START;
	struct test_file_info test_file = {
	    "/etc/services",
	    "/ifs/data/ran_unit_test_compress_encrypt_azure.txt",
	    REPLACE_FILE
	};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ_BOTH, NULL, true, NULL);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ_BOTH,
	    &sparse_1Ma, true, NULL);
}

TEST(test_cbm_file_read_write_bigfile_azure,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	bool expect_cache_hit = true;
	struct test_file_info test_file = {
	    g_input_file,
	    "/ifs/data/ran_unit_test_azure.txt-1",
	    REPLACE_FILE};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ,
	    NULL, true, &expect_cache_hit);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ,
	    &sparse_alt_even_byte_2m, true, &expect_cache_hit);
}

TEST(test_cbm_file_read_write_bigfile_with_compress_azure,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct test_file_info test_file = {
	    g_input_file,
	    "/ifs/data/ran_unit_test_compress_azure.txt-1",
	    REPLACE_FILE};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ_C, NULL, true, NULL);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ_C,
	    &sparse_alt_even, true, NULL);
}

TEST(test_cbm_file_read_write_bigfile_with_encrypt_azure,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct test_file_info test_file = {
	    g_input_file,
	    "/ifs/data/ran_unit_test_encrypt_azure.txt-1",
	    REPLACE_FILE};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ_E, NULL, true, NULL);
	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ_E,
	    &sparse_short, true, NULL);
}

TEST(test_cbm_file_read_write_bigfile_with_compress_encrypt_azure,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct test_file_info test_file = {
	    g_input_file,
	    "/ifs/data/ran_unit_test_compress_encrypt_azure.txt-1",
	    REPLACE_FILE};

	test_cbm_file_read_write_(
	    &test_file, CBM_TEST_COMMON_POLICY_AZ_BOTH, NULL, true, NULL);
}

/**
 * Test a case recover from the unwritten cache header
 */
TEST(test_failed_cache_header_write)
{
	ANNOUNCE_TEST_START;
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_failed_cache_header_write.txt";

	const char *buffer = "This is a test file";
	size_t len = strlen(buffer);

	cbm_test_file_helper test_fo(path, len, buffer);
	ifs_lin_t lin = test_fo.get_lin();

	error = cbm_test_archive_file_with_pol(lin,
	    CBM_TEST_COMMON_POLICY_RAN, false);

	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file =
		isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	cbm_test_fail_point ufp("cpool_no_cache_header");
	ufp.set("return(5)");

	char out_buf[256];
	memset(out_buf, 0, sizeof(out_buf));
	size_t outlen = isi_cbm_file_read(file, out_buf, 0, sizeof(out_buf),
	    CO_CACHE, &error);
	fail_if(!error || !isi_system_error_is_a(error, EIO),
	    "Expected fail point cpool_no_cache_header not met: error: %#{}.\n",
	    isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	ufp.set("off");
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	outlen = isi_cbm_file_read(file, out_buf, 0, sizeof(out_buf),
	    CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));
	fail_if(outlen != len, "The read length is not matched %d %d.",
	    len, outlen);

	fail_if(strncmp(buffer, out_buf, len),
	   "The data is not matched after stubbing.");

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

}

/**
 * Test reading past EOF
 */
TEST(test_read_past_EOF)
{
	ANNOUNCE_TEST_START;
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_read_past_EOF.txt";

	const char *buffer = "This is a test file";
	size_t len = (1024 * 100);
	char read_buff[1024];
	size_t readlen;

	cbm_test_file_helper test_fo(path, len, buffer);
	ifs_lin_t lin = test_fo.get_lin();

	error = cbm_test_archive_file_with_pol(lin,
	    CBM_TEST_COMMON_POLICY_RAN, false);

	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file =
		isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	// The file is currently NOT cached, we just archived.
	// Start read after EOF
	// We should get no error and 0 bytes returned
	memset(read_buff, 0, sizeof(read_buff));
	readlen = isi_cbm_file_read(file, read_buff,
	    (len + 100), sizeof(read_buff),
	    CO_CACHE, &error);
	fail_if(error,
	    "NOT Cached: Expected no error and max %d bytes read:"
	    "got %d bytes and error: %#{}.\n", 0, readlen,
	    isi_error_fmt(error));
	fail_if((readlen > 0),
	    "NOT Cached: Expected no error and max %d bytes read:"
	    "got %d bytes.\n", 0, readlen);
	isi_error_free(error);
	error = NULL;


	// Start read before EOF and end after EOF
	// We should successfully read up to EOF
	memset(read_buff, 0, sizeof(read_buff));
	readlen = isi_cbm_file_read(file, read_buff,
	    (len - (sizeof(read_buff)/2)), sizeof(read_buff),
	    CO_CACHE, &error);
	fail_if(error,
	    "NOT Cached: Expected no error and max %d bytes read:"
	    "got %d bytes and error: %#{}.\n", (sizeof(read_buff)/2), readlen,
	    isi_error_fmt(error));
	fail_if((readlen > (sizeof(read_buff)/2)),
	    "NOT Cached: Expected no error and max %d bytes read:"
	    "got %d bytes.\n", (sizeof(read_buff)/2), readlen);
	isi_error_free(error);
	error = NULL;

	// We read from the only region in the file, so it is now cached.
	// Repeat both tests.
	// Start read after EOF
	// We should get an error
	memset(read_buff, 0, sizeof(read_buff));
	readlen = isi_cbm_file_read(file, read_buff,
	    (len + 100), sizeof(read_buff),
	    CO_CACHE, &error);
	fail_if(error,
	    "Cached: Expected no error and max %d bytes read:"
	    "got %d bytes and error: %#{}.\n", 0, readlen,
	    isi_error_fmt(error));
	fail_if((readlen > 0),
	    "Cached: Expected no error and max %d bytes read:"
	    "got %d bytes.\n", 0, readlen);
	isi_error_free(error);
	error = NULL;

	// Start read before EOF and end after EOF
	// We should successfully read up to EOF
	memset(read_buff, 0, sizeof(read_buff));
	readlen = isi_cbm_file_read(file, read_buff,
	    (len - (sizeof(read_buff)/2)), sizeof(read_buff),
	    CO_CACHE, &error);
	fail_if(error,
	    "Cached: Expected no error and max %d bytes read:"
	    "got %d bytes and error: %#{}.\n", (sizeof(read_buff)/2), readlen,
	    isi_error_fmt(error));
	fail_if((readlen > (sizeof(read_buff)/2)),
	    "Cached: Expected no error and max %d bytes read:"
	    "got %d bytes.\n", (sizeof(read_buff)/2), readlen);
	isi_error_free(error);
	error = NULL;


	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));


}

void do_archive_read_recall(bool expect_error)
{
	struct isi_error *error = NULL;

	const char *fpath = "/ifs/test_retryable_file.txt";
	cbm_test_file_helper test_file(fpath, 1024 * 1024, "");
	char buffer[1024];

	error = cbm_test_archive_file_with_pol(test_file.get_lin(),
	    CBM_TEST_COMMON_POLICY_RAN, false);
	fail_if(error, "error not expected: %#{}", isi_error_fmt(error));

	struct isi_cbm_file *file = isi_cbm_file_open(
	    test_file.get_lin(), HEAD_SNAPID, 0, &error);
	fail_if(error, "error not expected: %#{}", isi_error_fmt(error));


	isi_cbm_file_read(file, buffer, 0, sizeof(buffer), CO_NOCACHE, &error);
	if (expect_error && !error)
		fail("expect error");
	if (!expect_error && error)
		fail("unexpect error: %#{}", isi_error_fmt(error));

	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	isi_cbm_recall(test_file.get_lin(),
	    HEAD_SNAPID, false, NULL, NULL, NULL, NULL, &error);
	if (expect_error && !error)
		fail("expect error");
	if (!expect_error && error)
		fail("unexpect error: %#{}", isi_error_fmt(error));

	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	isi_cbm_file_close(file, &error);
	fail_if(error, "unexpected error: %#{}", isi_error_fmt(error));
}

TEST(test_retryable_read)
{
	ANNOUNCE_TEST_START;
	cbm_test_fail_point ufp("get_object_fail");
	do_archive_read_recall(false /*no error expected*/);

	ufp.set("return(1)");
	do_archive_read_recall(false /*no error expected*/);

	ufp.set("return(4)");
	do_archive_read_recall(false /*no error expected*/);

	ufp.set("return(5)");
	do_archive_read_recall(true /*error expected*/);

	ufp.set("off");
}

//test that the failpoint in isi_cbm_file_queue_full_cache works
TEST(test_queue_failpoint)
{
	ANNOUNCE_TEST_START;
	struct isi_error *error = NULL;
	struct isi_cbm_file *file = NULL;
	cbm_test_file_helper test_file("/ifs/test_queue_cache.txt",1024,"");
	cbm_test_fail_point ufp("isi_cbm_file_queue_full_cache_fail");

	error = cbm_test_archive_file_with_pol(test_file.get_lin(),
	    CBM_TEST_COMMON_POLICY_RAN, false);
	ON_ISI_ERROR_GOTO(out,error);

	file = isi_cbm_file_open(test_file.get_lin(), HEAD_SNAPID, 0, &error);
	ON_ISI_ERROR_GOTO(out,error);

	ufp.set("return(1)");

	isi_cbm_file_queue_full_cache(file,&error);
	if (error) {
		fail_if(!isi_system_error_is_a(error,EBUSY),
		    "Expected EBUSY, got: %#{}", isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;
	} else
		fail("Did not recieve expected error");

	ufp.set("off");

out:
	if(file)
		isi_cbm_file_close(file, &error);

	fail_if(error, "unexpected error: %#{}", isi_error_fmt(error));

	isi_error_free(error);
}

enum test_in_progress_op
{
	TIP_TRUNCATE = 1,
	TIP_INVALIDATE = 2
};

struct test_in_progress_failpoint
{
	const char *failpoint_name;
	off_t length_failpoint;
	off_t length_final;
};

static void
read_buf(isi_cbm_file *file, char *buf, off_t length, uint64_t offset)
{
	struct isi_error *error = NULL;
	ssize_t read_bytes;

	read_bytes = isi_cbm_file_read(file, buf, offset, length,
	    CO_DEFAULT, &error);
	fail_if(error, "Error reading file: %#{}", isi_error_fmt(error));
	if (read_bytes != length) {
		// zero-fill the remaining
		memset(buf + read_bytes, 0, length - read_bytes);
	}
}

static void
file_cmp(isi_cbm_file *file1, isi_cbm_file *file2, off_t cmp_length,
    const char *failpoint_name)
{
	off_t buflen = 1024;
	char file1_buf[buflen], file2_buf[buflen];
	uint64_t offset = 0, length = 0;
	int different;

	while (cmp_length) {
		length = MIN(buflen, cmp_length);
		read_buf(file1, file1_buf, length, offset);
		read_buf(file2, file2_buf, length, offset);

		different = memcmp(file1_buf, file2_buf, length);
		fail_if(different, "Files %{} and %{} different somewhere in "
		    "the range (%ld, %ld) for failpoint %s",
		    isi_cbm_file_fmt(file1), isi_cbm_file_fmt(file2),
		    offset, offset + length, failpoint_name);

		cmp_length -= length;
		offset += length;
	}
}

static void
validate_op(test_in_progress_op op,
    isi_cbm_file *pre_file, isi_cbm_file *post_file,
    bool in_progress, off_t expect_post_length, const char *failpoint_name)
{
	struct isi_error *error = NULL;
	off_t pre_length, post_length;
	bool invalidating, truncating, ch_locked;

	pre_length = isi_cbm_file_get_filesize(pre_file, &error);
	fail_if(error, "Error getting pre_file size: %#{}",
	    isi_error_fmt(error));
	post_length = isi_cbm_file_get_filesize(post_file, &error);
	fail_if(error, "Error getting post_file size: %#{}",
	    isi_error_fmt(error));
	fail_if(expect_post_length != post_length, "Found file size %ld does "
	    "not match expected file size %d", post_length, expect_post_length);

	// Ensure the op state is correctly set. Cacheheader lock
	// is taken and in progress truncate/invalidate  ops are completed
	// by this routine on writable files.  The lock continues
	// to be held until we can test the cacheinfo state.
	ch_locked = isi_cbm_file_open_and_or_init_cache(post_file, true, true,
	    &error); // init the cache so that we can read its state
	fail_if(error, "Error opening and/or initing cache: %#{}",
	    isi_error_fmt(error));
	if (op == TIP_INVALIDATE) {
		invalidating = post_file->cacheinfo->test_state(
		    ISI_CBM_CACHE_FLAG_INVALIDATE, &error);
		fail_if(error, "Error testing invalidation state: %#{}",
		    isi_error_fmt(error));
		if (in_progress)
			fail_if(!invalidating, "%{} not set to "
			    "invalidating", isi_cbm_file_fmt(post_file));
		else
			fail_if(invalidating, "%{} set to "
			    "invalidating", isi_cbm_file_fmt(post_file));
		fail_if(pre_length != post_length, "File %{} changed its size "
		    "from %ld to %ld while invalidating",
		    isi_cbm_file_fmt(post_file), pre_length, post_length);
	} else {	// truncating
		truncating = post_file->cacheinfo->test_state(
		    ISI_CBM_CACHE_FLAG_TRUNCATE, &error);
		fail_if(error, "Error testing truncation state: %#{}",
		    isi_error_fmt(error));
		if (in_progress)
			fail_if(!truncating, "%{} not set to truncating",
			    isi_cbm_file_fmt(post_file));
		else
			fail_if(truncating, "%{} set to truncating",
			    isi_cbm_file_fmt(post_file));
	}
	if (ch_locked) {
		post_file->cacheinfo->cacheheader_unlock(&error);
		fail_if(error, "Error unlocking cache header: %#{}",
		    isi_error_fmt(error));
	}

	// Compare the files
	file_cmp(pre_file, post_file, post_length, failpoint_name);

}

void
test_snapped_in_progress(test_in_progress_op op,
    const test_in_progress_failpoint & tfp,
    size_t length, off_t trunc_length)
{
	struct isi_error *error = NULL;
	std::string fpath = "/ifs/";
	ifs_snapid_t s1, s2;
	bool want_system = false;
	const char *failpoint_name = tfp.failpoint_name;

	fpath += failpoint_name;
	// create stub file with 'num_regions' cache regions
	cbm_test_file_helper test_file(fpath.c_str(), length, "A");
	error = cbm_test_archive_file_with_pol(test_file.get_lin(),
	    CBM_TEST_COMMON_POLICY_RAN, false);
	fail_if(error, "Error archiving test file: %#{}", isi_error_fmt(error));

	isi_cbm_file *file = isi_cbm_file_open(test_file.get_lin(), HEAD_SNAPID,
	    0, &error);
	fail_if(error, "Error opening test file: %#{}", isi_error_fmt(error));

	// snap the file prior to the operation
	std::string name_s1 = "pre_";
	name_s1 += failpoint_name;
	remove_snapshot(name_s1);
	s1 = take_snapshot("/ifs", name_s1, want_system, &error);
	fail_if(error, "taking snap %s", name_s1.c_str());

	// set fail point so that operation will fail
	cbm_test_fail_point ufp(failpoint_name);
	ufp.set("return(1)");

	// operate on the file and ensure it fails
	if (op == TIP_TRUNCATE) {
		isi_cbm_file_ftruncate(file, trunc_length, &error);
	} else { // op == TIP_INVALIDATE
		// read so that a cache gets created; needed so
		// that 'process_header' is set
		char buf;
		read_buf(file, &buf, 1, 0);

		// now invalidate
		bool dirty = false;
		isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty,
		    &error);
	}
	fail_if(error == NULL, "expected failpoint %s error, none occured",
	    failpoint_name);
	isi_error_free(error);
	error = NULL;
	ufp.set("off");

	// snap the file after the operation
	std::string name_s2 = "post_";
	name_s2 += failpoint_name;
	remove_snapshot(name_s2);
	s2 = take_snapshot("/ifs", name_s2, want_system, &error);
	fail_if(error, "taking snap %s", name_s2.c_str());

	// validation
	isi_cbm_file *file_s1 = isi_cbm_file_open(
	    test_file.get_lin(), s1, 0, &error);
	fail_if(error, "Error opening snap1 file: %#{}", isi_error_fmt(error));
	isi_cbm_file *file_s2 = isi_cbm_file_open(
	    test_file.get_lin(), s2, 0, &error);
	fail_if(error, "Error opening snap2 file: %#{}", isi_error_fmt(error));
	bool in_progress = true;
	validate_op(op, file_s1, file_s2, in_progress,
	    tfp.length_failpoint, failpoint_name); // on snap
	in_progress = false;
	validate_op(op, file_s1, file, in_progress,
	    tfp.length_final, failpoint_name); // on head

	// cleanup
	isi_cbm_file_close(file_s2, &error);
	fail_if(error, "Error closing snap2 file: %#{}", isi_error_fmt(error));
	isi_cbm_file_close(file_s1, &error);
	fail_if(error, "Error closing snap1 file: %#{}", isi_error_fmt(error));
	isi_cbm_file_close(file, &error);
	fail_if(error, "Error closing head file: %#{}", isi_error_fmt(error));
	remove_snapshot(name_s1);
	remove_snapshot(name_s2);
}

TEST(test_snapped_in_progress_truncate_down)
{
	ANNOUNCE_TEST_START;
	int num_regions = 3;
	ASSERT(num_regions > 1);
	size_t length = ISI_CBM_CACHE_DEFAULT_REGIONSIZE*num_regions;
	off_t trunc_length = ISI_CBM_CACHE_DEFAULT_REGIONSIZE*(num_regions - 2) +
	    ISI_CBM_CACHE_DEFAULT_REGIONSIZE/2; // within 2nd last region
	test_in_progress_failpoint failpoints[] = {
	    {"cbm_cache_trunc_ext_state_eio", trunc_length, trunc_length},
	    {"cbm_cache_trunc_trunc_cache1_eio", trunc_length, trunc_length},
	    {"cbm_cache_trunc_trunc_cache2_eio", trunc_length, trunc_length},
	    {"cpool_read_short_cacheinfo_1", trunc_length, trunc_length},
	    {"cpool_write_short_cacheinfo_1", trunc_length, trunc_length},
	    {"cbm_file_ftruncate_fail_2", trunc_length, trunc_length},
	    {"cbm_file_ftruncate_fail_3", trunc_length, trunc_length}
	};
	size_t num_failpoints = sizeof(failpoints)/sizeof(failpoints[0]);

	for (size_t i = 0; i < num_failpoints; i++) {
		test_snapped_in_progress(TIP_TRUNCATE, failpoints[i],
		    length, trunc_length);
	}
}

TEST(test_snapped_in_progress_truncate_same)
{
	ANNOUNCE_TEST_START;
	int num_regions = 3;
	size_t length = ISI_CBM_CACHE_DEFAULT_REGIONSIZE*num_regions;
	off_t trunc_length = length;
	test_in_progress_failpoint failpoints[] = {
	    {"cbm_cache_trunc_ext_state_eio", trunc_length, trunc_length},
	    {"cbm_cache_trunc_ext_write_info_eio", trunc_length, trunc_length},
	    {"cpool_read_short_cacheinfo_3", trunc_length, trunc_length},
	    {"cpool_write_short_cacheinfo_2", trunc_length, trunc_length},
	    {"cbm_file_ftruncate_fail_2", trunc_length, trunc_length},
	    {"cbm_file_ftruncate_fail_3", trunc_length, trunc_length}
	};
	size_t num_failpoints = sizeof(failpoints)/sizeof(failpoints[0]);

	for (size_t i = 0; i < num_failpoints; i++) {
		test_snapped_in_progress(TIP_TRUNCATE, failpoints[i],
		    length, trunc_length);
	}
}

TEST(test_snapped_in_progress_truncate_up)
{
	ANNOUNCE_TEST_START;
	int num_regions = 2;
	size_t length = ISI_CBM_CACHE_DEFAULT_REGIONSIZE*num_regions;
	off_t trunc_length = length +
	    (ISI_CBM_CACHE_DEFAULT_REGIONSIZE/2); // after last region
	test_in_progress_failpoint failpoints[] = {
	    {"cbm_cache_trunc_ext_state_eio", length, length},
	    {"cbm_cache_trunc_ext_cache1_eio", length, length},
	    {"cbm_cache_trunc_ext_cache2_eio", length, length},
	    {"cbm_cache_trunc_ext_cache3_eio", length, length},
	    {"cbm_file_ftruncate_fail_2", length, length},
	    {"cbm_file_ftruncate_fail_3", trunc_length, trunc_length}
	};
	size_t num_failpoints = sizeof(failpoints)/sizeof(failpoints[0]);

	for (size_t i = 0; i < num_failpoints; i++) {
		test_snapped_in_progress(TIP_TRUNCATE, failpoints[i],
		    length, trunc_length);
	}
}

TEST(test_snapped_in_progress_invalidate)
{
	ANNOUNCE_TEST_START;
	int num_regions = 2;
	size_t length = ISI_CBM_CACHE_DEFAULT_REGIONSIZE*num_regions;
	test_in_progress_failpoint failpoints[] = {
	    {"cbm_invalidate_cache_fail_2", length, length},
	    {"cbm_invalidate_cache_fail_3", length, length}
	};
	size_t num_failpoints = sizeof(failpoints)/sizeof(failpoints[0]);

	for (size_t i = 0; i < num_failpoints; i++) {
		test_snapped_in_progress(TIP_INVALIDATE, failpoints[i],
		    length, 0);
	}
}

TEST(test_cbm_file_read_stale_config,  mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct isi_error *error = NULL;
	struct isi_cbm_file *file = NULL;
	const size_t file_size = 1024;
	char read_buf[file_size];
	cbm_test_file_helper test_file(
	    "/ifs/test_cbm_file_read_stale_config.txt", file_size, "");

	error = cbm_test_archive_file_with_pol(test_file.get_lin(),
	    CBM_TEST_COMMON_POLICY_RAN, false);
	fail_if(error, "failed to archive %s: %#{}",
	    test_file.get_filename(), isi_error_fmt(error));

	file = isi_cbm_file_open(test_file.get_lin(), HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed cbm open for %s : %#{}",
	    test_file.get_filename(), isi_error_fmt(error));
	fail_if(file == NULL);

	// Change the file's account info in the ppi only.
	// This has the effect of making the config look stale, when used later.
	{
		scoped_ppi_writer ppi_writer;
		struct isi_cfm_policy_provider *ppi = ppi_writer.get_ppi(&error);
		struct cpool_account *account = NULL;
		isi_cbm_id account_id;

		fail_if(error, "Failed to get ppi: %#{}", isi_error_fmt(error));
		account_id = file->mapinfo->get_account();
		account = cpool_account_get_by_id_and_cluster(ppi->cp_context,
		    account_id.get_id(),
		    account_id.get_cluster_id(), &error);
		fail_if(error, "cannot get the account (%d, %s): %#{}",
		    account_id.get_id(), account_id.get_cluster_id(),
		    isi_error_fmt(error));

		// Change particulars used to look up the file's account
		free(account->birth_cluster_id);
		account->birth_cluster_id = strdup("test_removed_cluster_id");
		free(account->account_name);
		account->account_name = strdup("test_removed_account_name");
	}

	// Attempt to read the file (should succeed)
	isi_cbm_file_read(file, read_buf, 0, file_size, CO_CACHE, &error);
	fail_if(error, "Failed to read cbm file %s: %#{}",
	    test_file.get_filename(), isi_error_fmt(error));

	// Cleanup
	isi_cbm_file_close(file, &error);
	file = NULL;
	fail_if(error, "Failed to close cbm file %s: %#{}",
	    test_file.get_filename(), isi_error_fmt(error));
	close_ppi();
}
