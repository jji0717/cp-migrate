#include <check.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/extattr.h>
#include <sys/uio.h>
#include <ifs/ifs_types.h>
#include <isi_util/isi_error.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_cbm/isi_cbm_cache.h>
#include <isi_cpool_cbm/isi_cbm_file.h>
#include <isi_cpool_cbm/isi_cbm_mapper.h>
#include <isi_cpool_cbm/isi_cbm_access.h>
#include <isi_cpool_cbm/isi_cbm_error.h>
#include <isi_cpool_cbm/isi_cbm_sparse.h>
#include <isi_util_cpp/isi_exception.h>

#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include <isi_ilog/ilog.h>

#include "check_cbm_common.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_error.h"

#define TEST_FILE_NAME			"/ifs/data/check_sparse"
#define OUTPUT_FILE_NAME		"/ifs/data/output_sparse"

const char INVALID[]	= "Invalid";
const char SPARSE[]	= "Sparse";
const char DATA[]	= "Data";
const char UNCHANGED[]	= "Ditto";

const char * region_type_to_text(int region_type)
{
	switch (region_type) {
	case RT_SPARSE:
		return SPARSE;
	case RT_DATA:
		return DATA;
	case RT_UNCHANGED:
		return UNCHANGED;
	default:
		return INVALID;
	}
}
#define SIZE_OF_SPA(spa)	(sizeof (spa)/sizeof(unsigned char))

static void
clear_failpoints()
{
}

#ifdef ENABLE_ILOG_USE
#define ANNOUNCE_TEST_START \
	ilog(IL_DEBUG, "Entered TEST %s (line %d)",  __FUNCTION__, __LINE__);\
	CHECK_TRACE("Entered TEST %s (line %d)\n",  __FUNCTION__, __LINE__);
#else
#define ANNOUNCE_TEST_START \
	CHECK_TRACE("Entered TEST %s (line %d)\n",  __FUNCTION__, __LINE__);
#endif

TEST_FIXTURE(suite_setup)
{
#ifdef ENABLE_ILOG_USE
	struct ilog_app_init init =  {
		"check_sparse",			// full app name
		"cpool",			// component
		"",				// job (opt)
		"check_sparse",			// Syslog program
		IL_TRACE_PLUS|IL_DETAILS|IL_CP_EXT,	// default level
		false,				// use syslog
		true,				// use stderr
		false,				// log_thread_id
		LOG_DAEMON,			// syslog facility
		"/ifs/logs/check_sparse.log",	// log file
		IL_TRACE_PLUS,			// syslog_threshold
		NULL,				//tags
	};

	char *ilogenv = getenv("CHECK_USE_ILOG");
	if (ilogenv != NULL) {
		ilog_init(&init, false, true);
		ilog(IL_NOTICE, "ilog initialized\n");
	}
#endif

//	fail_if(!cbm_test_cpool_ufp_init(), "Fail points failed to initiate");
}

TEST_FIXTURE(suite_teardown)
{
	clear_failpoints();
}

TEST_FIXTURE(setup_test)
{
}

TEST_FIXTURE(teardown_test)
{
}

static void
cleanup_file(const char *f)
{
	/*
	 * Remove the old files. This is done as a seperate loop as the loop
	 * below uses the files stubbed in one iteration in the next iteration
	 * for testing.
	 */
	int status = unlink(f);

	fail_if((status < 0 && errno != ENOENT),
	    "failed to remove file %s: %s (%d)",
	    f, strerror(errno), errno);
}

SUITE_DEFINE_FOR_FILE(check_sparse,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 120,
    .fixture_timeout = 1200);


/* Utility routines. */

static int
test_spa_maps(struct sparse_file_info *spf, const char *filename)
{
	int 	status = 0;

	cbm_sparse_test_file_helper test_file(filename, spf->filesize,
	    spf->num_segments, spf->segments);

	fail_if(!compare_maps_to_spf(spf, filename, true),
	    "Sparse maps do not compare for file %s",
	    filename);

	return status;
}

static void
restore_sparse_file(void * map, size_t map_size, struct sparse_file_info * spf,
    size_t length, const char *ifilename, const char *ofilename)
{
	ssize_t status = -1;
	size_t writelen = 0;
	struct  iovec iov[64];
	int     iov_cnt = 64;
	size_t	read_size = spf->filesize;
	char   *buffer = (char *)malloc(read_size);
	struct  isi_error *error = NULL;
	int     fd = -1;
	int	i;

	fd = open(ifilename, (O_RDONLY));
	fail_if (fd < 0, "Could not open file %s for input: %s (%d)",
	    ifilename, strerror(errno), errno);

	status = pread(fd, buffer, read_size, 0);
	fail_if((status != (ssize_t)read_size),
	    "Failed pread offset 0 size %ld, on %s returned %d %s",
	    read_size, ifilename,
	    (status < 0) ? strerror(errno) : "");
	close(fd);

	isi_cbm_sparse_map_to_iovec(NULL, map, map_size, 0, read_size, buffer,
	    spf->resolution, spf->chunk_size, iov, &iov_cnt, &writelen,
	    &error);

	fail_if(error, "map to iovec convertion failed: %{}",
	    isi_error_fmt(error));

	fd = open(ofilename, (O_RDWR | O_CREAT | O_TRUNC), 0644);
	fail_if (fd < 0, "Could not open file %s for output: %s (%d)",
	    ofilename, strerror(errno), errno);

	CHECK_P("Setting file size to be %ld\n", read_size);

	status = ftruncate(fd, read_size);
	fail_if(status < 0, "Could not set output file size for %s: %s (%d)",
	    ofilename, strerror(errno), errno);

	for (i = 0; i < iov_cnt; i++) {
		status = pwritev(fd, &iov[i], 1,
		    ((char *)(iov[i].iov_base) - buffer));
		fail_if((status != (ssize_t)iov[i].iov_len),
		    "Failed pwritev, returned %d %s",
		    (status < 0) ? strerror(errno) : "");
	}

	close(fd);
	if (buffer)
		free(buffer);
}

static void
compare_file(const char *a, const char *b)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	int status = -1;

	fmt_print(&fmt, "diff %s %s", a, b);

	status = system(fmt_string(&fmt));

	fail_if(status, "Files %s and %s do not compare", a, b);
}

static void
test_spa_end2end(struct sparse_file_info *spf, const char *ifilename,
    const char *ofilename)
{
	struct stat statbuf;
	struct isi_error *error = NULL;
	int map_size = spf->map_size;
	size_t filesize = spf->filesize;
	unsigned char spa_map[64];

	cbm_sparse_test_file_helper test_file(ifilename, spf->filesize,
	    spf->num_segments, spf->segments);

	fail_if(stat(ifilename, &statbuf), "Could not stat %s for %s: %s (%d)",
	    ifilename, spf->spf_name, strerror(errno), errno);

	memset(spa_map, 0x42, 64);
	isi_cbm_build_sparse_area_map(statbuf.st_ino, statbuf.st_snapid,
	    0, statbuf.st_size, spf->resolution, spf->region_size,
	    spa_map, map_size, &error);
	fail_if(error, "Failed to create sparse area map for %s: %{}",
	    spf->spf_name, isi_error_fmt(error));

	restore_sparse_file(spa_map, map_size, spf, filesize,
	    ifilename, ofilename);

	compare_file(ifilename, ofilename);
}

#define SPA_TEST(tst)						\
	struct fmt FMT_INIT_CLEAN(fn);				\
	fmt_print(&fn, "%s_%s", TEST_FILE_NAME, (tst).spf_name);\
	cleanup_file(fmt_string(&fn));				\
	test_spa_maps(&(tst), fmt_string(&fn));			\
	cleanup_file(fmt_string(&fn));


TEST(test_spamap_1M, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	SPA_TEST(sparse_1M);
}

TEST(test_spamap_1Ma, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	SPA_TEST(sparse_1Ma);
}

TEST(test_spamap_none, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	SPA_TEST(sparse_none);
}

TEST(test_spamap_all, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	SPA_TEST(sparse_all);
}

TEST(test_spamap_alt_even, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	SPA_TEST(sparse_alt_even);
}

TEST(test_spamap_alt_odd, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	SPA_TEST(sparse_alt_odd);
}

TEST(test_spamap_short, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	SPA_TEST(sparse_short);
}

TEST(test_spamap_alt_even_byte, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	SPA_TEST(sparse_alt_even_byte );
}

TEST(test_spamap_alt_odd_byte, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	SPA_TEST(sparse_alt_odd_byte);
}

TEST(test_spamap_alt_even_byte_2m, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	SPA_TEST(sparse_alt_even_byte_2m );
}

TEST(test_spamap_alt_odd_byte_2m, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	SPA_TEST(sparse_alt_odd_byte_2m);
}


TEST(test_e2e_1M, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	cleanup_file(TEST_FILE_NAME);
	cleanup_file(OUTPUT_FILE_NAME);
	test_spa_end2end(&sparse_1M, TEST_FILE_NAME, OUTPUT_FILE_NAME);
	cleanup_file(TEST_FILE_NAME);
	cleanup_file(OUTPUT_FILE_NAME);
}

TEST(test_e2e_1Ma, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	cleanup_file(TEST_FILE_NAME);
	cleanup_file(OUTPUT_FILE_NAME);
	test_spa_end2end(&sparse_1Ma, TEST_FILE_NAME, OUTPUT_FILE_NAME);
	cleanup_file(TEST_FILE_NAME);
	cleanup_file(OUTPUT_FILE_NAME);
}

TEST(test_e2e_odd_byte, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	cleanup_file(TEST_FILE_NAME);
	cleanup_file(OUTPUT_FILE_NAME);
	test_spa_end2end(&sparse_alt_odd_byte,
	    TEST_FILE_NAME, OUTPUT_FILE_NAME);
	cleanup_file(TEST_FILE_NAME);
	cleanup_file(OUTPUT_FILE_NAME);
}

TEST(test_e2e_even_byte, mem_check : CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	cleanup_file(TEST_FILE_NAME);
	cleanup_file(OUTPUT_FILE_NAME);
	test_spa_end2end(&sparse_alt_even_byte ,
	    TEST_FILE_NAME, OUTPUT_FILE_NAME);
	cleanup_file(TEST_FILE_NAME);
	cleanup_file(OUTPUT_FILE_NAME);
}

