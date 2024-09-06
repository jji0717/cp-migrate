#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/extattr.h>
#include <ifs/ifs_types.h>
#include <isi_util/isi_error.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_cbm/isi_cbm_cache.h>
#include <isi_cpool_cbm/isi_cbm_file.h>
#include <isi_cpool_cbm/isi_cbm_mapper.h>
#include <isi_cpool_cbm/isi_cbm_access.h>
#include <isi_cpool_cbm/isi_cbm_error.h>
#include <isi_util_cpp/isi_exception.h>
#include <check.h>

#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include <isi_ilog/ilog.h>

#include "check_cbm_common.h"

#define REGIONSIZE		1024
// We do not do real truncates, so filesize is not correctly kept up
// to date.  Make the initial file a multiple of region size so we
// can "fake out" the code to do correct boundary calculations.
#define DEFAULT_LAST_REGION	(89357 / REGIONSIZE)
#define FILESIZE		((DEFAULT_LAST_REGION + 1) * REGIONSIZE)
#define FILENAME		"/ifs/data/foo_cacheinfo_test"

#include "check_cache_common.h"

#define D_VERIFIER(rs, lin, num)  VERIFIER((rs), DFSZ, lin, num)
#define T_VERIFIER(rs, lin, num)  VERIFIER((rs), FSZ((rs + 1)), lin, num)

static cache_status_entry e0[] = {
	VENTRY(0,87,ICNC),
};

D_VERIFIER(DLR, 0, 0);

static cache_status_entry e1[] = {
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,81,ICNC),
	VENTRY(82,82,ICIP),
	VENTRY(83,87,ICNC),
};
D_VERIFIER(DLR, 0, 1);

static cache_status_entry e2[] = {
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,28,ICNC),
	VENTRY(29,29,ICCD),
	VENTRY(30,30,ICIP),
	VENTRY(31,34,ICCD),
	VENTRY(35,81,ICNC),
	VENTRY(82,82,ICIP),
	VENTRY(83,87,ICNC),
};
D_VERIFIER(DLR, 0, 2);

static cache_status_entry e3[] = { // add 28 cached
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,27,ICNC),
	VENTRY(28,28,ICC),
	VENTRY(29,29,ICCD),
	VENTRY(30,30,ICIP),
	VENTRY(31,34,ICCD),
	VENTRY(35,81,ICNC),
	VENTRY(82,82,ICIP),
	VENTRY(83,87,ICNC),
};
D_VERIFIER(DLR, 0, 3);

static cache_status_entry e4[] = { // truncate 32
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,27,ICNC),
	VENTRY(28,28,ICC),
	VENTRY(29,29,ICCD),
	VENTRY(30,30,ICIP),
	VENTRY(31,31,ICCD),
};
T_VERIFIER(31, 0, 4);

static cache_status_entry e5[] = { // truncate 35
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,27,ICNC),
	VENTRY(28,28,ICC),
	VENTRY(29,29,ICCD),
	VENTRY(30,30,ICIP),
	VENTRY(31,34,ICCD),
};
T_VERIFIER(34, 0, 5);

static cache_status_entry e6[] = { // truncate 31
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,27,ICNC),
	VENTRY(28,28,ICC),
	VENTRY(29,29,ICCD),
	VENTRY(30,30,ICIP),
};
T_VERIFIER(30, 0, 6);

static cache_status_entry e7[] = { // truncate 36
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,27,ICNC),
	VENTRY(28,28,ICC),
	VENTRY(29,29,ICCD),
	VENTRY(30,30,ICIP),
	VENTRY(31,35,ICCD),
};
T_VERIFIER(35, 0, 7);

static cache_status_entry e8[] = { // truncate 30
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,27,ICNC),
	VENTRY(28,28,ICC),
	VENTRY(29,29,ICCD),
};
T_VERIFIER(29, 0, 8);

static cache_status_entry e9[] = { // truncate 37
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,27,ICNC),
	VENTRY(28,28,ICC),
	VENTRY(29,36,ICCD),
};
T_VERIFIER(36, 0, 9);

static cache_status_entry e10[] = { // truncate 29
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,27,ICNC),
	VENTRY(28,28,ICC),
};
T_VERIFIER(28, 0, 10);

static cache_status_entry e11[] = { // truncate 38
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,27,ICNC),
	VENTRY(28,28,ICC),
	VENTRY(29,37,ICCD),
};
T_VERIFIER(37, 0, 11);

static cache_status_entry e12[] = { // truncate 28
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,27,ICNC),
};
T_VERIFIER(27, 0, 12);

static cache_status_entry e13[] = { // truncate 38
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,27,ICNC),
	VENTRY(28,37,ICCD),
};
T_VERIFIER(37, 0, 13);

static cache_status_entry e14[] = { // truncate 27
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,26,ICNC),
};
T_VERIFIER(26, 0, 14);

static cache_status_entry e15[] = { // truncate 39
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,26,ICNC),
	VENTRY(27,38,ICCD),
};
T_VERIFIER(38, 0, 15);

static cache_status_entry e16[] = { // truncate 55
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,26,ICNC),
	VENTRY(27,54,ICCD),
};
T_VERIFIER(54, 0, 16);

static cache_status_entry e17[] = { // cache 34
	VENTRY(0,4,ICNC),
	VENTRY(5,5,ICC),
	VENTRY(6,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,26,ICNC),
	VENTRY(27,33,ICCD),
	VENTRY(34,34,ICC),
	VENTRY(35,54,ICCD),
};
T_VERIFIER(54, 0, 17);

static cache_status_entry e18[] = { // clear 5
	VENTRY(0,8,ICNC),
	VENTRY(9,9,ICCD),
	VENTRY(10,26,ICNC),
	VENTRY(27,33,ICCD),
	VENTRY(34,34,ICC),
	VENTRY(35,54,ICCD),
};
T_VERIFIER(54, 0, 18);

struct command_str {
	char cmd;
	const void *val;
};

#define CMD_DUMP		{'N', NULL} // printout the cacheinfo (N->d)
#define CMD_COMPARE(v)		{'C', &v}   // Compare cacheinfo to v
#define CMD_CACHED(r)		{'r', #r}   // Mark region r as cached
#define CMD_DIRTY(r)		{'w', #r}   // Mark region r as dirty
#define CMD_INPROGRESS(r)	{'s', #r}   // Mark region r as in-progress
#define CMD_CLEAR(r)		{'c', #r}   // Mark region r as not-cached
#define CMD_TRUNCATE(r)		{'t', #r}   // Truncate cacheinfo at region r
#define CMD_REOPEN		{'z', NULL} // Delete cacheinfo object and
					    // perform reopen

static struct command_str commands[] = {
	CMD_DUMP,			//0
	CMD_COMPARE(v0),
	CMD_CACHED(5),
	CMD_DIRTY(9),
	CMD_INPROGRESS(82),
	CMD_DUMP,			//5
	CMD_COMPARE(v1),
	CMD_INPROGRESS(30),
	CMD_DIRTY(31),
	CMD_DIRTY(32),
	CMD_DIRTY(33),			//10
	CMD_DIRTY(29),
	CMD_DIRTY(34),
	CMD_DUMP,
	CMD_COMPARE(v2),
	CMD_REOPEN,			//15
	CMD_COMPARE(v2),
	CMD_CACHED(28),
	CMD_DUMP,
	CMD_COMPARE(v3),
	CMD_TRUNCATE(32),		//20
	CMD_DUMP,
	CMD_COMPARE(v4),
	CMD_TRUNCATE(35),
	CMD_DUMP,
	CMD_COMPARE(v5),		//25
	CMD_TRUNCATE(31),
	CMD_DUMP,
	CMD_COMPARE(v6),
	CMD_TRUNCATE(36),
	CMD_DUMP,			//30
	CMD_COMPARE(v7),
	CMD_TRUNCATE(30),
	CMD_DUMP,
	CMD_COMPARE(v8),
	CMD_TRUNCATE(37),		//35
	CMD_DUMP,
	CMD_COMPARE(v9),
	CMD_TRUNCATE(29),
	CMD_DUMP,
	CMD_COMPARE(v10),		//40
	CMD_TRUNCATE(38),
	CMD_DUMP,
	CMD_COMPARE(v11),
	CMD_TRUNCATE(28),
	CMD_DUMP,			//45
	CMD_COMPARE(v12),
	CMD_REOPEN,
	CMD_COMPARE(v12),
	CMD_TRUNCATE(38),
	CMD_DUMP,			//50
	CMD_COMPARE(v13),
	CMD_REOPEN,
	CMD_COMPARE(v13),
	CMD_TRUNCATE(27),
	CMD_DUMP,			//55
	CMD_COMPARE(v14),
	CMD_TRUNCATE(39),
	CMD_DUMP,
	CMD_COMPARE(v15),
	CMD_TRUNCATE(55),		//60
	CMD_DUMP,
	CMD_COMPARE(v16),
	CMD_CACHED(34),
	CMD_DUMP,
	CMD_COMPARE(v17),		//65
	CMD_CLEAR(5),
	CMD_DUMP,
	CMD_COMPARE(v18),		//68

};

static int commands_to_do = sizeof (commands)/sizeof (struct command_str);

static const char *filename = FILENAME;

static void throw_exception(void)
{
	throw cl_exception(CL_PARTIAL_FILE, "PreLeak memory from throw");
}

TEST_FIXTURE(setup_suite)
{
#ifdef ENABLE_ILOG_USE
	struct ilog_app_init init =  {
		"check_cacheinfo",		// full app name
		"cpool",			// component
		"",				// job (opt)
		"check_cacheinfo",		// syslog program
		IL_TRACE_PLUS,			// default level
		false,				// use syslog
		true,				// use stderr
		false,				// log_thread_id
		LOG_DAEMON,			// syslog facility
		"/ifs/logs/check_cacheinfo.log",	// log file
		IL_ERR_PLUS,			// syslog_threshold
		NULL,				//tags
	};

	char *ilogenv = getenv("CHECK_USE_ILOG");
	if (ilogenv != NULL){
		ilog_init(&init, false, true);
		ilog(IL_NOTICE, "ilog initialized\n");
	}
#endif

	cbm_test_common_setup(true, false);

	try {
		// Throw an exception before running any tests so that
		// the mutexes used by throw will be preallocated and
		// in the memory snapshot used to look for leaks.  In the
		// normal case the memory is cleaned up by image rundown
		// but we do not run down between tests here.
		throw_exception();
	}
	catch (...) {
		CHECK_P("threw and caught an exception to preAllocate memory\n");
	}
}

TEST_FIXTURE(cleanup_suite)
{
	cbm_test_common_cleanup(true, false);
}

TEST_FIXTURE(setup_test)
{
	cbm_test_enable_stub_access();
}

TEST_FIXTURE(teardown_test)
{
}

SUITE_DEFINE_FOR_FILE(check_cbm_cacheinfo,
    .mem_check = CK_MEM_LEAKS,
    .dmalloc_opts = NULL,
    .suite_setup = setup_suite,
    .suite_teardown = cleanup_suite,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .runner_data = NULL,
    .timeout = 60,
    .fixture_timeout = 1200);

#define RM_STRING	CBM_ACCESS_ON "rm -f %s" CBM_ACCESS_OFF

static void
compare_cacheinfo(cpool_cache *cacheinfo, struct cache_verify *verifier,
    int item)
{
	struct isi_error *error = NULL;
	int i;
	enum isi_cbm_cache_status status;
	off_t start_region, end_region;
	int results;

	struct cpool_cache_header c_header = cacheinfo->get_header();

	results = memcmp(&c_header, &verifier->header, sizeof(c_header));
	if (results != 0) {
		printf("\nnew header, version %d, flags 0x%08x, lin %llu, "
		    "old_file_size %llu, region_size %llu, "
		    "data_offset %lld, last_region %lld, "
		    "sequence %llu, foot %llu\n",
		    c_header.version, c_header.flags, c_header.lin,
		    c_header.old_file_size, c_header.region_size,
		    c_header.data_offset, c_header.last_region,
		    c_header.sequence, c_header.foot);
		printf("verifer,    version %d, flags 0x%08x, lin %llu, "
		    "old_file_size %llu, region_size %llu, "
		    "data_offset %lld, last_region %lld, "
		    "sequence %llu, foot %llu\n",
		    verifier->header.version, verifier->header.flags,
		    verifier->header.lin, verifier->header.old_file_size,
		    verifier->header.region_size, verifier->header.data_offset,
		    verifier->header.last_region, verifier->header.sequence,
		    verifier->header.foot);
	}
	fail_if((results != 0), "Cacheinfo header did not verify, item %d",
	    item);


	start_region = 0;
	for (i = 0; i < verifier->num_entries; i++) {
		cacheinfo->get_status_range(&start_region, &end_region, 0,
		    &status, &error);
		fail_if(error, "Could not get range information entry %d "
		    "(item %d): %{}",
		    i, item, isi_error_fmt(error));

		fail_if(start_region != verifier->entries[i].start,
		    "Start region does not match for entry %d "
		    "(item %d): found %ld, expected %ld",
		    i, item, start_region, verifier->entries[i].start);

		fail_if(end_region != verifier->entries[i].end,
		    "End region does not match for entry %d "
		    "(item %d): found %ld, expected %ld",
		    i, item, end_region, verifier->entries[i].end);

		fail_if(status != verifier->entries[i].status,
		    "Status fo region does not match for entry %d "
		    "(item %d): found %d, expected %d",
		    i, item, status, verifier->entries[i].status);

		start_region = end_region + 1;
	}
}

enum testoper  {
	TEST_WRITE	= 1,
	TEST_TRUNC	= 2,
	TEST_DUMP	= 4,
	TEST_COMPARE	= 8,
	TEST_ZAP	= 16,
	TEST_NOOP	= 99

};

bool
update_cacheinfo(struct isi_cbm_file *file, char cmd, const void *val,
    ifs_lin_t lin, int item)
{
	enum testoper operat = TEST_NOOP;
	enum isi_cbm_cache_status status = ISI_CPOOL_CACHE_INVALID;
	off_t region;
	cache_verify *comp_val;
	struct isi_error *error = NULL;
	size_t file_size;
	size_t region_size = REGIONSIZE;
	int state;

	switch(cmd) {
	case 'r':
		operat = TEST_WRITE;
		status = ISI_CPOOL_CACHE_CACHED;
		break;
	case 'w':
		operat = TEST_WRITE;
		status = ISI_CPOOL_CACHE_DIRTY;
		break;
	case 'c':
		operat = TEST_WRITE;
		status = ISI_CPOOL_CACHE_NOTCACHED;
		break;
	case 's':
		operat = TEST_WRITE;
		status = ISI_CPOOL_CACHE_INPROGRESS;
		break;
	case 't':
		operat = TEST_TRUNC;
		break;
	case 'd':
		operat = TEST_DUMP;
		break;
	case 'C':
		operat = TEST_COMPARE;
		break;
	case 'z':
		operat = TEST_ZAP;
		break;
	case 'N':
		operat = TEST_NOOP;  //allow for wholesale replacement...
		break;
	default:
		fail_if(true,
			    "Unexpected status %c for region %s\n",
		    cmd, (val?val:"** N/S **"));
	}

	CHECK_P("*** operation %c ", cmd);
	if (operat == TEST_COMPARE) {
		comp_val = (struct cache_verify *) val;
		/*
		 *  Since we did a static initialization of the verifier, we
		 *  will fill in the lin here.  Note that we will have to do
		 *  the same for the file size when we track that in the
		 *  header as well.
		 */
		comp_val->header.lin = lin;
	} else if (operat != TEST_DUMP && operat != TEST_ZAP &&
	    operat != TEST_NOOP) {
		region = atoi((char *)val);
		CHECK_P("%ld ", region);
	}
	CHECK_P("***\n");

	switch(operat) {
	case TEST_WRITE:
		file->cacheinfo->cache_info_write(region, status, &error);
		fail_if(error, "Error writing to cache entry %lld: %{}",
		    region, isi_error_fmt(error));
		break;
	case TEST_TRUNC:
		file_size = region * region_size;
		state = file->cacheinfo->cache_truncate(file_size, &error);
		fail_if(error, "Error truncating cacheinfo to %lld regions "
		    "(%lld file size): %{}, item %d",
		    region, file_size, isi_error_fmt(error), item);
		fail_if(!(state & ISI_CBM_CACHE_FLAG_TRUNCATE),
		    "Expected cache state to contain %d during truncate, "
		    "item %d", state, item);
		file->cacheinfo->clear_state(state, &error);
		fail_if(error, "Error clearing truncate state from cacheinfo "
		    "for region %d: %{}, item %d",
		    region, isi_error_fmt(error), item);
		break;
	case TEST_DUMP:
		printf("\nDumping cache info for item %d: \n", item);
		file->cacheinfo->dump(true, &error);
		fail_if(error, "Error dumping cache: %{}",
		    isi_error_fmt(error));
		break;
	case TEST_ZAP:
		if (file->cacheinfo) {
			delete file->cacheinfo;
			file->cacheinfo = NULL;
		}
		file->cacheinfo = new cpool_cache(file);
		file->cacheinfo->cache_open(file->fd,
		    false, false, &error);
		fail_if(error, "Error reopening cache: %{}",
		    isi_error_fmt(error));
		file->cacheinfo->cacheheader_lock(false, &error);
		fail_if(error, "Error relocking cache: %{}",
		    isi_error_fmt(error));
		break;
	case TEST_COMPARE:
		compare_cacheinfo(file->cacheinfo, comp_val, item);
		break;
	case TEST_NOOP:
		break;
	default:
		fail_if(true,
		    "Unexpected command %d for region %s\n",
		    cmd, (val?val:"** N/S **"));
		break;
	}
	fail_if(error, "Cannot perform operation %d on region %s, error %{}",
	    operat, (val?val:"** N/S **"), isi_error_fmt(error));

	return true;
}


TEST(test_cacheinfo)
{
	int		status		= -1;
	struct stat	statbuf;
	int		fd		= -1;
	ifs_lin_t	lin;
	struct isi_cbm_file	*file	= NULL;
	u_int64_t	filerev;
	struct isi_error *error		= NULL;
	int		j;
	isi_cfm_mapinfo	mapinfo(ISI_CFM_MAP_TYPE_OBJECTINFO);

	unlink(filename);
	fd = open(filename,
	    (O_RDWR | O_CREAT | O_TRUNC),
	    (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH));
	fail_if(fd == -1,
	    "Opening of file %s failed with error: %s (%d)\n",
	    filename, strerror(errno), errno);

	ftruncate(fd, FILESIZE);
	fail_if(fd == -1,
	    "Setting size of file %s to %ld failed with error: %s (%d)\n",
	    filename, FILESIZE, strerror(errno),
	    errno);

	status = fstat(fd, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)\n",
	    filename, strerror(errno), errno);

	status = getfilerev(fd, &filerev);
	fail_if(status,
	    "Get filerev for file %s failed with error: %s (%d)\n",
	    filename, strerror(errno), errno);

	lin = statbuf.st_ino;
	mapinfo.set_filesize(statbuf.st_size);
	mapinfo.set_lin(lin);

	mapinfo.write_map(fd, filerev, false, false, &error);
	fail_if(error,
	    "Create stub for file %s failed with error: %{}",
	    filename, isi_error_fmt(error));

	close(fd);
	fd = -1;

	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "CBM Opening of file %s failed with error: %{}\n",
	    filename, isi_error_fmt(error));

	fail_if(file->cacheinfo->is_cache_open(),
	    "Cacheinfo is reported to be open when it hasn't been opened\n");

	file->cacheinfo->cache_initialize_override(file->fd,
	    REGIONSIZE, statbuf.st_size, lin, &error);

	fail_if(error, "Could intialize cacheinfo for %s: %{}\n",
	    filename, isi_error_fmt(error));

	fail_if(!file->cacheinfo->is_cache_open(),
	    "Cacheinfo is not reported open when it has been open\n");

	for (j = 0; j < commands_to_do; j++) {
		update_cacheinfo(file, commands[j].cmd,
		    commands[j].val, lin, j);
	}

	file->cacheinfo->cacheheader_unlock(isi_error_suppress());
	isi_cbm_file_close(file, isi_error_suppress());
	file = NULL;

	status = unlink(filename);
	fail_if(status, "Cannot remove test file %s", filename);
}

struct test_input {
	off_t offset;
	size_t length;
	bool result;
	off_t start;
	off_t end;
};

TEST(test_cacheinfo_helpers)
{
	int status = -1;
	int pass = -1;

	struct stat statbuf;

	struct isi_error *error = NULL;

	off_t start, end, lastregion, offset;
	size_t regionsize, lastregion_size, length;

	struct test_input *tests = NULL;

	char *test_name = NULL;
	int num_tests;
	bool retval;

	int fd   = -1;
	ifs_lin_t lin;
	struct isi_cbm_file *file = NULL;
	u_int64_t filerev;
	isi_cfm_mapinfo mapinfo(ISI_CFM_MAP_TYPE_OBJECTINFO);

	status = -1;

	unlink(filename);

	fd = open(filename,
	    (O_RDWR | O_CREAT | O_TRUNC),
	    (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH));

	fail_if(fd == -1,
	    "Opening of file %s failed with error: %s (%d)",
	    filename, strerror(errno), errno);

	ftruncate(fd, FILESIZE);
	fail_if(fd == -1,
	    "Setting size of file %s to %ld failed with error: %s (%d)",
	    filename, FILESIZE, strerror(errno),
	    errno);

	status = fstat(fd, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    filename, strerror(errno), errno);

	status = getfilerev(fd, &filerev);
	fail_if(status,
	    "Get filerev for file %s failed with error: %s (%d)",
	    filename, strerror(errno), errno);

	lin = statbuf.st_ino;
	mapinfo.set_filesize(statbuf.st_size);
	mapinfo.set_lin(lin);

	mapinfo.write_map(fd, filerev, false, false, &error);
	fail_if(error,
	    "Create stub for file %s failed with error: %{}",
	    filename, isi_error_fmt(error));

	close(fd);
	fd = -1;

	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "CBM Opening of file %s failed with error: %{}",
	    filename, isi_error_fmt(error));
	fail_if(file == NULL);

	fail_if(file->cacheinfo->is_cache_open(),
	    "Cacheinfo is reported to be open when it hasn't been opened");

	file->cacheinfo->cache_initialize_override(file->fd, REGIONSIZE,
	    statbuf.st_size, statbuf.st_ino, &error);

	fail_if(error, "Could intialize cacheinfo for %s: %{}",
	    filename, isi_error_fmt(error));

	fail_if(!file->cacheinfo->is_cache_open(),
	    "Cacheinfo is not reported open when it has been open");

	regionsize = file->cacheinfo->get_regionsize();
	lastregion = file->cacheinfo->get_last_region();
	lastregion_size = FILESIZE % file->cacheinfo->get_regionsize();
	if (lastregion_size == 0) {
		lastregion_size = regionsize;
	}

#define RZ	REGIONSIZE
#define FZ	FILESIZE
#define LRZ	lastregion_size
#define LR	lastregion

#define true_or_false(v) ((v)?"True":"False")

	struct test_input b2r_tests[] = {
		/* offset       length         return  start     end    */
		{-1,		FZ,		false,	-1,	-1},
		{0, 		FZ,		true,	0, 	LR},
		{RZ,	 	FZ-RZ,		true,	1, 	LR},
		{RZ, 		FZ-2*RZ,	true,	1, 	LR-1},
		{RZ, 		FZ-LRZ,        	false,	-1, 	-1},
		{0,		FZ-LRZ,		true,   0,	LR-1},
		{0, 		FZ-LRZ+1,	true,	0, 	LR},
		{0, 		FZ-LRZ-1,	true,	0, 	LR-1},
		{RZ*2,		RZ*2-1,		true,	2,	3},
	};

	int b2r_num_tests = sizeof (tests)/sizeof (struct test_input);
	//int b2r_num_tests = sizeof (b2r_tests)/sizeof (struct test_input);

	struct test_input r2b_tests[] = {
		/* offset       length         return  start     end    */
		{-1,		-1,		false,	-1,	LR},
		{-1,		-1,		false,	LR+1,	-1},
		{0,		-1,		false,	0,	LR+1},
		{2*RZ,		-1,		false,	2,	1},
		{0,		FZ,		true,	0,	LR},
		{0,		RZ,		true,	0,	0},
		{RZ,		RZ,		true,	1,	1},
		{FZ-LRZ,	LRZ,		true,	LR,	LR},
		{2*RZ,		3*RZ,		true,	2,	4},
		{3*RZ,		RZ,		true,	3,	3},
		{FZ-LRZ-RZ,	RZ+LRZ,		true,	LR-1,	LR},
	};

	int r2b_num_tests = sizeof(r2b_tests)/sizeof(struct test_input);

	test_name = strdup("bytes_to_regions");
	tests = b2r_tests;
	num_tests = b2r_num_tests;

	for (pass = 0; pass < num_tests; pass++) {
		start = 0;
		end = 0;
		retval = file->cacheinfo->convert_bytes_to_regions(
		    tests[pass].offset, tests[pass].length,
		    &start, &end, &error);

		fail_if(error, "Could not convert bytes to regions for %s: %{}",
		    filename, isi_error_fmt(error));
		fail_if(retval != tests[pass].result,
		    "%s returned unexpected results, expected %s "
		    "returned %s, pass %d", test_name,
		    true_or_false(tests[pass].result), true_or_false(retval),
		    pass);
		fail_if((start != tests[pass].start || end != tests[pass].end),
		    "%s failed, expected (%ld, %ld), "
		    "found (%ld, %ld), for (%ld, %ld), pass %d", test_name,
		    tests[pass].start, tests[pass].end,
		    start, end, tests[pass].offset, tests[pass].length, pass);
	}

	if (test_name) {
		free(test_name);
		test_name = NULL;
	}

	test_name = strdup("regions_to_bytes");
	tests = r2b_tests;
	num_tests = r2b_num_tests;
	for (pass = 0; pass < num_tests; pass++) {
		offset = 0;
		length = 0;
		retval = file->cacheinfo->convert_regions_to_bytes(
		    tests[pass].start, tests[pass].end,
		    &offset, &length, &error);

		fail_if(error, "Could not convert regions to bytes for %s: %{}",
		    filename, isi_error_fmt(error));
		fail_if(retval != tests[pass].result,
		    "%s returned unexpected results, expected %s "
		    "returned %s, pass %d", test_name,
		    true_or_false(tests[pass].result), true_or_false(retval),
		    pass);
		fail_if((offset != tests[pass].offset ||
			length != tests[pass].length),
		    "%s failed, expected (%ld, %ld), "
		    "found (%ld, %ld), for (%ld, %ld), pass %d", test_name,
		    tests[pass].offset, tests[pass].length,
		    offset, length, tests[pass].start, tests[pass].end, pass);
	}

	if (test_name) {
		free(test_name);
		test_name = NULL;
	}

	file->cacheinfo->cacheheader_unlock(isi_error_suppress());
	isi_cbm_file_close(file, isi_error_suppress());

	status = unlink(filename);
	fail_if(status, "Cannot remove test file %s", filename);

}

TEST(check_version)
{
	int status = -1;
	struct stat statbuf;

	int fd   = -1;
	ifs_lin_t lin;
	struct isi_cbm_file *file = NULL;
	u_int64_t filerev;
	isi_cfm_mapinfo mapinfo(ISI_CFM_MAP_TYPE_OBJECTINFO);
	cpool_cache *cacheinfo = NULL;

	struct isi_error *error = NULL;

	status = -1;

	unlink(filename);
	fd = open(filename,
	    (O_RDWR | O_CREAT | O_TRUNC),
	    (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH));
	fail_if(fd == -1,
	    "Opening of file %s failed with error: %s (%d)\n",
	    filename, strerror(errno), errno);

	ftruncate(fd, FILESIZE);
	fail_if(fd == -1,
	    "Setting size of file %s to %ld failed with error: %s (%d)\n",
	    filename, FILESIZE, strerror(errno),
	    errno);

	status = fstat(fd, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)\n",
	    filename, strerror(errno), errno);

	status = getfilerev(fd, &filerev);
	fail_if(status,
	    "Get filerev for file %s failed with error: %s (%d)\n",
	    filename, strerror(errno), errno);

	lin = statbuf.st_ino;
	mapinfo.set_filesize(statbuf.st_size);
	mapinfo.set_lin(lin);

	mapinfo.write_map(fd, filerev, false, false, &error);
	fail_if(error,
	    "Create stub for file %s failed with error: %{}",
	    filename, isi_error_fmt(error));

	close(fd);
	fd = -1;

	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "CBM Opening of file %s failed with error: %{}\n",
	    filename, isi_error_fmt(error));

	cacheinfo = file->cacheinfo;

	fail_if(cacheinfo->is_cache_open(),
	    "Cacheinfo is reported to be open when it hasn't been opened\n");

	cacheinfo->cache_initialize_override(file->fd, REGIONSIZE,
	    statbuf.st_size, lin, &error);

	fail_if(error, "Could intialize cacheinfo for %s: %{}\n",
	    filename, isi_error_fmt(error));

	fail_if(!cacheinfo->is_cache_open(),
	    "Cacheinfo is not reported open when it has been open\n");

	/*
	 * Corrupt cache version here
	 */
	int adsd_fd = enc_openat(file->fd, "." , ENC_DEFAULT, O_RDONLY|O_XATTR);
	fail_if (adsd_fd < 0,
	    "cannot open ADS dir for header corruption: %s (%d)",
	    strerror(errno), errno);

	int attr_fd = enc_openat(adsd_fd, ISI_CBM_CACHE_CACHEINFO_NAME,
	    ENC_DEFAULT, O_RDWR|O_SYNC);
	fail_if(attr_fd < 0,
	    "Cannot open cacheinfo ADS for header corruption: %s (%d)",
	    strerror(errno), errno);

	int version = -2;
	int bytes = write(attr_fd, &version, sizeof(version));
	fail_if(bytes != sizeof(version),
	    "Cannot write corrupting version number, write returned %d",
	    bytes);

	close(attr_fd);
	close(adsd_fd);

	cacheinfo->refresh_header(&error);
	fail_if((!error || !isi_cbm_error_is_a(error,
		    CBM_CACHE_VERSION_MISMATCH)),
	    "Expected to find invalid cache version, error returned was %{}",
	    isi_error_fmt(error));

	isi_error_free(error);

	isi_cbm_file_close(file, isi_error_suppress());

	status = unlink(filename);
	fail_if(status, "Cannot remove test file %s", filename);

}
