#include <fcntl.h>
#include <stdio.h>
#include <check.h>

#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>

#include <isi_ilog/ilog.h>

#include "check_cbm_common.h"
#include "isi_cpool_cbm.h"
#include "isi_cbm_file.h"
#include "isi_cbm_mapper.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_error_util.h"
#include "isi_cbm_scoped_flock.h"

#define CP_STRING	CBM_ACCESS_ON "cp -f %s %s" CBM_ACCESS_OFF

#define CHECK_TEMP_FILE 	"/tmp/test_cpool_write.tmp.XXXXXX"

using namespace isi_cloud;

const char *test_file_name = "/ifs/check_cbm_scoped_flock.txt";
int fd = -1, fd1 = -1;

static void
create_test_file()
{
	fd = open(test_file_name, O_CREAT|O_RDWR, 0777);
	fail_if(fd < 0, "%s: failed, error = %s(%d)", __FUNCTION__,
	    strerror(errno), errno);
	fd1 = open(test_file_name, O_CREAT|O_RDWR, 0777);
	fail_if(fd1 < 0, "%s: failed, error = %s(%d)", __FUNCTION__,
	    strerror(errno), errno);
}

static void
remove_test_file()
{
	int ret;

	if (fd >= 0) {
		ret = close(fd);
		fail_if(ret, "%s: failed, error = %s(%d)", __FUNCTION__,
		    strerror(errno), errno);
	}
	ret = unlink(test_file_name);
	if (ret)
		fail_if(errno != ENOENT, "%s: failed, error = %s(%d)",
		    __FUNCTION__, strerror(errno), errno);
}

TEST_FIXTURE(suite_setup)
{
	remove_test_file();
	create_test_file();

	cbm_test_leak_check_primer(true);
	isi_cloud::cl_exception unused(isi_cloud::CL_OK, "unused");
}

TEST_FIXTURE(suite_teardown)
{
	remove_test_file();
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

SUITE_DEFINE_FOR_FILE(check_cbm_scoped_flock,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 15,
    .fixture_timeout = 1200);

enum lock_test_class {LTC_STUB, LTC_SYNCH};
enum locking_test_type {LTT_SR, LTT_EX};

struct lock_test_entry
{
	cbm_error_type_values expect_error;
	lock_test_class locker_class;
	ifs_cpool_flock_locker_t locker_type;
	locking_test_type locking_type;
	bool blocking;

};

void lock_entry(struct lock_test_entry &entry, isi_cbm_scoped_flock &sf)
{
	struct isi_error *error = NULL;

	if (entry.locking_type == LTT_SR)
		sf.lock_sr(fd, entry.blocking, &error);
	else
		sf.lock_ex(fd, entry.blocking, &error);

	if (entry.expect_error == CBM_SUCCESS) {
		fail_if (error, "Failed to lock the file: %#{}",
		     isi_error_fmt(error));
	} else {
		bool err_match = isi_cbm_error_is_a(error, entry.expect_error);
		fail_if (!err_match, "Expected error %d not found: %#{}",
		     isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;
	}
}

void test_locking(lock_test_entry *entries, uint32_t num)
{
	if (num == 0)
		return;

	lock_test_entry &entry = entries[0];

	if (entry.locker_class == LTC_STUB) {
		scoped_stub_lock sl;
		lock_entry(entry, sl);
		test_locking(entries + 1, num - 1);
	} else {
		scoped_sync_lock sl;
		lock_entry(entry, sl);
		test_locking(entries + 1, num - 1);
	} 
}

/**
 * Verify scoped stub and sync locks (in that order) as thread-level exclusive
 * locks works
 */ 
TEST(test_cbm_scoped_flock_thread_basic,  mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	lock_test_entry entries[] = {
	    {CBM_SUCCESS, LTC_STUB, CFLT_THREAD, LTT_EX, true},
	    {CBM_SUCCESS, LTC_SYNCH, CFLT_THREAD, LTT_EX, true}};
	size_t num = sizeof(entries)/sizeof(entries[0]);

	// exclusive
	test_locking(entries, num);

	// shared
	entries[0].locking_type = entries[1].locking_type = LTT_SR;
	test_locking(entries, num);

}

/**
 * Verify scoped stub and sync locks (in that order) as process-level exclusive
 * locks works
 */ 
TEST(test_cbm_scoped_flock_process_basic,  mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	lock_test_entry entries[] = {
	    {CBM_SUCCESS, LTC_STUB, CFLT_PROCESS, LTT_EX, true},
	    {CBM_SUCCESS, LTC_SYNCH, CFLT_PROCESS, LTT_EX, true}
	    };
	size_t num = sizeof(entries)/sizeof(entries[0]);

	// exclusive
	test_locking(entries, num);

	// shared
	entries[0].locking_type = entries[1].locking_type = LTT_SR;
	test_locking(entries, num);
}

/**
 * Verify that two different threads can take a shared read scoped lock
 */ 
TEST(test_cbm_scoped_flock_shared_read,  mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	// Note: using CFLT_PROCESS in the first entry to artificially create
	// a different locker than the one in the second entry
	lock_test_class tcs[] = {LTC_STUB, LTC_SYNCH};
	for (size_t i = 0; i < sizeof(tcs)/sizeof(tcs[0]); i++) {
		lock_test_entry entries[] = {
		    {CBM_SUCCESS, LTC_SYNCH, CFLT_PROCESS, LTT_SR, true},
		    {CBM_SUCCESS, LTC_SYNCH, CFLT_THREAD, LTT_SR, true}
		    };
	size_t num = sizeof(entries)/sizeof(entries[0]);
	test_locking(entries, num);
	}

}

/**
 * Verify that shared read scoped locks can be stacked 
 */ 
TEST(test_cbm_scoped_flock_stackability,  mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	lock_test_class tcs[] = {LTC_STUB, LTC_SYNCH};
	for (size_t i = 0; i < sizeof(tcs)/sizeof(tcs[0]); i++) {
		lock_test_entry entries[] = {
		    {CBM_SUCCESS, tcs[i], CFLT_PROCESS, LTT_SR, true},
		    {CBM_SUCCESS, tcs[i], CFLT_PROCESS, LTT_SR, true}
		    };
		size_t num = sizeof(entries)/sizeof(entries[0]);
		test_locking(entries, num);
	}
}


/**
 * Verify various case where lock stacking must not happen
 */ 
TEST(test_cbm_scoped_flock_no_stackability,  mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	lock_test_class tcs[] = {LTC_STUB, LTC_SYNCH};
	cbm_error_type_values err = CBM_DOMAIN_LOCK_CONTENTION;

	// scoped exclusive locks at process level
	for (size_t i = 0; i < sizeof(tcs)/sizeof(tcs[0]); i++) {
		lock_test_entry entries[] = {
		    {CBM_SUCCESS, tcs[i], CFLT_PROCESS, LTT_EX, true},
		    {err, tcs[i], CFLT_PROCESS, LTT_EX, false}
		    };
		size_t num = sizeof(entries)/sizeof(entries[0]);
		test_locking(entries, num);
	}

	// scoped exclusive locks at thread level
	for (size_t i = 0; i < sizeof(tcs)/sizeof(tcs[0]); i++) {
		lock_test_entry entries[] = {
		    {CBM_SUCCESS, tcs[i], CFLT_THREAD, LTT_EX, true},
		    {err, tcs[i], CFLT_PROCESS, LTT_EX, false}
		    };
		size_t num = sizeof(entries)/sizeof(entries[0]);
		test_locking(entries, num);
	}

	// scoped exclusive lock followed by scoped shared-read lock
	for (size_t i = 0; i < sizeof(tcs)/sizeof(tcs[0]); i++) {
		lock_test_entry entries[] = {
		    {CBM_SUCCESS, tcs[i], CFLT_PROCESS, LTT_EX, true},
		    {err, tcs[i], CFLT_PROCESS, LTT_SR, false}
		    };
		size_t num = sizeof(entries)/sizeof(entries[0]);
		test_locking(entries, num);
	}

	// scoped shared-read lock followed by scoped exclusive lock
	for (size_t i = 0; i < sizeof(tcs)/sizeof(tcs[0]); i++) {
		lock_test_entry entries[] = {
		    {CBM_SUCCESS, tcs[i], CFLT_PROCESS, LTT_SR, true},
		    {err, tcs[i], CFLT_PROCESS, LTT_EX, false}
		    };
		size_t num = sizeof(entries)/sizeof(entries[0]);
		test_locking(entries, num);
	}


}

