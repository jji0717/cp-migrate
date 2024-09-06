#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include "isi_cbm_file.h"
#include "isi_cbm_mapper.h"

#include "check_cbm_common.h"
#include "isi_cpool_cbm.h"
#include "isi_cbm_test_util.h"

#include <check.h>

using namespace isi_cloud;


TEST_FIXTURE(suite_setup)
{
	cbm_test_common_setup(true, true);
}

TEST_FIXTURE(suite_teardown)
{
	cbm_test_common_cleanup(true, true);
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

SUITE_DEFINE_FOR_FILE(check_compression_filter,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 360,
    .fixture_timeout = 1200);

TEST(test_cbm_file_compression_filter,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	// list of files to check
	struct test_file_info test_files [] =  {
	    // following files are supposed to be compressed
	    {"/etc/services",
	     "/ifs/data/ran_unit_test_compress.txt-2",
	     REPLACE_FILE
	    },
	    {"/etc/services",
	     "/ifs/data/ran_unit_test_compress.bz-2",
	     REPLACE_FILE
	    },
	    {"/etc/services",
	     "/ifs/data/ran_unit_test_compress.",
	     REPLACE_FILE
	    },
	    {"/etc/services",
	     "/ifs/data/ran_unit_test_compress",
	     REPLACE_FILE
	    },
	    // following files are supposed to be skipped
	    {"/etc/services",
	     "/ifs/data/ran_unit_test_compress.bz",
	     REPLACE_FILE
	    },
	    {"/etc/services",
	     "/ifs/data/ran_unit_test_compress.ogg",
	     REPLACE_FILE
	    }
	};

	struct stat statbuf;
	int cnt = int(sizeof(test_files)/sizeof(test_files[0]));

	for (int i=0; i<cnt; i++) {
		struct fmt FMT_INIT_CLEAN(str);
		struct isi_error *error;
		struct isi_cbm_file *file_obj = NULL;
		ifs_lin_t lin;

		unlink(test_files[i].output_file);

		fmt_print(&str, CBM_ACCESS_ON "cat %s >> %s" CBM_ACCESS_OFF,
		    test_files[i].input_file, test_files[i].output_file);

		for (int j=0; j<i+1; j++)
			fail_if(system(fmt_string(&str)),
			    "Can't create test file %s", fmt_string(&str));

		fail_if(stat(test_files[i].output_file, &statbuf) == -1,
		    "Stat of file %s failed with error: %s (%d)",
		    test_files[i].output_file, strerror(errno), errno);

		lin = statbuf.st_ino;

		// archive the file
		error = cbm_test_archive_file_with_pol(
		    lin, CBM_TEST_COMMON_POLICY_RAN_C, true);
		if (error != NULL) {
			fail_if(true, "Unable to stub file lin %#{}, %#{}",
			    lin_fmt(lin), isi_error_fmt(error));

			isi_error_free(error);
		}

		// check file object
		file_obj = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);

		fail_if(!file_obj, "Unable to open file %s, %#{}",
		    test_files[i].output_file, isi_error_fmt(error));

		file_obj->mapinfo->is_compressed();

		if (i < 4)
			fail_if(!file_obj->mapinfo->is_compressed(),
			    "File %s not compressed", test_files[i].output_file);
		else
			fail_if(file_obj->mapinfo->is_compressed(),
			    "File %s compressed", test_files[i].output_file);

		isi_cbm_file_close(file_obj, &error);
		fail_if(!file_obj, "Unable to close file %s, %#{}",
		    test_files[i].output_file, isi_error_fmt(error));

		// remove test file
		unlink(test_files[i].output_file);
	}
}
