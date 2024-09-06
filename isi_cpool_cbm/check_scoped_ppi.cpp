
#include <isi_util/checker.h>
#include <isi_cpool_cbm/isi_cbm_policyinfo.h>

#include "isi_cbm_scoped_ppi.h"





TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(check_scoped_ppi,
    .mem_check = CK_MEM_LEAKS,
    .test_teardown = test_teardown,
    .timeout = 60,
    .fixture_timeout = 1200);

TEST_FIXTURE(test_teardown)
{
	close_ppi();
}

TEST(test_scoped_reader)
{
for (int i = 0; i < 2; i++) {
	struct isi_error *error = NULL;
	scoped_ppi_reader reader;

	const struct isi_cfm_policy_provider * ppi = reader.get_ppi(&error);
	fail_if(error, "Failed to get ppi %#{}", isi_error_fmt(error));

	fail_if(ppi->sp_context == NULL, "sp_context is null");
	fail_if(ppi->cp_context == NULL, "cp_context is null");
}

}

TEST(test_scoped_reader_nested)
{
	struct isi_error *error = NULL;
	{
		scoped_ppi_reader reader_1;

		const struct isi_cfm_policy_provider * ppi_1 =
			reader_1.get_ppi(&error);
		fail_if(error, "Failed to get ppi %#{}",
			isi_error_fmt(error));

		fail_if(ppi_1->sp_context == NULL, "sp_context is null");
		fail_if(ppi_1->cp_context == NULL, "cp_context is null");
		{
			scoped_ppi_reader reader_2;

			const struct isi_cfm_policy_provider * ppi_2 =
			reader_2.get_ppi(&error);
			fail_if(error, "Failed to get ppi %#{}",
			    isi_error_fmt(error));
			fail_if(ppi_1 != ppi_2);
		}
	}
}



TEST(test_scoped_writer)
{
for (int i = 0; i < 2; i++) {
	struct isi_error *error = NULL;
	scoped_ppi_writer writer;

	struct isi_cfm_policy_provider * ppi = writer.get_ppi(&error);
	fail_if(error, "Failed to get ppi %#{}", isi_error_fmt(error));

	fail_if(ppi->sp_context == NULL, "sp_context is null");
	fail_if(ppi->cp_context == NULL, "cp_context is null");
	
	writer.commit_smartpools(ppi, &error);
	fail_if(error, "Failed to commit %#{}", isi_error_fmt(error));
}
}

TEST(test_empty_ptr_reader)
{
	ptr_ppi_reader reader;

	void * val = reader.get();
	fail_if(val != NULL);
}

TEST(test_ptr_reader)
{
	struct isi_error *error = NULL;
	ptr_ppi_reader reader = get_ppi_reader(&error);

	fail_if(error, "Failed to get ppi reader %#{}", isi_error_fmt(error));

	const struct isi_cfm_policy_provider * ppi = reader->get_ppi(&error);
	fail_if(error, "Failed to get ppi %#{}", isi_error_fmt(error));

	fail_if(ppi->sp_context == NULL, "sp_context is null");
	fail_if(ppi->cp_context == NULL, "cp_context is null");

}

TEST(test_ptr_writer)
{
	struct isi_error *error = NULL;

	ptr_ppi_writer writer = get_ppi_writer(&error);
	fail_if(error, "Failed to get ppi writer %#{}", isi_error_fmt(error));

	struct isi_cfm_policy_provider * ppi = writer->get_ppi(&error);
	fail_if(error, "Failed to get ppi %#{}", isi_error_fmt(error));

	fail_if(ppi->sp_context == NULL, "sp_context is null");
	fail_if(ppi->cp_context == NULL, "cp_context is null");

	writer->commit_cpool(ppi, &error);
	fail_if(error, "Failed to commit %#{}", isi_error_fmt(error));

	ppi = writer->get_ppi(&error);

	writer->commit_smartpools(ppi, &error);
	fail_if(error, "Failed to commit %#{}", isi_error_fmt(error));

}

