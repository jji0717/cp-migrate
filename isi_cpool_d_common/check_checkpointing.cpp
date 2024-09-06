
#include <check.h>
#include <list>
#include <fcntl.h>
#include <map>
#include <memory>
#include <stdio.h>
#include <string>
#include <unistd.h>

#include <isi_cpool_d_common/task.h>
#include "ckpt.h"
#include <isi_ilog/ilog.h>

using namespace isi_cloud;

TEST_FIXTURE(suite_setup)
{
	struct ilog_app_init init = {
		"check_checkpointing",			 // full app name
		"cpool_d_common",				 // component
		"",								 // job (opt)
		"check_checkpointing",			 // syslog program
		IL_TRACE_PLUS,					 // default level
		false,							 // use syslog
		false,							 // use stderr
		false /*true*/,					 // log_thread_id
		LOG_DAEMON,						 // syslog facility
		"/root/check_checkpointing.log", // log file
		IL_ERR_PLUS,					 // syslog_threshold
		NULL,							 // tags
	};
	ilog_init(&init, false, true);
	ilog(IL_NOTICE, "ilog initialized\n");
}

TEST_FIXTURE(suite_teardown)
{
}

TEST_FIXTURE(setup_test)
{
}

TEST_FIXTURE(teardown_test)
{
}

SUITE_DEFINE_FOR_FILE(check_checkpoint,
					  .mem_check = CK_MEM_LEAKS,
					  .suite_setup = suite_setup,
					  .suite_teardown = suite_teardown,
					  .test_setup = setup_test,
					  .test_teardown = teardown_test,
					  .timeout = 60,
					  .fixture_timeout = 1200);

class sync_test_check_point
{
public:
	static bool get_ckpt_data(const void *context,
							  void **data, size_t *ckpt_size,
							  bool *del_pending, off_t offset);

	static bool set_ckpt_data(const void *context,
							  void *data, size_t ckpt_size,
							  bool *del_pending, off_t offset);

	static void delete_ckpt(const void *context);

	sync_test_check_point(ifs_lin_t lin, ifs_snapid_t snapid);

private:
	std::auto_ptr<ckpt_intf> impl;
};

void sync_test_check_point::delete_ckpt(const void *context)
{
	sync_test_check_point *ckpt = (sync_test_check_point *)context;

	return ckpt->impl->remove();
}

bool sync_test_check_point::get_ckpt_data(const void *context,
										  void **data, size_t *ckpt_size,
										  bool *del_pending, off_t offset)
{
	sync_test_check_point *ckpt = (sync_test_check_point *)context;

	return ckpt->impl->get(*data, *ckpt_size, offset);
}

bool sync_test_check_point::set_ckpt_data(const void *context,
										  void *data, size_t ckpt_size,
										  bool *del_pending, off_t offset)
{
	sync_test_check_point *ckpt = (sync_test_check_point *)context;

	return ckpt->impl->set(data, ckpt_size, offset);
}

sync_test_check_point::sync_test_check_point(ifs_lin_t lin,
											 ifs_snapid_t snapid)
{
	impl.reset(new file_ckpt(lin, snapid));
}

TEST(test_checkpointing)
{
	const char *path = "/ifs/test_checkpointing.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	struct stat sb = {0};
	int status = fstat(fd, &sb);

	fail_if((status == -1),
			"Stat of file %s failed with error: %s (%d)",
			path, strerror(errno), errno);
	ifs_lin_t lin = sb.st_ino;

	sync_test_check_point sckpt(lin, HEAD_SNAPID);
	bool del_pending = false;
	// Prepare data to be written
	char data[1024];
	for (int i = 0; i < 1024; i++)
	{
		data[i] = i;
	}
	// int i;
	// for (i = 0; i < 100; i++)
	// {
	// 	data[i] = 'a';
	// }
	// for (i = 100; i < 200; i++)
	// {
	// 	data[i] = 'b';
	// }
	// for (i = 200; i < 300; i++)
	// {
	// 	data[i] = 'c';
	// }

	// Write all 1024 to checkpoint
	fail_if(!sckpt.set_ckpt_data(&sckpt, data, 1024, &del_pending, 0), ////offset: 0
			"Writing checkpoint failed");
	// Real all 1024 from checkpoint
	size_t rsize = 1024;
	void *rdata = NULL;
	fail_if(!sckpt.get_ckpt_data(&sckpt, &rdata, &rsize, &del_pending, 0),
			"Reading checkpoint failed");
	// Make sure that we indeed write the right data
	// and read the right data.
	fail_if(rsize != 1024,
			"Didn't read 1024 bytes %lld", rsize);
	fail_if(memcmp(data, rdata, rsize),
			"Read data is not what we wrote.");
	free((void *)rdata);
	rdata = NULL;
	// Make sure we are writing correctly 100 bytes from offset.
	// return;

	fail_if(!sckpt.set_ckpt_data(&sckpt, &(data[100]), 100, &del_pending, 50), /// 100-150
			"Writing checkpoint failed");
	rsize = 100;
	fail_if(!sckpt.get_ckpt_data(&sckpt, &rdata, &rsize, &del_pending, 50),
			"Reading checkpoint failed");
	fail_if(rsize != 100,
			"Didn't read 100 bytes %lld", rsize);
	fail_if(memcmp(&(data[100]), rdata, rsize),
			"Read data is not what we wrote.");
	free((void *)rdata);

	sckpt.delete_ckpt(&sckpt);

	fsync(fd);
	close(fd);
	remove(path);
}
