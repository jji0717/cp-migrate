#include <ifs/ifs_lin_open.h>
#include <isi_cpool_d_common/cpool_fd_store.h>
#include <isi_cpool_d_common/task.h>
#include <isi_ilog/ilog.h>

#include "ckpt.h"

file_ckpt::file_ckpt(ifs_lin_t lin, ifs_snapid_t snapid)
	: ckpt_fd_(-1)
{
	struct fmt FMT_INIT_CLEAN(ckpt_fname);
	fmt_print(&ckpt_fname, "%{}", lin_snapid_fmt(lin, snapid));

	file_name_ = fmt_string(&ckpt_fname);
	ilog(IL_INFO, "%s called file_name:%s", __func__, file_name_.c_str());
}

file_ckpt::file_ckpt(const std::string &fname)
	: ckpt_fd_(-1), file_name_(fname)
{
}

file_ckpt::~file_ckpt()
{
	if (ckpt_fd_ >= 0)
		close(ckpt_fd_);
}

bool file_ckpt::get(void *&data, size_t &size, off_t offset)
{
	ASSERT(data == NULL && size > 0 && offset >= 0);

	struct isi_error *error = NULL;
	ssize_t ret = 0;

	open_if_not(&error);
	ON_ISI_ERROR_GOTO(out, error);

	data = malloc(size);
	ASSERT(data, "out of memory: %ld bytes allocation failed", size);

	ret = pread(ckpt_fd_, (void *)data, size, offset); /////pread ???

	if (ret < 0)
	{
		ON_SYSERR_GOTO(out, errno, error,
					   "read from file failed, offset: %zd size: %zd",
					   offset, size);
	}
	else if (ret == 0)
	{
		free(data);
		data = NULL;
	}

	size = ret;

out:
	if (error)
	{
		free(data);
		data = NULL;
		size = 0;

		ilog(IL_ERR, "error reading checkpoint file: %#{}",
			 isi_error_fmt(error));
		isi_error_free(error);
		return false;
	}
	return true;
}

bool file_ckpt::set(void *data, size_t size, off_t offset)
{
	ASSERT(data && size > 0 && offset >= 0);

	struct isi_error *error = NULL;

	open_if_not(&error);
	ON_ISI_ERROR_GOTO(out, error);

	if (pwrite(ckpt_fd_, data, size, offset) != (ssize_t)size) ////pwrite ???
	{
		ON_SYSERR_GOTO(out, errno, error,
					   "write to file failed, offset: %zd size: %zd",
					   offset, size);
	}

out:
	if (error)
	{
		ilog(IL_ERR, "error writing to checkpoint file: %#{}",
			 isi_error_fmt(error));
		isi_error_free(error);
		return false;
	}
	return true;
}

void file_ckpt::remove()
{
	struct isi_error *error = NULL;

	int dir_fd = cpool_fd_store::getInstance().get_file_ckpt_dir_fd(&error);
	ON_ISI_ERROR_GOTO(out, error);

	if (ckpt_fd_ >= 0)
		close(ckpt_fd_);
	ckpt_fd_ = -1;

	if (unlinkat(dir_fd, file_name_.c_str(), 0) < 0)
	{
		ON_SYSERR_GOTO(out, errno, error,
					   "failed to remove file %s",
					   file_name_.c_str());
	}

out:
	if (error)
	{
		ilog(IL_ERR, "error removing checkpoint file: %#{}",
			 isi_error_fmt(error));
		isi_error_free(error);
	}
}

void file_ckpt::open_if_not(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (ckpt_fd_ < 0)
	{
		int dir_fd =
			cpool_fd_store::getInstance().get_file_ckpt_dir_fd(&error); ///"/ifs/.ifsvar/modules/cloud/ckpt";
		ON_ISI_ERROR_GOTO(out, error);

		ckpt_fd_ = openat(dir_fd, file_name_.c_str(), O_CREAT | O_RDWR); /// 目录+文件名
		ilog(IL_INFO, "%s called ckpt_fd:%d filename:%s", __func__, ckpt_fd_, file_name_.c_str());
		if (ckpt_fd_ < 0)
		{
			ON_SYSERR_GOTO(out, errno, error,
						   "failed to open/create file: %s",
						   file_name_.c_str());
		}
	}

out:
	isi_error_handle(error, error_out);
}
