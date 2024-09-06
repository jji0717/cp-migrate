#include <sys/mount.h>
#include <sys/param.h>

#include <errno.h>

#include <isi_rest_server/api_error.h>
#include <isi_util/isi_error.h>
#include <isi_util_cpp/isi_exception.h>

#include "file_utils.hpp"

scoped_fd::scoped_fd(const std::string &path, const std::string &fld, int flags)
	: fd_(-1), path_(path), field_(fld)
{
	int fd = open(path.c_str(), flags);

	if (fd < 0)
		throw_from_errno(errno);

	if (fstat(fd, &stat_) != 0) {
		close(fd);
		throw_from_errno(errno);
	}

	fd_ = fd;
}

scoped_fd::scoped_fd(int fd, const std::string &fld)
	: fd_(-1), field_(fld)
{
	if (fd < 0)
		throw isi_exception("Attempt to manage invalid fd: %d", fd);

	char buf[32];
	sprintf(buf, "FD=%d", fd);

	if (fstat(fd, &stat_) != 0) {
		close(fd);
		throw_from_errno(errno);
	}

	path_ = buf;
	fd_   = fd;
}

scoped_fd::scoped_fd(int fd, const allow_invalid &u, const std::string &fld)
	: fd_(fd), field_(fld)
{
	char buf[32];
	sprintf(buf, "FD=%d", fd);
	memset(&stat_, 0, sizeof stat_);
	path_ = buf;
}

void
scoped_fd::throw_from_errno(int error) const
{
	const char *str = path_.c_str();

	switch (error) {
	case ENOENT:
	case ENOTDIR:
		throw api_exception(field_,
		    field_.empty() ? AEC_NOT_FOUND : AEC_BAD_REQUEST,
		    "Path not found '%s'", str);

	case EACCES:
	case EPERM:
		throw api_exception(field_, AEC_FORBIDDEN,
		    "Path access denied '%s'", str);

	default:
		throw ISI_ERRNO_EXCEPTION("Error opening path '%s'", str);
	}
}

scoped_ifs_fd::scoped_ifs_fd(const std::string &path, const std::string &fld,
    int flags)
	: scoped_fd(path, fld, flags)
{
	struct statfs sfs;

	if (fstatfs(*this, &sfs) != 0)
		throw_from_errno(errno);

	if (strcmp(sfs.f_fstypename, "efs") != 0)
		throw api_exception(get_field(), AEC_BAD_REQUEST,
		    "non /ifs path: '%s'", path.c_str());
}
