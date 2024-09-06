#ifndef FILE_UTILS_HPP_
#define FILE_UTILS_HPP_

#include <sys/stat.h>

#include <fcntl.h>
#include <unistd.h>

#include <string>

class scoped_fd {
public:
	struct allow_invalid { };

	/** Open and construct.
	 *
	 * @param p path
	 * @param n input field name associated with file, if empty string,
	 *          the file is a resource rather than associated with an
	 *          input field.
	 */
	scoped_fd(const std::string &p, const std::string &n = "",
	    int f = O_RDONLY);

	/** Manage passed in fd.
	 *
	 * @param f file descriptor to manage
	 * @param n input field name associated with file, if empty string,
	 *          the file is a resource rather than associated with an
	 *          input field.
	 */
	scoped_fd(int f, const std::string &n = "");

	/** Manage passed in fd, allow invalid fd.
	 *
	 * @param f file descriptor to manage
	 * @param u unused overload picker
	 * @param n input field name associated with file, if empty string,
	 *          the file is a resource rather than associated with an
	 *          input field.
	 */
	scoped_fd(int f, const allow_invalid &u, const std::string &n = "");

	inline ~scoped_fd();

	/** Get fd */
	inline operator int() const;

	/** Return stat() data */
	inline const struct stat &get_stat() const;

	inline const std::string &get_field() const { return field_; }

protected:
	void throw_from_errno(int error) const;

private:
	int         fd_;
	std::string path_;
	std::string field_;
	struct stat stat_;

	/* uncopyable, unassignable */
	scoped_fd(const scoped_fd &);
	scoped_fd &operator=(const scoped_fd &);
};

class scoped_ifs_fd : public scoped_fd {
public:
	/** Open and construct.
	 *
	 * Also checks to see if file is on /ifs.
	 * See @ref scoped_fd for params.
	 */
	scoped_ifs_fd(const std::string &p, const std::string &n = "",
	    int f = O_RDONLY);
};

/*  _       _ _
 * (_)_ __ | (_)_ __   ___
 * | | '_ \| | | '_ \ / _ \
 * | | | | | | | | | |  __/
 * |_|_| |_|_|_|_| |_|\___|
 */

scoped_fd::~scoped_fd()
{
	if (fd_ >= 0)
		close(fd_);
}

scoped_fd::operator int() const
{
	return fd_;
}

const struct stat &
scoped_fd::get_stat() const
{
	return stat_;
}


#endif
