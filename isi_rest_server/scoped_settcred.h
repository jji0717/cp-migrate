#ifndef __SCOPED_SETTCRED_H__
#define __SCOPED_SETTCRED_H__

#include <sys/types.h>

class scoped_settcred {
public:
	scoped_settcred(int fd, bool run_as_user);

	~scoped_settcred();

private:
	const bool run_as_user_;

	/* uncopyable, unassignable */
	scoped_settcred(const scoped_settcred &);
	scoped_settcred &operator=(const scoped_settcred &);
};

class scoped_settcred_root {
public:
	scoped_settcred_root(int fd, bool run_as_root);

	~scoped_settcred_root();

private:
	const int  user_token_fd_;
	const bool run_as_root_;

	/* uncopyable, unassignable */
	scoped_settcred_root(const scoped_settcred_root &);
	scoped_settcred_root &operator=(const scoped_settcred_root &);
};

/* Change the zone of the current thread */
class scoped_set_zone {
public:
	scoped_set_zone(int cred_fd, bool use_cred, zid_t zid);
	~scoped_set_zone();

private:
	bool set_zid_;

	/* uncopyable, unassignable */
	scoped_set_zone(const scoped_set_zone &);
	scoped_set_zone &operator=(const scoped_set_zone &);
};

#endif

