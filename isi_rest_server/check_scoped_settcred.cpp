#include "scoped_settcred.h"

#include <unistd.h>

#include <sys/types.h>

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <sys/isi_ntoken.h>

SUITE_DEFINE_FOR_FILE(scoped_settcred);

static const u_int ROOT_USER = 0;
static const u_int ADMIN_USER = 10;
static const u_int FTP_USER = 14;
static const u_int FTP_GROUP = 80;

static int
create_user(void)
{
	struct native_token tok = {};

	tok.nt_uid = ADMIN_USER;
	tok.nt_gid = ADMIN_USER;
	tok.nt_fields = NTOKEN_UID | NTOKEN_GID;

	return buildtcred(&tok, 0);
}

static u_int
gettuid(int thread)
{
	int user_fd = gettcred(NULL, thread);
	fail_unless(user_fd >= 0, "Errno: %d", errno);

	struct native_token *tok = fd_to_ntoken(user_fd);
	fail_unless(tok != NULL);

	u_int uid = tok->nt_uid;
	ntoken_free(tok);

	return uid;
}

#define fail_unless_tuid(uid) \
    fail_unless(gettuid(uid == ROOT_USER ? 0 : 1) == uid)

TEST(test_scoped_settcred)
{
	int user_fd = create_user();
	fail_unless(user_fd >= 0);

	fail_unless_tuid(ROOT_USER);

	{
		scoped_settcred cred(user_fd, false);
		fail_unless_tuid(ROOT_USER);
	}

	{
		scoped_settcred cred(user_fd, true);
		fail_unless_tuid(ADMIN_USER);
	}

	fail_unless_tuid(ROOT_USER);
}

TEST(test_scoped_settcred_root)
{
	int user_fd = create_user();
	fail_unless(user_fd >= 0);

	fail_unless_tuid(ROOT_USER);

	{
		scoped_settcred cred(user_fd, true);
		fail_unless_tuid(ADMIN_USER);

		{
			scoped_settcred_root cred2(user_fd, false);
			fail_unless_tuid(ADMIN_USER);
		}

		{
			scoped_settcred_root cred2(user_fd, true);
			fail_unless_tuid(ROOT_USER);
		}

		fail_unless_tuid(ADMIN_USER);
	}

	fail_unless_tuid(ROOT_USER);
}

TEST(test_scoped_setzone)
{
	int user_fd = create_user();
	fail_unless(user_fd >= 0);

	fail_unless_tuid(ROOT_USER);

	{
		scoped_set_zone sz(user_fd, false, 5);
		fail_unless_tuid(ROOT_USER);
	}

	fail_unless_tuid(ROOT_USER);
}

