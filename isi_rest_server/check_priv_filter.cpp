#include "priv_filter.h"

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <sys/isi_ntoken.h>
#include <sys/isi_privilege.h>

SUITE_DEFINE_FOR_FILE(priv_filter);

static const int PRIV_A = ISI_PRIV_LOGIN_CONSOLE;
static const int PRIV_B = ISI_PRIV_AUTH;
static const int PRIV_C = ISI_PRIV_SMB;

static const int ADMIN_USER = 10;

static int
create_priv(int priv) {
	struct native_token tok = {};
	struct isi_priv privs_in[] = {
		{-1, ISI_PRIV_FLAG_READWRITE}};
	privs_in[0].priv_id = priv;

	tok.nt_uid = ADMIN_USER;
	tok.nt_gid = ADMIN_USER;
	tok.nt_iprivs = privs_in;
	tok.nt_niprivs = 1;
	tok.nt_fields = NTOKEN_UID | NTOKEN_GID | NTOKEN_IPRIVS;

	return buildtcred(&tok, 0);
}

static int
create_priv(int first, int second) {
	struct native_token tok = {};
	struct isi_priv privs_in[] = {
		{-1, ISI_PRIV_FLAG_READWRITE},
		{-1, ISI_PRIV_FLAG_READWRITE}};

	privs_in[0].priv_id = first;
	privs_in[1].priv_id = second;

	tok.nt_uid = ADMIN_USER;
	tok.nt_gid = ADMIN_USER;
	tok.nt_iprivs = privs_in;
	tok.nt_niprivs = 2;
	tok.nt_fields = NTOKEN_UID | NTOKEN_GID | NTOKEN_IPRIVS;

	return buildtcred(&tok, 0);
}


TEST(priv_filter_default_priv) {
	priv_filter filter_A(PRIV_A);

	int user_A = create_priv(PRIV_A);
	int user_AB = create_priv(PRIV_A, PRIV_B);
	int user_C = create_priv(PRIV_C);

	fail_unless(filter_A.check_priv(user_A, request::GET) == true);
	fail_unless(filter_A.check_priv(user_AB, request::GET) == true);
	fail_unless(filter_A.check_priv(user_C, request::GET) == false);
}

TEST(priv_filter_or) {
	priv_filter filter_A_or_B(PRIV_A);
	filter_A_or_B.add_priv(PRIV_B);

	int user_A = create_priv(PRIV_A);
	int user_AB = create_priv(PRIV_A, PRIV_B);
	int user_C = create_priv(PRIV_C);

	fail_unless(filter_A_or_B.check_priv(user_A, request::GET) == true);
	fail_unless(filter_A_or_B.check_priv(user_AB, request::GET) == true);
	fail_unless(filter_A_or_B.check_priv(user_C, request::GET) == false);
}

TEST(priv_filter_and) {
	priv_filter filter_A_and_B(PRIV_A);
	filter_A_and_B.add_priv(PRIV_B);
	filter_A_and_B.require_all_privs();

	int user_A = create_priv(PRIV_A);
	int user_AB = create_priv(PRIV_A, PRIV_B);
	int user_C = create_priv(PRIV_C);

	fail_unless(filter_A_and_B.check_priv(user_A, request::GET) == false);
	fail_unless(filter_A_and_B.check_priv(user_AB, request::GET) == true);
	fail_unless(filter_A_and_B.check_priv(user_C, request::GET) == false);
}

TEST(priv_filter_method_only) {
	priv_filter filter_post_only;
	filter_post_only.add_priv(PRIV_A, request::POST);
	filter_post_only.add_priv(PRIV_B, request::POST);

	int user_A = create_priv(PRIV_A);
	int user_AB = create_priv(PRIV_A, PRIV_B);
	int user_C = create_priv(PRIV_C);

	fail_unless(filter_post_only.check_priv(user_A, request::PUT) == true);
	fail_unless(filter_post_only.check_priv(user_AB, request::GET) == true);
	fail_unless(filter_post_only.check_priv(user_C, request::DELETE) == true);

	fail_unless(filter_post_only.check_priv(user_A, request::POST) == true);
	fail_unless(filter_post_only.check_priv(user_AB, request::POST) == true);
	fail_unless(filter_post_only.check_priv(user_C, request::POST) == false);
}

TEST(priv_filter_method_only_and) {
	priv_filter filter_post_only;
	filter_post_only.add_priv(PRIV_A, request::POST);
	filter_post_only.add_priv(PRIV_B, request::POST);
	filter_post_only.require_all_privs(request::POST);

	int user_A = create_priv(PRIV_A);
	int user_AB = create_priv(PRIV_A, PRIV_B);
	int user_C = create_priv(PRIV_C);

	fail_unless(filter_post_only.check_priv(user_A, request::PUT) == true);
	fail_unless(filter_post_only.check_priv(user_AB, request::GET) == true);
	fail_unless(filter_post_only.check_priv(user_C, request::DELETE) == true);

	fail_unless(filter_post_only.check_priv(user_A, request::POST) == false);
	fail_unless(filter_post_only.check_priv(user_AB, request::POST) == true);
	fail_unless(filter_post_only.check_priv(user_C, request::POST) == false);
}






