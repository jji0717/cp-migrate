#ifndef TEST_HELPER_H
#define TEST_HELPER_H
void init_pw_ctx(struct migr_pworker_ctx *pw_ctx);

u_int64_t repstate_tester(char *root, const char *policy, char *repstate_name,
    struct isi_error **error_out);

#define fail_if_isi_error(err) \
do {\
	if (err) {\
		struct fmt FMT_INIT_CLEAN(fmt);\
		fmt_print(&fmt, "isi_error: %{}", isi_error_fmt(err));\
		fail_unless(!err, "%s", fmt_string(&fmt));\
		fmt_clean(&fmt);\
	}\
} while(0)


#endif

