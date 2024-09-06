#include <check.h>
#include <ifs/ifs_types.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_sbtree/sbtree.h>
#include <isi_util/isi_error.h>
#include <ifs/btree/btree_on_disk.h>
#include <ifs/sbt/sbt.h>
#include <ifs/ifs_procoptions.h>
#include <sys/extattr.h>
#include <sys/stat.h>
#include <sys/uio.h>

#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_cbm/isi_cbm_write.h>
#include <isi_cpool_cbm/isi_cbm_scoped_flock.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_security/cpool_security.h>
#include <isi_cpool_cbm/isi_cbm_coi.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include <isi_cpool_cbm/isi_cbm_index_types.h>
#include <isi_cpool_d_common/hsbt.h>
#include <isi_cpool_d_common/task.h>

#include "check_cbm_common.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_error.h"
#include "isi_cbm_mapper.h"
#include "isi_cbm_sync.h"

#define CBM_ATIME_TEST_ENABLED

static bool old_atime_value;
static long old_atime_grace;

static int
set_and_save_atime(bool new_value, long new_grace)
{
	int ret = 0;
	size_t new_value_sz = sizeof(old_atime_value);
	size_t new_grace_sz = sizeof(old_atime_grace);

	ret = sysctlbyname(
	    "efs.bam.atime_enabled",
	    (void *)&old_atime_value, &new_value_sz,
	    (void *)(&new_value), sizeof(new_value));

	if (ret != 0)
		return ret;

	return sysctlbyname(
	    "efs.bam.atime_grace_period",
	    (void *)&old_atime_grace, &new_grace_sz,
	    (void *)(&new_grace), sizeof(new_grace));
}

static int
restore_atime()
{
	int ret_atime = 0;
	int ret_grace = 0;

	/*
	 * try reseting both even if first fails, return
	 * failure if either fails
	 */

	ret_atime = sysctlbyname(
	    "efs.bam.atime_enabled", NULL, NULL,
	    (void *)&old_atime_value, sizeof(old_atime_value));

	ret_grace = sysctlbyname(
	    "efs.bam.atime_grace_period", NULL, NULL,
	    (void *)&old_atime_grace, sizeof(old_atime_grace));

	return (ret_grace ? ret_grace : ret_atime);
}

static int
enable_atime(void)
{
	int ret = 0;

	const char value = 1;
	const long grace = 1;
	void *sysctl_value = (void *)(&(value));
	void *sysctl_grace = (void *)(&(grace));

	ret = sysctlbyname(
	    "efs.bam.atime_enabled", NULL, NULL,
	    sysctl_value, sizeof(value));
	fail_if(ret, "failed to set atime_enabled to %d", value);

	ret = sysctlbyname(
	    "efs.bam.atime_grace_period", NULL, NULL,
	    sysctl_grace, sizeof(grace));
	fail_if(ret, "failed to set atime_grace_period to %ld", grace);

	return 0;
}

static int
disable_atime(void)
{
	const char value = 0;
	void *sysctl_value = (void *)(&(value));

	return sysctlbyname(
	    "efs.bam.atime_enabled", NULL, NULL,
	    sysctl_value, sizeof(value));
}


TEST_FIXTURE(suite_setup)
{
#ifdef ENABLE_ILOG_USE
	struct ilog_app_init init =  {
		"check_times",			// full app name
		"cpool",			// component
		"",				// job (opt)
		"check_times",			// Syslog program
		IL_TRACE_PLUS|IL_DETAILS|IL_CP_EXT,	// default level
		false,				// use syslog
		true,				// use stderr
		false,				// log_thread_id
		LOG_DAEMON,			// syslog facility
		"/ifs/logs/check_times.log",	// log file
		IL_ERR_PLUS,			// syslog_threshold
		NULL,				//tags
	};

	char *env = getenv("CHECK_USE_ILOG");
	if (env != NULL) {
		ilog_init(&init, false, true);
		ilog(IL_NOTICE, "ilog initialized\n");
	}
#endif

	cbm_test_common_cleanup(true, true);
	cbm_test_common_setup(true, true);
	fail_if(!cbm_test_cpool_ufp_init(), "Fail points failed to initiate");
	set_and_save_atime(0, 1);
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
	// Make sure atime is not enabled
	restore_atime();

	cl_provider_module::destroy();
	checksum_cache_cleanup();
}

SUITE_DEFINE_FOR_FILE(check_times,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 120,
    .fixture_timeout = 1200);

#define BUF_READ_SIZE    8192

enum cktime_opt {
	CKTIME_NOOP = 0,
	CKTIME_READ,
	CKTIME_WRITE,
	CKTIME_TRUNCATE,
	CKTIME_STUB,
	CKTIME_STUB_OPEN,
	CKTIME_STUB_CLOSE,
	CKTIME_STUB_READ,
	CKTIME_STUB_WRITE,
	CKTIME_STUB_SYNC,
	CKTIME_STUB_INVAL,
	CKTIME_RECALL,
	CKTIME_RESET_BASE,
};

#define OP_STR_SIZE (32)
static  char op_string[OP_STR_SIZE];
char * op_to_str(enum cktime_opt operation)
{
	switch (operation) {
	case CKTIME_NOOP:
		strncpy(op_string, "CKTIME_NOOP", OP_STR_SIZE);
		break;
	case CKTIME_READ:
		strncpy(op_string, "CKTIME_READ", OP_STR_SIZE);
		break;
	case CKTIME_WRITE:
		strncpy(op_string, "CKTIME_WRITE", OP_STR_SIZE);
		break;
	case CKTIME_TRUNCATE:
		strncpy(op_string, "CKTIME_TRUNCATE", OP_STR_SIZE);
		break;
	case CKTIME_STUB:
		strncpy(op_string, "CKTIME_STUB", OP_STR_SIZE);
		break;
	case CKTIME_STUB_OPEN:
		strncpy(op_string, "CKTIME_STUB_OPEN", OP_STR_SIZE);
		break;
	case CKTIME_STUB_CLOSE:
		strncpy(op_string, "CKTIME_STUB_CLOSE", OP_STR_SIZE);
		break;
	case CKTIME_STUB_READ:
		strncpy(op_string, "CKTIME_STUB_READ", OP_STR_SIZE);
		break;
	case CKTIME_STUB_WRITE:
		strncpy(op_string, "CKTIME_STUB_WRITE", OP_STR_SIZE);
		break;
	case CKTIME_STUB_SYNC:
		strncpy(op_string, "CKTIME_STUB_SYNC", OP_STR_SIZE);
		break;
	case CKTIME_STUB_INVAL:
		strncpy(op_string, "CKTIME_STUB_INVAL", OP_STR_SIZE);
		break;
	case CKTIME_RECALL:
		strncpy(op_string, "CKTIME_RECALL", OP_STR_SIZE);
		break;
	case CKTIME_RESET_BASE:
		strncpy(op_string, "CKTIME_RESET_BASE", OP_STR_SIZE);
		break;
	default:
		snprintf(op_string, OP_STR_SIZE,
		    "unknown operation %d", operation);
		break;
	}
	return op_string;
}

struct step_info {
	long		pre_delay;
	size_t		size;
	off_t		offset;
	int		flags;
	enum cktime_opt	operation;
	long		post_delay;
	bool		sync_it;
	unsigned int	cmptime_flags;
	const char	*policy;
};

struct time_info {
	timespec ctime;
	timespec atime;
	timespec mtime;
};

#define CMPTIME_DISABLE		(0x0000)
#define CMPTIME_ENABLE		(0x0001)
#define CMPTIME_CTIME_MATCH	(0x0002)
#define CMPTIME_MTIME_MATCH	(0x0004)
#define CMPTIME_ATIME_MATCH	(0x0008)

#define CMPTIME_MATCH_NONE	(CMPTIME_ENABLE)

#define CMPTIME_MATCH_ALL	(CMPTIME_ENABLE |      \
				 CMPTIME_CTIME_MATCH | \
                                 CMPTIME_MTIME_MATCH | \
				 CMPTIME_ATIME_MATCH)

#define CMPTIME_MATCH_ALL_BUT_C	(CMPTIME_ENABLE |      \
				 CMPTIME_MTIME_MATCH | \
                                 CMPTIME_ATIME_MATCH)
#define CMPTIME_MATCH_ALL_BUT_M	(CMPTIME_ENABLE |      \
				 CMPTIME_CTIME_MATCH | \
                                 CMPTIME_ATIME_MATCH)
#define CMPTIME_MATCH_ALL_BUT_A	(CMPTIME_ENABLE |      \
				 CMPTIME_CTIME_MATCH | \
                                 CMPTIME_MTIME_MATCH)

#define CMPTIME_MATCH_JUST_C	(CMPTIME_ENABLE |      \
				 CMPTIME_CTIME_MATCH)
#define CMPTIME_MATCH_JUST_M	(CMPTIME_ENABLE |      \
				 CMPTIME_MTIME_MATCH)
#define CMPTIME_MATCH_JUST_A	(CMPTIME_ENABLE |      \
				 CMPTIME_ATIME_MATCH)

static void
reset_times(struct time_info *orig_times, int fd, const char *fname);

static bool
compare_times(struct time_info *orig_times, int fd, const char *fname,
    unsigned int cmptime_flags);


struct step_info  test_time_steps[] = {
	{ // before
		0,	// pre delay
		1024,	// size
		0,	// offset
		0,	// write flags
		CKTIME_WRITE, // operation;
		5,	// post delay;,
		true,  // sync
		CMPTIME_MATCH_NONE, //cmptime_flags
		NULL,
	},
	{ // reset0
		0,
		0,
		0,
		0,
		CKTIME_RESET_BASE,
		0,
		false,  // sync
		CMPTIME_DISABLE, //cmptime_flags
		NULL, // policy
	},
	{ // notime
		0,
		1024,
		0,
		O_IGNORETIME|O_SYNC,
		CKTIME_WRITE,
		5,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		NULL,
	},
	{ // after
		0,
		1024,
		0,
		0,
		CKTIME_WRITE,
		5,
		false,  // sync
		CMPTIME_MATCH_NONE, //cmptime_flags
		NULL,
	},
	{ // reset1
		0,
		0,
		0,
		0,
		CKTIME_RESET_BASE,
		0,
		false,  // sync
		CMPTIME_DISABLE, //cmptime_flags
		NULL, // policy
	},
	{ // stub
		0,
		0,
		0,
		0,
		CKTIME_STUB,
		5,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		CBM_TEST_COMMON_POLICY_RAN, // policy
	},
	{ // open
		0,
		0,
		0,
		0,
		CKTIME_STUB_OPEN,
		0,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		NULL, // policy
	},
	{ // poststub_fix
		0,
		1024,
		0,
		0,
		CKTIME_STUB_READ,
		5,
		false,  // sync
		CMPTIME_MATCH_ALL,
		NULL,
	},
	{ // reset2
		0,
		0,
		0,
		0,
		CKTIME_RESET_BASE,
		0,
		false,  // sync
		CMPTIME_DISABLE, //cmptime_flags
		NULL, // policy
	},
	{ // poststub_read
		0,
		1024,
		0,
		0,
		CKTIME_STUB_READ,
		5,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		NULL,
	},
	{ // poststub_write
		0,
		1024,
		0,
		0,
		CKTIME_STUB_WRITE,
		5,
		true,  // sync
		CMPTIME_MATCH_NONE, //cmptime_flags
		NULL,
	},
	{ // reset3
		0,
		0,
		0,
		0,
		CKTIME_RESET_BASE,
		0,
		false,  // sync
		CMPTIME_DISABLE, //cmptime_flags
		NULL, // policy
	},
	{ // poststub_wb
		0,
		1024,
		0,
		0,
		CKTIME_STUB_SYNC,
		0,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		NULL,
	},
	{ // poststub_inval
		0,
		1024,
		0,
		0,
		CKTIME_STUB_INVAL,
		0,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		NULL,
	},
	{ // poststub_end
		0,
		1024,
		0,
		0,
		CKTIME_STUB_READ,
		20,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		NULL,
	},
	{ // close
		0,
		0,
		0,
		0,
		CKTIME_STUB_CLOSE,
		0,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		NULL, // policy
	},
	{ // recall
		0,
		0,
		0,
		0,
		CKTIME_RECALL,
		0,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		NULL, // policy
	},
};

struct step_info  atime_enabled_steps[] = {
	{ // before
		0,	// pre delay
		1024,	// size
		0,	// offset
		0,	// write flags
		CKTIME_WRITE, // operation;
		5,	// post delay;,
		true,  // sync
		CMPTIME_MATCH_JUST_A, //cmptime_flags
		NULL,
	},
	{ // reset
		0,
		0,
		0,
		0,
		CKTIME_RESET_BASE,
		0,
		false,  // sync
		CMPTIME_DISABLE, //cmptime_flags
		NULL, // policy
	},
	{ // notime
		0,
		1024,
		0,
		O_IGNORETIME|O_SYNC,
		CKTIME_WRITE,
		5,
		true,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		NULL,
	},
	{ // after
		0,
		128,
		128,
		0,
		CKTIME_WRITE,
		5,
		true,  // sync
		CMPTIME_MATCH_JUST_A, //cmptime_flags
		NULL,
	},
	{ // reset
		0,
		0,
		0,
		0,
		CKTIME_RESET_BASE,
		0,
		false,  // sync
		CMPTIME_DISABLE, //cmptime_flags
		NULL, // policy
	},
	{ // stub
		0,
		0,
		0,
		0,
		CKTIME_STUB,
		5,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		CBM_TEST_COMMON_POLICY_RAN, // policy
	},
	{ // open
		0,
		0,
		0,
		0,
		CKTIME_STUB_OPEN,
		0,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		NULL, // policy
	},
	{ // poststub_fix
		0,
		1024,
		0,
		0,
		CKTIME_STUB_READ,
		5,
		false,  // sync
		// since CP1 read on uncached region does not get data from
		// disk but from cloud, the atime does not get updated.
		CMPTIME_MATCH_ALL,
		NULL,
	},
	{ // reset2
		0,
		0,
		0,
		0,
		CKTIME_RESET_BASE,
		0,
		false,  // sync
		CMPTIME_DISABLE, //cmptime_flags
		NULL, // policy
	},
	{ // poststub_read
		0,
		1024,
		0,
		0,
		CKTIME_STUB_READ,
		5,
		false,  // sync
		CMPTIME_MATCH_ALL_BUT_A, //cmptime_flags
		NULL,
	},
	{ // reset3
		0,
		0,
		0,
		0,
		CKTIME_RESET_BASE,
		0,
		false,  // sync
		CMPTIME_DISABLE, //cmptime_flags
		NULL, // policy
	},
	{ // poststub_write
		0,
		1024,
		0,
		0,
		CKTIME_STUB_WRITE,
		5,
		true,  // sync
		CMPTIME_MATCH_JUST_A, //cmptime_flags
		NULL,
	},
	{ // reset4
		0,
		0,
		0,
		0,
		CKTIME_RESET_BASE,
		0,
		false,  // sync
		CMPTIME_DISABLE, //cmptime_flags
		NULL, // policy
	},
	{ // poststub_truncate
		0,
		1020,
		0,
		0,
		CKTIME_TRUNCATE,
		5,
		true,  // sync
		CMPTIME_MATCH_JUST_A, //cmptime_flags
		NULL,
	},
	{ // reset5
		0,
		0,
		0,
		0,
		CKTIME_RESET_BASE,
		0,
		false,  // sync
		CMPTIME_DISABLE, //cmptime_flags
		NULL, // policy
	},
	{ // poststub_wb
		0,
		1024,
		0,
		0,
		CKTIME_STUB_SYNC,
		0,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		NULL,
	},
	{ // poststub_inval
		0,
		1024,
		0,
		0,
		CKTIME_STUB_INVAL,
		0,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		NULL,
	},
	{ // poststub_end
		0,
		1024,
		0,
		0,
		CKTIME_STUB_READ,
		20,
		false,  // sync
		CMPTIME_MATCH_ALL_BUT_A, //cmptime_flags
		NULL,
	},
	{ // reset6
		0,
		0,
		0,
		0,
		CKTIME_RESET_BASE,
		0,
		false,  // sync
		CMPTIME_DISABLE, //cmptime_flags
		NULL, // policy
	},
	{ // close
		0,
		0,
		0,
		0,
		CKTIME_STUB_CLOSE,
		0,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		NULL, // policy
	},
	{ // recall
		0,
		0,
		0,
		0,
		CKTIME_RECALL,
		0,
		false,  // sync
		CMPTIME_MATCH_ALL, //cmptime_flags
		NULL, // policy
	},
};


/*
 * common routine to archive file
 */
static void
stub_file(const char *pol_name, const char *fname, ifs_lin_t lin)
{
	struct isi_error *error = NULL;

	error = cbm_test_archive_file_with_pol(lin, pol_name, true);
		fail_if(error,
		    "Cannot archive %s, lin %#{}, error: %#{}",
		    fname, lin_fmt(lin), isi_error_fmt(error));
}

/*
 * common routine to recall file
 */
static void
recall_file(const char *fname, ifs_lin_t lin)
{
	struct isi_error *error = NULL;

	isi_cbm_recall(lin, HEAD_SNAPID, false, NULL, NULL, NULL, NULL,
	    &error);

	fail_if(error,
	    "Cannot recall %s, lin %#{}, error: %#{}",
	    fname, lin_fmt(lin), isi_error_fmt(error));
}

/* declaring this here */
void isi_cbm_invalidate_cache_i(struct isi_cbm_file *file_obj, ifs_lin_t lin,
    bool *dirty_data, bool override, bool cpool_d,
    struct isi_error **error_out);

/*
 * common routine to invalidate file
 */
static void
invalidate_file(struct isi_cbm_file *file_obj,
    const char *fname, ifs_lin_t lin)
{
	struct isi_error *error = NULL;
	bool dirty;

	isi_cbm_invalidate_cache_i(file_obj, lin, &dirty,
	    false, false, &error);

	fail_if(error,
	    "Cannot invalidate %s, lin %#{}, error: %#{}",
	    fname, lin_fmt(lin), isi_error_fmt(error));
}

static void
cbm_test_sync(uint64_t lin, uint64_t snapid,
    get_ckpt_data_func_t get_ckpt_cb, set_ckpt_data_func_t set_ckpt_cb,
    void *ctx, isi_error **error_out)
{
	isi_cbm_sync_option opt = {blocking: true, skip_settle: false};
	isi_cbm_sync_opt(lin, snapid, get_ckpt_cb, set_ckpt_cb, ctx,
	    opt, error_out);
}

static void *
fill_buf(char *buf, long fill_size) {
	int i;
	char *pbuf = buf;
	off_t offset = 0;

	for (i = 0; i < fill_size;
	    i += sizeof(off_t),
	    pbuf += sizeof(off_t), offset++) {
		snprintf(pbuf, sizeof(off_t) + 1, "%7lx%c",
		    offset, ((offset + 1) % 10) ? ' ' : '\n');
	}
	return buf;
}

static long
generate_buffer(char **buf, size_t *buf_size, size_t new_size) {
	char *pbuf = NULL;

	if (*buf == NULL || new_size > *buf_size) {
		pbuf = (char *)realloc(*buf, new_size + 1);
		if (!pbuf) {
			fprintf(stderr,
			    "Could not allocate buf of size %ld\n",
			    new_size);
			fflush(stderr);
			new_size = -1;
			goto out;
		}
		fill_buf(pbuf, new_size);
		*buf_size = new_size;
		*buf = pbuf;
	}
out:
	return new_size;
}

static void
clean_file(const char * fname)
{
	fail_if(((unlink(fname) < 0) && (errno != ENOENT)),
	    "Could not unlink file %s : %s (%d)",
	    fname, strerror(errno), errno);
}

static bool compare_one_time(const char *time_name, int type,
    struct timespec *orig_time, struct timespec *cur_time,
    unsigned int cmptime_flags);

inline static bool
compare_ctime(struct time_info *orig_times, struct time_info *cur_times,
    unsigned int cmptime_flags)
{
	return compare_one_time("ctime", CMPTIME_CTIME_MATCH,
	    &orig_times->ctime, &cur_times->ctime,
	    cmptime_flags);
}

inline static bool
compare_mtime(struct time_info *orig_times, struct time_info *cur_times,
    unsigned int cmptime_flags)
{
	return compare_one_time("mtime", CMPTIME_MTIME_MATCH,
	    &orig_times->mtime, &cur_times->mtime,
	    cmptime_flags);
}

inline static bool
compare_atime(struct time_info *orig_times, struct time_info *cur_times,
    unsigned int cmptime_flags)
{
	return compare_one_time("atime", CMPTIME_ATIME_MATCH,
	    &orig_times->atime, &cur_times->atime,
	    cmptime_flags);
}

static void
do_step(const char *fname, ifs_lin_t lin,
    struct isi_cbm_file **file_obj, int fd, char **buf, size_t *buf_size,
    struct step_info *step, int step_num,
    struct time_info *orig_times)
{
	size_t gen_buf_size = -1;
	int status = -1;
	struct isi_error *error = NULL;
	bool cmptimes_passed = true;


#ifndef CKTIME_VERBOSE
	CHECK_TRACE("  Step %2d: %18s, %s\n",
	    step_num, op_to_str(step->operation), fname);
#else
	int err;
	bool noatime = false;
	err = ifs_getnoatime(&noatime);
	if (err)
		noatime = false;

	CHECK_TRACE("  Step %2d: %18s, %s\n"
		    "    pre_delay  %5ld"
		    "    size       %5lu"
		    "    noatime    %s\n"
		    "    offset     %5ld"
		    "    flags      %5d"
		    "    post_delay %5ld\n"
		    "    sync_it    %s"
		    "    cmptim flg %05x"
		    "    policy     %s\n",
	    step_num, fname, op_to_str(step->operation),
	    step->pre_delay, step->size, truefalse(noatime),
	    step->offset, step->flags, step->post_delay,
	    truefalse(step->sync_it), step->cmptime_flags, step->policy);
#endif
	if (step->pre_delay > 0) {
		sleep(step->pre_delay);
	}

	/* generate_write_buffer if needed, generate_buffer will check */
	gen_buf_size = generate_buffer(buf, buf_size, step->size);
	fail_if (gen_buf_size < step->size,
	    "Could not allocate buffer of size %ld for write to %s "
	    "step %d\n",
	    step->size, fname, step_num);

	switch(step->operation) {
	case CKTIME_NOOP:
		break;
	case CKTIME_READ:
		status = pread(fd, *buf, step->size, step->offset);
		fail_if(status < 0,
		    "Could not read %ld bytes from %s at offset %ld: %s (%d) "
		    "step %d\n",
		    step->size, fname, step->offset, strerror(errno), errno,
		    step_num);
		break;
	case CKTIME_WRITE:
		struct iovec iov;
		iov.iov_len = gen_buf_size;
		iov.iov_base = buf;
		if (step->flags)
			status = ifs_cpool_pwritev(fd, &iov, 1,
			    step->offset, step->flags);
		else
			status = pwritev(fd, &iov, 1, step->offset);

		fail_if(status < 0,
		    "Could not write %ld bytes to %s at offset %ld: %s (%d) "
		    "step %d\n",
		    gen_buf_size, fname, step->offset, strerror(errno), errno,
		    step_num);
		break;
	case CKTIME_TRUNCATE:
		isi_cbm_file_truncate(lin, HEAD_SNAPID, step->size, &error);
		fail_if(error != NULL,
		    "failed to truncate %s (%#{}) to %ld in step %d: %#{}",
		    fname, lin_fmt(lin), step->size, step_num,
		    isi_error_fmt(error));
		break;
	case CKTIME_STUB:
		cbm_test_disable_atime_updates();

		stub_file(step->policy, fname, lin);

		cbm_test_enable_atime_updates();
		break;
	case CKTIME_STUB_OPEN:
		*file_obj = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
		fail_if(error, "Cannot open stub file %s: %{} "
		    "step %d\n",
		    fname, isi_error_fmt(error), step_num);
		break;
	case CKTIME_STUB_CLOSE:
		isi_cbm_file_close(*file_obj, &error);
		fail_if(error, "Cannot close stub file %s: %{}",
		    fname, isi_error_fmt(error));
		*file_obj = NULL;
		break;
	case CKTIME_STUB_READ:
		ASSERT(*file_obj);
		isi_cbm_file_read(*file_obj, *buf, step->offset,
		    gen_buf_size, CO_DEFAULT, &error);

		fail_if(error, "Cannot read stubbed file %s: %{} "
		    "step %d\n",
		    fname, isi_error_fmt(error), step_num);
		break;
	case CKTIME_STUB_WRITE:
		ASSERT(*file_obj);
		isi_cbm_file_write(*file_obj, *buf, step->offset,
		    gen_buf_size, &error);

		fail_if(error, "Cannot write stubbed file %s: %{} "
		    "step %d\n",
		    fname, isi_error_fmt(error), step_num);
		break;
	case CKTIME_STUB_SYNC:
		cbm_test_disable_atime_updates();

		cbm_test_sync(lin, HEAD_SNAPID,
		    NULL, NULL, NULL, &error);
		fail_if(error, "Failed to sync the file: %#{} "
		    "step %d\n",
		    isi_error_fmt(error), step_num);

		cbm_test_enable_atime_updates();
		break;
	case CKTIME_STUB_INVAL:
		ASSERT(*file_obj);

		cbm_test_disable_atime_updates();

		invalidate_file(*file_obj, fname, lin);

		cbm_test_enable_atime_updates();
		break;
	case CKTIME_RECALL:
		cbm_test_disable_atime_updates();

		recall_file(fname, lin);

		cbm_test_enable_atime_updates();
		break;
	case CKTIME_RESET_BASE:
		reset_times(orig_times, fd, fname);
		break;
	default:
		fail_if(true, "Unrecognised option in do_step %d "
		    "step %d\n",
		    step->operation, step_num);
	}

	if (step->post_delay > 0) {
		sleep(step->post_delay);
	}

	if (step->sync_it) {
		fsync(fd);
	}

	cmptimes_passed = compare_times(orig_times, fd, fname,
		step->cmptime_flags);
	fail_if( !cmptimes_passed,
	    "Compare times failed for %s, flags 0x%04x step %d",
	    fname, step->cmptime_flags, step_num);

	return;
}

static void
reset_times(struct time_info *orig_times, int fd, const char *fname)
{
	int status = -1;
	struct stat statbuf;

	// fstat to get the current file times
	status = fstat(fd, &statbuf);
	fail_if(status < 0, "Cannot stat file %s (%d): %s (%d)",
	    fname, fd, strerror(errno), errno);

	orig_times->ctime = statbuf.st_ctim;
	orig_times->mtime = statbuf.st_mtim;
	orig_times->atime = statbuf.st_atim;
}

static bool
compare_timespec(struct timespec *t1, struct timespec *t2)
{
	if (t1->tv_sec == t2->tv_sec && t1->tv_nsec == t2->tv_nsec)
		return true;
	else
		return false;
}

static bool
compare_one_time(const char *time_name, int type,
    struct timespec *orig_time,
    struct timespec *cur_time,
    unsigned int cmptime_flags)
{
	bool retval = true;

	bool time_compare = compare_timespec(orig_time, cur_time);
	bool type_compare = (cmptime_flags & type);

	if (type_compare && !time_compare) {
		fprintf(stderr,
		    "*** orig %s: %ld.%ld does not equal cur %s: %ld.%ld\n",
		    time_name, orig_time->tv_sec, orig_time->tv_nsec,
		    time_name, cur_time->tv_sec, cur_time->tv_nsec);
		retval = false;
	} else if (!type_compare && time_compare) {
		fprintf(stderr,
		    "*** orig %s: %ld.%ld equals cur %s: %ld.%ld\n",
		    time_name, orig_time->tv_sec, orig_time->tv_nsec,
		    time_name, cur_time->tv_sec, cur_time->tv_nsec);
		retval = false;
	}
	return retval;
}

static bool
compare_times(struct time_info *orig_times, int fd, const char *fname,
    unsigned int cmptime_flags)
{
	int status = -1;
	struct stat statbuf;
	bool retval = true;
	time_info cur_times;

	if (!(cmptime_flags & CMPTIME_ENABLE)) {
		goto out;
	}

	// fstat to get the current file times
	status = fstat(fd, &statbuf);
	fail_if(status < 0, "Cannot stat file %s (%d): %s (%d)",
	    fname, fd, strerror(errno), errno);

	cur_times.ctime = statbuf.st_ctim;
	cur_times.mtime = statbuf.st_mtim;
	cur_times.atime = statbuf.st_atim;

#ifdef CKTIME_VERBOSE
#define CKTIME_PR(t) (t).tv_sec,(t).tv_nsec
	CHECK_TRACE("               orig                          cur\n"
	    "ctime:   %ld.%ld        %ld.%ld\n"
	    "mtime:   %ld.%ld        %ld.%ld\n"
	    "atime:   %ld.%ld        %ld.%ld\n",
	    CKTIME_PR(orig_times->ctime), CKTIME_PR(cur_times.ctime),
	    CKTIME_PR(orig_times->mtime), CKTIME_PR(cur_times.mtime),
	    CKTIME_PR(orig_times->atime), CKTIME_PR(cur_times.atime));
	system("sysctl efs.bam.atime_enabled;sysctl efs.bam.atime_grace_period");
#endif
	retval = compare_ctime (orig_times, &cur_times, cmptime_flags);
	if (!compare_mtime (orig_times, &cur_times, cmptime_flags)) {
		retval = false;
	}
	if (!compare_atime (orig_times, &cur_times, cmptime_flags)) {
		retval = false;
	}

	if (!retval) {
		fprintf(stderr, "\n");
	}
out:
	return retval;
}

static void
time_test(struct step_info *steps, int num_steps,
    const char * test_name, const char * fname)
{
	char *buf = NULL;
	int status = -1;
	int step_num = 0;
	size_t size = 0;
	time_info orig_times;
	ifs_lin_t lin;
	struct stat statbuf;
	struct isi_cbm_file *file_obj;
	struct step_info *step = NULL;
	int fd;
	int indx;

	CHECK_TRACE("\n\nTest %s, %s\n", test_name, fname);

	fd = open(fname, O_CREAT | O_RDWR, 0666);

	fail_if(fd < 0, "Cannot open file %s for time testing: %s (%d)",
	    fname, strerror(errno), errno);

	// fstat to get the lin and the times
	status = fstat(fd, &statbuf);
	fail_if(status < 0, "Cannot stat file %s: %s (%d)",
	    fname, strerror(errno), errno);

	lin = statbuf.st_ino;

	// Set the main time to compare against
	reset_times(&orig_times, fd, fname);

	for (indx = 0; indx < num_steps; indx++) {
		step = &steps[indx];
		do_step(fname, lin, &file_obj, fd,
		    &buf, &size, step, step_num++, &orig_times);
	}

	if (buf) {
		free(buf);
	}

	if (fd >= 0) {
		close(fd);
	}

	fail_if(unlink(fname) < 0,
	    "Could not unlink file %s: %s (%d)",
	    fname, strerror(errno), errno);
}

TEST(test_time, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
#define TEST_TIME_FILE_NAME "/ifs/data/test_time_cktimes.test"

	int num_steps = sizeof(test_time_steps) / sizeof (struct step_info);

	// Make sure atime is not enabled
	disable_atime();

	// Delete any files that may linger from a previous failure
	clean_file(TEST_TIME_FILE_NAME);

	time_test(test_time_steps, num_steps,
	    __FUNCTION__, TEST_TIME_FILE_NAME);
}

#ifdef CBM_ATIME_TEST_ENABLED
/*
 * disabling these tests as the atime checks are not valid and the results are
 * unpredictable based on general atime settings.
 */
TEST(atime_enabled, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
#define ATIME_ENABLED_FILE_NAME "/ifs/data/atime_enabled_cktimes.test"

	int num_steps = sizeof(atime_enabled_steps) / sizeof (struct step_info);

	// Delete any files that may linger from a previous failure
	clean_file(ATIME_ENABLED_FILE_NAME);

	enable_atime();

	time_test(atime_enabled_steps, num_steps,
	    __FUNCTION__, ATIME_ENABLED_FILE_NAME);

	disable_atime();
}
#endif
