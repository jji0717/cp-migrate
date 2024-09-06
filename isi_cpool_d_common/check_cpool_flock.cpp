#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <check.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/sysctl.h>
#include <sys/stat.h>
#include <sys/proc.h>
#include <ifs/ifs_syscalls.h>
extern "C"
{
#include <sys/isi_cifs_brl.h>
#include <isi_ecs/isi_ecs_cbrl.h>
}
#include <isi_util/check_helper.h>
#include "isi_cpool_revoked_fd.h"
#include "ifs_cpool_flock.h"


static char name[256] = "";

#define STR(__x) #__x
#define FAIL(__cond, __err) \
	do { \
		int err = (__cond); \
		if (err == 0) { \
			fail(\
			    "Failure " STR(__cond) "Failure %s: Expected " \
			    "error %d, but got no error.", \
			    STR(__cond), (__err)); \
		} else if ((err != 0) && (errno != (__err))) { \
			fail(\
			    "Failure " STR(__cond) "Failure %s: expected %d, but got %d:%s", \
			    STR(__cond), (__err), errno, strerror(errno)); \
		} \
	} while(0)

#define DO(__cond) \
	do { \
		int err = (__cond); \
		fail_unless(err == 0, \
		    "Failure " STR(__cond) " Failure %s: errno=%d: %s", \
		    STR(__cond), errno, strerror(errno)); \
	} while (0)
/**
 * Helper structures
 */

#define HIGH_RANGE (OFF_MAX-101)

struct test_range {
	uint64_t start;
	uint64_t len;
};

bool semlock_failed = false;
static const struct test_range overflow = {HIGH_RANGE, HIGH_RANGE+1};
static const struct test_range spanning = {HIGH_RANGE-50, 100};

#define TEST_RANGES 6
static const struct test_range test_r[TEST_RANGES] = {
	{1,100}, /* Bytes 1-100 */
	{1+HIGH_RANGE,100}, /* High range */
	{HIGH_RANGE,100}, /* Spanning */
	{HIGH_RANGE-1,100}, /* Spanning */
	{HIGH_RANGE-50,100}, /* Spanning */
	{HIGH_RANGE-1,2}, /* Barely spanning */
};

#define ONE_BYTE_TEST_RANGES 5
static const struct test_range one_byte[ONE_BYTE_TEST_RANGES] = {
	{0,1}, /* Bytes 0-0 */
	{1,1}, /* Bytes 1-1 */
	{HIGH_RANGE,1},
	{HIGH_RANGE+1,1},
	{-1,1},
};

struct lck {
	int fd;
	int type; // Lock type READ WRITE and so on.
	struct test_range tr;
	enum cpool_lock_domain domain;
	uint32_t pid;
	uint32_t tid;
};

/* Used to simply unwind the stack of locks, unlocking them all. */
#define MAX_LOCKS 100
static struct lck lock_stack[MAX_LOCKS] = {{0}};
static struct lck lock_init = {0, false, {0, 0}};
static int curlock = 0;

#define EXCLUSIVE CPOOL_LK_X
#define SHARED CPOOL_LK_SR


/**
 * ASYNC
 */

/* List of pending locks and their IDs */
struct pending_lock {
	TAILQ_ENTRY(pending_lock) next;
	struct lck lock;
	uint64_t id;
	bool pending;
};

TAILQ_HEAD(tailqhead, pending_lock) pending_locks;

static bool is_pending(uint64_t id)
{
	struct pending_lock *cur;

	TAILQ_FOREACH(cur, &pending_locks, next) {
		if (cur->id == id)
			return cur->pending;
	}

	fail(__FILE__, __LINE__, "Did not find id in pending queue");
	return false;
}

static void free_pending(void)
{
	struct pending_lock *p, *tmp;

	TAILQ_FOREACH_SAFE(p, &pending_locks, next, tmp) {
		TAILQ_REMOVE(&pending_locks, p, next);
		free(p);
		p = NULL;
	}
}


/* Monotonically increasing id counter. */
uint64_t ids = 15000;

static int cpool_event_fd = -1;
static int received_events = 0;

static bool last_event_success = false;
static bool last_event_failure = false;
static int last_event_id = -1;

static void test_async_success(uint64_t id)
{
	struct pending_lock *cur, *tmp;

	//printf("Async success called!\n");

	TAILQ_FOREACH_SAFE(cur, &pending_locks, next, tmp) {
		if (cur->id == id)
			cur->pending = false;
	}

	received_events++;
	last_event_id = id;
	last_event_success = true;
	last_event_failure = false;
}

static void test_async_failure(uint64_t id)
{
	//printf("ASYNC FAILURE! id=%llu\n", id);
	received_events++;
	last_event_id = id;
	last_event_failure = true;
	last_event_success = false;
}

struct cbrl_event_ops event_ops =
    {.cbrl_async_success = test_async_success, .cbrl_async_failure = test_async_failure};


/* Domains used by cpools. Maintain order shown since some tests rely on it. */
enum cpool_lock_domain domains[] = {
	LOCK_DOMAIN_CPOOL_CACHE,
	LOCK_DOMAIN_CPOOL_JOBS,
	LOCK_DOMAIN_CPOOL_STUB,
	LOCK_DOMAIN_CPOOL_SYNCH,
};

#define MAX_TRIES 10
static bool receive_events(int expected)
{
	int tries = 0;
	int read = 0;

	received_events = 0;

	while (tries < MAX_TRIES)
	{
		usleep(1000);
		/*printf("calling dispatcher, expecting %d events. "
		    "Currently at %d.\n", expected, received_events); */
		read = cbrl_event_dispatcher(&event_ops);
		if ((read == 0) && received_events == expected)
			break;
		else if (read < 0)
			printf("Got error from dispatcher = %d\n", errno);

		tries++;

	}
	/* printf("Expected %d events and got %d events.\n", expected,
	    received_events); */

	if (received_events == expected)
		return true;

	return false;
}


/**
 * Test Fixtures
 */
static inline void
fail_if_exists(char *n)
{
	struct stat st = {};

	fail_if(stat(n, &st) == 0);
}

static void
blow_away(char *n)
{
	int rc;
	struct stat st = {};

	if (!stat(n, &st)) {
		if (S_ISDIR(st.st_mode)) {
			rc = rmdir(n);
			fail_if(rc, "rmdir: %s", strerror(errno));
		} else {
			rc = unlink(n);
			fail_if(rc, "unlink: %s", strerror(errno));
		}
	}

	fail_if_exists(n);
}

#define OFF "off"
#define LOCK_ASYNC_FAIL_EINVAL "efs.fail_point.cpool_lock_async_fail_einval"
#define LOCK_ASYNC_SLEEP "efs.fail_point.cpool_lock_async_sleep"
#define ASYNC_CB_SLEEP "efs.fail_point.cpool_async_cb_sleep"
#define ASYNC_CB_FAKE_CANCEL "efs.fail_point.cpool_async_cb_fake_cancel"


static int
enable_failpoint(const char *name, const char *value)
{
	int error;

	error = sysctlbyname(name, NULL, 0, (void *)value, strlen(value));
	if (error) {
		printf("Failed enabling failpoint %s to value %s. Error=%d.\n",
		    name, value, errno);
	};

	return error;
}

static int
disable_failpoint(const char *name)
{
	int error;

	error = sysctlbyname(name, NULL, 0, (void *)OFF, strlen(OFF));
	if (error) {
		printf("Failed disabling failpoint! Error=%d, Failpoint=%s\n",
		    errno, name);
	}

	return error;
}

void
disable_failpoints(void)
{
	disable_failpoint(LOCK_ASYNC_FAIL_EINVAL);
	disable_failpoint(LOCK_ASYNC_SLEEP);
	disable_failpoint(ASYNC_CB_SLEEP);
	disable_failpoint(ASYNC_CB_FAKE_CANCEL);
}


TEST_FIXTURE(cpool_flock_suite_setup)
{
	//printf("ZACK suite setup running - Why is this running twice?\n");
}

TEST_FIXTURE(cpool_flock_suite_teardown)
{
	//printf("suite teardown running\n");
}

TEST_FIXTURE(setup)
{
	int i;

	sprintf(name, "/ifs/check_cpool_flock_%d", getpid());
	blow_away(name);

	curlock = 0;
	for (i = 0; i < MAX_LOCKS; i++)
		lock_stack[i] = lock_init;

	TAILQ_INIT(&pending_locks);
	cpool_event_fd = cbrl_event_register();
	fail_if(cpool_event_fd == -1);
	disable_failpoints();
}

TEST_FIXTURE(teardown)
{
	blow_away(name);
	cbrl_event_unregister();
	disable_failpoints();
}

SUITE_DEFINE_FOR_FILE(check_cpool_flock,
    .mem_check = CK_MEM_LEAKS,
    .dmalloc_opts = 0,
    .suite_setup = cpool_flock_suite_setup,
    .suite_teardown = cpool_flock_suite_teardown,
    .test_setup = setup,
    .test_teardown = teardown,
    .runner_data = 0,
    .timeout = 15,
    .fixture_timeout = 1200);


/**
 * Helper functions.
 */

/*
static struct test_range getrange(struct test_range r, uint64_t start_offset,
    uint64_t len_offset)
{
	struct test_range ret = r;
	ret.start += start_offset;
	ret.len += len_offset;
	return ret;
}
*/

static struct test_range getmid(struct test_range r)
{
	struct test_range ret;
	ret.start = r.start + (r.len / 2);
	ret.len = 1;
	return ret;
}
static struct test_range getfirst(struct test_range r)
{
	struct test_range ret;
	ret.start = r.start;
	ret.len = 1;
	return ret;
}

static struct test_range getlast(struct test_range r)
{
	struct test_range ret;
	ret.start = r.start + r.len - 1;
	ret.len = 1;
	return ret;

}
static struct test_range getpre(struct test_range r)
{
	struct test_range ret;
	ret.start = r.start - 1;
	ret.len = 1;
	return ret;
}
static struct test_range getpost(struct test_range r)
{
	struct test_range ret;
	ret.start = r.start + r.len;
	ret.len = 1;
	return ret;
}

static int openit(void)
{
	int fd;

	fd = open(name, O_CREAT|O_RDWR, 0777);
	fail_if(fd < 0, "open failed, error = %s(%d)", strerror(errno), errno);

	return fd;
}

static void closeit(int fd)
{
	close(fd);
	curlock = 0;
}


static int lock(int fd, enum cpool_lock_domain domain, short l_type,
    short l_whence, struct test_range tr, uint32_t pid=0, uint32_t tid=0)
{
	int err;
	long my_tid;
	thr_self(&my_tid);
	struct flock my_flock ={
		.l_start = tr.start,
		.l_len = tr.len,
		.l_pid = getpid(),
		.l_type = l_type,
		.l_whence = l_whence,
		.l_sysid = 0,
	};
	if (pid == 0 && tid == 0) {
		pid = (uint32_t) getpid();
		tid = (uint32_t) ((uint64_t) (my_tid) & 0XFFFFFFFF);
		err = ifs_cpool_flock(fd, domain, F_SETLK, &my_flock, 0);
	} else
		err = ifs_cpool_flock(fd, domain, F_SETLK, &my_flock, 0,
		    false, 0, pid, tid);
	if (err == 0) {
		ASSERT(curlock != MAX_LOCKS);
		lock_stack[curlock].fd = fd;
		lock_stack[curlock].type = l_type;
		lock_stack[curlock].tr = tr;
		lock_stack[curlock].domain = domain;
		lock_stack[curlock].pid = pid;
		lock_stack[curlock].tid = tid;
		curlock++;
	}
	return err;
}


/* The "type" parameter will be ignored by the syscall. */
/* TODO Remove from lock_stack? */
static int unlock(int fd, enum cpool_lock_domain domain, short l_type,
    short l_whence, struct test_range tr, uint32_t pid=0, uint32_t tid=0)
{
	int err;
	struct flock my_flock={
		.l_start = tr.start,
		.l_len = tr.len,
		.l_pid = getpid(),
		.l_type = l_type,
		.l_whence = l_whence,
		.l_sysid = 0,
	 };
	 if (pid == 0 && tid ==0)
		err = ifs_cpool_flock(fd, domain, F_UNLCK, &my_flock, 0);
	 else
		err = ifs_cpool_flock(fd, domain, F_UNLCK, &my_flock, 0,
		    false, 0, pid, tid);
	 return err;
}

static int async(int fd, enum cpool_lock_domain domain, short l_type,
    short l_whence, struct test_range tr, uint64_t id,
    uint32_t pid=0, uint32_t tid=0)
{
	int ret;
	long my_tid;
	thr_self(&my_tid);
	struct flock my_flock={
		.l_start = tr.start,
		.l_len = tr.len,
		.l_pid = getpid(),
		.l_type = l_type,
		.l_whence = l_whence,
		.l_sysid = 0,
	 };
	struct lck l = {fd, l_type, tr, domain, pid, tid};
	struct pending_lock *p;
	if (pid == 0 && tid == 0) {
		pid = (uint32_t) getpid();
		tid = (uint32_t) ((uint64_t) (my_tid) & 0XFFFFFFFF);
	}

	ret = ifs_cpool_flock(fd, domain, F_SETLK, &my_flock, 0,
	    true, id, pid, tid);

	if (ret != 0 && errno == EINPROGRESS) {
		/* Add lock to pending_locks. */
		p = (struct pending_lock *) malloc(sizeof(struct pending_lock));
		p->lock = l;
		p->id = id;
		p->pending = true;
		TAILQ_INSERT_TAIL(&pending_locks, p, next);
	}

	return ret;
}


static int cancel(int fd, struct test_range tr,
    enum cpool_lock_domain domain, uint64_t id)
{
	int ret;
	long my_tid;
	thr_self(&my_tid);
	struct flock my_flock;
	u_int32_t pid = (uint32_t) getpid();
	u_int32_t tid = (uint32_t) ((uint64_t) (my_tid) & 0XFFFFFFFF);

	ret = ifs_cpool_flock(fd, domain, F_CANCEL, &my_flock, 0,
	    false, id, pid, tid);

	return ret;
}

static void unlocksome(int i)
{
	struct lck temp;
	bool max = false;

	if (i == MAX_LOCKS)
		max = true;

	// fail_if(curlock == 0);
	do {
		curlock--;
		temp = lock_stack[curlock];
		fail_if(unlock(temp.fd, temp.domain, temp.type,
		    SEEK_SET, temp.tr, temp.pid, temp.tid));
		i--;
	} while (curlock > 0 && i > 0);

	if (!max)
		fail_if(i != 0);
}


static void unlockall()
{
	unlocksome(MAX_LOCKS);
}

bool lock_stackable(enum cpool_lock_domain domain)
{
	return (domain == LOCK_DOMAIN_CPOOL_CACHE) ||
	     (domain == LOCK_DOMAIN_CPOOL_JOBS);
}
/**
 * Tests.
 */
TEST(invalid)
{
	int fd;
	long tid;
	thr_self(&tid);
	struct flock my_flock={
		.l_start = test_r[0].start,
		.l_len = test_r[0].len,
		.l_pid = getpid(),
		.l_type = CPOOL_LK_X,
		.l_whence = SEEK_SET,
		.l_sysid = 0,
	 };

	fd = openit();
	int status = 0;

	// Wrong fd
	status = ifs_cpool_flock(fd+1, LOCK_DOMAIN_CPOOL_CACHE, F_SETLK, &my_flock, F_SETLKW);
	fail_if((status == 0),
	    "Lock failed with error: %s (%d)",
	    strerror(errno), errno);


	/* Wrong operation. */
	FAIL(ifs_cpool_flock(fd, LOCK_DOMAIN_CPOOL_CACHE, F_SETLK_REMOTE, &my_flock, F_SETLKW),
	    EINVAL);
	FAIL(ifs_cpool_flock(fd, LOCK_DOMAIN_CPOOL_CACHE, F_DUP2FD, &my_flock, F_SETLKW),
	    EINVAL);

	/* Wrong domains */

	/* Lock overflow. */
	FAIL(lock(fd, (enum cpool_lock_domain) (LOCK_DOMAIN_CPOOL_CACHE-1), CPOOL_LK_X, SEEK_SET, overflow), EINVAL);
	FAIL(lock(fd, (enum cpool_lock_domain) (LOCK_DOMAIN_CPOOL_JOBS+1), CPOOL_LK_X, SEEK_SET, overflow), EINVAL);

	/* Unlock failures. */
	DO(lock(fd, LOCK_DOMAIN_CPOOL_CACHE, CPOOL_LK_X, SEEK_SET, test_r[0]));

	FAIL(unlock(fd, LOCK_DOMAIN_CPOOL_CACHE, CPOOL_LK_X, SEEK_SET,
	    getmid(test_r[0]), getpid(), tid), EINVAL);
	FAIL(unlock(fd, LOCK_DOMAIN_CPOOL_CACHE, CPOOL_LK_X, SEEK_SET,
	    getfirst(test_r[0]), getpid(), tid), EINVAL);
	FAIL(unlock(fd, LOCK_DOMAIN_CPOOL_CACHE, CPOOL_LK_X, SEEK_SET,
	    getlast(test_r[0]), getpid(), tid), EINVAL);
	FAIL(ifs_cpool_flock(fd, LOCK_DOMAIN_CPOOL_CACHE, F_UNLCK, &my_flock, F_SETLKW,
	    false, 0,
	    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EINVAL);
	FAIL(ifs_cpool_flock(fd, LOCK_DOMAIN_CPOOL_CACHE, F_UNLCK, &my_flock, F_SETLKW,
	    false, 0,
	    (uint32_t) getpid() + 1, (uint32_t) ((uint64_t) (tid) & 0XFFFFFFFF)), EINVAL);
	FAIL(ifs_cpool_flock(fd, LOCK_DOMAIN_CPOOL_STUB, F_UNLCK, &my_flock,
	    F_SETLKW, false, 0, getpid(), tid), EINVAL);

	FAIL(ifs_cpool_flock(fd, LOCK_DOMAIN_CPOOL_STUB, F_UNLCK, &my_flock,
	    F_SETLKW, false, 0, getpid(), 0), EINVAL);
	FAIL(ifs_cpool_flock(fd, LOCK_DOMAIN_CPOOL_SYNCH, F_UNLCK, &my_flock,
	    F_SETLKW, false, 0, getpid(), tid), EINVAL);
	FAIL(ifs_cpool_flock(fd, LOCK_DOMAIN_CPOOL_STUB, F_UNLCK, &my_flock,
	    F_SETLKW, false, 0, getpid(), 0), EINVAL);

	DO(unlock(fd, LOCK_DOMAIN_CPOOL_CACHE, CPOOL_LK_SR, SEEK_SET, test_r[0]));
	closeit(fd);
}

/**
 * Test all domains simoultaneously with file close
 *  as an event that cleans all locks.
 */
TEST(lock_close_lock)
{
	int fd = -1;
	static struct test_range rng;
	int j = 0, i = 0;
	enum cpool_lock_domain domain;

	fd = openit();

	// Exclusive lock any ranges for stackable (i.e. cache and jobs) domains
	for (j = 0; j < 2; j++) {
		domain = domains[j];
		// No lock ranges for STUB & SYNC domains.
		if (domain == LOCK_DOMAIN_CPOOL_STUB ||
		    domain == LOCK_DOMAIN_CPOOL_SYNCH) {
			continue;
		}
		for (i = 0; i < TEST_RANGES; i++) {
			rng = test_r[i];
			DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
			DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, getpre(rng)));
			DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, getpost(rng)));
			fail_if(fd == -1);
		}
	}
	// Exclusive lock only non-overlapping ranges for non-stackable domains
	for (j = 2; j < 4; j++) {
		domain = domains[j];
		// No lock ranges for STUB & SYNC domains.
		if (domain == LOCK_DOMAIN_CPOOL_STUB ||
		    domain == LOCK_DOMAIN_CPOOL_SYNCH) {
			continue;
		}
		for (i = 0; i < 2; i++) {
			rng = test_r[i];
			DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
			fail_if(fd == -1);
		}
	}

	closeit(fd);
	fd = openit();
	for (j = 0; j < 4; j++) {
		domain = domains[j];
		// No lock ranges for STUB & SYNC domains.
		if (domain == LOCK_DOMAIN_CPOOL_STUB ||
		    domain == LOCK_DOMAIN_CPOOL_SYNCH) {
			continue;
		}

			for (i = 0; i < TEST_RANGES; i++) {
				rng = test_r[i];
				DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng)); 
			DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
		}
	}
	unlockall();
	closeit(fd);
}

static void
test_contention_table(enum cpool_lock_domain domain)
{
	int fd = -1;
	static struct test_range rng;
	long tid;
	thr_self(&tid);
	fd = openit();
	rng = test_r[0];

	// Test that Shared Read locks can be taken simoultaneously for the same range.
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
			    (uint32_t) getpid(), 0));
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
		     (uint32_t) getpid()+1, (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
		     (uint32_t) getpid()+1, (uint32_t) ((uint64_t) (tid+2) & 0XFFFFFFFF)));
	// Unlock all the shared locks.
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
			    (uint32_t) getpid(), 0));
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
		     (uint32_t) getpid()+1, (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
		     (uint32_t) getpid()+1, (uint32_t) ((uint64_t) (tid+2) & 0XFFFFFFFF)));

	// Make sure that all locks are gone by taking an exclusive lock.
	DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
		     (uint32_t) getpid()+3, (uint32_t) ((uint64_t) (tid+2) & 0XFFFFFFFF)));
	DO(unlock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
		     (uint32_t) getpid()+3, (uint32_t) ((uint64_t) (tid+2) & 0XFFFFFFFF)));


	// Make sure you cannot take a SW anc Exclusive while holding a SR
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	FAIL(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);
	FAIL(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
			    (uint32_t) getpid(), 0), EWOULDBLOCK);
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
			    (uint32_t) getpid(), 0), EWOULDBLOCK);
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));

	// Test that Shared Write locks can be taken simoultaneously for the same range.
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
			    (uint32_t) getpid(), 0));
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
		     (uint32_t) getpid()+1, (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
		     (uint32_t) getpid()+1, (uint32_t) ((uint64_t) (tid+2) & 0XFFFFFFFF)));

	// Unlock all the shared write locks.
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
			    (uint32_t) getpid(), 0));
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
		     (uint32_t) getpid()+1, (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
		     (uint32_t) getpid()+1, (uint32_t) ((uint64_t) (tid+2) & 0XFFFFFFFF)));

	// Make sure that all locks are gone by taking an exclusive lock.
	DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
		     (uint32_t) getpid()+3, (uint32_t) ((uint64_t) (tid+2) & 0XFFFFFFFF)));
	DO(unlock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
		     (uint32_t) getpid()+3, (uint32_t) ((uint64_t) (tid+2) & 0XFFFFFFFF)));

	// Make sure you cannot take a SR anc Exclusive while holding a SW
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	FAIL(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);
	FAIL(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
			    (uint32_t) getpid(), 0), EWOULDBLOCK);
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
			    (uint32_t) getpid(), 0), EWOULDBLOCK);
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));

	// Make sure you can take only one exclusive lock on a file range
	DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
	FAIL(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
		     (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);
	FAIL(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
	      (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
		     (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);
	DO(unlock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));

	// Test contention between SR and Exclusive lock
	// including same thread and different thread.
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	if (lock_stackable(domain)) {
		DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
		FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
		    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)),
		    EWOULDBLOCK);
		DO(unlock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
	} else {
		FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng),
		    EWOULDBLOCK);
	}
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
	    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)),
	    EWOULDBLOCK);
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
	    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	DO(unlock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
	    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));

	closeit(fd);
}

/**
 * Test contention table
 */
TEST(contention_table)
{
	for (size_t i = 0; i < sizeof(domains)/sizeof(domains[0]); i++)
		test_contention_table(domains[i]);
}


static void
test_recursive_locking(enum cpool_lock_domain domain)
{
	int fd = -1;
	static struct test_range rng;
	long tid;
	thr_self(&tid);
	rng = test_r[0];
	fd = openit();


	// Test shared read to exclusive stackability
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	if (lock_stackable(domain)) {
		DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
		// this will unlock the exclusive only:
		DO(unlock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
	} else {
		FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng),
		    EWOULDBLOCK);
	}

	// locking exclusive from another thread should fail,
	// because the shared lock was maintained:
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
	    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)),
	    EWOULDBLOCK);

	// lock shared from another thread shall succeed:
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));

	// unlock the shared from thread 1
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));

	// now the other thread can lock it exclusively
	DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
	    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	DO(unlock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
	    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));

	// Test exclusive lock stackability
	DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
	if (lock_stackable(domain)) {
		DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
		DO(unlock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
	} else {
		FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng),
		    EWOULDBLOCK);
	}

	// the following shall fail, the outer lock is still in place
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
	    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)),
	    EWOULDBLOCK);

	// while we have the exclusive lock, do shared lock, should succeed
	// for stackable domains and fail otherwise
	if (lock_stackable(domain)) {
		DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
		DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	} else {
		FAIL(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng),
		    EWOULDBLOCK);
	}

	// unlock the outer one:
	DO(unlock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));

	// lock shared from another thread shall succeed:
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));

	// Test Shared Read stackability
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	if (lock_stackable(domain)) {
		DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
		DO(unlock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
	} else {
		FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng),
		    EWOULDBLOCK);
	}
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));

	// Test that Shared Write lock is stackable
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	if (lock_stackable(domain)) {
		DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
		DO(unlock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
	} else {
		FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng),
		    EWOULDBLOCK);
	}
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));

	// Test locking the whole file.
	rng.start = 0;
	rng.len   = OFF_MAX;
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));

	// Test failing to lock one byte in the beginning;
	rng.start = 0;
	rng.len   = 1;
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
		  (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);

	// Test failing to take a lock at the end.
	rng.start = OFF_MAX - 1;
	rng.len   = 1;

	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
		  (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);

	// Test failing to take a lock at the middle
	rng.start = OFF_MAX/2;
	rng.len   = 1;

	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
		  (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);

	// unlock the whole file.
	rng.start = 0;
	rng.len   = OFF_MAX;
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));


	// What locking [0,0] means? It means lock the whole file!
	rng.start = 0;
	rng.len   = 0;
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	// Test locking [0,0]
	rng.start = 0;
	rng.len   = 0;

	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
		  (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);
	// Test locking [1,0]
	rng.start = 1;
	rng.len   = 0;

	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
		  (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);


	// Test failing to take a lock at the end.
	rng.start = OFF_MAX - 1;
	rng.len   = 1;

	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
		  (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);

	// Test failing to take zero size lock at the end.
	rng.start = OFF_MAX - 1;
	rng.len   = 0;

	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
		  (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);

	rng.start = 0;
	rng.len   = 0;
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	rng = test_r[0];
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	closeit(fd);
}

/**
 * Test Recursive Behavior and locking the whole file
 */
TEST(recursive_locking)
{
	for (size_t i = 0; i < sizeof(domains)/sizeof(domains[0]); i++)
		test_recursive_locking(domains[i]);
}


static void
test_lock_unlock_close(enum cpool_lock_domain domain)
{
	int i,j;
	int fd = -1;
	static struct test_range rng;
	long tid;
	thr_self(&tid);
	rng = test_r[0];

	/*
	 * j = 0, we use unlockall().
	 * j = 1, we use closeit().
	 */
	for (j = 0; j < 2; j++) {
		if (fd == -1)
			fd = openit();

		for (i = 0; i < TEST_RANGES; i++) {
			if ((j == 1) && (fd == -1))
				fd = openit();
			rng = test_r[i];
			DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
			FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);
			FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, getmid(rng),
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);
			FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, getfirst(rng),
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);
			FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, getlast(rng),
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);
			DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, getpre(rng),
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
			DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, getpost(rng),
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
			if (j == 0)
				unlockall();
			else {
				fail_if(fd == -1);
				closeit(fd);
				fd = openit();
			}


			DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
			DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
			FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)),  EWOULDBLOCK);

			if (j == 0)
				unlockall();
			else {
				fail_if(fd == -1);
				closeit(fd);
				fd = -1;
			}
		}

		if (fd != -1) {
			closeit(fd);
			fd = -1;
		}
	}
}

/**
 * Test sync lock, unlock and close.
 */
TEST(lock_unlock_close)
{
	for (size_t i = 0; i < sizeof(domains)/sizeof(domains[0]); i++) {
		// No lock ranges for STUB & SYNC domains.
		if (domains[i] == LOCK_DOMAIN_CPOOL_STUB ||
		    domains[i] == LOCK_DOMAIN_CPOOL_SYNCH) {
			continue;
		}
		test_lock_unlock_close(domains[i]);
	}
}

static void
test_zero_byte_locks(enum cpool_lock_domain domain)
{
	/* Ranges for zero byte lock tests. */
	static const struct test_range zero1 = {100,0};
	static const struct test_range zero2 = {101,0};
	static const struct test_range zero3 = {102,0};
	static const struct test_range one1 = {100,1};
	static const struct test_range one2 = {101,1};
	static const struct test_range one3 = {102,1};
	static const struct test_range two1 = {100,2};
	static const struct test_range two2 = {101,2};
	int fd;

	fd = openit();
	DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, zero1));
	if (lock_stackable(domain)) {
		DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, zero2));
		DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, zero3));
		DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, one1));
		DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, one2));
		DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, one3));
	} else {
		FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, zero2),
		    EWOULDBLOCK);
		FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, zero3),
		    EWOULDBLOCK);
		FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, one1), EWOULDBLOCK);
		FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, one2), EWOULDBLOCK);
		FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, one3), EWOULDBLOCK);
	}
	unlockall();

	DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, zero2));
	if (lock_stackable(domain)) {
		DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, two1));
		DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, two2));
	} else {
		FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, two1), EWOULDBLOCK);
		FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, two2), EWOULDBLOCK);
	}
	unlockall();

	closeit(fd);
}

/**
 * Test zero byte locks with other zero byte locks, one byte locks and 2 byte
 * locks.
 */
TEST(zero_byte_locks)
{
	for (size_t i = 0; i < sizeof(domains)/sizeof(domains[0]); i++)
		test_zero_byte_locks(domains[i]);
}


static void
test_convert_locking(enum cpool_lock_domain domain)
{
	int fd = -1;
	static struct test_range rng;
	long tid;
	thr_self(&tid);
	rng = test_r[0];
	struct flock my_flock = {
		.l_start = test_r[0].start,
		.l_len = test_r[0].len,
		.l_pid = getpid(),
		.l_type = CPOOL_LK_X,
		.l_whence = SEEK_SET,
		.l_sysid = 0,
	};
	// STUB domain does not support convert
	// this is due to the fact that it supports
	// Asynchromous locks.
	if (domain == LOCK_DOMAIN_CPOOL_STUB)
		return;

	fd = openit();
	// TEST CASES for upgrading a Shared Read lock
	// ===========================================
	//
	// Upgrade a Shared Read lock to Shared Write
	// ------------------------------------------
	my_flock.l_type = CPOOL_LK_SW;
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	DO(ifs_cpool_flock(fd, domain, F_CONVERTLK,
	    &my_flock, F_SETLKW));
	// Taking a Shared Read from another thread should block
	FAIL(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t) getpid(),
	    (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)),  EWOULDBLOCK);
	// Taking an exclusive lock from a different thread should block
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)),  EWOULDBLOCK);
	// Taking a Shared Write from another thread should not block.
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)));
	// Unlock both shared write locks
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)));

	// Upgrade for a shared write would block since other
	// thread alread holds a shared read to the resource
	//----------------------------------------------------
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)));
	// We will directly call system ifs_cpool_flock for this
	// lock convertion sceanrio as CloudPools layer call only
	// handles lock downgrade from exclusive to sharedRead, and
        // would assert F_CONVERTLK on EWOULDBLOCK because this is an 
	// unexpected error (163938).
	FAIL(ifs_cpool_flock(fd, domain, F_CONVERTLK, &my_flock, 0,
	    false, 0, getpid(), tid), EWOULDBLOCK);
	// Unlock both shared read locks
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)));

	// Upgrade for a Exclusive  would block since other
	// thread alread holds a shared read to the resource
	//----------------------------------------------------
	my_flock.l_type = CPOOL_LK_X;
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)));
	// We will directly call system ifs_cpool_flock for this
	// lock convertion sceanrio as CloudPools layer call only
	// handles lock downgrade from exclusive to sharedRead, and
	// would assert F_CONVERTLK on EWOULDBLOCK because this is an 
	// unexpected error (163938)
	FAIL(ifs_cpool_flock(fd, domain, F_CONVERTLK, &my_flock, 0,
	    false, 0, getpid(), tid), EWOULDBLOCK);
	// Unlock both shared read locks
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)));

	// Upgrade for Shared Read to Exclusive will work
	// If no one else is holding the a lock
	--------------------------------------------
	my_flock.l_type = CPOOL_LK_X;
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	DO(ifs_cpool_flock(fd, domain, F_CONVERTLK,
	    &my_flock, 0));
	// Taking a Shared Read from another thread should block
	FAIL(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t) getpid(),
	    (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)),  EWOULDBLOCK);
	// Taking a Shared Write from another thread should block
	FAIL(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
	    (uint32_t) getpid(),
	    (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)),  EWOULDBLOCK);

	// Taking an exclusive lock from a different thread should block
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)),  EWOULDBLOCK);

	DO(unlock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));

	// TEST CASES for converting a Shared Write lock
	// =============================================

	// Test Downgrading a Shared Write to a Shared Read.
	//----------------------------------------------------
	my_flock.l_type = CPOOL_LK_SR;
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	DO(ifs_cpool_flock(fd, domain, F_CONVERTLK,
	    &my_flock, 0));
	// Taking a Shared Read from another thread should succeed
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t) getpid(),
	    (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	// Taking a Shared Write from another thread should block
	FAIL(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
	    (uint32_t) getpid(),
	    (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)),  EWOULDBLOCK);

	// Taking an exclusive lock from a different thread should block
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)),  EWOULDBLOCK);
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t) getpid(),
	    (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));

	// Test Upgrading a Shared Write to a Exclusive.
	//----------------------------------------------------
	my_flock.l_type = CPOOL_LK_X;
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	DO(ifs_cpool_flock(fd, domain, F_CONVERTLK,
	    &my_flock, 0));
	// Taking a Shared Read from another thread should succeed
	FAIL(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t) getpid(),
	    (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);
	// Taking a Shared Write from another thread should block
	FAIL(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
	    (uint32_t) getpid(),
	    (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)),  EWOULDBLOCK);

	// Taking an exclusive lock from a different thread should block
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)),  EWOULDBLOCK);
	DO(unlock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));

	// Upgrade for a Exclusive  would block since other
	// thread alread holds a shared write to the resource
	//----------------------------------------------------
	my_flock.l_type = CPOOL_LK_X;
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)));
	// We will directly call system ifs_cpool_flock for this
	// lock convertion sceanrio as CloudPools layer call only
	// handles lock downgrade from exclusive to sharedRead, and
	// would assert F_CONVERTLK on EWOULDBLOCK because this is an 
	// unexpected error (163938)
	FAIL(ifs_cpool_flock(fd, domain, F_CONVERTLK, &my_flock, 0,
	    false, 0, getpid(), tid), EWOULDBLOCK);
	// Unlock both shared read locks
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)));

	// Downgrade for a shared read  would block since other
	// thread alread holds a shared write to the resource
	//----------------------------------------------------
	my_flock.l_type = CPOOL_LK_SR;
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)));
	// We will directly call system ifs_cpool_flock for this
	// lock convertion sceanrio as CloudPools layer call only
	// handles lock downgrade from exclusive to sharedRead, and
	// would assert F_CONVERTLK on EWOULDBLOCK because this is an 
	// unexpected error (163938)
	FAIL(ifs_cpool_flock(fd, domain, F_CONVERTLK, &my_flock, 0,
	    false, 0, getpid(), tid), EWOULDBLOCK);
	// Unlock both shared read locks
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)));


	// TEST CASES for converting an Exclusive lock
	// =============================================

	// Test Downgrading an Exclusive lock to a Shared Read.
	//----------------------------------------------------
	my_flock.l_type = CPOOL_LK_SR;
	DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
	DO(ifs_cpool_flock(fd, domain, F_CONVERTLK,
	    &my_flock, 0));
	// Taking a Shared Read from another thread should succeed
	DO(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t) getpid(),
	    (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	// Taking a Shared Write from another thread should block
	FAIL(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
	    (uint32_t) getpid(),
	    (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)),  EWOULDBLOCK);

	// Taking an exclusive lock from a different thread should block
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)),  EWOULDBLOCK);
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng));
	DO(unlock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t) getpid(),
	    (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));


	// Test Downgrading a Shared Write to a Shared Write.
	//----------------------------------------------------
	my_flock.l_type = CPOOL_LK_SW;
	DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
	DO(ifs_cpool_flock(fd, domain, F_CONVERTLK,
	    &my_flock, 0));
	// Taking a Shared Write from another thread should succeed
	DO(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
	    (uint32_t) getpid(),
	    (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));
	// Taking a Shared Read from another thread should block
	FAIL(lock(fd, domain, CPOOL_LK_SR, SEEK_SET, rng,
	    (uint32_t) getpid(),
	    (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)),  EWOULDBLOCK);

	// Taking an exclusive lock from a different thread should block
	FAIL(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng,
	    (uint32_t)getpid(),
	    (uint32_t)((uint64_t)(tid + 1) & 0XFFFFFFFF)),  EWOULDBLOCK);
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng));
	DO(unlock(fd, domain, CPOOL_LK_SW, SEEK_SET, rng,
	    (uint32_t) getpid(),
	    (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)));

	// Testing that we cannot convert locks that do not exist
	// We will directly call system ifs_cpool_flock for this
	// lock convertion sceanrio as CloudPools layer call only
	// handles lock downgrade from exclusive to sharedRead, and
	// would assert F_CONVERTLK on EWOULDBLOCK because this is an 
	// unexpected error (163938)
	// =======================================================
	my_flock.l_type = CPOOL_LK_SW;
	FAIL(ifs_cpool_flock(fd, domain, F_CONVERTLK, &my_flock, 0,
	    false, 0, getpid(), tid), EINVAL);

	my_flock.l_type = CPOOL_LK_SR;
	FAIL(ifs_cpool_flock(fd, domain, F_CONVERTLK, &my_flock, 0,
	    false, 0, getpid(), tid), EINVAL);

	my_flock.l_type = CPOOL_LK_X;
	FAIL(ifs_cpool_flock(fd, domain, F_CONVERTLK, &my_flock, 0,
	    false, 0, getpid(), tid), EINVAL);

	closeit(fd);
}

TEST(lock_close_lock_no_range)
{
	int fd = -1;
	long tid;
	thr_self(&tid);

	static struct test_range rng;
	int j = 0, i = 0;
	enum cpool_lock_domain domain = LOCK_DOMAIN_CPOOL_CACHE;
	enum cpool_lock_domain domains[] = {
		LOCK_DOMAIN_CPOOL_CACHE,
		LOCK_DOMAIN_CPOOL_STUB,
		LOCK_DOMAIN_CPOOL_SYNCH,
		LOCK_DOMAIN_CPOOL_JOBS,
	};
	for (j = 0; j < 4; j++) {
		domain = domains[j];
		// No lock ranges for STUB & SYNC domains.
		if (domain == LOCK_DOMAIN_CPOOL_CACHE ||
		    domain == LOCK_DOMAIN_CPOOL_JOBS) {
			continue;
		}
		for (i = 0; i < TEST_RANGES; i++) {
			fd = openit();
			rng = test_r[i];
			DO(lock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
			FAIL(lock(fd, domain, CPOOL_LK_SW, SEEK_SET, getpost(rng),
			    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid+1) & 0XFFFFFFFF)), EWOULDBLOCK);
			fail_if(fd == -1);
			DO(unlock(fd, domain, CPOOL_LK_X, SEEK_SET, rng));
		}
	}
	closeit(fd);
}

/**
 * Test convert locking
 */
TEST(convert_locking)
{
	for (size_t i = 0; i < sizeof(domains)/sizeof(domains[0]); i++)
		test_convert_locking(domains[i]);
}

/**
 * Test locking exclusive and getting an async exclusive lock when the first
 * exclusive is unlocked.
 */
TEST(async_lock)
{
	int fd;
	uint64_t id=0;
	static struct test_range rng;
	rng = test_r[0];
	id = ids++;
	fd = openit();
	DO(lock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));
	FAIL(async(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X,
	    SEEK_SET, rng, id), EINPROGRESS);
	DO(unlock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));
	DO(!(is_pending(id) == true)); /* ! because it looks for 0. */

	fail_unless(receive_events(1));
	DO(!(is_pending(id) == false)); /* ! because it looks for 0. */

	DO(unlock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));

	closeit(fd);
	free_pending();

}

/**
 * Test locking exclusive and getting 2 async shared locks when the exclusive
 * is unlocked.
 */
TEST(async_lock2)
{
	int fd, fd1, fd2;
	uint64_t id1, id2;
	static struct test_range rng;
	rng = test_r[0];

	id1 = ids++;
	id2 = ids++;

	fd = openit();
	fd1 = openit();
	fd2 = openit();
	DO(lock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));
	FAIL(async(fd1, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_SR,
	    SEEK_SET, rng, id1), EINPROGRESS);
	FAIL(async(fd2, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_SR,
	    SEEK_SET, rng, id2), EINPROGRESS);
	DO(unlock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));
	DO(!(is_pending(id1)== true)); /* ! because it looks for 0. */
	DO(!(is_pending(id2)== true)); /* ! because it looks for 0. */

	fail_unless(receive_events(2));
	DO(!(is_pending(id1)== false)); /* ! because it looks for 0. */
	DO(!(is_pending(id2)== false)); /* ! because it looks for 0. */

	DO(unlock(fd1, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_SR, SEEK_SET, rng));
	DO(unlock(fd2, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_SR, SEEK_SET, rng));
	closeit(fd);
	closeit(fd1);
	closeit(fd2);
	free_pending();

}

/**
 * Test canceling a lock in progress.
 */
TEST(cancel_lock)
{
	int fd;
	uint64_t id;
	static struct test_range rng;
	rng = test_r[0];

	id = ids++;

	fd = openit();
	DO(lock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));
	FAIL(async(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_SR,
	    SEEK_SET, rng, id), EINPROGRESS);

	FAIL(cancel(fd + 1, rng, LOCK_DOMAIN_CPOOL_STUB, id),  EBADF);
	FAIL(cancel(fd, rng, LOCK_DOMAIN_CPOOL_STUB, id - 1),  EINVAL);
	DO(cancel(fd, rng, LOCK_DOMAIN_CPOOL_STUB, id));

	closeit(fd);
	fail_unless(receive_events(0));
	free_pending();

}

/**
 * Test closing the file with async locks in progress (they should be
 * canceled.)
 */
TEST(async_close)
{
	int  fd;
	uint64_t id, id2;
	static struct test_range rng;
	rng = test_r[0];

	id = ids++;
	id2 = ids++;

	fd = openit();
	DO(lock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));
	FAIL(async(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X,
	    SEEK_SET, rng, id), EINPROGRESS);
	FAIL(async(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X,
	    SEEK_SET, rng, id2), EINPROGRESS);

	closeit(fd);
	/* No events expected in the close case. */
	fail_unless(receive_events(0));
	free_pending();
}


/**
 * Test canceling a lock that has been locked.
 */
TEST(cancel_locked)
{
	int  fd;
	uint64_t id;
	static struct test_range rng;
	rng = test_r[0];

	id = ids++;

	fd = openit();

	/* Get lock to guarantee the second goes async. */
	DO(lock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));
	FAIL(async(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X,
	    SEEK_SET, rng, id), EINPROGRESS);
	DO(unlock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));

	/* Get async lock. */
	fail_unless(receive_events(1));

	/* Try to cancel it, simulating a race. */
	FAIL(cancel(fd, rng, LOCK_DOMAIN_CPOOL_STUB, id), EINVAL);

	closeit(fd);
	free_pending();

}


/**
 * Dual-fd test. Both are taking the same locks, one gives up.
 */
TEST(dual_lock)
{
	int fd1, fd2;
	uint64_t id1, id2;
	static struct test_range rng;
	rng = test_r[0];

	id1 = ids++;
	id2 = ids++;
	fd1 = openit();
	fd2 = openit();

	DO(lock(fd1, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_SR, SEEK_SET, rng));
	DO(lock(fd2, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_SR, SEEK_SET, rng));


	FAIL(async(fd1, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X,
	    SEEK_SET, rng, id1), EINPROGRESS);
	FAIL(async(fd2, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X,
	    SEEK_SET, rng, id2), EINPROGRESS);

	closeit(fd1);
	DO(unlock(fd2, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_SR, SEEK_SET, rng));

	fail_unless(receive_events(1));
	fail_unless(last_event_id == (int) id2);
	fail_unless(last_event_success == true);


	free_pending();
}

/**
 * FAILPOINTS TESTING
 */


/**
 * Test that the lock is failed when the failpoint is set.
 */
TEST(failpoint_lock_async_fail_einval)
{
	int fd;
	uint64_t id;
	static struct test_range rng;
	rng = test_r[0];

	fail_if(enable_failpoint(LOCK_ASYNC_FAIL_EINVAL, "return(1)"));

	id = ids++;
	fd = openit();

	DO(lock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_SR, SEEK_SET, rng));
	FAIL(async(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_SR,
	    SEEK_SET, rng, id), EINPROGRESS);

	fail_unless(receive_events(1));
	fail_unless(last_event_id == (int) id);
	fail_unless(last_event_failure == true);
	DO(unlock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_SR, SEEK_SET, rng));

	closeit(fd);
	free_pending();

	fail_if(disable_failpoint(LOCK_ASYNC_FAIL_EINVAL));
}

/**
 * Same as cpool_failpoint_cpool_lock_async_fail_einval, but 
 * should cause cpool_lock_async to race with the callback, 
 * hitting cleanup code in lock_async. 
 */
TEST(failpoint_lock_async_sleep)
{
	int fd;
	uint64_t id;
	static struct test_range rng;
	rng = test_r[0];


	fail_if(enable_failpoint(LOCK_ASYNC_FAIL_EINVAL, "return(1)"));
	fail_if(enable_failpoint(LOCK_ASYNC_SLEEP, "return(10)"));

	id = ids++;
	fd = openit();

	DO(lock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));
	FAIL(async(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X,
	    SEEK_SET, rng, id), EINPROGRESS);

	fail_unless(receive_events(1));
	fail_unless(last_event_id == (int) id);
	fail_unless(last_event_failure == true);
	DO(unlock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));

	closeit(fd);
	free_pending();

	fail_if(disable_failpoint(LOCK_ASYNC_SLEEP));
	fail_if(disable_failpoint(LOCK_ASYNC_FAIL_EINVAL));
}


/**
 * Hit race condition between cancel and async_lock.
 */
TEST(failpoint_async_cb_sleep)
{
	int fd;
	uint64_t id;
	static struct test_range rng;
	rng = test_r[0];

	fail_if(enable_failpoint(ASYNC_CB_SLEEP, "return(100)"));
	fail_if(enable_failpoint(LOCK_ASYNC_SLEEP, "return(10)"));

	id = ids++;
	fd = openit();

	DO(lock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));
	FAIL(async(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X,
	    SEEK_SET, rng, id), EINPROGRESS);
	DO(cancel(fd, rng, LOCK_DOMAIN_CPOOL_STUB, id));
	DO(unlock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));

	closeit(fd);
	free_pending();

	fail_if(disable_failpoint(LOCK_ASYNC_SLEEP));
	fail_if(disable_failpoint(ASYNC_CB_SLEEP));
}


/**
 * Hit race condition between callback and unlock_all.
 */
TEST(failpoint_cb_races_unlockall)
{
	int fd;
	uint64_t id;
	static struct test_range rng;
	rng = test_r[0];

	fail_if(enable_failpoint(ASYNC_CB_SLEEP, "return(100)"));

	id = ids++;
	fd = openit();

	DO(lock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));
	FAIL(async(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X,
	    SEEK_SET, rng, id), EINPROGRESS);


	closeit(fd);
	free_pending();

	fail_if(disable_failpoint(ASYNC_CB_SLEEP));
	usleep(100);
}

/**
 * Async lock, with simulated cancel request.
 */
TEST(failpoint_async_cb_fake_cancel)
{
	int fd;
	uint64_t id;
	int err;
	int i = 1;
	static struct test_range rng;
	rng = test_r[0];

	fail_if(enable_failpoint(ASYNC_CB_FAKE_CANCEL, "return(1)"));

	id = ids++;
	fd = openit();

	DO(lock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));
	FAIL(async(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X,
	    SEEK_SET, rng, id), EINPROGRESS);
	DO(unlock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));

	/*
	 * Get lock because above async was canceled by failpoint.
	 *
	 * Loop and sleep to give time for cancellation. It shouldn't be
	 * treated as an error for this to fail for a bit and getting the
	 * timing right is tricky.
	 */
	for (;;) {
		usleep(100 * i);
		if (i < 5)
			err = lock(fd, LOCK_DOMAIN_CPOOL_STUB,
				   CPOOL_LK_X, SEEK_SET, rng);
		else
			DO(lock(fd, LOCK_DOMAIN_CPOOL_STUB,
				CPOOL_LK_X, SEEK_SET, rng));

		if (err == 0)
			break;
		i++;
		printf("\nfailpoint_async_cb_fake_cancel: Trying again (#%d). "
		    "error=%d\n", i, errno);
	}

	/* Should be no events. */
	fail_unless(receive_events(0));

	closeit(fd);
	free_pending();

	fail_if(disable_failpoint(ASYNC_CB_FAKE_CANCEL));
}

/**
 * Async lock that fails, with simulated cancel request.
 */
TEST(failpoint_async_cb_fake_cancel_fail)
{
	int fd;
	uint64_t id;
	static struct test_range rng;
	rng = test_r[0];

	fail_if(enable_failpoint(LOCK_ASYNC_FAIL_EINVAL, "return(1)"));
	fail_if(enable_failpoint(ASYNC_CB_FAKE_CANCEL, "return(1)"));

	id = ids++;
	fd = openit();

	DO(lock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));
	FAIL(async(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X,
	    SEEK_SET, rng, id), EINPROGRESS);
	DO(unlock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));

	/* Get lock because above async was canceled by failpoint. */
	DO(lock(fd, LOCK_DOMAIN_CPOOL_STUB, CPOOL_LK_X, SEEK_SET, rng));

	/* Should be no events. */
	fail_unless(receive_events(0));

	closeit(fd);
	free_pending();

	fail_if(disable_failpoint(ASYNC_CB_FAKE_CANCEL));
	fail_if(disable_failpoint(LOCK_ASYNC_FAIL_EINVAL));
}

/**
 * Make sure that isi_cpool_revoked_fd function
 * works as expected.
 */
TEST(test_isi_cpool_revoked_fd)
{
	int fd;
	fd = openit();
	fail_if(isi_cpool_revoked_fd(fd), "FD is not revoked at this point");

}


