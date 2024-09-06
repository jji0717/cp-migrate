#include <isi_migrate/migr/check_helper.h>
#include <isi_migrate/migr/isirep.h>

#include "sworker.h"

SUITE_DEFINE_FOR_FILE(migr_lists, .timeout = 60, .mem_check = CK_MEM_DISABLE);

static bool
file_exists(char *dir, char *fname) {
	char tmpbuf[64];
	struct stat st;

	sprintf(tmpbuf, "%s/%s", dir, fname);
	return (stat(tmpbuf, &st) == 0);
}

struct migr_sworker_ctx sw_ctx;
TEST(foo, .attrs="overnight")
{
	char *testdir;
	struct filelist flist;
	struct isi_error *error = NULL;
	struct migr_dir *mdir = NULL;
	struct migr_dirent *mde = NULL;
	int sockfds[2];
	int sourcedirfd, targetdirfd;
	char sourcedir[32], targetdir[32];
	struct generic_msg m;
	struct ptr_vec a, b;
	int i;
	
	char *keep_files[] = {
		"aaaaaaaaaa",
		"aaaaaaaaaA",
		"aaaaaaaaAa",
		"aaaaaaaaAA",
		"aaaaaaaAaa",
		"aaaaaaaAaA",
		"aaaaaaaAAa",
		"aaaaaaaAAA",
		"aaaaaaAaaa",
		"aaaaaaAaaA" };

	char delete_file_a[] = "AAAAAAAAAA";
	char delete_file_b[] = "foobar";

	printf("\n");
	testdir = create_new_dir();
	printf("testdir: %s\n", testdir);

	/* create socket pair for simulated pworker/sworker connection */
	fail_unless(socketpair(AF_LOCAL, SOCK_STREAM, 0, sockfds) == 0);

	/* register the receiver socket */
	migr_add_fd(sockfds[1], NULL, "check");
	
	/* create source dir */
	sprintf(sourcedir, "%s/source", testdir);
	fail_unless(mkdir(sourcedir, 0777) == 0);
	sourcedirfd = open(sourcedir, O_RDONLY);
	
	/* create target dir */
	sprintf(targetdir, "%s/target", testdir);
	fail_unless(mkdir(targetdir, 0777) == 0);
	targetdirfd = open(targetdir, O_RDONLY);

	/* create corresponding files on source and target with same hash
	 * values and inconsistent collision bits by creating them in different
	 * orders */
	for (i = 0; i < 10; i++) {
		create_file(sourcedir, keep_files[i]);
		create_file(targetdir, keep_files[9 - i]);
	}

	/* create files on target that don't exist in the source directory.
	 * one has the same hash value as the "keepers", the other has a
	 * different hash. both should be deleted after last list message is
	 * processed */
	create_file(targetdir, delete_file_a);
	create_file(targetdir, delete_file_b);

	/* set up relevant contexts */
	sw_ctx.del.mdir = NULL;
	sw_ctx.del.in_progress = false;
	sw_ctx.del.hold_queue = &a;
	sw_ctx.del.next_hold_queue = &b;
	pvec_init(sw_ctx.del.hold_queue);
	pvec_init(sw_ctx.del.next_hold_queue);
	sw_ctx.worker.cwd_fd = targetdirfd;
	sw_ctx.global.logdeleted = false;
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;

	init_statistics(&sw_ctx.stats.tw_cur_stats);
	init_statistics(&sw_ctx.stats.stf_cur_stats);

	/* override the list size so the list will be split into multiple
	 * messages */
	listoverridesize(30);
	
	/* add files in the source dir to the list and flush it */
	flist.list = NULL;
	flist.size = 0;
	listreset(&flist);
	mdir = migr_dir_new(dup(sourcedirfd), NULL, true, &error);
	while ((mde = migr_dir_read(mdir, &error))) {
		listadd(&flist, mde->dirent->d_name, mde->dirent->d_encoding,
		    mde->cookie, LIST_TO_SYNC, sockfds[0], NULL);
		printf("adding %s to list\n", mde->dirent->d_name);
		migr_dir_unref(mdir, mde);
	}
	listflush(&flist, LIST_TO_SYNC, sockfds[0]);

	/* handle the list messages */
	while (read_msg_from_fd(sockfds[1], &m, false, true, NULL) == 0) {
		printf("listmsg2 begin: %d end: %d contd: %d last: %d "
		    "used: %d\n",
		    m.body.list2.range_begin, m.body.list2.range_end,
		    m.body.list2.continued, m.body.list2.last,
		    m.body.list2.used);

		process_list(m.body.list2.list, m.body.list2.used,
		    m.body.list2.range_begin, m.body.list2.range_end,
		    m.body.list2.continued, m.body.list2.last, &sw_ctx,
		    &error);
	}

	/* check that files that are supposed to exist are still there */
	for (i = 0; i < 10; i++)
		fail_unless(file_exists(targetdir, keep_files[i]));

	/* check that files that were supposed to be deleted are gone */
	fail_unless(!file_exists(targetdir, delete_file_a));
	fail_unless(!file_exists(targetdir, delete_file_b));

	
	siq_stat_free(sw_ctx.stats.stf_cur_stats);
	siq_stat_free(sw_ctx.stats.tw_cur_stats);
	/* cleanup */
	migr_dir_free(mdir);
	close(sourcedirfd);
	close(targetdirfd);
	clean_dir(testdir);
}
