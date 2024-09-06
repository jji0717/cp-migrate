#include <ifs/ifs_lin_open.h>

#include <isi_domain/dom.h>
#include <isi_domain/worm.h>
#include "isi_snapshot/snapshot.h"

#include <isi_migrate/config/siq_job.h>
#include "isirep.h"
#include "check_helper.h"

SUITE_DEFINE_FOR_FILE(migr_utils, .timeout = 120);

static void
do_find_domain_by_lin_test(ifs_lin_t lin, enum domain_type type,
    bool ready_only, ifs_domainid_t exp_dom, uint32_t exp_gen, bool exp_error)
{
	ifs_domainid_t dom_out = 0;
	uint32_t gen_out = 0;
	struct isi_error *error = NULL;

	find_domain_by_lin(lin, type, ready_only, &dom_out, &gen_out, &error);

	fail_if((error == NULL) == exp_error, (error != NULL) ?
	    isi_error_get_message(error) : "No error-- error expected.");
	fail_unless(dom_out == exp_dom, "Exp dom %llu, got %llu", exp_dom,
	   dom_out);
	fail_unless(gen_out == exp_gen, "Exp gen %u, got %u", exp_gen, gen_out);
}

TEST(test_find_domain_by_lin)
{
	ifs_domainid_t dom1 = 0;
	ifs_domainid_t dom2 = 0;

	uint32_t gen1 = 0;
	uint32_t gen2 = 0;

	char *dirname = NULL;
	char filename[MAXPATHLEN];

	int fd = -1;
	struct stat dst = {};
	struct stat st = {};

	dirname = create_new_dir();

	fail_if_err(stat(dirname, &dst));

	//XXXDPL Add DT_WORM testcases on done merging from BR_RIPT_WORM.

	// No domains exist. Fail unless all iterations of find_domain_by_lin()
	// return no results.
	do_find_domain_by_lin_test(dst.st_ino, DT_SYNCIQ, false, 0, 0, false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SYNCIQ, true, 0, 0, false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SNAP_REVERT, false, 0, 0,
	    false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SNAP_REVERT, true, 0, 0, false);
	fail_if_err(dom_make_domain(dst.st_ino, &dom1, &gen1));
	fail_if_err(dom_set_siq(dom1));
	fail_unless(dom1 >= DOMAIN_FIRST_ID);
	fail_if_err(ifs_domain_add_bylin(dst.st_ino, dom1));

	fail_if_err(dom_make_domain(dst.st_ino, &dom2, &gen2));
	fail_if_err(dom_set_siq(dom2));
	fail_unless(dom2 >= DOMAIN_FIRST_ID);
	fail_if_err(ifs_domain_add_bylin(dst.st_ino, dom2));

	sprintf(filename, "%s/test.file.XXXXXX", dirname);
	(void)mktemp(filename);

	fd = open(filename, O_CREAT | O_RDWR, 0600);
	fail_unless(fd > 0);
	fail_if_err(fstat(fd, &st));
	close(fd);
	fd = -1;

	// Both SIQ domains non-ready, so should return the lowest domain id
	// for the non-ready SIQ find only.

	do_find_domain_by_lin_test(dst.st_ino, DT_SYNCIQ, false, dom1, gen1,
	    false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SYNCIQ, true, 0, 0, false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SNAP_REVERT, false, 0, 0,
	    false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SNAP_REVERT, true, 0, 0,
	    false);

	// find_domain_by_lin() only matches domains rooted at the specified
	// LIN. LINs contained within the root LIN do not match the root LINs
	// domain set.

	do_find_domain_by_lin_test(st.st_ino, DT_SYNCIQ, false, 0, 0, false);
	do_find_domain_by_lin_test(st.st_ino, DT_SYNCIQ, true, 0, 0, false);
	do_find_domain_by_lin_test(st.st_ino, DT_SNAP_REVERT, false, 0, 0,
	    false);
	do_find_domain_by_lin_test(st.st_ino, DT_SNAP_REVERT, true, 0, 0, false);

	fail_if_err(unlink(filename));

	fail_if_err(dom_mark_ready(dom2));
	do_find_domain_by_lin_test(dst.st_ino, DT_SYNCIQ, false, dom1, gen1,
	    false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SYNCIQ, true, dom2, gen2,
	    false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SNAP_REVERT, false, 0, 0,
	    false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SNAP_REVERT, true, 0, 0,
	    false);

	fail_if_err(dom_remove_domain(dom1));
	fail_if_err(dom_remove_domain(dom2));

	dom1 = 0;
	dom2 = 0;

	// Domains marked with DOM_RESTRICTED_WRITE but not DOM_SNAPREVERT are
	// legacy SyncIQ domains and should match searches for DT_SYNCIQ.

	fail_if_err(dom_make_domain(dst.st_ino, &dom1, &gen1));
	fail_unless(dom1 >= DOMAIN_FIRST_ID);
	fail_if_err(ifs_domain_add_bylin(dst.st_ino, dom1));

	fail_if_err(dom_make_domain(dst.st_ino, &dom2, &gen2));
	fail_unless(dom2 >= DOMAIN_FIRST_ID);
	fail_if_err(ifs_domain_add_bylin(dst.st_ino, dom2));

	/* Bug 157261 - DOM_RESTRICTED_WRITE is enforced immediately */
	fail_if_err(dom_set_readonly(dom1, true));
	fail_if_err(dom_set_readonly(dom2, true));

	do_find_domain_by_lin_test(dst.st_ino, DT_SYNCIQ, false, dom1, gen1,
	    false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SYNCIQ, true, 0, 0, false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SNAP_REVERT, false, 0, 0,
	    false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SNAP_REVERT, true, 0, 0,
	    false);

	fail_if_err(dom_mark_ready(dom2));
	do_find_domain_by_lin_test(dst.st_ino, DT_SYNCIQ, false, dom1, gen1,
	    false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SYNCIQ, true, dom2, gen2,
	    false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SNAP_REVERT, false, 0, 0,
	    false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SNAP_REVERT, true, 0, 0,
	    false);

	// Domains marked with DOM_RESTRICTED_WRITE and DOM_SNAPREVERT are
	// snaprevert domains and should NOT match searches for DT_SYNCIQ.

	fail_if_err(dom_set_snaprevert(dom2));
	do_find_domain_by_lin_test(dst.st_ino, DT_SYNCIQ, false, dom1, gen1,
	    false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SYNCIQ, true, 0, 0, false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SNAP_REVERT, false, dom2,
	    gen2, false);
	do_find_domain_by_lin_test(dst.st_ino, DT_SNAP_REVERT, true, dom2, gen2,
	    false);

	fail_if_err(dom_remove_domain(dom1));
	fail_if_err(dom_remove_domain(dom2));

	dom1 = 0;
	dom2 = 0;

	rmdir(dirname);
	free(dirname);
}

/*
 * Test case for recreate_links_from_snap_version().
 * Make sure we can re-create all the links by looking
 * at the snapshot version of the lin.
 */
TEST(create_links_from_snap)
{
	struct isi_error *ie = NULL;
	char *dir;
	char *file1="filechanged";
	char *file1_link="filechanged_link";
	char *dir1="dir_link";
	char *tmpdir="tmp_working_dir";
	struct stat file1_stat;
	struct stat file2_stat;
	char path[MAXPATHLEN];
	char snap_path[MAXPATHLEN];
	struct isi_str snap_str1, *paths[2];
	ifs_snapid_t snapid1;
	struct fmt FMT_INIT_CLEAN(fmt1);
	struct fmt FMT_INIT_CLEAN(fmt2);
	int fd;

	dir = create_new_dir();

	sprintf(path, "%s/%s", dir,dir1);
	fail_if_err(mkdir(path, 0755));

	sprintf(path, "%s/%s", dir,tmpdir);
	fail_if_err(mkdir(path, 0755));

	create_file(dir, file1);

	sprintf(path, "ln %s/%s %s/%s", dir, file1, dir, tmpdir);
	system(path);

	/* Create a link within the same directory */
	sprintf(path, "ln %s/%s %s/%s", dir, file1, dir, file1_link);
	system(path);

	fmt_print(&fmt1, "%s/%s", dir,file1);
	// Get lin number for file
	fd = open(fmt_string(&fmt1), O_RDONLY);
	fail_unless(fd != -1, "%s: %s", fmt_string(&fmt1),
	    strerror(errno));
	fail_unless(fstat(fd, &file1_stat) != -1, "%s : %s",file1,
		strerror(errno));
	close(fd);

	/* Assign lin number as snapshot name */
	sprintf(snap_path, "%s_%llu", file1, file1_stat.st_ino);

	paths[0] = isi_str_alloc(dir, strlen(dir)+1, ENC_UTF8,
	    ISI_STR_NO_MALLOC);
	paths[1] = NULL;
	// create first snapshot
	isi_str_init(&snap_str1, snap_path, strlen(snap_path)+1, ENC_UTF8,
	    ISI_STR_NO_MALLOC);
	snapid1 = snapshot_create(&snap_str1, (time_t)0, paths, NULL, &ie);
	fail_if_error(ie);

	/* Unlink all the links */
	remove_all_entries_fullscan(file1_stat.st_ino, true, &ie);
	fail_if_error(ie);

	/* Make sure all the links are gone on HEAD. */
	sprintf(path, "%s/%s", dir, file1);
	fail_if(stat(path, &file2_stat) == 0);

	/* Make sure all the links are gone on HEAD. */
	sprintf(path, "%s/%s", dir, file1_link);
	fail_if(stat(path, &file2_stat) == 0);

	/* Make sure all the links are gone on HEAD. */
	sprintf(path, "%s/%s/%s", dir, tmpdir, file1);
	fail_if(stat(path, &file2_stat) == 0);

	/* Create a lin in tmp working dir */
	sprintf(path, "%s/%s", dir, tmpdir);
	create_file(path, file1);

	fmt_print(&fmt2, "%s/%s", path,file1);

	// Get lin number for file
	fd = open(fmt_string(&fmt2), O_RDONLY);
	fail_unless(fd != -1, "%s: %s", fmt_string(&fmt1),
	    strerror(errno));
	fail_unless(fstat(fd, &file2_stat) != -1, "%s : %s",file1,
		strerror(errno));
	close(fd);

	/* Now re-create all the links on the HEAD version. */

	recreate_links_from_snap_version(file1_stat.st_ino, snapid1,
	    file2_stat.st_ino, false, &ie);
	fail_if_error(ie);

	/* Make sure all the links are present on HEAD. */
	sprintf(path, "%s/%s", dir, file1);
	fail_if(stat(path, &file2_stat) != 0);

	/* Make sure all the links are present on HEAD. */
	sprintf(path, "%s/%s", dir, file1_link);
	fail_if(stat(path, &file2_stat) != 0);

	/* Make sure all the links are gone on HEAD. */
	sprintf(path, "%s/%s/%s", dir, tmpdir, file1);
	fail_if(stat(path, &file2_stat) != 0);

	clean_dir(dir);
	snapshot_destroy(&snap_str1, &ie);
	fail_if_error(ie);
	isi_str_free(paths[0]);
}

TEST(test_dir_get_dirent_path)
{
	char *pdirname = NULL;
	char dirname[] = "dir";
	char *path = NULL;

	struct stat pdst = {};
	struct stat dst = {};
	int path_len;
	int fd = -1;

	struct isi_error *error = NULL;

	pdirname = create_new_dir();
	fail_if_err(stat(pdirname, &pdst));

	fd = ifs_lin_open(pdst.st_ino, HEAD_SNAPID, O_RDONLY);
	fail_unless(fd > 0);

	fail_if(enc_mkdirat(fd, dirname, ENC_DEFAULT, 0777));

	fail_if(enc_fstatat(fd, dirname, ENC_DEFAULT, &dst, AT_SYMLINK_NOFOLLOW
	    ));
	dir_get_dirent_path(ROOT_LIN, dst.st_ino, HEAD_SNAPID, pdst.st_ino,
	    &path, &path_len, &error);
	fail_unless(path_len);
	enc_unlinkat(fd, dirname, ENC_DEFAULT, AT_REMOVEDIR);
	free(path);
	rmdir(pdirname);
	free(pdirname);
}

#define DEL_ENTRY_NAME "del_entry"
TEST(test_try_force_delete_entry, .attrs="overnight")
{
	/* create non-worm file, dont worm delete */
	char *dirname = NULL, fname[MAXPATHLEN] = DEL_ENTRY_NAME;
	struct stat dst = {}, st = {};
	ifs_domainid_t worm_dom = 0;
	uint32_t worm_dom_gen = 0;
	int ret, dfd, fd;

	dirname = create_new_dir();
	fail_if_err(stat(dirname, &dst));

	dfd = ifs_lin_open(dst.st_ino, HEAD_SNAPID, O_RDONLY);
	fail_if(dfd == -1);

	fd = enc_openat(dfd, fname, ENC_DEFAULT, O_CREAT | O_RDWR, 0600);
	fail_if(fd == -1);
	fail_if_err(fstat(fd, &st));

	close(fd);
	fd = -1;

	ret = try_force_delete_entry(dfd, fname, ENC_DEFAULT);
	fail_unless(ret == -1);

	fail_if_err(dom_make_domain(dst.st_ino, &worm_dom, &worm_dom_gen));
	fail_if_err(dom_set_worm(worm_dom));
	fail_if_err(dom_set_min_offset(worm_dom, WORM_RETAIN_FOREVER));
	fail_if_err(dom_toggle_privdel(worm_dom, true));
	fail_if_err(ifs_domain_add_bylin(dst.st_ino, worm_dom));
	fail_if_err(ifs_domain_add_bylin(st.st_ino, worm_dom));
	fail_if_err(dom_mark_ready(worm_dom));

	fd = ifs_lin_open(st.st_ino, HEAD_SNAPID, O_RDONLY);
	fail_if(fd == -1);
	/* worm commit file */
	fail_if_err(fchmod(fd, 0400));
	close(fd);

	fail_if_err(try_force_delete_entry(dfd, fname, ENC_DEFAULT));

	close(dfd);
	clean_dir(dirname);

}

TEST(test_max_concurrent_snapshots)
{
	int lock_fd = -1;
	int lock_fd2 = -1;
	struct isi_error *error = NULL;

	lock_fd = get_concurrent_snapshot_lock(1, &error);
	fail_if(error);
	fail_unless(lock_fd >= 0);

	lock_fd2 = get_concurrent_snapshot_lock(1, &error);
	fail_if(error);
	fail_unless(lock_fd2 == -1);

	lock_fd2 = get_concurrent_snapshot_lock(2, &error);
	fail_if(error);
	fail_unless(lock_fd2 >= 0);

	close(lock_fd2);
	lock_fd2 = -1;

	close(lock_fd);
	lock_fd = -1;

	lock_fd2 = get_concurrent_snapshot_lock(1, &error);
	fail_if(error);
	fail_unless(lock_fd2 >= 0);

	close(lock_fd2);
	lock_fd2 = -1;
}
