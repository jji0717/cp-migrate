#include <stdio.h>
#include <errno.h>
#include <fcntl.h>

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/param.h>
#include <isi_util/util_adt.h>
#include <fts.h>
#include "pworker.h"
#include "test_helper.h"


u_int64_t repstate_tester(char *root, const char *policy,
    char *repstate_name, struct isi_error **error_out)
{
	FTS *fts = NULL;
	FTSENT *ent;
	char *fts_args[2];
	struct uint64_uint64_map map;
	uint64_t ctr;
	struct stat st;
	int ret;
	const uint64_t *key_ptr;
	const uint64_t *val_ptr;
	struct rep_ctx *repctx = NULL;
	struct rep_iter *rit = NULL;
	struct rep_entry re;
	u_int64_t re_key;
	uint64_t rs_entries = 0;
	struct isi_error *error = NULL;
	uint64_t missing = 0;
	uint64_t extra = 0;
	uint64_t mismatch = 0;
	bool got_one;
	bool found;

	uint64_uint64_map_init(&map);

	fts_args[0] = root;
	fts_args[1] = NULL;

	ret = stat(root, &st);
	if (ret < 0) {
		perror("Stat failed");
		error = isi_system_error_new(errno, "Stat failed");
		goto out;
	}
	fts = fts_open(fts_args, FTS_PHYSICAL, NULL);
	if (!fts) {
		perror("fts_open failed");
		error = isi_system_error_new(errno, "fts open failed");
		goto out;
	}
	while ((ent = fts_read(fts))) {
		if (ent->fts_info == FTS_D && (
		    0 == strcmp(ent->fts_name, ".snapshot") ||
		    0 == strcmp(ent->fts_name, ".ifsvar") )) {
			fts_set(fts, ent, FTS_SKIP);
		} else if (ent->fts_info != FTS_DP &&
		    ent->fts_info != FTS_DC) {
			if (uint64_uint64_map_contains(&map, ent->fts_ino))
				ctr = *uint64_uint64_map_find(&map,
				    ent->fts_ino);
			else
				ctr = 0;
			ctr++;
			uint64_uint64_map_add(&map, ent->fts_ino, &ctr);
		}
	}


	open_repstate(policy, repstate_name, &repctx, &error);
	if (error) {
		fprintf(stderr, "Error opening repstate %s\n", repstate_name);
		goto out;
	}
	rs_entries = 0;
	rit = new_rep_iter(repctx);
	while (true) {
		got_one = rep_iter_next(rit, &re_key, &re, &error);
		if (error)
			goto out;
		if (!got_one)
			break;
		rs_entries++;
		if (re.lcount) {
			val_ptr = uint64_uint64_map_find(&map, re_key);
			if (!val_ptr) {
				printf("Repstate has extra entry: lin %llx, "
				    "lcount %d, is_dir %d, not_on_target %d\n",
				    re_key, re.lcount, re.is_dir,
				    re.not_on_target);
				extra++;
			} else if (*val_ptr != re.lcount) {
				printf("Mismatched lcount: lin %llx,"
				    " rep count %d, check count %d,"
				    " is_dir %d, not_on_target %d\n",
				    re_key, (int)re.lcount, (int)*val_ptr,
				    re.is_dir, re.not_on_target);
				mismatch++;
			}
		}
	}
	close_rep_iter(rit);
	rit = NULL;

	UINT64_UINT64_MAP_FOREACH(key_ptr, val_ptr, &map) {
		found = get_entry(*key_ptr, &re, repctx, &error);
		if (error)
			goto out;
		if (!found || !re.lcount) {
			printf("Repstate missing entry: lin %llx, lcount %d,"
			    " being removed %d\n",
			    *key_ptr, (int)*val_ptr, found);
			missing++;
		}
	}

	if (!error && (missing || extra || mismatch)) {
		error = isi_system_error_new(errno, "Bad repstate:"
		    " missing: %lld, extra: %lld, mismatch: %lld",
		    missing, extra, mismatch);
	}

 out:
	if (fts)
		fts_close(fts);
	if (repctx)
		close_repstate(repctx);
	if (rit)
		close_rep_iter(rit);
	uint64_uint64_map_clean(&map);
	isi_error_handle(error, error_out);
	return rs_entries;
}

void
init_pw_ctx(struct migr_pworker_ctx *pw_ctx)
{
	memset(pw_ctx, 0, sizeof(*pw_ctx));
	pw_ctx->flags |= FLAG_STF_SYNC;
	pw_ctx->action = SIQ_ACT_SYNC;
	pw_ctx->sworker = SOCK_LOCAL_OPERATION;
	pw_ctx->coord = SOCK_LOCAL_OPERATION;
	pw_ctx->file_state.fd = -1;
	pw_ctx->wi_lin = 1000;
	pw_ctx->stf_sync = true;
}
