#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/linmap.h"
#include "isi_migrate/migr/siq_btree.h"
#include "isi_migrate/migr/repstate.h"
#include "isi_migrate/migr/treewalk.h"
#include "sbtree.h"

void usage(void);

/*
 * Given a lin mapping file, create a new linmap 
 * with all lins flipped.
 */
static void
flip_linmap(char *pol_name, char *map_name,
    char *map_file, struct isi_error **error_out)
{
	FILE *fp = NULL;
	struct map_ctx *ctx = NULL;
	struct isi_error *error = NULL;
	ifs_lin_t lin1, lin2;
	char c;

	/* First remove the linmap if it exists */
	remove_linmap(pol_name, map_name, true, &error);
	if (error)
		goto out;

	/* Create the linmap */
	create_linmap(pol_name, map_name, false, &error);
	if (error)
		goto out;

	/* Open the linmap */
	open_linmap(pol_name, map_name, &ctx, &error);
	if (error)
		goto out;

	/* Now open the linmap file to get mappings */
	fp = fopen(map_file, "r");
	if (fp == NULL) {
		error = isi_system_error_new(errno,
		    "Unable to open linmap file");
		goto out;
	}
	while (fscanf(fp, "%llx%c%llx\n", &lin1, &c, &lin2) != EOF) {
	//	printf("new slin %llu dlin %llu\n", lin2, lin1);
		set_mapping(lin2, lin1, ctx, &error);
		if (error)
			goto out;
	}

out:
	if (fp)
		fclose(fp);
	close_linmap(ctx);
	isi_error_handle(error, error_out);
}

/*
 * Given repstate mapping file , create a new repstate by flipping the lins
 * with linmap database.
 */
static void
flip_repstate(char *pol_name, char *rep_name, ifs_snapid_t snapid,
    char *lmap_name, char *rep_file, struct isi_error **error_out)
{
	FILE *fp = NULL;
	char c;
	bool found;
	char str_temp[1024];
	struct map_ctx *lmap_ctx = NULL;
	struct rep_entry entry = {};
	struct rep_ctx *rep_ctx = NULL;
	struct isi_error *error = NULL;
	ifs_lin_t slin, dlin;
	struct rep_info_entry rie;

	ASSERT(snapid > 0);
	/* Open the linmap */
	open_linmap(lmap_name, lmap_name, &lmap_ctx, &error);
	if (error)
		goto out;

	/* Remove the old repstate mapping */
	remove_repstate(pol_name, rep_name, true, &error);
	if (error)
		goto out;

	/* Create new repstate map */
	create_repstate(pol_name, rep_name, false, &error);
	if (error)
		goto out;

	/* Open the repstate */
	open_repstate(pol_name, rep_name, &rep_ctx, &error);
	if (error)
		goto out;

	/* Add repinfo entry first */
	rie.initial_sync_snap = snapid;
	rie.is_sync_mode = true;
	set_rep_info(&rie, rep_ctx, false, &error);
	if (error)
		goto out;

	/* Now open the linmap file to gt mappings */
	fp = fopen(rep_file, "r");
	if (fp == NULL) {
		error = isi_system_error_new(errno,
		    "Unable to open linmap file");
		goto out;
	}
	while (fscanf(fp, "%llx %[^:]%c%u %[^:]%c%u %[^:]%c%u "
		    "%[^\n]\n", &slin, str_temp, &c, &entry.lcount,
		    str_temp, &c, (unsigned int *)&entry.non_hashed, str_temp,
		    &c, (unsigned int *)&entry.is_dir, str_temp) != EOF) {
		/* Lookup the lin is linmap */
		found = get_mapping(slin, &dlin, lmap_ctx, &error);
		if (error)
			goto out;
		ASSERT(found == true);
	//	printf("lin %llu entry.lcount %u entry.non_hashed %u "
	//	    "entry.is_dir %u \n", lin, entry.lcount, entry.non_hashed,
	//	    entry.is_dir);
		set_entry(dlin, &entry, rep_ctx, 0, NULL, &error);
		if (error)
			goto out;
	}

out:
	if (fp)
		fclose(fp);
	close_linmap(lmap_ctx);
	close_repstate(rep_ctx);
	isi_error_handle(error, error_out);
}

void
usage(void)
{
	fprintf(stderr,
"\n"
"usage:\n"
"    flip repstate lins with lin mappings\n"
"    isi_flip_mapping -r pol_name rep_name snap_id linmap_name repstate_file\n"
"\n    flip lin mappings\n"
"    isi_flip_mapping -l pol_name linmap_name linmap_file\n"
"\n");
	exit(EXIT_FAILURE);
}

int
main(int argc, char **argv)
{
	char c, d;
	char *pol_name = NULL;
	char *rep_name = NULL;
	char *linmap_name = NULL;
	char *linmap_file = NULL;
	char *repstate_file = NULL;
	ifs_snapid_t snapid;
	struct isi_error *error = NULL;

	if (argc < 5)
		usage();

	if (sscanf(argv[1], "-%c%c", &c, &d) != 1)
		usage();

	pol_name = argv[2];

	switch (c) {
	case 'l':
		linmap_name = argv[3];
		linmap_file = argv[4];
		flip_linmap(pol_name, linmap_name, linmap_file, &error);
		if (error)
			goto out;
		break;

	case 'r':
		if (argc != 7)
			usage();
		rep_name = argv[3];
		snapid = atoll(argv[4]);
		linmap_name = argv[5];
		repstate_file = argv[6];	
		flip_repstate(pol_name, rep_name, snapid, linmap_name,
		    repstate_file, &error);
		if (error)
			goto out;
		break;

	default:
		usage();
	}

out:
	if (error) {
		printf("%s", isi_error_get_message(error));
		isi_error_free(error);
	}
}
