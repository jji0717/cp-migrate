#include <stdlib.h>
#include <stdio.h>

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/siq_btree.h"
#include "isi_migrate/migr/compliance_map.h"
#include "sbtree.h"

#define MAPDIR "synciq/dst"

void usage(void);

static void
list_compliance_maps(char *pol_name)
{
	int root_fd = -1;
	int dir_fd = -1;
	DIR *dir = NULL;
	struct dirent *de = NULL;
	bool one_or_more = false;
	struct isi_error *error = NULL;

	if (pol_name) {
		/* list maps in a specific policy directory */
		dir_fd = siq_btree_open_dir(siq_btree_compliance_map, pol_name,
		    &error);
		if (error) {
			printf("%s\n", isi_error_get_message(error));
			isi_error_free(error);
			goto out;
		}
		ASSERT(dir_fd >= 0);
	} else {
		/* list policy directories */
		root_fd = sbt_open_root_dir(O_RDONLY);
		if (root_fd == -1)
			goto out;

		dir_fd = enc_openat(root_fd, MAPDIR, ENC_DEFAULT, O_RDONLY);
		if (dir_fd == -1) {
			printf("synciq compliance map directory does not "
			    "exist\n");
			goto out;
		}
	}

	/* fdopendir() takes ownership dir_fd regardless of result. */
	dir = fdopendir(dir_fd);
	if (dir == NULL) {
		printf("error opening compliance map dir%s%s\n",
		    pol_name ? " for policy " : "", pol_name ? : "");
		goto out;
	}
	dir_fd = -1;

	for (;;) {
		de = readdir(dir);
		if (de == NULL)
			break;

		if (!strcmp(de->d_name, ".") ||
		    !strcmp(de->d_name, "..") ||
		    !strcmp(de->d_name, ".snapshot"))
			continue;

		printf("%s\n", de->d_name);
		one_or_more = true;
	}

	if (one_or_more == false) {
		if (pol_name)
			printf("no compliance maps found in compliance map "
			    "dir for policy %s\n", pol_name);
		else
			printf("no compliance map dirs found\n");
	}

out:
	if (root_fd != -1)
		close(root_fd);
	if (dir_fd != -1)
		close(dir_fd);
	if (dir != NULL)
		closedir(dir);
}

void
usage(void)
{
	fprintf(stderr,
"usage:\n"
"    isi_compliance_map_mod -l                             (list compliance map directories)\n"
"    isi_compliance_map_mod -l pol_name                    (list compliance maps in a directory)\n"
"    isi_compliance_map_mod -c pol_name map_name           (create a new compliance map)\n"
"    isi_compliance_map_mod -p pol_name map_name           (reprotect a compliance map)\n"
"    isi_compliance_map_mod -k pol_name map_name           (kill a compliance map)\n"
"    isi_compliance_map_mod -g pol_name map_name olin      (print compliance mapping)\n"
"    isi_compliance_map_mod -s pol_name map_name olin nlin (set compliance mapping)\n"
"    isi_compliance_map_mod -d pol_name map_name olin      (delete compliance mapping)\n"
"    isi_compliance_map_mod -a pol_name map_name           (print all compliance mappings)\n"
"    isi_compliance_map_mod -r pol_name map_name low high  (print inclusive range)\n");
	exit(EXIT_FAILURE);
}

int
main(int argc, char **argv)
{
	struct compliance_map_ctx *ctx = NULL;
	uint64_t old_lin = 0;
	uint64_t new_lin = 0;
	uint64_t low, high;
	char c, d;
	char *pol_name = NULL;
	char *map_name = NULL;
	bool found;
	struct isi_error *error = NULL;
	struct compliance_map_iter *iter;

	if (argc < 2)
		usage();

	if (sscanf(argv[1], "-%c%c", &c, &d) != 1)
		usage();

	if (argc >= 3)
		pol_name = argv[2];

	if (argc >= 4) {
		map_name = argv[3];
		open_compliance_map(pol_name, map_name, &ctx, &error);
		if (error && c != 'c') {
			printf("invalid pol_name %s or map_name %s specified\n",
			    pol_name, map_name);
			isi_error_free(error);
			error = NULL;
			goto out;
		}
		error = NULL;
	}

	if (argc >= 5 && (sscanf(argv[4], "%llx", &old_lin) != 1)) {
		printf("invalid argument %s specified\n", argv[3]);
		goto out;
	}

	if (argc >= 6 && (sscanf(argv[5], "%llx", &new_lin) != 1)) {
		printf("invalid argument %s specified\n", argv[4]);
		goto out;
	}

	switch (c) {
	case 'l':
		if (argc > 3)
			usage();
		list_compliance_maps(pol_name);
		break;

	case 'c':
		if (argc != 4)
			usage();
		if (ctx) {
			printf("compliance_map %s already exists\n", map_name);
			goto out;
		}
		create_compliance_map(pol_name, map_name, false, &error);
		if (error)
			goto out;
		break;

	case 'p':
		if (argc != 4)
			usage();
		if (ctx) {
			close_compliance_map(ctx);
			ctx = NULL;
			create_compliance_map(pol_name, map_name, true, &error);
			if (error)
				goto out;
		}
		break;

	case 'k':
		if (argc != 4)
			usage();
		remove_compliance_map(pol_name, map_name, false, &error);
		if (error)
			goto out;
		break;

	case 'g':
		if (argc != 5)
			usage();
		found = get_compliance_mapping(old_lin, &new_lin, ctx, &error);
		if (error)
			goto out;
		if (found)
			printf("%llx-%llx\n", old_lin, new_lin);
		else
			printf("src_lin %llx not in map\n", old_lin);
		break;

	case 's':
		if (argc != 6)
			usage();
		set_compliance_mapping(old_lin, new_lin, ctx, &error);
		if (error)
			goto out;
		break;

	case 'd':
		if (argc != 5)
			usage();
		found = remove_compliance_mapping(old_lin, ctx, &error);
		if (!found)
			printf("src_lin %llx not in map\n", old_lin);
		break;

	case 'a':
		if (argc != 4)
			usage();
		iter = new_compliance_map_iter(ctx);
		while (true) {
			found = compliance_map_iter_next(iter, &old_lin,
			    &new_lin, &error);
			if (error || !found)
				break;
			printf("%llx-%llx\n", old_lin, new_lin);
		}
		close_compliance_map_iter(iter);
		break;

	case 'r':
		if (argc != 6)
			usage();
		iter = new_compliance_map_iter(ctx);
		low = old_lin;
		high = new_lin;
		iter->key.keys[0] = low;
		iter->key.keys[1] = 0;
		while (true) {
			found = compliance_map_iter_next(iter, &old_lin,
			    &new_lin, &error);
			if (error || !found || old_lin > high)
				break;
			printf("%llx-%llx\n", old_lin, new_lin);
		}
		close_compliance_map_iter(iter);
		break;

	default:
		usage();
	}

out:
	if (error) {
		printf("%s", isi_error_get_message(error));
		isi_error_free(error);
	}
	close_compliance_map(ctx);
}
