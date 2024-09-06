#include <stdlib.h>
#include <stdio.h>

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/siq_btree.h"
#include "isi_migrate/migr/linmap.h"
#include "sbtree.h"

#include <ifs/ifs_lin_open.h>

#define MAPDIR "synciq/dst"

void usage(void);

static void
list_linmaps(char *pol_name)
{
	int root_fd = -1;
	int dir_fd = -1;
	DIR *dir = NULL;
	struct dirent *de = NULL;
	bool one_or_more = false;
	struct isi_error *error = NULL;

	if (pol_name) {
		/* list maps in a specific policy directory */
		dir_fd = siq_btree_open_dir(siq_btree_linmap, pol_name,
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
			printf("synciq linmap directory does not exist\n");
			goto out;
		}
	}

	/* fdopendir() takes ownership dir_fd regardless of result. */
	dir = fdopendir(dir_fd);
	if (dir == NULL) {
		printf("error opening linmap dir%s%s\n",
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
			printf("no linmaps found in linmap dir for policy "
			    "%s\n", pol_name);
		else
			printf("no linmap dirs found\n");
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
"    isi_linmap_mod -l                             (list map directories)\n"
"    isi_linmap_mod -l pol_name                    (list maps in a directory)\n"
"    isi_linmap_mod -c pol_name map_name           (create a new linmap)\n"
"    isi_linmap_mod -p pol_name map_name           (reprotect a linmap)\n"
"    isi_linmap_mod -k pol_name map_name           (kill a linmap)\n"
"    isi_linmap_mod -g pol_name map_name slin      (print mapping)\n"
"    isi_linmap_mod -s pol_name map_name slin dlin (set mapping)\n"
"    isi_linmap_mod -d pol_name map_name slin      (delete mapping)\n"
"    isi_linmap_mod -a pol_name map_name           (print all mappings)\n"
"    isi_linmap_mod -r pol_name map_name low high  (print inclusive range)\n"
"    isi_linmap_mod -S pol_name map_name low high  (stat inclusive range)\n");
	exit(EXIT_FAILURE);
}

int
main(int argc, char **argv)
{
	struct map_ctx *ctx = NULL;
	uint64_t slin = 0;
	uint64_t dlin = 0;
	uint64_t low, high;
	char c, d;
	char *pol_name = NULL;
	char *map_name = NULL;
	bool found;
	struct isi_error *error = NULL;
	struct map_iter *iter;
	int fd = -1;

	if (argc < 2)
		usage();

	if (sscanf(argv[1], "-%c%c", &c, &d) != 1)
		usage();

	if (argc >= 3)
		pol_name = argv[2];

	if (argc >= 4) {
		map_name = argv[3];	
		open_linmap(pol_name, map_name, &ctx, &error);
		if (error && c != 'c') {
			printf("invalid pol_name %s or map_name %s specified\n",
			    pol_name, map_name);
			isi_error_free(error);
			error = NULL;
			goto out;
		}
		error = NULL;
	}

	if (argc >= 5 && (sscanf(argv[4], "%llx", &slin) != 1)) {
		printf("invalid argument %s specified\n", argv[3]);
		goto out;
	}

	if (argc >= 6 && (sscanf(argv[5], "%llx", &dlin) != 1)) {
		printf("invalid argument %s specified\n", argv[4]);
		goto out;
	}

	switch (c) {
	case 'l':
		if (argc > 3)
			usage();
		list_linmaps(pol_name);
		break;

	case 'c':
		if (argc != 4)
			usage();
		if (ctx) {
			printf("linmap %s already exists\n", map_name);
			goto out;
		}
		create_linmap(pol_name, map_name, false, &error);
		if (error)
			goto out;
		break;

	case 'p':
		if (argc != 4)
			usage();
		if (ctx) {
			close_linmap(ctx);
			ctx = NULL;
			create_linmap(pol_name, map_name, true, &error);
			if (error)
				goto out;
		}
		break;

	case 'k':
		if (argc != 4)
			usage();
		remove_linmap(pol_name, map_name, false, &error);
		if (error)
			goto out;
		break;

	case 'g':
		if (argc != 5)
			usage();
		found = get_mapping(slin, &dlin, ctx, &error);
		if (error)
			goto out;
		if (found)
			printf("%llx-%llx\n", slin, dlin);
		else
			printf("src_lin %llx not in map\n", slin);
		break;

	case 's':
		if (argc != 6)
			usage();
		set_mapping(slin, dlin, ctx, &error);
		if (error)
			goto out;
		break;

	case 'd':
		if (argc != 5)
			usage();
		found = remove_mapping(slin, ctx, &error);
		if (!found)
			printf("src_lin %llx not in map\n", slin);
		break;

	case 'a':
		if (argc != 4)
			usage();
		iter = new_map_iter(ctx);
		while (true) {
			found = map_iter_next(iter, &slin, &dlin, &error);
			if (error || !found)
				break;
			printf("%llx-%llx\n", slin, dlin);
		}
		close_map_iter(iter);
		break;

	case 'r':
		if (argc != 6)
			usage();
		iter = new_map_iter(ctx);
		low = slin;
		high = dlin;
		iter->key.keys[0] = low;
		iter->key.keys[1] = 0;
		while (true) {
			found = map_iter_next(iter, &slin, &dlin, &error);
			if (error || !found || slin > high)
				break;
			printf("%llx-%llx\n", slin, dlin);
		}
		close_map_iter(iter);
		break;

	case 'S':
		if (argc != 6)
			usage();
		iter = new_map_iter(ctx);
		low = slin;
		high = dlin;
		iter->key.keys[0] = low;
		iter->key.keys[1] = 0;
		while (true) {
			found = map_iter_next(iter, &slin, &dlin, &error);
			if (error || !found || slin > high)
				break;
			fd = ifs_lin_open(dlin, HEAD_SNAPID, O_RDONLY);
			if (fd == -1) {
				printf("Unable to open dlin %llx "
				    "(slin %llx): %s\n", dlin, slin,
				    strerror(errno));
			} else {
				close(fd);
				fd = -1;
			}
		}
		close_map_iter(iter);
		if (fd != -1)
			close(fd);
		break;

	default:
		usage();
	}

out:
	if (error) {
		printf("%s", isi_error_get_message(error));
		isi_error_free(error);
	}
	close_linmap(ctx);
}
