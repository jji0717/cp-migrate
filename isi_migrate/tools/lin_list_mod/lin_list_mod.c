#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/siq_btree.h"
#include "isi_migrate/migr/lin_list.h"
#include "lin_list_mod.h"
#include "sbtree.h"

#define LINLISTDIR "synciq/dst"

void usage(void);

enum actions {
	ACT_INVALID = 0,
	ACT_L = 'l',
	ACT_C = 'c',
	ACT_P = 'p',
	ACT_K = 'k',
	ACT_X = 'x',
	ACT_A = 'a',
	ACT_S = 's',
	ACT_M = 'm',
	ACT_D = 'd',
	ACT_R = 'r',
	ACT_IX = 'i'*256+'x',
	ACT_IS = 'i'*256+'s',
	ACT_LR = 'l'*256+'r',
	ACT_LC = 'l'*256+'c'
};

enum rep_opts {
	OPT_ID = -2,
	OPT_C = -3
};

static struct option longopts[] = {
	{"is_dir", required_argument, NULL, OPT_ID},
	{"commit", required_argument, NULL, OPT_C},
	{NULL, 0, NULL, 0}
};

enum lin_list_info_opts {
	OPT_TYPE = -2
};

static struct option linlistinfoopts[] = {
	{"type", required_argument, NULL, OPT_TYPE},
	{NULL, 0, NULL, 0}
};

void
print_info(struct lin_list_info_entry llie)
{
	printf("Lin list info type: ");
	switch (llie.type) {
	case RESYNC:
		printf("resync\n");
		break;
	case COMMIT:
		printf("commit\n");
		break;
	case DIR_DELS:
		printf("directory delete\n");
		break;
	default:
		ASSERT(0, "Unexpected type: %d", llie.type);
	}
}

void
print_entry(uint64_t lin, struct lin_list_entry ent, enum lin_list_type type)
{
	switch (type) {
	case RESYNC:
		printf("%llx is_dir:%d\n", lin, ent.is_dir);
		break;
	case COMMIT:
		printf("%llx commit:%d\n", lin, ent.commit);
		break;
	case DIR_DELS:
		printf("%llx commit:%d\n", lin, ent.commit);
		break;
	default:
		ASSERT(0, "Unexpected type: %d", type);
	}
}

static bool
to_bool(const char *arg)
{
	bool res = false;

	if (strlen(arg) != 1)
		usage();
	if (*arg == '1')
		res = true;
	else if (*arg == '0')
		res = false;
	else
		usage();
	return res;
}

static int
char_to_entry_type(const char *arg)
{
	int res = 0;

	if (strlen(arg) != 1)
		usage();
	res = atoi(arg);
	if (res < 0 || res > 2)
		usage();
	return res;
}

static void
set_lin_list_info_helper(struct lin_list_ctx *ctx, int argc, char **argv,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct lin_list_info_entry entry = {};
	int ret;

	/* valid call will include at least one flag and value */
	if (argc < 6)
		usage();

	/* skip to the flag/values and parse them. note that getopt_long needs
	 * to start on the argument preceding the first option flag. normally,
	 * this would be the name of the executable */
	argv += 3;
	argc -= 3;
	while (true) {
		ret = getopt_long(argc, argv, "", linlistinfoopts, NULL);
		if (ret == ':' || ret == '?')
			usage();
		if (ret == -1)
			break;

		switch (ret) {
		case OPT_TYPE:
			entry.type = char_to_entry_type(optarg);
			break;
		default:
			ASSERT(0);
		}

	}

	/* store entry with mix of existing/default and modified values */
	set_lin_list_info(&entry, ctx, false, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}



static void
set_helper(uint64_t lin, struct lin_list_ctx *ctx, int argc, char **argv,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct lin_list_entry entry;
	struct lin_list_info_entry llie;
	int ret;
	bool found;

	/* valid call will include at least one flag and value */
	if (argc < 6)
		usage();

	found = get_lin_list_info(&llie, ctx, &error);
	if (error)
		goto out;

	if (!found) {
		printf("Please set a valid lin list info entry before "
		    "continuing.\n");
		goto out;
	}

	/* skip to the flag/values and parse them. note that getopt_long needs
	 * to start on the argument preceding the first option flag. normally,
	 * this would be the name of the executable */
	argv += 3;
	argc -= 3;
	while (true) {
		ret = getopt_long(argc, argv, "", longopts, NULL);
		if (ret == ':' || ret == '?')
			usage();
		if (ret == -1)
			break;

		switch (ret) {
		case OPT_ID:
			if (llie.type != RESYNC) {
				printf("Please use a resync lin list for this "
				    "field.\n");
				goto out;
			}
			entry.is_dir = to_bool(optarg);
			break;
		case OPT_C:
			if (llie.type != COMMIT && llie.type != DIR_DELS) {
				printf("Please use a commit or dir dels lin list "
				    "for this field.\n");
				goto out;
			}
			entry.commit = to_bool(optarg);
			break;
		default:
			ASSERT(0);
		}

	}

	/* store entry with mix of existing/default and modified values */
	set_lin_list_entry(lin, entry, ctx, &error);
	if (error)
		goto out;

 out:
	isi_error_handle(error, error_out);
}

static void
list_lin_lists(char *pol_id, enum lin_list_type type)
{
	int root_fd = -1;
	int dir_fd = -1;
	DIR *dir = NULL;
	struct dirent *de = NULL;
	bool one_or_more = false;
	bool found;
	struct lin_list_ctx *ctx = NULL;
	struct lin_list_info_entry llie;
	struct isi_error *error = NULL;

	if (pol_id) {
		/* list lin_list states in a specific policy directory */
		dir_fd = siq_btree_open_dir(siq_btree_lin_list, pol_id,
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

		dir_fd = enc_openat(root_fd, LINLISTDIR, ENC_DEFAULT, O_RDONLY);
		if (dir_fd == -1) {
			printf("synciq lin list directory does not "
			    "exist\n");
			goto out;
		}
	}

	dir = fdopendir(dir_fd);
	if (dir == NULL) {
		printf("error opening lin list dir%s%s\n",
		    pol_id ? " for policy " : "", pol_id ? : "");
		goto out;
	}
	/* fdopendir() takes ownership of dir_fd on success. */
	dir_fd = -1;

	for (;;) {
		de = readdir(dir);
		if (de == NULL)
			break;

		if (!strcmp(de->d_name, ".") ||
		    !strcmp(de->d_name, "..") ||
		    !strcmp(de->d_name, ".snapshot"))
			continue;

		if (pol_id != NULL && type != MAX_LIN_LIST) {
			open_lin_list(pol_id, de->d_name, &ctx, &error);
			if (error)
				goto out;

			found = get_lin_list_info(&llie, ctx, &error);
			if (error)
				goto out;

			close_lin_list(ctx);
			ctx = NULL;

			if (!found || llie.type != type)
				continue;
		}

		printf("%s\n", de->d_name);
		one_or_more = true;
	}

	if (one_or_more == false) {
		if (pol_id)
			printf("no lin list found in lin_list state dir "
			    "for policy %s\n", pol_id);
		else
			printf("no lin list dirs found\n");
	}

out:
	if (ctx != NULL)
		close_lin_list(ctx);
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
"\n"
"usage:\n"
"    isi_lin_list_mod -l                           (list lin list directories)\n"
"    isi_lin_list_mod -l  pol_id                   (list lin lists in a directory)\n"
"    isi_lin_list_mod -lr pol_id                   (list resync lists in a directory)\n"
"    isi_lin_list_mod -lc pol_id                   (list commit lists in a directory)\n"
"\n"
"lin_list:\n"
"    isi_lin_list_mod -c  pol_id lin_list_name          (create a new lin list)\n"
"    isi_lin_list_mod -p  pol_id lin_list_name          (reprotect lin list)\n"
"    isi_lin_list_mod -k  pol_id lin_list_name          (kill a lin list)\n"
"    isi_lin_list_mod -x  pol_id lin_list_name lin      (print entry)\n"
"    isi_lin_list_mod -a  pol_id lin_list_name          (print all entries)\n"
"    isi_lin_list_mod -s  pol_id lin_list_name lin -x y (set entry)\n"
"    isi_lin_list_mod -m  pol_id lin_list_name lin -x y (modify entry)\n"
"    isi_lin_list_mod -d  pol_id lin_list_name lin      (delete entry)\n"
"    isi_lin_list_mod -r  pol_id lin_list_name low high (print inclusive range)\n"
"\n"
"lin list info:\n"
"    isi_lin_list_mod -ix pol_id lin_list_name          (print lin list info entry)\n"
"    isi_lin_list_mod -is pol_id lin_list_name -x y     (set lin list entry)\n"
"\n"
"For lin list (resync) set and modify operations, lin is followed by one or\n"
"more flag/val pairs where flag is one of the following:\n"
"    --is_dir         (bool)\n"
"and value is an appropriate value for the field (0 or 1 for bool).\n"
"\n"
"For lin list (commit) set and modify operations, lin is followed by one or\n"
"more flag/val pairs where flag is one of the following:\n"
"    --commit         (bool)\n"
"and value is an appropriate value for the field (0 or 1 for bool).\n"
"For set operations, unspecified fields are set to 0 or false. For mod\n"
"operations, unspecified fields retain the existing value. Attempting to mod\n"
"a non-existing entry will result in failure.\n"
"\n"
"example:\n"
"    isi_lin_list_mod -s <pol_id> <pol_id>_resync 1001b0008 --is_dir 0\n"
"\n"
"For lin list info entries, the following flag is available:\n"
"    --type           (integer)\n"
"and value is either 0 for resync, 1 for commit or 2 for dir dels.\n"
"\n");
	exit(EXIT_FAILURE);
}

int
main(int argc, char **argv)
{
	struct lin_list_ctx *ctx = NULL, dummy_ctx = {};
	uint64_t lin = 0;
	uint64_t low, high;
	char c, d;
	char *pol_id = NULL;
	char *lin_list_name = NULL;
	bool found;
	int res;
	struct lin_list_entry ent;
	struct isi_error *error = NULL;
	struct lin_list_iter *iter;
	struct lin_list_info_entry llie;
	enum actions action = ACT_INVALID;

	if (argc < 2)
		usage();

	res = sscanf(argv[1], "-%c%c", &c, &d);
	if (res == 1)
		action = c;
	else if (res == 2)
		action = c * 256 + d;
	else
		usage();

	if (argc >= 3)
		pol_id = argv[2];

	if (argc >= 4) {
		lin_list_name = argv[3];
		open_lin_list(pol_id, lin_list_name, &ctx, &error);
		if (error && action != ACT_C) {
			printf("invalid pol_id %s or map_name %s specified\n",
			    pol_id, lin_list_name);
			isi_error_free(error);
			error = NULL;
			goto out;
		}
		error = NULL;
	}

	if (argc >= 5 && (sscanf(argv[4], "%llx", &lin) != 1) &&
	    action != ACT_IS) {
		printf("invalid argument %s specified\n", argv[3]);
		goto out;
	}

	if (ctx == NULL) {
		/* Placate static analysis. */
		ctx = &dummy_ctx;
	}

	switch (action) {
	case ACT_L:
		if (argc > 3)
			usage();
		list_lin_lists(pol_id, MAX_LIN_LIST);
		break;

	case ACT_LR:
		if (argc > 3)
			usage();
		list_lin_lists(pol_id, RESYNC);
		break;

	case ACT_LC:
		if (argc > 3)
			usage();
		list_lin_lists(pol_id, COMMIT);
		break;

	case ACT_C:
		if (argc != 4)
			usage();
		if (ctx != NULL && ctx != &dummy_ctx) {
			printf("lin list %s already exists\n", lin_list_name);
			goto out;
		}
		create_lin_list(pol_id, lin_list_name, false, &error);
		if (error) {
			printf("failed to create lin list %s\n",
			    lin_list_name);
			goto out;
		}
		break;

	case ACT_P:
		if (argc != 4)
			usage();
		if (ctx != NULL && ctx != &dummy_ctx) {
			close_lin_list(ctx);
			ctx = NULL;
			create_lin_list(pol_id, lin_list_name, true, &error);
			if (error) {
				printf("failed to reprotect lin list %s\n",
				    lin_list_name);
				goto out;
			}
		}
		break;

	case ACT_K:
		if (argc != 4)
			usage();
		remove_lin_list(pol_id, lin_list_name, false, &error);
		if (error)
			goto out;
		break;

	case ACT_X:
		if (argc != 5)
			usage();
		found = get_lin_list_entry(lin, &ent, ctx, &error);
		if (error)
			goto out;
		get_lin_list_info(&llie, ctx, &error);
		if (error)
			goto out;
		if (found)
			print_entry(lin, ent, llie.type);
		else
			printf("entry for lin %llx does not exist\n", lin);
		break;

	case ACT_S:
		set_helper(lin, ctx, argc, argv, &error);
		if (error)
			goto out;
		break;

	case ACT_M:
		set_helper(lin, ctx, argc, argv, &error);
		if (error)
			goto out;
		break;

	case ACT_D:
		if (argc != 5)
			usage();
		found = remove_lin_list_entry(lin, ctx, &error);
		if (!found)
			printf("entry for lin %llx does not exist\n", lin);
		break;

	case ACT_A:
		if (argc != 4)
			usage();
		get_lin_list_info(&llie, ctx, &error);
		if (error)
			goto out;
		iter = new_lin_list_iter(ctx);
		while (true) {
			found = lin_list_iter_next(iter, &lin, &ent, &error);
			if (error || !found)
				break;
			print_entry(lin, ent, llie.type);
		}
		close_lin_list_iter(iter);
		break;

	case ACT_R:
		if (argc != 6)
			usage();
		get_lin_list_info(&llie, ctx, &error);
		if (error)
			goto out;
		iter = new_lin_list_iter(ctx);
		low = lin;
		if (sscanf(argv[4], "%llx", &high) != 1) {
			printf("invalid argument %s specified\n", argv[4]);
			goto out;
		}
		iter->key.keys[0] = low;
		iter->key.keys[1] = 0;
		while (true) {
			found = lin_list_iter_next(iter, &lin, &ent, &error);
			if (error || !found || lin > high)
				break;
			print_entry(lin, ent, llie.type);
		}
		close_lin_list_iter(iter);
		break;

	case ACT_IX:
		if (argc != 4)
			usage();
		found = get_lin_list_info(&llie, ctx, &error);
		if (error)
			goto out;
		if (found)
			print_info(llie);
		break;


	case ACT_IS:
		set_lin_list_info_helper(ctx, argc, argv, &error);
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
	if (ctx != &dummy_ctx) {
		close_lin_list(ctx);
	}
}
