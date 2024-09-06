#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>

#include "ifs/btree/btree.h"
#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/siq_btree.h"
#include "isi_migrate/migr/cpools_sync_state.h"
#include "sbtree.h"

#define REPDIR "synciq/src"

void usage(void);

enum actions {
	ACT_INVALID = 0,
	ACT_LL = 'l' * 256 + 'l',
	ACT_CC = 'c' * 256 + 'c',
	ACT_CK = 'c' * 256 + 'k',
	ACT_CX = 'c' * 256 + 'x',
	ACT_CA = 'c' * 256 + 'a',
	ACT_CS = 'c' * 256 + 's',
	ACT_CM = 'c' * 256 + 'm',
	ACT_CD = 'c' * 256 + 'd',
	ACT_CR = 'c' * 256 + 'r',
};

enum cpss_opts {
	OPT_ST = -2,
};

static struct option longopts[] = {
	{"sync-type", required_argument, NULL, OPT_ST},
	{NULL, 0, NULL, 0}
};

static void
print_entry(uint64_t lin, struct cpss_entry *entry)
{
	ASSERT(entry);
	printf("%llx sync-type: %d\n",
	    lin, entry->sync_type);
}

static void
set_helper(uint64_t lin, struct cpss_ctx *ctx, int argc, char **argv,
    bool modify_existing, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct cpss_entry entry = {};
	bool found;
	int ret;

	/* valid call will include at least one flag and value */
	if (argc < 6)
		usage();

	/* if this is a modify operation, populate entry with existing values.
	 * if modify is specified and the entry doesn't exist, error out */
	if (modify_existing) {
		found = get_cpss_entry(lin, &entry, ctx, &error);
		if (error)
			goto out;
		if (!found) {
			error = isi_system_error_new(EINVAL,
			    "entry does not exist for lin %llx", lin);
			goto out;
		}
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
		case OPT_ST:
			entry.sync_type = strtoul(optarg, NULL, 10);
			if (entry.sync_type == 0 && errno == EINVAL) {
				error = isi_system_error_new(EINVAL,
				    "bad argument %s", optarg);
				goto out;
			}
			break;

		default:
			ASSERT(0);
		}

	}

	/* store entry with mix of existing/default and modified values */
	set_cpss_entry(lin, &entry, ctx, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}

static void
list_cps_states(char *pol_id)
{
	int root_fd = -1;
	int dir_fd = -1;
	DIR *dir = NULL;
	struct dirent *de = NULL;
	bool one_or_more = false;
	struct isi_error *error = NULL;

	if (pol_id) {
		/* list cpstate sbts in a specific policy directory */
		dir_fd = siq_btree_open_dir(siq_btree_cpools_sync_state, pol_id,
		    &error);
		if (error) {
			printf("%s\n", isi_error_get_message(error));
			isi_error_free(error);
			goto out;
		} else
			ASSERT(dir_fd > 0);
	} else {
		/* list policy directories */
		root_fd = sbt_open_root_dir(O_RDONLY);
		if (root_fd == -1)
			goto out;

		dir_fd = enc_openat(root_fd, REPDIR, ENC_DEFAULT, O_RDONLY);
		if (dir_fd == -1) {
			printf("synciq cpools sync state dir does not exist\n");
			goto out;
		}
	}

	ASSERT(dir_fd > 0);
	dir = fdopendir(dir_fd);
	if (dir == NULL) {
		printf("error opening cpools sync state dir%s%s\n",
		    pol_id ? " for policy " : "", pol_id ? : "");
		goto out;
	} else
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
		if (pol_id)
			printf("no cpools sync states found for policy "
			    "%s\n", pol_id);
		else
			printf("no cpools sync state dirs found\n");
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
"\n"
"usage:\n"
"    isi_cpstate_mod -ll                          (list cpss directories)\n"
"    isi_cpstate_mod -ll pol_id                   (list cpss's in a directory)\n"
"\n"
"cpstate:\n"
"    isi_cpstate_mod -cc pol_id cpss_name          (create a new cpstate)\n"
"    isi_cpstate_mod -ck pol_id cpss_name          (kill a cpstate)\n"
"    isi_cpstate_mod -cx pol_id cpss_name lin      (print entry)\n"
"    isi_cpstate_mod -ca pol_id cpss_name          (print all entries)\n"
"    isi_cpstate_mod -cs pol_id cpss_name lin -x y (set entry)\n"
"    isi_cpstate_mod -cm pol_id cpss_name lin -x y (modify entry)\n"
"    isi_cpstate_mod -cd pol_id cpss_name lin      (delete entry)\n"
"    isi_cpstate_mod -cr pol_id cpss_name low high (print inclusive range)\n"
"\n"
"For cpstate set and modify operations, lin is followed by one or more \n"
"flag/val pairs where flag is one of the following:\n"
"    --sync-type      (integer)\n"
"and value is an appropriate value for the field (0 or 1 for bool). for set\n"
"operations, unspecified fields are set to 0 or false. for mod operations,\n"
"unspecified fields retain the existing value. attempting to mod a non-\n"
"existing entry will result in failure.\n"
"\n"
"example:\n"
"    isi_cpstate_mod pol_snap_3 -cs 1001b0008 --sync-type 1\n"
"\n");
	exit(EXIT_FAILURE);
}

int
main(int argc, char **argv)
{
	struct cpss_entry entry;
	struct cpss_ctx *ctx = NULL;
	uint64_t lin = 0;
	uint64_t low, high;
	char c, d;
	char *pol_id = NULL;
	char *cpss_name = NULL;
	bool found;
	struct isi_error *error = NULL;
	struct cpss_iter *iter;
	enum actions action = ACT_INVALID;

	if (argc < 2)
		usage();

	if (sscanf(argv[1], "-%c%c", &c, &d) != 2)
		usage();
	action = c * 256 + d;

	if (argc >= 3)
		pol_id = argv[2];

	if (argc >= 4) {
		cpss_name = argv[3];
		open_cpss(pol_id, cpss_name, &ctx, &error);
		if (error && action != ACT_CC) {
			printf("invalid pol_id %s or map_name %s specified\n",
			    pol_id, cpss_name);
			isi_error_free(error);
			error = NULL;
			goto out;
		}
		error = NULL;
	}

	if (argc >= 5 && (sscanf(argv[4], "%llx", &lin) != 1)) {
		printf("invalid argument %s specified\n", argv[3]);
		goto out;
	}

	switch (action) {
	case ACT_LL:
		if (argc > 3)
			usage();
		list_cps_states(pol_id);
		break;

	case ACT_CC:
		if (argc != 4)
			usage();
		if (ctx) {
			printf("cpools state %s already exists\n", cpss_name);
			goto out;
		}
		create_cpss(pol_id, cpss_name, false, &error);
		if (error) {
			printf("failed to create cpools state %s\n", cpss_name);
			goto out;
		}
		break;


	case ACT_CK:
		if (argc != 4)
			usage();
		remove_cpss(pol_id, cpss_name, false, &error);
		if (error)
			goto out;
		break;

	case ACT_CX:
		if (argc != 5)
			usage();
		found = get_cpss_entry(lin, &entry, ctx, &error);
		if (error)
			goto out;
		if (found)
			print_entry(lin, &entry);
		else
			printf("entry for lin %llx does not exist\n", lin);
		break;

	case ACT_CS:
		set_helper(lin, ctx, argc, argv, false, &error);
		if (error)
			goto out;
		break;

	case ACT_CM:
		set_helper(lin, ctx, argc, argv, true, &error);
		if (error)
			goto out;
		break;

	case ACT_CD:
		if (argc != 5)
			usage();
		found = remove_cpss_entry(lin, ctx, &error);
		if (!found)
			printf("entry for lin %llx does not exist\n", lin);
		break;

	case ACT_CA:
		if (argc != 4)
			usage();
		iter = new_cpss_iter(ctx);
		while (true) {
			found = cpss_iter_next(iter, &lin, &entry, &error);
			if (error || !found)
				break;
			print_entry(lin, &entry);
		}
		close_cpss_iter(iter);
		break;

	case ACT_CR:
		if (argc != 6)
			usage();
		iter = new_cpss_iter(ctx);
		low = lin;
		if (sscanf(argv[4], "%llx", &high) != 1) {
			printf("invalid argument %s specified\n", argv[4]);
			goto out;
		}
		btree_key_set(&iter->key, low, 0);
		while (true) {
			found = cpss_iter_next(iter, &lin, &entry, &error);
			if (error || !found || lin > high)
				break;
			print_entry(lin, &entry);
		}
		close_cpss_iter(iter);
		break;

	default:
		usage();
	}

out:
	if (error) {
		printf("%s", isi_error_get_message(error));
		isi_error_free(error);
	}
	close_cpss(ctx);
}
