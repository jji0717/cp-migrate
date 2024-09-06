#include <stdio.h>
#include <wchar.h>
#include <wctype.h>
#include <limits.h>
#include <dirent.h>
#include <locale.h>
#include <getopt.h>

#include <ifs/ifs_lin_open.h>
#include <isi_changelist/changelist.h>

#include <isi_migrate/migr/summ_stf.h>
#include <isi_migrate/migr/siq_btree.h>

#if CLE_VER_NOW > CLE_VER_2
#error "Get/set/print up-level ent data."
#endif

#define EMPTY_PATH ""

#define STR_SLASH_IFS			"/ifs"
#define STR_SLASH_IFS_SLASH		"/ifs/"

#define STR_DEFAULT_PATH_PREFIX		""
#define STR_ADDED_PATH_PREFIX		"+"
#define STR_REMOVED_PATH_PREFIX		"-"
#define MAX_PATH_PREFIX_SIZE		(1+1)

#define STR_DEFAULT_PATH_SUFFIX		""
#define STR_DIR_PATH_SUFFIX		"/"
#define STR_NONDIR_PATH_SUFFIX		STR_DEFAULT_PATH_SUFFIX
#define MAX_PATH_SUFFIX_SIZE		(1+1)

#define PATH_DISPLAY_OPT_NONE			0x00
#define PATH_DISPLAY_OPT_SHQUOTE		0x01
#define PATH_DISPLAY_OPT_PATH_ONLY		0x02
#define PATH_DISPLAY_OPT_PREPEND_ADD_REM	0x04
#define PATH_DISPLAY_OPT_APPEND_DIR_SLASH	0x08
#define PATH_DISPLAY_OPT_HIDE_OLD_DESCENDANTS	0x10

enum chng_list_opts {
	OPT_VER	= -2,
	OPT_PATH = -3,
	OPT_TYPE = -4,
	OPT_ST_SIZE = -5,
	OPT_ST_ATIME = -6,
	OPT_ST_ATIMENSEC = -7,
	OPT_ST_CTIME = -8,
	OPT_ST_CTIMENSEC = -9,
	OPT_ST_MTIME = -10,
	OPT_ST_MTIMENSEC = -11,
	OPT_ST_MODE = -12,
	OPT_ST_FLAGS = 13,
	OPT_CLE_FLAGS = -14,
	OPT_PARENT_LIN = -15,
	OPT_PATH_DEPTH = -16,
	OPT_PHYS_SIZE = -17,
	OPT_PROT_LEVEL = -18,
	OPT_PROT_POLICY = -19,
	OPT_DATA_POOL = -20,
	OPT_METADATA_POOL = -21,
	OPT_ADS_COUNT = -22,
	OPT_ST_NLINK = -23,
	OPT_ST_UID = -24,
	OPT_ST_GID = -25,
	OPT_ST_BTIME = -26,
};

static struct option chng_list_mod_opts[] = {
	{"ver", optional_argument, NULL, OPT_VER},
        {"path", optional_argument, NULL, OPT_PATH},
        {"type", optional_argument, NULL, OPT_TYPE},
        {"size", optional_argument, NULL, OPT_ST_SIZE},
        {"atime", optional_argument, NULL, OPT_ST_ATIME},
        {"atimensec", optional_argument, NULL, OPT_ST_ATIMENSEC},
        {"ctime", optional_argument, NULL, OPT_ST_CTIME},
        {"ctimensec", optional_argument, NULL, OPT_ST_CTIMENSEC},
	{"mtime", optional_argument, NULL, OPT_ST_MTIME},
        {"mtimensec", optional_argument, NULL, OPT_ST_MTIMENSEC},
        {"mode", optional_argument, NULL, OPT_ST_MODE},
        {"flags", optional_argument, NULL, OPT_ST_FLAGS},
        {"cle_flags", optional_argument, NULL, OPT_CLE_FLAGS},
        {"parent_lin", optional_argument, NULL, OPT_PARENT_LIN},
        {"path_depth", optional_argument, NULL, OPT_PATH_DEPTH},
        {"phys_size", optional_argument, NULL, OPT_PHYS_SIZE},
        {"prot_level", optional_argument, NULL, OPT_PROT_LEVEL},
        {"prot_policy", optional_argument, NULL, OPT_PROT_POLICY},
        {"data_pool", optional_argument, NULL, OPT_DATA_POOL},
        {"metadata_pool", optional_argument, NULL, OPT_METADATA_POOL},
        {"ads_count", optional_argument, NULL, OPT_ADS_COUNT},
        {"nlink", optional_argument, NULL, OPT_ST_NLINK},
        {"uid", optional_argument, NULL, OPT_ST_UID},
        {"gid", optional_argument, NULL, OPT_ST_GID},
        {"btime", optional_argument, NULL, OPT_ST_BTIME},
	{NULL, 0, NULL, 0}
};

enum nonprintable_char_action {
	NPCA_DEFAULT,
	NPCA_USE_RAW,
	NPCA_REPLACE_WITH_OCTAL,
	NPCA_REPLACE_WITH_C_ESCAPE_OR_OCTAL,
	NPCA_REPLACE_WITH_QUESTION_MARK
};

bool debug = false;

//  From isi_migrate/migr/isirep.h.
void
dir_get_utf8_str_path(char *prefix, ifs_lin_t root_lin, ifs_lin_t lin,
    ifs_snapid_t snap, ifs_lin_t parent_lin, char **utf8_str_out,
    struct isi_error **error_out);

const char normal_usage[] =
"Description:\n"
"    Manage snapshot changelists.\n"
"\n"
"Usage:\n"
"    isi_changelist_mod -a cl_name ...            Display all entries.\n"
"    isi_changelist_mod -h                        Display help.\n"
"    isi_changelist_mod -i {cl_name | --all} ...  Describe changelist(s).\n"
"    isi_changelist_mod -k {cl_name | --all}      Kill changelist(s).\n"
"    isi_changelist_mod -l                        List changelists.\n"
"\n"
"Options for entry display (-a) and changelist describe (-i):\n"
"    --B         Replace non-printable path characters with octal codes.\n"
"                See ascii(7).\n"
"    --b         As --B, but use C escape codes whenever possible,\n"
"                e.g. \\t for TAB.\n"
"    --p         Only display path.\n"
"    --q         Replace non-printable path characters with '?'.\n"
"    --s         Append '/' to paths of directories.\n"
"    --t         Use shquote(3) on path strings to make them suitable for\n"
"                command-line arguments.\n"
"    --w         Display raw path strings. (Default.)\n"
"\n"
"Additional options for entry display (-a):\n"
"    --d den     Fractional entry range denominator (2 <= den <= 1024).\n"
"    --n num     Fractional entry range numerator (1 <= num <= den).\n"
"    --v         Prepend '+' and '-' to paths of added and removed entries.\n"
"\n"
"    For changelist entry st_* field descriptions, see stat(2).\n"
"\n"
"    The changelist entry status information word cle_flags has the\n"
"    following bits:\n"
"\n"
"    #define CLE_ADDED           0000001  /* added or moved to */\n"
"    #define CLE_REMOVED         0000002  /* removed or moved from */\n"
"    #define CLE_PATH_CHANGED    0000004  /* path changed (moved to/from) */\n"
"    #define CLE_HAS_ADS         0000010  /* has Alternate Data Stream(s) */\n"
"    #define CLE_ADS             0000020  /* Alternate Data Stream */\n"
"    #define CLE_HAS_HARDLINKS   0000040  /* hardlinks exist */\n"
;

const char debug_usage[] =
"Description:\n"
"    Manage snapshot changelists.\n"
"\n"
"Usage:\n"
"    isi_changelist_mod -a cl_name ...            Display all entries.\n"
"    isi_changelist_mod -c oldsnap newsnap ...    Create empty changelist.\n"
"    isi_changelist_mod -d cl_name lin            Delete entry.\n"
"    isi_changelist_mod -f cl_name                Finalize changelist.\n"
//"    isi_changelist_mod -g cl_name lin ...      Print entry.\n"
"    isi_changelist_mod -h                        Display help.\n"
"    isi_changelist_mod -i {cl_name | --all} ...  Describe changelist(s).\n"
"    isi_changelist_mod -k {cl_name | --all}      Kill changelist(s).\n"
"    isi_changelist_mod -l                        List changelists.\n"
//"    isi_changelist_mod -r cl_name low high ... Print inclusive range.\n"
"    isi_changelist_mod -s cl_name lin ...        Set entry.\n"
"    isi_changelist_mod [-x] ...                  Enter extended mode.\n"
"\n"
"Options for entry display (-a) and changelist describe (-i):\n"
"    --B         Replace non-printable path characters with octal codes.\n"
"                See ascii(7).\n"
"    --b         As --B, but use C escape codes whenever possible,\n"
"                e.g. \\t for TAB.\n"
//"    --h         Hide entries for descendants of removed directories\n"
//"                (-a only).\n"
"    --p         Only display path.\n"
"    --q         Replace non-printable path characters with '?'.\n"
"    --s         Append '/' to paths of directories.\n"
"    --t         Use shquote(3) on path strings to make them suitable for\n"
"                command-line arguments.\n"
"    --w         Display raw path strings. (Default.)\n"
"\n"
"Additional options for entry display (-a):\n"
"    --d den     Fractional entry range denominator (2 <= den <= 1024).\n"
"    --n num     Fractional entry range numerator (1 <= num <= den).\n"
"    --v         Prepend '+' and '-' to paths of added and removed entries.\n"
"\n"
"    For changelist entry st_* field descriptions, see stat(2).\n"
"\n"
"    The changelist entry status information word cle_flags has the\n"
"    following bits:\n"
"\n"
"    #define CLE_ADDED           0000001  /* added or moved to */\n"
"    #define CLE_REMOVED         0000002  /* removed or moved from */\n"
"    #define CLE_PATH_CHANGED    0000004  /* path changed (moved to/from) */\n"
"    #define CLE_HAS_ADS         0000010  /* has Alternate Data Stream(s) */\n"
"    #define CLE_ADS             0000020  /* Alternate Data Stream */\n"
"    #define CLE_HAS_HARDLINKS   0000040  /* hardlinks exist */\n"
"    #define CLE_PARENT_REMOVED  0000100  /* parent removed */\n"
"    #define CLE_PATH_LOOKUP_REQ 0000200  /* runtime path lookup required */\n"
"\n"
"Options for changelist create (-c):\n"
"    --ver       <unsigned integer <= 255>\n"
"    --root      <string>\n"
"    --jobid     <unsigned integer>\n"
"    --jobphase  <unsigned integer <= 255>\n"
"\n"
"Options for entry set (-s):\n"
"    --path      <string)\n"
"    --type      <unsigned integer <= 255>\n"
"    --size      <unsigned long integer>\n"
"    --atime     <long integer>\n"
"    --atimensec <integer>\n"
"    --ctime     <long integer>\n"
"    --ctimensec <integer>\n"
"    --mtime     <long integer>\n"
"    --mtimensec <integer>\n"
;

//
//  Helper routines.
//

static void
usage(void)
{
	printf((debug ? debug_usage : normal_usage));
	exit(EXIT_FAILURE);
}

static void
arg_base10_to_uint16(char *arg, char *arg_name, uint16_t *value)
{
	char c;

	if (sscanf(arg, "%hd%c", value, &c) != 1 || *arg == '.') {
		printf("Invalid %s value '%s' specified.\n", arg_name, arg);
		exit(EXIT_FAILURE);
	}
}

static void
arg_base10_to_int32(char *arg, char *arg_name, int32_t *value)
{
	char	c;

	if (sscanf(arg, "%d%c", value, &c) != 1) {
		printf("Invalid %s value '%s' specified.\n", arg_name, arg);
		exit(EXIT_FAILURE);
	}
}

static void
arg_base10_to_uint32(char *arg, char *arg_name, uint32_t *value)
{
	char	c;

	if (sscanf(arg, "%u%c", value, &c) != 1 || *arg == '-') {
		printf("Invalid %s value '%s' specified.\n", arg_name, arg);
		exit(EXIT_FAILURE);
	}
}

static void
arg_base10_to_int64(char *arg, char *arg_name, int64_t *value)
{
	char	c;

	if (sscanf(arg, "%lld%c", value, &c) != 1) {
		printf("Invalid %s value '%s' specified.\n", arg_name, arg);
		exit(EXIT_FAILURE);
	}
}

static void
arg_base10_to_uint64(char *arg, char *arg_name, uint64_t *value)
{
	char	c;

	if (sscanf(arg, "%llu%c", value, &c) != 1 || *arg == '-') {
		printf("Invalid %s value '%s' specified.\n", arg_name, arg);
		exit(EXIT_FAILURE);
	}
}

static bool
crack_cl_name(char *cl_name, ifs_snapid_t *old, ifs_snapid_t *new,
    bool *inprog, struct cl_ctx **ctx, struct isi_error **ie_out)
{
	bool			result;
	struct isi_error	*ie = NULL;

	//  Decompose changelist name into its constituent parts.
	result = decomp_changelist_name(cl_name, old, new, inprog);

	if (!result) {
		printf("Invalid cl_name value '%s' specified.\n", cl_name);
		exit(EXIT_FAILURE);
	}

	//  If the caller wants the changelist opened then try to do so.
	if (ctx) {
		open_changelist(*old, *new, *inprog, ctx, &ie);
		if (ie)
			result = false;
	}

	isi_error_handle(ie, ie_out);
	return result;
}

static void
update_num_entries(struct cl_ctx *ctx, bool added_entry,
    struct isi_error **ie_out)
{
	struct cl_buf		buf;
	struct isi_error	*ie = NULL;
	struct changelist_entry	*ent = NULL;

	get_change_entry(ctx, METADATA_KEY, &buf, &ent, &ie);
	if (ie)
		goto out;

	remove_change_entry(ctx, METADATA_KEY, &ie);
	if (ie)
		goto out;

	if (added_entry)
		ent->u.meta.num_cl_entries++;
	else
		ent->u.meta.num_cl_entries--;

	set_change_entry(ctx, METADATA_KEY, ent, &ie);

out:
	isi_error_handle(ie, ie_out);
}

static void
get_options(int argc, char **argv, bool display_all_entries,
    uint8_t *path_display_opts, enum nonprintable_char_action *npc_action,
    u_int *slice_num, uint *slice_den)
{
	int		i;
	char		c, d;
	u_int		den = 0, num = 0;
	uint8_t		pd_opts = PATH_DISPLAY_OPT_NONE;

	enum nonprintable_char_action	npc_act = NPCA_DEFAULT;

	for (i = 0; i < argc; i++) {

		if (sscanf(argv[i], "--%c%c", &c, &d) != 1)
			usage();

		//  Non-printable character options.
		if (c == 'B' && npc_act == NPCA_DEFAULT) {
			npc_act = NPCA_REPLACE_WITH_OCTAL;

		} else if (c == 'b' && npc_act == NPCA_DEFAULT) {
			npc_act = NPCA_REPLACE_WITH_C_ESCAPE_OR_OCTAL;

		} else if (c == 'd' && display_all_entries && den == 0 &&
		    (i + 1) < argc) {
			arg_base10_to_uint32(argv[++i], "--d", &den);
			if (den < 2 || den > 1024)
				usage();

		} else if (c == 'n' && display_all_entries && num == 0 &&
		    (i + 1) < argc) {
			arg_base10_to_uint32(argv[++i], "--n", &num);
			if (num < 1 || num > 1024)
				usage();

		} else if (c == 'q' && npc_act == NPCA_DEFAULT) {
			npc_act = NPCA_REPLACE_WITH_QUESTION_MARK;

		} else if (c == 'w' && npc_act == NPCA_DEFAULT) {
			npc_act = NPCA_USE_RAW;

		//  Common path display options.
		} else if (c == 'p' && 0 ==
		    (pd_opts & PATH_DISPLAY_OPT_PATH_ONLY)) {
			pd_opts |= PATH_DISPLAY_OPT_PATH_ONLY;

		} else if (c == 's' && 0 ==
		    (pd_opts & PATH_DISPLAY_OPT_APPEND_DIR_SLASH)) {
			pd_opts |= PATH_DISPLAY_OPT_APPEND_DIR_SLASH;

		} else if (c == 't' && 0 ==
		    (pd_opts & PATH_DISPLAY_OPT_SHQUOTE)) {
			pd_opts |= PATH_DISPLAY_OPT_SHQUOTE;

		//  display_all_entries-specific path display options.
		//} else if (c == 'h' && display_all_entries && 0 ==
		//    (pd_opts & PATH_DISPLAY_OPT_HIDE_OLD_DESCENDANTS)) {
		//	pd_opts |= PATH_DISPLAY_OPT_HIDE_OLD_DESCENDANTS;

		} else if (c == 'v' && display_all_entries && 0 ==
		    (pd_opts & PATH_DISPLAY_OPT_PREPEND_ADD_REM)) {
			pd_opts |= PATH_DISPLAY_OPT_PREPEND_ADD_REM;

		} else {
			usage();
		}
	}

	if (den != 0 || num != 0) {
		if (den == 0 || num == 0 || den < num) {
			usage();
		}
	}

	if (slice_num != NULL)
		*slice_num = num;

	if (slice_den != NULL)
		*slice_den = den;

	*npc_action = npc_act;
	*path_display_opts = pd_opts;
}

//  Copied from src/isilon/bin/isi_change_list/isi_change_list.c,
//  added NULL termination of buf.
static int
prn_printable(const char *s, char *buf, size_t buflen)
{
	mbstate_t mbs;
	wchar_t wc;
	int i, n;
	size_t clen;

	memset(&mbs, 0, sizeof(mbs));
	n = 0;
	while ((clen = mbrtowc(&wc, s, MB_LEN_MAX, &mbs)) != 0 &&
	    n + clen < buflen) {
		if (clen == (size_t)-1) {
			*buf++ = '?';
			s++;
			n++;
			memset(&mbs, 0, sizeof(mbs));
			continue;
		}
		if (clen == (size_t)-2) {
			*buf++ = '?';
			n++;
			break;
		}
		if (!iswprint(wc)) {
			*buf++ = '?';
			s += clen;
			n++;
			continue;
		}
		for (i = 0; i < (int)clen; i++)
			*buf++ = (unsigned char)s[i];
		s += clen;
		n += wcwidth(wc);
	}
	
	if (buf != NULL) {
		*buf = 0;
	}

	return (n);
}

//  Copied from src/isilon/bin/isi_change_list/isi_change_list.c,
//  added NULL termination of buf.
static int
prn_octal(const char *s, bool octal_escape, char *buf, size_t buflen)
{
	static const char esc[] = "\\\\\"\"\aa\bb\ff\nn\rr\tt\vv";
	const char *p;
	mbstate_t mbs;
	wchar_t wc;
	size_t clen;
	unsigned char ch;
	int goodchar, i, len, prtlen;

	memset(&mbs, 0, sizeof(mbs));
	len = 0;
	while ((clen = mbrtowc(&wc, s, MB_LEN_MAX, &mbs)) != 0 &&
	    len + clen < buflen) {
		goodchar = clen != (size_t)-1 && clen != (size_t)-2;
		if (goodchar && iswprint(wc) && wc != L'\"' && wc != L'\\') {
			for (i = 0; i < (int)clen; i++)
				*buf++ = (unsigned char)s[i];
			len += wcwidth(wc);
		} else if (goodchar && octal_escape && wc >= 0 &&
		    wc <= (wchar_t)UCHAR_MAX &&
		    (p = strchr(esc, (char)wc)) != NULL) {
			*buf++ = '\\';
			*buf++ = p[1];
			len += 2;
		} else {
			if (goodchar)
				prtlen = clen;
			else if (clen == (size_t)-1)
				prtlen = 1;
			else
				prtlen = strlen(s);
			for (i = 0; i < prtlen; i++) {
				ch = (unsigned char)s[i];
				*buf++ = '\\';
				*buf++ = '0' + (ch >> 6);
				*buf++ = '0' + ((ch >> 3) & 7);
				*buf++ = '0' + (ch & 7);
				len += 4;
			}
		}
		if (clen == (size_t)-2)
			break;
		if (clen == (size_t)-1) {
			memset(&mbs, 0, sizeof(mbs));
			s++;
		} else
			s += clen;
	}

	if (buf != NULL) {
		*buf = 0;
	}

	return (len);
}

static void
resolve_nonprintable_chars(char *in, size_t in_size,
    enum nonprintable_char_action npc_action, char **out, size_t *out_size)
{
	size_t	needed_size = in_size;

	if (npc_action == NPCA_REPLACE_WITH_OCTAL ||
	    npc_action == NPCA_REPLACE_WITH_C_ESCAPE_OR_OCTAL) {
		needed_size *= 4;
	}

	*out = malloc(needed_size);
	ASSERT(*out != NULL);
	*out_size = needed_size;
	
	switch (npc_action) {
	case NPCA_DEFAULT:
	case NPCA_USE_RAW:
		memcpy(*out, in, in_size);
		break;
	case NPCA_REPLACE_WITH_OCTAL:
		prn_octal(in, false, *out, *out_size);
		break;
	case NPCA_REPLACE_WITH_C_ESCAPE_OR_OCTAL:
		prn_octal(in, true, *out, *out_size);
		break;
	case NPCA_REPLACE_WITH_QUESTION_MARK:
		prn_printable(in, *out, *out_size);
		break;
	default:
		ASSERT(false);
		break;
	}
}

static void
get_root_path(struct changelist_entry *ent, ifs_snapid_t snapid,
    enum nonprintable_char_action npc_action, char **root_path,
    size_t *root_path_size, struct isi_error **ie_out)
{
	char 			*tmp_path = NULL;
	size_t			tmp_path_size = ent->u.meta.root_path_size;
	struct isi_error		*ie = NULL;
	struct changelist_entry_v2 	*ent2 = (__typeof__(ent2))ent;

	if (ent->ver < CLE_VER_1) {
		printf("Invalid changelist metadata ver (%d).\n", ent->ver);
		exit(EXIT_FAILURE);
	}

	*root_path = NULL;
	*root_path_size = 0;

	if (tmp_path_size > 0) {
		tmp_path = get_change_entry_data(ent,
		    &ent->u.meta.root_path_offset);

	} else {
		ASSERT(ent->ver > CLE_VER_1);

		if (ent2->u.meta.root_path_lin == ROOT_LIN) {
			*root_path = strdup(STR_SLASH_IFS);
			ASSERT(*root_path != NULL);
			*root_path_size = 1 + strlen(*root_path);
			goto out;
		}

		dir_get_utf8_str_path(STR_SLASH_IFS_SLASH, ROOT_LIN,
		    ent2->u.meta.root_path_lin, snapid, 0, &tmp_path, &ie);
		if (ie != NULL) {
			goto out;
		}

		tmp_path_size = 1 + strlen(tmp_path);
	}

	resolve_nonprintable_chars(tmp_path, tmp_path_size, npc_action,
	    root_path, root_path_size);

out:
	if (ent->u.meta.root_path_size == 0) {
		free(tmp_path);
	}

	isi_error_handle(ie, ie_out);
}

static void
get_relative_path(struct changelist_entry_v2 *ent, ifs_snapid_t old,
    ifs_snapid_t new, ifs_lin_t root_path_lin, char **path,
    struct isi_error **ie_out)
{
	struct dirent			*dp;

	int				result;
	char				*prefix = "/", *d_name, separator;
	char				*tmp = NULL, *tmp2 = NULL;
	char				buf[4 * sizeof(dp->d_name)];
	size_t				buf_size = sizeof(buf);
	uint16_t			cle_flags = ent->u.file.u3.cle_flags;
	ifs_lin_t			lin, parent_lin;
	ifs_snapid_t			snapid;
	struct isi_error		*ie = NULL;
	struct changelist_entry		*ent1 = (__typeof__(ent1))ent;
	struct cl_path_recovery_ctx	*recovery_ctx = NULL;

	snapid = ((cle_flags & CLE_REMOVED) > 0 ? old : new);

	if (ent->u.file.path_size > 0) {
		recovery_ctx = (__typeof__(recovery_ctx))
		    get_change_entry_data(ent1, &ent->u.file.path_offset);
	}

	if (recovery_ctx == NULL) {
		//  No hardlinks involved, so the combination of
		//  root_path_lin, lin, and change type uniquely
		//  identifies the whole path.
		lin = ent->u.file.lin;
		parent_lin = 0;

	} else if ((cle_flags & CLE_HAS_HARDLINKS) > 0) {
		//  Hardlink.
		if (!recovery_ctx->use_ent) {
			lin = recovery_ctx->hardlink_lin;
			parent_lin = recovery_ctx->hardlink_parent_lin;
			if (recovery_ctx->use_old_snap) {
				//  This is case 1 as described in the job's
				//  visit_changed_lin_1(), i.e. path1 exists
				//  in the new snap (but is not the default
				//  there) and path2 doesn't exist in the old
				//  snap. Thus, even tho' path1 is flagged as
				//  "changed" the path lookup must occur in
				//  the old snap, otherwise path2 will be
				//  returned.
				snapid = old;
			}
		} else {
			lin = recovery_ctx->hardlink_parent_lin;
			parent_lin = 0;
		}

	} else if ((cle_flags & CLE_ADS) > 0) {
		//  ADS for a hardlink.
		lin = recovery_ctx->hardlink_lin;
		parent_lin = recovery_ctx->hardlink_parent_lin;

	} else {
		ASSERT(false);
	}

	dir_get_utf8_str_path(prefix, root_path_lin, lin, snapid, parent_lin,
	    &tmp, &ie);
	if (ie != NULL)
		goto out;

	if (recovery_ctx == NULL ||
	    ((cle_flags & CLE_HAS_HARDLINKS) > 0 && !recovery_ctx->use_ent)) {
		//  Done.
		goto out;
	}

	separator = ((cle_flags & CLE_HAS_HARDLINKS) > 0 ?
	    CL_DIR_SEPARATOR : CL_ADS_SEPARATOR);

	dp = &recovery_ctx->ent;
	if (dp->d_encoding == ENC_UTF8) {
		d_name = dp->d_name;

	} else {
		result = enc_conv(dp->d_name, dp->d_encoding, buf,
		    &buf_size, ENC_UTF8);

		if (result < 0) {
			ie = isi_system_error_new(errno, "enc_conv");
			goto out;
		}
		d_name = buf;
	}

	result = asprintf(&tmp2, "%s%c%s", tmp, separator, d_name);
	if (result < 0) {
		ie = isi_system_error_new(errno, "asprintf");
		goto out;
	}
	free(tmp);
	tmp = tmp2;

out:
	if (ie != NULL) {
		free(tmp);
		tmp = NULL;
	}
	*path = tmp;

	isi_error_handle(ie, ie_out);
}

static void
synthesize_v2_entry(uint64_t key, struct changelist_entry *ent,
    struct changelist_entry_v2 *ent2)
{
	memset(ent2, 0, sizeof(*ent2));
	ent2->ver = ent->ver;
	ent2->type = ent->type;
	ent2->size = ent->size;
	ent2->u.file.size = ent->u.file.size;
	ent2->u.file.atime = ent->u.file.atime;
	ent2->u.file.ctime = ent->u.file.mtime;
	ent2->u.file.mtime = ent->u.file.ctime;

	switch (ent->u.file.type) {
	case CLE_FT_REG:
		ent2->u.file.u1.st_mode = S_IFREG;
		break;
	case CLE_FT_DIR: 
		ent2->u.file.u1.st_mode = S_IFDIR;
		break;
	case CLE_FT_SYM: 
		ent2->u.file.u1.st_mode = S_IFLNK;
		break;
	case CLE_FT_FIFO: 
		ent2->u.file.u1.st_mode = S_IFIFO;
		break;
	case CLE_FT_SOCK: 
		ent2->u.file.u1.st_mode = S_IFSOCK;
		break;
	case CLE_FT_CHAR: 
		ent2->u.file.u1.st_mode = S_IFCHR;
		break;
	case CLE_FT_BLOCK: 
		ent2->u.file.u1.st_mode = S_IFBLK;
		break;
 	default:
		//ent2->u.file.u1.st_mode = 0;
		break;
	}

	//ent2->u.file.u2.st_flags = 0;
	ent2->u.file.u3.cle_flags = (ent->u.file.type == CLE_FT_REMOVED ?
	    CLE_REMOVED : 0);
	//ent2->u.file._type_ = 0;

	//  N.B. Just need path_size/offset here, not the actual path data.
	ent2->u.file.path_size = ent->u.file.path_size;
	ent2->u.file.path_offset = ent->u.file.path_offset;
	ent2->u.file.lin = key;
}

static int
compare_cl_names(const void *item1, const void *item2)
{
	#define SHOW_NAME1_FIRST	-1
	#define SHOW_NAME2_FIRST	1

	int		result = 0;
	char		*name1 = *((char **) item1);
	char		*name2 = *((char **) item2);
	bool		is_chglist1, is_inprog1, is_split1;
	bool		is_chglist2, is_inprog2, is_split2;
	ifs_snapid_t	old1, new1, old2, new2;

	//  Sort order:
	//      - Normal changelists (inprog & finalized)
	//          - Lower old snapid
	//              - Lower new snapid
	//      - Split changelists (old > new), only displayed for -x
	//          - Lower old snapid
	//              - Lower new snapid
	//      - Non-changelists (i.e. "." & ".."), only display for -x
	//          - lexigraphic order

	is_chglist1 = decomp_changelist_name(name1, &old1, &new1, &is_inprog1);
	is_chglist2 = decomp_changelist_name(name2, &old2, &new2, &is_inprog2);

	is_split1 = (is_chglist1 && old1 > new1);
	is_split2 = (is_chglist2 && old2 > new2);

	if (is_chglist1) {
		if (is_chglist2) {
			if (is_split1) {
				if (is_split2) {
					//  Both are split changelists,
					//  sort on old snapid.
					if (old1 < old2) {
						result = SHOW_NAME1_FIRST;

					} else if (old1 > old2) {
						result = SHOW_NAME2_FIRST;

					} else {
						//  Old snapids match,
						//  sort on new snapid.
						ASSERT(new1 != new2);
						result = (new1 < new2 ?
						    SHOW_NAME1_FIRST :
						    SHOW_NAME2_FIRST);
					}
				} else {
					//  name1 is split, name2 is normal.
					result = SHOW_NAME2_FIRST;
				}
			} else {
				if (is_split2) {
					//  name1 is normal, name2 is split.
					result = SHOW_NAME1_FIRST;

				} else {
					//  Both are normal changelists,
					//  sort on old snapid.
					if (old1 < old2) {
						result = SHOW_NAME1_FIRST;

					} else if (old1 > old2) {
						result = SHOW_NAME2_FIRST;

					} else {
						//  Old snapids match,
						//  sort on new snapid.
						ASSERT(new1 != new2);
						result = (new1 < new2 ?
						    SHOW_NAME1_FIRST :
						    SHOW_NAME2_FIRST);
					}
				}
			}
		} else {
			//  name1 is a changelist, name2 isn't.
			result = SHOW_NAME1_FIRST;
		}
	} else {
		if (is_chglist2) {
			//  name1 isn't a changelist, name2 is.
			result = SHOW_NAME2_FIRST;
		} else {
			//  Neither are changelists.
			result = strcmp(name1, name2);
		}
	}

	ASSERT(result != 0);
	return result;
}

static void
get_changelist_dir_items(bool raw, bool sort, char*** items, u_int *num_items,
    struct isi_error **ie_out)
{
	DIR			*dir = NULL;
	int			dir_fd = -1;
	char			**names = NULL, **tmp;
	bool			inprog, is_chglist;
	u_int			used = 0, total = 0;
	ifs_snapid_t		old, new;
	struct dirent		*de = NULL;
	struct isi_error	*ie = NULL;


	*items = NULL;
	*num_items = 0;

	dir_fd = cl_btree_open_dir(&ie);
	if (dir_fd < 0)
		goto out;

	dir = fdopendir(dir_fd);
	if (dir == NULL) {
		ie = isi_system_error_new(errno, "fdopendir()");
		goto out;
	} else {
		dir_fd = -1;
	}

	for (;;) {
		de = readdir(dir);
		if (de == NULL)
			break;

		is_chglist = decomp_changelist_name(de->d_name, &old, &new,
		    &inprog);

		//  If not raw the then don't show ".", ".." or
		//  split-lin changelists.
		if (!raw && (!is_chglist || old > new))
			continue;

		if ((used + 1) > total) {
			total = 2 * (used + 1);
			tmp = malloc(total * sizeof(*tmp));
			ASSERT(tmp != NULL);
			if (names != NULL) {
				memcpy(tmp, names, used * sizeof(*tmp));
				free(names);
			}
			names = tmp;
		}

		names[used++] = strdup(de->d_name);
	}

	if (sort && names != NULL) {
		qsort(names, used, sizeof(*names), compare_cl_names);
	}

	*items = names;
	*num_items = used;

out:
	if (dir != NULL)
		closedir(dir);
	if (dir_fd != -1)
		close(dir_fd);

	isi_error_handle(ie, ie_out);
}

static void
print_entry(struct changelist_entry *ent, uint64_t key, ifs_snapid_t old,
    ifs_snapid_t new, ifs_lin_t root_path_lin, char *root_path,
    size_t root_path_size, uint8_t path_display_opts,
    enum nonprintable_char_action npc_action, struct isi_error **ie_out)
{
	char			*tmp_path;
	char			*prefix = STR_DEFAULT_PATH_PREFIX;
	char			*suffix = STR_DEFAULT_PATH_SUFFIX;
	char			*relative_path = NULL;
	char			*full_path = NULL, *shquote_path = NULL;
	size_t			count;
	size_t			relative_path_size = 0;
	size_t			full_path_size = 0, shquote_path_size = 0;
	uint16_t		cle_flags;
	struct isi_error	*ie = NULL;

	struct changelist_entry_v2	*ent2, synth_ent2;
	struct cl_path_recovery_ctx	*rec_ctx;

	if (ent->ver > CLE_VER_1) {
		ent2 = (__typeof__(ent2))ent;
		cle_flags = ent2->u.file.u3.cle_flags;

		//  In the case of directory moves, removes, & renames
		//  isi_change_list does not display the old
		//  (in-scope) paths of the directory's descendants.
		//  An option to persist that behavior is provided.
		//if ((cle_flags & CLE_PARENT_REMOVED) > 0 &&
		//    (path_display_opts &
		//    PATH_DISPLAY_OPT_HIDE_OLD_DESCENDANTS) > 0) {
		//	goto out;
		//}

		if ((cle_flags & CLE_PATH_LOOKUP_REQ) == 0) {
			tmp_path = get_change_entry_data(ent,
			    &ent2->u.file.path_offset);

			resolve_nonprintable_chars(tmp_path,
			    ent2->u.file.path_size, npc_action,
			    &relative_path, &relative_path_size);

		} else {
			tmp_path = NULL;
			get_relative_path(ent2, old, new,
			    root_path_lin, &tmp_path, &ie);
			if (ie != NULL || tmp_path == NULL)
				goto out;

			resolve_nonprintable_chars(tmp_path,
			    1 + strlen(tmp_path), npc_action,
			    &relative_path, &relative_path_size);
			free(tmp_path);
		}

	} else if (ent->ver == CLE_VER_1) {
		//  Don't bother recovering from path truncation on
		//  v1 entries.
		tmp_path = get_change_entry_data(ent,
		    &ent->u.file.path_offset);
		resolve_nonprintable_chars(tmp_path,
		    ent->u.file.path_size, npc_action,
		    &relative_path, &relative_path_size);

		//  Synthesize a v2 entry for display purposes.
		ent2 = &synth_ent2;
		synthesize_v2_entry(key, ent, ent2);

	} else {
		printf("Invalid ver (%d) on entry with key %lld.\n",
		    ent->ver, key);
		exit(EXIT_FAILURE);
	}

	cle_flags = ent2->u.file.u3.cle_flags;

	if ((path_display_opts & PATH_DISPLAY_OPT_PATH_ONLY) == 0) {
		if (debug) {
			printf("ver=%d type=%d size=%d path_size=%d "
			    "path_offset=%d path_depth=%d parent_lin=%lld "
			    "phys_size=%lld prot_level=%lld prot_policy=%lld "
			    "data_pool=%d metadata_pool=%d ads_count=%d "
			    "st_nlink=%u st_uid=%u st_gid=%u st_btime=%lld ",
			    ent2->ver, ent2->type, ent2->size,
			    ent2->u.file.path_size, ent2->u.file.path_offset,
			    ent2->u.file.path_depth, ent2->u.file.parent_lin,
			    ent2->u.file.phys_size, ent2->u.file.prot_level,
			    ent2->u.file.prot_policy, ent2->u.file.data_pool,
			    ent2->u.file.metadata_pool, ent2->u.file.ads_count,
			    ent2->u.file.st_nlink, ent2->u.file.st_uid,
			    ent2->u.file.st_gid, ent2->u.file.st_btime);

			if ((cle_flags & CLE_PATH_LOOKUP_REQ) > 0 &&
			    ent->u.file.path_size > 0) {
				rec_ctx = (__typeof__(rec_ctx))
				    get_change_entry_data(ent,
				    &ent->u.file.path_offset);

				printf("hardlink_lin=%lld "
				    "hardlink_parent_lin=%lld "
				    "use_ent=%d use_old_snap=%d ",
				    rec_ctx->hardlink_lin,
				    rec_ctx->hardlink_parent_lin,
				    rec_ctx->use_ent,
				    rec_ctx->use_old_snap);

				if (rec_ctx->use_ent) {
					printf("ent_d_fileno=%lld ",
					rec_ctx->ent.d_fileno);
				}
			}

			//  Normally, keys are composed of lins and (for
			//  removed entries) the CLE_KEY_REMOVED_MASK.
			//  Keys for hardlinked lins are generated
			//  differently, so print those here.
			if ((key & CLE_KEY_HASH_MASK) > 0) {
				printf("key=%lld ", key);
			}

		} else {
			//  Flags to hide if not in extended mode.
			cle_flags &= ~(CLE_PARENT_REMOVED | 
			    CLE_PATH_LOOKUP_REQ);
		}

		printf("st_ino=%lld st_mode=0%o st_size=%lld "
		    "st_atime=%lld st_mtime=%lld st_ctime=%lld "
		    "st_flags=%u cl_flags=0%o path=",
		    ent2->u.file.lin, ent2->u.file.u1.st_mode,
		    ent2->u.file.size, ent2->u.file.atime,
		    ent2->u.file.mtime, ent2->u.file.ctime,
		    ent2->u.file.u2.st_flags, cle_flags);
	}

	if ((path_display_opts &
	    PATH_DISPLAY_OPT_PREPEND_ADD_REM) > 0) {
		if ((cle_flags & CLE_REMOVED) > 0) {
			prefix = STR_REMOVED_PATH_PREFIX;
		} else if ((cle_flags & CLE_ADDED) > 0) {
			prefix = STR_ADDED_PATH_PREFIX;
		} else {
			prefix = STR_DEFAULT_PATH_PREFIX;
		}
	}

	if ((path_display_opts &
	    PATH_DISPLAY_OPT_APPEND_DIR_SLASH) > 0) {
		suffix = (S_ISDIR(ent2->u.file.u1.st_mode) ?
		    STR_DIR_PATH_SUFFIX : STR_NONDIR_PATH_SUFFIX);
	}

	if ((path_display_opts & PATH_DISPLAY_OPT_SHQUOTE) == 0) {
		printf("%s%s%s%s\n", prefix, root_path, relative_path,
		    suffix);
	} else {
		count = MAX_PATH_PREFIX_SIZE + root_path_size +
		    relative_path_size + MAX_PATH_SUFFIX_SIZE;
		if (full_path_size < count) {
			free(full_path);
			full_path = malloc(count);
			ASSERT(full_path);
			full_path_size = count;

			//  Worst case: every other char a single
			//  quote, i.e. 'a'\''b'\''c'\'...
			free(shquote_path);
			shquote_path = malloc(3 * count);
			ASSERT(shquote_path);
			shquote_path_size = 3 * count;
		}
		snprintf(full_path, full_path_size, "%s%s%s%s",
		    prefix, root_path, relative_path, suffix);

		//  XXX Per 'man shquote': "This implementation does
		//  not currently handle strings containing multi-
		//  byte characters properly..."
		count = shquote(full_path, shquote_path,
		    shquote_path_size);
		if (count == -1) {
			ie = isi_system_error_new(errno, "shquote(%s)",
			    full_path);
			goto out;
		}
		printf("%s\n", shquote_path);
	}

out:
	free(relative_path);
	free(full_path);
	free(shquote_path);

	isi_error_handle(ie, ie_out);
}

//
//  Command routines.
//

static void
list_chglists(int argc, char **argv, struct isi_error **ie_out)
{
	char			**items = NULL;
	u_int			num_items = 0, i;
	struct isi_error	*ie = NULL;

	if (argc != 0)
		usage();

	get_changelist_dir_items(debug, true, &items, &num_items, &ie);
	if (ie) {
		goto out;
	}

	for (i = 0; i < num_items; i++) {
		printf("%s\n", items[i]);
		free(items[i]);
	}

	free(items);

	if (num_items == 0) {
		printf("No changelists found.\n");
	}

out:
	isi_error_handle(ie, ie_out);
}

static void
create_chglist(int argc, char **argv, struct isi_error **ie_out)
{
	int			i;
	char			*root = "";
	bool			exist_ok = false, inprog = true;
	bool			root_set = false, jobid_set = false;
	bool			ver_set = false, jobphase_set = false;
	uint32_t		ver = CLE_VER_1;
	uint32_t		jobphase = 3;	//  CL_PHASE_ENUM
	jd_jid_t		jobid = 0;
	ifs_snapid_t		old, new;
	struct cl_ctx		*ctx = NULL;
	struct cl_buf		buf;
	struct isi_error	*ie = NULL;
	struct changelist_entry	*ent = (__typeof__(ent))buf.buffer;

	struct changelist_entry_v2	*ent_v2 = (__typeof__(ent_v2))ent;

	if (argc < 2 || argc > 10 || (argc % 2) != 0)
		usage();

	arg_base10_to_uint64(argv[0], "old", &old);
	arg_base10_to_uint64(argv[1], "new", &new);

	//  Parse any optional args.
	for (i = 2; i < argc; i += 2) {
		if (strcasecmp(argv[i], "--ver") == 0 && !ver_set) {
			arg_base10_to_uint32(argv[i+1], "ver", &ver);
			if (ver > 255) {
				printf("Invalid ver value '%s' specified.\n",
				    argv[i+1]);
				exit(EXIT_FAILURE);
			}
			ver_set = true;
		}
		else if (strcasecmp(argv[i], "--root") == 0 && !root_set) {
			root = argv[i+1];
			if (strlen(root) >= MAX_CHANGELIST_PATH_SIZE) {
				printf("Warning: truncating root.\n");
				root[MAX_CHANGELIST_PATH_SIZE-1] = 0;
			}
			root_set = true;
		} else if (strcasecmp(argv[i], "--jobid") == 0 &&
		    !jobid_set) {
			arg_base10_to_uint32(argv[i+1], "jobid", &jobid);
			jobid_set = true;
		}
		else if (strcasecmp(argv[i], "--jobphase") == 0 &&
		    !jobphase_set) {
			arg_base10_to_uint32(argv[i+1], "jobphase", &jobphase);
			if (jobphase > 255) {
				printf("Invalid jobphase value '%s' "
				    "specified.\n", argv[i+1]);
				exit(EXIT_FAILURE);
			}
			jobphase_set = true;
		}
		else
			usage();
	}

	create_changelist(old, new, exist_ok, &ie);
	if (ie)
		goto out;

	open_changelist(old, new, inprog, &ctx, &ie);
	if (ie)
		goto out;

	memset(ent, 0, sizeof(*ent));
	ent->ver = (uint8_t)ver;
	ent->type = CLE_TYPE_METADATA;
	ent->size = sizeof(*ent);

	//ent->u.meta.num_cl_entries = 0;
	ent->u.meta.owner = jobid;
	if (ver >= CLE_VER_2) {
		ent_v2->u.meta.job_phase = (uint8_t)jobphase;
	}
	set_change_entry_data(ent, sizeof(buf.buffer),
	    &ent->u.meta.root_path_size, &ent->u.meta.root_path_offset,
	    root, (uint16_t) strlen(root) + 1, &ie);

	set_change_entry(ctx, METADATA_KEY, ent, &ie);

out:
	close_changelist(ctx);

	isi_error_handle(ie, ie_out);
}

static void
describe_chglist(int argc, char **argv, struct isi_error **ie_out)
{
	char			*cl_name, *root_path= NULL, **items = NULL;
	char			*full_path = NULL, *shquote_path = NULL;
	char			*suffix = STR_DEFAULT_PATH_SUFFIX;
	bool			inprog, found;
	u_int			num_items = 0, i;
	size_t			root_path_size = 0, full_path_size = 0;
	size_t			shquote_path_size = 0, count;
	uint8_t			path_display_opts = PATH_DISPLAY_OPT_NONE;
	ifs_snapid_t		old, new;
	struct cl_ctx		*ctx = NULL;
	struct cl_buf		buf;
	struct isi_error	*ie = NULL;

	struct changelist_entry		*ent;
	struct changelist_entry_v2	*ent2;
	enum nonprintable_char_action	npc_action = NPCA_DEFAULT;

	if (argc < 1)
		usage();

	if (strcmp(argv[0], "--all") == 0) {
		get_changelist_dir_items(false, true, &items, &num_items, &ie);
		if (ie) {
			goto out;
		}

	} else {
		cl_name = argv[0];
		if (!crack_cl_name(cl_name, &old, &new, &inprog, NULL, &ie)) {
			goto out;
		}
		items = malloc(1 * sizeof(*items));
		ASSERT(items != NULL);
		items[0] = strdup(cl_name);
		num_items = 1;
	}

	get_options(argc - 1, argv + 1, false, &path_display_opts,
	    &npc_action, NULL, NULL); 

	for (i = 0; i < num_items; i++) {

		if (!crack_cl_name(items[i], &old, &new, &inprog, &ctx, &ie)) {
			goto out;
		}

		found = get_change_entry(ctx, METADATA_KEY, &buf, &ent, &ie);
		if (ie != NULL)
			goto out;

		printf("name=%s ", items[i]);

		//  If there's valid no metadata entry then all bets are off.
		if (!found) {
			printf("Error: changelist metadata not found.\n");
			exit(EXIT_FAILURE);
		}

		get_root_path(ent, old, npc_action, &root_path,
		    &root_path_size, &ie);
		if (ie != NULL) {
			goto out;
		}

		if ((path_display_opts & PATH_DISPLAY_OPT_PATH_ONLY) == 0) {
			if (debug) {
				printf("ver=%d type=%d size=%d "
				    "root_path_size=%d root_path_offset=%d ",
				    ent->ver, ent->type, ent->size,
				    ent->u.meta.root_path_size,
				    ent->u.meta.root_path_offset);

				if (ent->ver > CLE_VER_1) {
					ent2 = (__typeof__(ent2))ent;
					printf("job_phase=%d "
					    "root_path_lin=%lld ",
					    ent2->u.meta.job_phase,
					    ent2->u.meta.root_path_lin);
				}
			}

			printf("num_entries=%lld owner=%d path=",
			    ent->u.meta.num_cl_entries, ent->u.meta.owner);
		}

		if ((path_display_opts &
		    PATH_DISPLAY_OPT_APPEND_DIR_SLASH) > 0) {
			suffix =  STR_DIR_PATH_SUFFIX;
		}

		if ((path_display_opts & PATH_DISPLAY_OPT_SHQUOTE) == 0) {
			printf("%s%s\n", root_path, suffix);
		} else {
			count = root_path_size + MAX_PATH_SUFFIX_SIZE;
			full_path = malloc(count);
			ASSERT(full_path);
			full_path_size = count;

			//  Worst case: every other char a single
			//  quote, i.e. 'a'\''b'\''c'\'...
			free(shquote_path);
			shquote_path = malloc(3 * count);
			ASSERT(shquote_path);
			shquote_path_size = 3 * count;

			snprintf(full_path, full_path_size, "%s%s", root_path,
			    suffix);

			//  XXX Per 'man shquote': "This implementation does
			//  not currently handle strings containing multi-
			//  byte characters properly..."
			count = shquote(full_path, shquote_path,
			    shquote_path_size);
			if (count == -1) {
				ie = isi_system_error_new(errno, "shquote(%s)",
				    full_path);
				goto out;
			}
			printf("%s\n", shquote_path);
		}

		free(full_path);
		free(root_path);
		free(shquote_path);
		close_changelist(ctx);

		full_path = root_path = shquote_path = NULL;
		ctx = NULL;
	}

out:
	if (items != NULL) {
		for (i = 0; i < num_items; i++) {
			free(items[i]);
		}
		free(items);
	}
	free(shquote_path);
	free(full_path);
	free(root_path);
	close_changelist(ctx);

	isi_error_handle(ie, ie_out);
}

static void
finalize_chglist(int argc, char **argv, struct isi_error **ie_out)
{
	char			*cl_name;
	bool			inprog;
	ifs_snapid_t		old, new;
	struct isi_error	*ie = NULL;

	if (argc != 1)
		usage();

	cl_name = argv[0];
	if (!crack_cl_name(cl_name, &old, &new, &inprog, NULL, &ie))
		goto out;

	finalize_changelist(old, new, &ie);

out:
	isi_error_handle(ie, ie_out);
}

static void
kill_chglist(int argc, char **argv, struct isi_error **ie_out)
{
	bool			inprog;
	char			*cl_name, **items = NULL, name[MAXNAMELEN];
	u_int			num_items = 0, i;
	ifs_snapid_t		old, new;
	struct isi_error	*ie = NULL;

	if (argc != 1)
		usage();

	if (strcmp(argv[0], "--all") == 0) {
		get_changelist_dir_items(false, false, &items, &num_items, &ie);
		if (ie) {
			goto out;
		}

	} else {
		cl_name = argv[0];
		if (!crack_cl_name(cl_name, &old, &new, &inprog, NULL, &ie)) {
			goto out;
		}
		items = malloc(1 * sizeof(*items));
		ASSERT(items != NULL);
		items[0] = strdup(cl_name);
		num_items = 1;
	}

	for (i = 0; i < num_items; i++) {

		if (!crack_cl_name(items[i], &old, &new, &inprog, NULL, &ie)) {
			goto out;
		}

		//  First, since changelist consumers aren't aware of them,
		//  silently remove any split-lin changelist and/or summary stf
		//  that still exist (missing_ok=true).
		if (old < new) {
			remove_changelist(new, old, true, true, &ie);
			if (ie != NULL) {
				goto out;
			}

			get_changelist_summ_stf_name(name, old, new);
			siq_btree_remove(siq_btree_summ_stf, CHANGELIST_SUMM_DIR,
			    name, true, &ie);
			if (ie != NULL) {
				goto out;
			}
		}

		//  Now remove the specified changelist (missing_ok=false).
		remove_changelist(old, new, inprog, false, &ie);
		if (ie != NULL) {
			goto out;
		}

		if (debug) {
			printf("Removed %s\n", items[i]);
		}
	}

out:
	if (items != NULL) {
		for (i = 0; i < num_items; i++) {
			free(items[i]);
		}
		free(items);
	}

	isi_error_handle(ie, ie_out);
}

static void
set_entry(int argc, char **argv, struct isi_error **ie_out)
{
	int			ret;
	uint32_t		ver, type;
	char			*cl_name, path[MAX_CHANGELIST_PATH_SIZE];
	bool			inprog, exists, path_set = false;
	bool			type_set = false, size_set = false;
	bool			atime_set = false, atimensec_set = false;
	bool			ctime_set = false, ctimensec_set = false;
	bool			mtime_set = false, mtimensec_set = false;
	bool			st_mode_set = false, st_flags_set = false,
				cle_flags_set = false, parent_lin_set = false,
				path_depth_set = false, phys_size_set = false,
				prot_level_set = false, prot_policy_set = false,
				data_pool_set = false, ads_count_set = false,
				metadata_pool_set = false, nlink_set = false,
				uid_set = false, gid_set = false,
				btime_set = false;
	size_t			size;
	ifs_lin_t		lin;
	ifs_snapid_t		old, new;
	struct cl_ctx		*ctx = NULL;
	struct cl_buf		buf;
	struct isi_error	*ie = NULL;

	uint64_t		key;

	struct changelist_entry	*ent = (__typeof__(ent))buf.buffer;
	struct changelist_entry_v2 *ent2 = (__typeof__(ent2))buf.buffer;

	if (!(argc >= 2 && argc <= 60 && (argc % 2) == 0))
		usage();

	cl_name = argv[0];
	arg_base10_to_uint64(argv[1], "lin", &lin);

	if (!crack_cl_name(cl_name, &old, &new, &inprog, &ctx, &ie))
		goto out;

	if (lin < BASE_LIN || lin > MAX_LIN) {
		printf("Only entries in the LIN range %lld-%lld may be set."
		    "\n", BASE_LIN, MAX_LIN);
		exit(EXIT_FAILURE);
	}

	memset(ent, 0, sizeof(*ent));
	ent->ver = CLE_VER_1;
	ent->type = CLE_TYPE_FILE;
	ent->size = sizeof(*ent);

	//  Parse any optional args.
	while(true) {
		ret = getopt_long(argc, argv, "", chng_list_mod_opts, NULL);
		if (ret == -1)
			break;
		switch (ret) {
			case OPT_VER:
				arg_base10_to_uint32(argv[optind], "ver", &ver);
				printf("ver = %d CLE_VER_NOW = %d\n",
				    ent->ver, CLE_VER_NOW);
				if (ver > CLE_VER_NOW) {
					printf(
					    "Invalid ver value '%s' specified."
					    "\n", argv[optind]);
					exit(EXIT_FAILURE);
				}
				ent->ver = (__typeof__(ent->ver))ver;
				ent->size = sizeof(*ent);
				if (ent->ver == CLE_VER_2) {
					ent2->size = sizeof(*ent2);
					ent2->u.file.lin = lin;
				}
				get_change_entry_key(ent->ver, lin, false,
				    &key);
				break;
			case OPT_PATH:
				if (path_set) {
					usage();
					break;
				}
				strcpy(path, argv[optind]);
				path_set = true;
				size = strlen(path) + 1;
				if (size >= MAX_CHANGELIST_PATH_SIZE) {
					printf("Warning: truncating path.\n");
					size = MAX_CHANGELIST_PATH_SIZE;
					path[size-1] = 0;
				}
				set_change_entry_data(ent, sizeof(buf.buffer),
				    &ent->u.file.path_size, &ent->u.file.path_offset,
				    path, (uint16_t)size, &ie);
				if (ie)
					goto out;
				break;
			case OPT_TYPE:
				if (type_set) {
                                        usage();
                                        break;
                                }
				type_set = true;
				arg_base10_to_uint32(argv[optind], "type", &type);
				if (type > 255) {
					printf("Invalid type value '%s' "
					    "specified.\n", argv[optind]);
					exit(EXIT_FAILURE);
				}
				ent->u.file.type =
				    (__typeof__(ent->u.file.type))type;
				break;
			case OPT_ST_SIZE:
				if (size_set) {
                                        usage();
                                        break;
                                }
                                size_set = true;
				arg_base10_to_uint64(argv[optind], "size",
				    &ent->u.file.size);
				break;
			case OPT_ST_ATIME:
				if (atime_set) {
                                        usage();
                                        break;
                                }
                                atime_set = true;
				arg_base10_to_int64(argv[optind], "atime",
				    &ent->u.file.atime);
				break;
			case OPT_ST_ATIMENSEC:
				if (atimensec_set) {
                                        usage();
                                        break;
                                }
                                atimensec_set = true;
				arg_base10_to_int32(argv[optind], "atimensec",
				    &ent->u.file.atimensec);
				break;
			case OPT_ST_CTIME:
				if (ctime_set) {
                                        usage();
                                        break;
                                }
                                ctime_set = true;
				arg_base10_to_int64(argv[optind], "ctime",
				    &ent->u.file.ctime);
				break;
			case OPT_ST_CTIMENSEC:
				if (ctimensec_set) {
                                        usage();
                                        break;
                                }
                                ctimensec_set = true;
				arg_base10_to_int32(argv[optind], "ctimensec",
				    &ent->u.file.ctimensec);
                                break;
			case OPT_ST_MTIME:
                                if (mtime_set) {
                                        usage();
                                        break;
                                }
                                mtime_set = true;
				arg_base10_to_int64(argv[optind], "mtime",
				    &ent->u.file.mtime);
                                break;
			case OPT_ST_MTIMENSEC:
                                if (mtimensec_set) {
                                        usage();
                                        break;
                                }
                                mtimensec_set = true;
				arg_base10_to_int32(argv[optind], "mtimensec",
				    &ent->u.file.mtimensec);
                                break;
                        case OPT_ST_MODE:
                                if (st_mode_set) {
                                        usage();
                                        break;
                                }
                                st_mode_set = true;
				arg_base10_to_uint16(argv[optind], "mode",
	                            &ent2->u.file.u1.st_mode);
                                break;
                        case OPT_ST_FLAGS:
                                if (st_flags_set) {
                                        usage();
                                        break;
                                }
                                st_flags_set = true;
				arg_base10_to_uint32(argv[optind], "flags",
				    &ent2->u.file.u2.st_flags);
                                break;
			case OPT_CLE_FLAGS:
                                if (cle_flags_set) {
                                        usage();
                                        break;
                                }
                                cle_flags_set = true;
				arg_base10_to_uint32(argv[optind], "cle_flags",
				    &ent2->u.file.u3.cle_flags);
				break;
                        case OPT_PARENT_LIN:
                                if (parent_lin_set) {
                                        usage();
                                        break;
                                }
                                parent_lin_set = true;
				arg_base10_to_uint64(argv[optind], "parent_lin",
	                            &ent2->u.file.parent_lin);
				break;
			case OPT_PATH_DEPTH:
                                if (path_depth_set) {
                                        usage();
                                        break;
                                }
                                path_depth_set = true;
	                        arg_base10_to_uint32(argv[optind], "path_depth",
				    &ent2->u.file.path_depth);
                                break;
                        case OPT_PHYS_SIZE:
                                if (phys_size_set) {
                                        usage();
                                        break;
                                }
                                phys_size_set = true;
				arg_base10_to_uint64(argv[optind], "phys_size",
	                            &ent2->u.file.phys_size);
                                break;
			case OPT_PROT_LEVEL:
                                if (prot_level_set) {
                                        usage();
                                        break;
                                }
                                prot_level_set = true;
	                        arg_base10_to_uint64(argv[optind], "prot_level",
				    &ent2->u.file.prot_level);
				break;
                        case OPT_PROT_POLICY:
                                if (prot_policy_set) {
                                        usage();
                                        break;
                                }
                                prot_policy_set = true;
				arg_base10_to_uint64(argv[optind], "prot_policy",
	                            &ent2->u.file.prot_policy);
                                break;
			case OPT_DATA_POOL:
                                if (data_pool_set) {
                                        usage();
                                        break;
                                }
                                data_pool_set = true;
	                        arg_base10_to_int32(argv[optind], "data_pool",
				    &ent2->u.file.data_pool);
                                break;
			case OPT_METADATA_POOL:
                                if (metadata_pool_set) {
                                        usage();
                                        break;
                                }
                                metadata_pool_set = true;
	                        arg_base10_to_int32(argv[optind], "metadata_pool",
				    &ent2->u.file.metadata_pool);
                                break;
			case OPT_ADS_COUNT:
                                if (ads_count_set) {
                                        usage();
                                        break;
                                }
                                ads_count_set = true;
	                        arg_base10_to_uint32(argv[optind], "ads_count",
				    &ent2->u.file.ads_count);
                                break;
			case OPT_ST_NLINK:
                                if (nlink_set) {
                                        usage();
                                        break;
                                }
				nlink_set = true;
	                        arg_base10_to_uint32(argv[optind], "nlink",
				    &ent2->u.file.st_nlink);
                                break;
			case OPT_ST_UID:
                                if (uid_set) {
                                        usage();
                                        break;
                                }
                                uid_set = true;
	                        arg_base10_to_uint32(argv[optind], "uid",
				    &ent2->u.file.st_uid);
                                break;
			case OPT_ST_GID:
                                if (gid_set) {
                                        usage();
                                        break;
                                }
                                gid_set = true;
	                        arg_base10_to_uint32(argv[optind], "gid",
				    &ent2->u.file.st_gid);
                                break;
			case OPT_ST_BTIME:
                                if (btime_set) {
                                        usage();
                                        break;
                                }
                                btime_set = true;
	                        arg_base10_to_int64(argv[optind], "btime",
				    &ent2->u.file.st_btime);
				break;
			default:
				usage();
				break;
		}
	}

	if (!path_set) {
		set_change_entry_data(ent, sizeof(buf.buffer),
		    &ent->u.file.path_size, &ent->u.file.path_offset,
		    EMPTY_PATH, sizeof(EMPTY_PATH), &ie);
		if (ie)
			goto out;
	}

	exists = change_entry_exists(ctx, lin, &ie);
	if (ie)
		goto out;

	set_change_entry(ctx, lin, ent, &ie);
	if (ie)
		goto out;

	if (!exists)
		update_num_entries(ctx, true, &ie);

out:
	close_changelist(ctx);

	isi_error_handle(ie, ie_out);
}

static void
delete_entry(int argc, char **argv, struct isi_error **ie_out)
{
	bool			inprog, exists;
	char			*cl_name;
	ifs_lin_t		key;
	ifs_snapid_t		old, new;
	struct cl_ctx		*ctx = NULL;
	struct isi_error	*ie = NULL;

	if (argc != 2)
		usage();

	cl_name = argv[0];
	arg_base10_to_uint64(argv[1], "key", &key);

	if (key <= METADATA_KEY) {
		printf("Invalid key value %lld\n", key);
		exit(EXIT_FAILURE);
	}

	if (!crack_cl_name(cl_name, &old, &new, &inprog, &ctx, &ie))
		goto out;

	exists = remove_change_entry(ctx, key, &ie);
	if (ie)
		goto out;

	if (exists)
		update_num_entries(ctx, false, &ie);

out:
	close_changelist(ctx);

	isi_error_handle(ie, ie_out);
}

static void
print_all_entries(int argc, char **argv, struct isi_error **ie_out)
{
	char			*cl_name, *root_path = NULL;
	bool			inprog, found;
	u_int			slice_num, slice_den;
	size_t			root_path_size = 0;
	uint8_t			path_display_opts = PATH_DISPLAY_OPT_NONE;
	uint64_t		key, max_key = (uint64_t)-1;
	ifs_lin_t		root_path_lin = 0;
	ifs_snapid_t		old, new;
	struct cl_ctx		*ctx = NULL;
	struct cl_iter		*iter = NULL;
	struct isi_error	*ie = NULL;

	struct cl_buf			cl_buf;
	struct changelist_entry		*cl_ent;
	struct changelist_entry_v2	*cl_ent2;
	enum nonprintable_char_action	npc_action = NPCA_DEFAULT;

	if (argc < 1)
		usage();

	cl_name = argv[0];

	if (!crack_cl_name(cl_name, &old, &new, &inprog, &ctx, &ie))
		goto out;

	get_options(argc - 1, argv + 1, true, &path_display_opts,
	    &npc_action, &slice_num, &slice_den); 

	found = get_change_entry(ctx, METADATA_KEY, &cl_buf, &cl_ent, &ie);
	if (ie != NULL) {
		goto out;
	}

	if (found) {
		get_root_path(cl_ent, old, npc_action, &root_path, &root_path_size,
		    &ie);
		if (ie != NULL)
			goto out;
		if (cl_ent->ver == CLE_VER_2) {
			cl_ent2 = (__typeof__(cl_ent2))cl_ent;
			root_path_lin = cl_ent2->u.meta.root_path_lin;
		}
	}
	if (slice_num == 0) {
		key = METADATA_KEY;

	} else if (slice_num < slice_den) {
		found = get_change_entry_at(ctx, slice_num - 1, slice_den,
		    &key, &ie);
		if (ie != NULL || !found) {
			goto out;
		}

		found = get_change_entry_at(ctx, slice_num, slice_den,
		    &max_key, &ie);
		if (ie != NULL || !found || key >= max_key) {
			goto out;
		}

	} else {
		found = get_change_entry_at(ctx, slice_num - 1, slice_den,
		    &key, &ie);
		if (ie != NULL || !found) {
			goto out;
		}
	}

	if (key == METADATA_KEY) {
		key++;
	}

	iter = new_cl_iter(ctx, key);
	ASSERT(iter != NULL);

	for (;;) {
		found = cl_iter_next(iter, &key, &cl_ent, &ie);
		if (ie != NULL || !found || key >= max_key) {
			goto out;
		}

		print_entry(cl_ent, key, old, new, root_path_lin, root_path,
		    root_path_size, path_display_opts, npc_action, &ie);
		if (ie != NULL) {
			goto out;
		}
	}

out:
	free(root_path);
	close_cl_iter(iter);
	close_changelist(ctx);

	isi_error_handle(ie, ie_out);
}

int
main(int argc, char **argv)
{
	int			next_arg = 1;
	char			c, d;
	struct isi_error	*ie = NULL;

	if (argc < 2)
		usage();

	if (sscanf(argv[next_arg++], "-%c%c", &c, &d) != 1)
		usage();

	if (c == 'x') {
		debug = true;
		if (argc < 3)
			usage();
		if (sscanf(argv[next_arg++], "-%c%c", &c, &d) != 1)
			usage();
	}

	//  XXX Not clear whether setlocale is needed here.
	//  locale = setlocale(LC_CTYPE, "UTF-8");

	switch (c) {
	case 'a':
		print_all_entries(argc - next_arg, argv + next_arg, &ie);
		break;
	case 'c':
		create_chglist(argc - next_arg, argv + next_arg, &ie);
		break;
	case 'd':
		delete_entry(argc - next_arg, argv + next_arg, &ie);
		break;
	case 'f':
		finalize_chglist(argc - next_arg, argv + next_arg, &ie);
		break;
	case 'i':
		describe_chglist(argc - next_arg, argv + next_arg, &ie);
		break;
	case 'k':
		kill_chglist(argc - next_arg, argv + next_arg, &ie);
		break;
	case 'l':
		list_chglists(argc - next_arg, argv + next_arg, &ie);
		break;
	case 's':
		set_entry(argc - next_arg, argv + next_arg, &ie);
		break;
	default:
		usage();
	}

	if (ie) {
		if (debug) {
			printf("%s\n", isi_error_get_detailed_message(ie));
		} else {
			printf("%s\n", isi_error_get_message(ie));
		}
		isi_error_free(ie);
		exit(EXIT_FAILURE);
	}
}

