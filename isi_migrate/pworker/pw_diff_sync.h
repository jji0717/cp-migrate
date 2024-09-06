#ifndef __PW_DIFF_SYNC_H__
#define __PW_DIFF_SYNC_H__

struct ds_config {
	char *name;
	int *value_p;
};

/* XXX We need to determine optimal values for these !!! */

/* Any file this length or under is automatically sent without any hash
 * comparisons */
int JUST_SEND_THE_FILE_LEN  = 1024 * 32;

/* The length in bytes of the short hashes calculated from the files */
int SHORT_HASH_LEN = 1024 * 8;

/* The number of bytes in the files used to calculate each full hash
   piece. Use zero to force full hash of entire file */
int MIN_FULL_HASH_PIECE_LEN = ( 4096 * 1024);

/* Maximum number of files in cache */
int MAX_CACHE_SIZE = DS_MAX_CACHE_SIZE;

/* Number of pieces a file is divided into for full hashing */
int HASH_PIECES_PER_FILE = 100;

enum hash_type HASH_TYPE = HASH_MD4;

char *DIFF_SYNC_CONFIG_FILE = "/ifs/.ifsvar/modules/tsm/ds_config";

/* This is so we can determine optimal values for some variables used for
 * diff sync. For release, this will either be removed, or be added to
 * tsm.xml */
struct ds_config ds_config[] = {
	{"just_send_len", &JUST_SEND_THE_FILE_LEN},
	{"short_hash_len", &SHORT_HASH_LEN},
	{"min_full_hash_piece", &MIN_FULL_HASH_PIECE_LEN},
	{"max_cache_size", &MAX_CACHE_SIZE},
	{"pieces_per_file", &HASH_PIECES_PER_FILE},
	{"hash_type", (int *)&HASH_TYPE}
};

#endif // __PW_DIFF_SYNC_H__
