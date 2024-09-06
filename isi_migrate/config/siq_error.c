#define ISI_ERROR_PROTECTED
#include "isi_migrate/siq_error_protected.h"
#include "isi_migrate/siq_error.h"
#include "isi_migrate/config/siq_alert.h"
#include <isi_util/isi_assert.h>
#include <isi_celog/isi_celog.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * 	isi_siq_error public functions
 */
struct isi_error *
_isi_siq_error_new(int siqerr, const char *function, const char *file,
    int line, const char *format, ...)
{
	va_list ap;
	struct isi_siq_error *se = malloc(sizeof *se);
	va_start(ap, format);
	isi_siq_error_init(se, ISI_SIQ_ERROR_CLASS, siqerr, 
		function, file,
		line, format, ap);
	va_end(ap);
	return &se->super;
}


int
isi_siq_error_get_siqerr(const struct isi_siq_error *siqe)
{
	return siqe->siqerr;
}

char *
isi_siq_error_get_errstr(const struct isi_siq_error *siqe)
{
	return siqe->errstr;
}

bool
isi_siq_error_get_msgsent(const struct isi_siq_error *siqe)
{
	return siqe->msgsent;
}

void
isi_siq_error_set_msgsent(struct isi_siq_error *siqe, const bool value)
{
	siqe->msgsent = value;
}

/*
 *	isi_siq_fs_error public functions
 */
struct isi_error *
_isi_siq_fs_error_new(int siqerr, int error,
    const char *dir,  u_int64_t dirlin, unsigned eveclen, const enc_t *evec,
    const char *filename, unsigned enc, u_int64_t filelin,
    const char *function, const char *file,
    int line, const char *format, ...)
{
	va_list ap;
	struct isi_siq_fs_error *siqfse = malloc(sizeof *siqfse);
	va_start(ap, format);

	isi_siq_error_init(&(siqfse->super), ISI_SIQ_FS_ERROR_CLASS, siqerr, 
		function, file, line, format, ap);
	va_end(ap);

	siqfse->error = error;
	siqfse->dir = NULL;
	siqfse->filename = NULL;
	siqfse->evec = NULL;

	if (dir)
		siqfse->dir = strdup(dir);
	siqfse->dirlin = dirlin;
	siqfse->eveclen = eveclen;

	if (evec && eveclen) {
		siqfse->evec = malloc(eveclen);
		memcpy(siqfse->evec, evec, eveclen);
	}

	if (filename)
		siqfse->filename = strdup(filename);

	siqfse->enc = enc;
	siqfse->filelin = filelin;
	
	return (struct isi_error *)siqfse;
}

int
isi_siq_fs_error_get_unix_error(const struct isi_siq_fs_error *siqfse)
{
	return siqfse->error;
}

char *
isi_siq_fs_error_get_file(const struct isi_siq_fs_error *siqfse)
{
	return siqfse->filename;
}

unsigned int
isi_siq_fs_error_get_enc(const struct isi_siq_fs_error *siqfse)
{
	return siqfse->enc;
}

unsigned int
isi_siq_fs_error_get_eveclen(const struct isi_siq_fs_error *siqfse)
{
	return siqfse->eveclen;
}

enc_t *
isi_siq_fs_error_get_evec(const struct isi_siq_fs_error *siqfse)
{
	return siqfse->evec;
}

u_int64_t
isi_siq_fs_error_get_filelin(const struct isi_siq_fs_error *siqfse)
{
	return siqfse->filelin;
}

char *
isi_siq_fs_error_get_dir(const struct isi_siq_fs_error *siqfse)
{
	return siqfse->dir;
}

u_int64_t
isi_siq_fs_error_get_dirlin(const struct isi_siq_fs_error *siqfse)
{
	return siqfse->dirlin;
}


/*
 * 	isi_siq_error protected members
 */
void
isi_siq_error_init(struct isi_siq_error *siqe,
	const struct isi_error_class *c, int siqerr,
	const char *function, const char *file, int line,
	const char *format, va_list ap)
{
	siqe->siqerr = siqerr;
	siqe->errstr = NULL;
	siqe->msgsent = false;
	isi_error_init(&siqe->super, c, function, file, line, format, ap);
}

void
isi_siq_error_free_impl(struct isi_error *e)
{
	struct isi_siq_error *siqe = (struct isi_siq_error *)e;

	/* If errstr , free it */
	if (siqe->errstr)
		free(siqe->errstr);
	
	isi_error_free_impl(e);
}

void
isi_siq_error_format_impl(struct isi_error *e, const char *format,
    va_list ap)
{
	size_t printed;
	int	ret = 0;
	struct isi_siq_error *siqe = (void *)e;
	va_list copy_ap;


	if (format) {
		va_copy(copy_ap, ap);
		ret = vasprintf(&(siqe->errstr), format, copy_ap);
		ASSERT(ret != -1);
		va_end(copy_ap);

		printed = fmt_vprint(&e->fmt, format, ap);
	}
}

ISI_ERROR_CLASS_DEFINE(isi_siq_error, ISI_SIQ_ERROR_CLASS,
    ISI_ERROR_CLASS);


/*
 *	isi_siq_fs_error private functions
 */
void
isi_siq_fs_error_free_impl(struct isi_error *e)
{
	struct isi_siq_fs_error *siqfse = (struct isi_siq_fs_error *)e;

	if (siqfse->dir)
		free(siqfse->dir);

	if (siqfse->filename)
		free(siqfse->filename);

	if (siqfse->evec)
		free(siqfse->evec);

	isi_siq_error_free_impl((struct isi_error *)siqfse);
}

void
isi_siq_fs_error_format_impl(struct isi_error *e, const char *format,
    va_list ap)
{
	struct isi_siq_fs_error *siqfse = (struct isi_siq_fs_error *)e;
	isi_siq_error_format_impl((struct isi_error *)siqfse, format, ap);
	if (format) {
		fmt_print(&e->fmt, "%s%s",": ", strerror(siqfse->error));
	}

}

ISI_ERROR_CLASS_DEFINE(isi_siq_fs_error, ISI_SIQ_FS_ERROR_CLASS,
    ISI_SIQ_ERROR_CLASS);


struct siq_err_map_entry siq_err_map[] = 
{
	{E_SIQ_FILE_TRAN,
	 EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_NONE},

	{E_SIQ_FILE_DATA,
	 EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_NONE},

	{E_SIQ_SELF_PATH_OLAP,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_MAKE_UNRUNNABLE,
	 SIQ_ALERT_SELF_PATH_OVERLAP},

	{E_SIQ_LIC_UNAV,
	 EA_JOB_FAIL | EA_SIQ_REPORT,
	 SIQ_ALERT_LICENSE},

	{E_SIQ_ADS_P_OPEN,
	 EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_NONE},

	{E_SIQ_CONN_TGT,
	 EA_SIQ_REPORT | EA_WRK_FAIL,
	 SIQ_ALERT_TARGET_CONNECTIVITY},

	{E_SIQ_TGT_DIR,
	 EA_SIQ_REPORT | EA_JOB_FAIL,
	 SIQ_ALERT_NONE},

	{E_SIQ_BBD_CHKSUM,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_BBD_CHECKSUM},

	{E_SIQ_WRK_DEATH,
	 EA_JOB_FAIL | EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_GEN_CONF,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_GEN_NET,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_CONF_CONSIST,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_CONF_NOMEM,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_CONF_CONTEXT,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_CONF_PARSE,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_CONF_COMPOSE,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_CONF_INVAL,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_CONF_NOENT,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_CONF_EXIST,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_INV_DIRENT,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_NONE},

	{E_SIQ_DIRENT_HASH_MISS,
	 EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_BBD_CHECKSUM},

	{E_SIQ_TMP_RMDIR,
	 EA_JOB_FAIL | EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_TDB_ERR,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_TDB_WARN,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_LINK_EXCPT,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_UNLINK_ENT,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_LINK_ENT,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_DEL_LIN,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_LIN_UPDT,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_LIN_COMMIT,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_WORK_INIT,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_WORK_SPLIT,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_PPRED_REC,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_CC_DEL_LIN,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_CC_DIR_CHG,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_FLIP_LINS,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_SQL_ERR,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_REPORT_ERR,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_WORK_CLEANUP,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_WORK_SAVE,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_SUMM_TREE,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_STALE_DIRS,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_IDMAP_SEND,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_TGT_CANCEL,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_UPGRADE,
	 EA_SIQ_REPORT,
	 SIQ_ALERT_POLICY_UPGRADE},

	{E_SIQ_TGT_PATH_OLAP,
	 EA_SIQ_REPORT | EA_MAKE_UNRUNNABLE,
	 SIQ_ALERT_TARGET_PATH_OVERLAP},

	{E_SIQ_TGT_ASSOC_BRK,
	 EA_SIQ_REPORT | EA_MAKE_UNRUNNABLE,
	 SIQ_ALERT_TARGET_ASSOC},

	{E_SIQ_TGT_MISMATCH,
	 EA_SIQ_REPORT | EA_MAKE_UNRUNNABLE,
	 SIQ_ALERT_TARGET_ASSOC},

	{E_SIQ_DATABASE_ENTRIES,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_DOMAIN_MARK,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_BACKOFF_RETRY,
	 EA_NONE,
	 SIQ_ALERT_NONE},

	{E_SIQ_VER_UNSUPPORTED,
	 EA_JOB_FAIL | EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_CLOUDPOOLS,
	 EA_JOB_FAIL | EA_SIQ_REPORT,
	 SIQ_ALERT_NONE},

	{E_SIQ_CC_COMP_RESYNC,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_COMP_CONFLICT_CLEANUP,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_COMP_CONFLICT_LIST,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_CT_COMP_DIR_LINKS,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_COMP_COMMIT,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_COMP_MAP_PROCESS,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_MSG_FIELD,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_FS_ERROR,
	 SIQ_ALERT_FS},

	{E_SIQ_COMP_TGTDIR_ENOENT,
	 EA_JOB_FAIL | EA_SIQ_REPORT | EA_MAKE_UNRUNNABLE,
	 SIQ_ALERT_NONE},

	{E_SIQ_TGT_WORK_LOCK,
	 SIQ_ALERT_NONE},
};

static const struct siq_err_map_entry *
siq_err_find_map_entry(int siqerr)
{
	int i;
	struct siq_err_map_entry *entry = NULL; 

	for (i = 0;
	    i < (sizeof(siq_err_map) / sizeof(struct siq_err_map_entry));
	    i++) {
		if (siq_err_map[i].siqerr == siqerr) {
			entry = &siq_err_map[i];
			break;
		}
	}

	return entry;
}

bool
siq_err_has_attr(int siqerr, int attr)
{
	const struct siq_err_map_entry *entry = NULL;

	entry = siq_err_find_map_entry(siqerr);
	ASSERT(entry);

	return !!(entry->attr & attr);
}


enum siq_alert_type
siq_err_get_alert_type(int siqerr)
{
	const struct siq_err_map_entry *entry = NULL;

	entry = siq_err_find_map_entry(siqerr);
	return entry->siq_alert_type;
}
