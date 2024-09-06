#ifndef __SIQ_ERROR_H__
#define __SIQ_ERROR_H__

#include <isi_util/enc.h>
#include <isi_util/isi_error.h>
#include <isi_migrate/config/siq_alert.h>

struct isi_siq_error;
struct isi_siq_fs_error;
struct generic_msg;

ISI_ERROR_CLASS_DECLARE(ISI_SIQ_ERROR_CLASS);

struct isi_error *
_isi_siq_error_new(int siqerr, const char *function, const char *file,
    int line, const char *format, ...) __nonnull();
#define isi_siq_error_new(siqerr, args...) \
	_isi_siq_error_new(siqerr, __FUNCTION__, __FILE__, __LINE__, "" args)


int isi_siq_error_get_siqerr(const struct isi_siq_error *siqe);
char * isi_siq_error_get_errstr(const struct isi_siq_error *siqe);
void isi_siq_error_set_msgsent(struct isi_siq_error *siqe, const bool value);
bool isi_siq_error_get_msgsent(const struct isi_siq_error *siqe);


ISI_ERROR_CLASS_DECLARE(ISI_SIQ_FS_ERROR_CLASS);

struct isi_error *
_isi_siq_fs_error_new(int siqerr, int error,
    const char *dir, u_int64_t dirlin, unsigned eveclen, const enc_t *evec,
    const char *filename, unsigned enc, u_int64_t filelin,
    const char *function, const char *file,
    int line, const char *format, ...); 
#define isi_siq_fs_error_new(siqerr, error, \
		dir, dirlin, eveclen, evec,filename, enc, filelin, \
		args...) \
		_isi_siq_fs_error_new(siqerr, error, \
		dir, dirlin, eveclen, evec, filename, enc, filelin, \
		__FUNCTION__, __FILE__, __LINE__, "" args)


char * isi_siq_fs_error_get_file(const struct isi_siq_fs_error *siqfse);
unsigned int isi_siq_fs_error_get_enc(const struct isi_siq_fs_error *siqfse);
u_int64_t isi_siq_fs_error_get_filelin(const struct isi_siq_fs_error *siqfse);
char * isi_siq_fs_error_get_dir(const struct isi_siq_fs_error *siqfse);
u_int64_t isi_siq_fs_error_get_dirlin(const struct isi_siq_fs_error *siqfse);
unsigned isi_siq_fs_error_get_eveclen(const struct isi_siq_fs_error *siqfse);
enc_t * isi_siq_fs_error_get_evec(const struct isi_siq_fs_error *siqfse);
int isi_siq_fs_error_get_unix_error(const struct isi_siq_fs_error *siqfse);


void siq_send_error(int fd, struct isi_siq_error *e, int errloc);
void siq_construct_msg(struct generic_msg *mp, char *node,
    struct isi_error *e, int errloc);
bool siq_err_has_attr(int siqerr, int attr);
enum siq_alert_type siq_err_get_alert_type(int siqerr);
void handle_stf_error_fatal(int fd, struct isi_error *error, int siq_err,
    int errloc, uint64_t dirlin, uint64_t filelin);


/*
 *	SyncIQ Errors
 * 	
 *	Every SyncIQ failure should be reported in terms of 
 *	SyncIQ errors. We should not utilize errno for 
 *	error communication between workers and coordinator
 * 	
 *	SyncIQ allocated as int
 *
 */
enum siqerrors {
	E_SIQ_NONE = 0,
	E_SIQ_FILE_TRAN,	/* File metadata transfer error */
	E_SIQ_FILE_DATA,	/* File data transfer error */
	E_SIQ_SELF_PATH_OLAP,	/* Source and target paths overlap */
	E_SIQ_LIC_UNAV,		/* SyncIQ license unavailable */
	E_SIQ_ADS_P_OPEN,	/* ADS parent directory open error */
	E_SIQ_CONN_TGT,		/* Connecion failure with target node */
	E_SIQ_TGT_DIR,		/* Failed to establish target directory */
	E_SIQ_BBD_CHKSUM,	/* Block based delta hash mismatch */
	E_SIQ_WRK_DEATH,	/* Multiple workers per sync job */
	E_SIQ_GEN_CONF,		/* Generic configuration error */
	E_SIQ_GEN_NET,		/* Generic network error */
	E_SIQ_CONF_CONSIST,	
	E_SIQ_CONF_NOMEM,
	E_SIQ_CONF_CONTEXT,
	E_SIQ_CONF_PARSE,
	E_SIQ_CONF_COMPOSE,
	E_SIQ_CONF_INVAL,
	E_SIQ_CONF_NOENT,
	E_SIQ_CONF_EXIST,
	E_SIQ_INV_DIRENT,
	E_SIQ_DIRENT_HASH_MISS,
	E_SIQ_TMP_RMDIR,
	E_SIQ_TDB_ERR,
	E_SIQ_TDB_WARN,
	E_SIQ_LINK_EXCPT,	/* Lin link exception */
	E_SIQ_UNLINK_ENT,	/* unlink dirent error */
	E_SIQ_LINK_ENT,		/* link dirent error */
	E_SIQ_DEL_LIN,		/* lin delete error */
	E_SIQ_LIN_UPDT,		/* Lin update error */
	E_SIQ_LIN_COMMIT,	/* lin commit error */
	E_SIQ_WORK_INIT,	/* work initialization error */
	E_SIQ_WORK_SPLIT,	/* Work split error */
	E_SIQ_PPRED_REC,	/* path predicate build error */
	E_SIQ_CC_DEL_LIN,	/* Del lin cc(change compute) error */
	E_SIQ_CC_DIR_CHG,	/* Directory change cc error */
	E_SIQ_FLIP_LINS,	/* lin flips error */
	E_SIQ_SQL_ERR,		/* A sqlite problem */
	E_SIQ_REPORT_ERR,	/* Report error */
	E_SIQ_WORK_CLEANUP,	/* work cleanup errors */
	E_SIQ_WORK_SAVE,	/* work save errors */
	E_SIQ_SUMM_TREE,	/* Summary tree construction error */
	E_SIQ_STALE_DIRS,	/* Stale dirs cleanup */
	E_SIQ_IDMAP_SEND,	/* idmap transmission error */
	E_SIQ_TGT_CANCEL,	/* Target job cencellation */
	E_SIQ_UPGRADE,		/* Policy upgrade error */
	E_SIQ_TGT_PATH_OLAP,	/* 2 policies sync to same target path */
	E_SIQ_TGT_ASSOC_BRK,	/* Target association broken for policy */
	E_SIQ_TGT_MISMATCH,	/* Target id mismatch */
	E_SIQ_SCHED_RUNNING,	/* Already running */
	E_SIQ_DATABASE_ENTRIES,	/* Database entry update/mirror error*/
	E_SIQ_DOMAIN_MARK, 	/* Domain mark treewalk */
	E_SIQ_BACKOFF_RETRY,	/* Backoff delay requires retry */
	E_SIQ_VER_UNSUPPORTED,	/* Riptide: Operation unsupported for
				   current version */
	E_SIQ_CLOUDPOOLS,	/* Riptide: Cloudpools-related error */

	E_SIQ_CC_COMP_RESYNC,	/* Resync list merging error */
	E_SIQ_COMP_CONFLICT_CLEANUP,
				/* Halfpipe: compliance conflict cleanup */
	E_SIQ_COMP_CONFLICT_LIST,
				/* Halfpipe: compliance conflict list */
	E_SIQ_CT_COMP_DIR_LINKS,/* Resync directory change ct error */
	E_SIQ_COMP_COMMIT,	/* Halfpipe: Late stage commit related error */
	E_SIQ_COMP_MAP_PROCESS,	/* Compliance map processing error */
	E_SIQ_MSG_FIELD,	/* Generic message field-related error */

	E_SIQ_COMP_TGTDIR_ENOENT,
				/* Halfpipe: target compliance v2 dir doesn't
				   exist */
	E_SIQ_TGT_WORK_LOCK,	/* Target work item lock error */
	E_SIQ_MAX_ERROR		/* Do not cross MAX_SIQ_ERROR */
};

/*
 *      Error attributes
 *
 * Each error can have its own attributes which can decide the
 * the action taken, or indicate the error is filesystem related.
 */
#define	EA_NONE			0x00	/* No-op */
#define	EA_JOB_FAIL		0x01	/* Fail the job */
#define	EA_WRK_FAIL		0x02	/* Fail the worker */
#define	EA_SIQ_REPORT		0x08	/* Put error into job report */
#define EA_FS_ERROR		0x10	/* Filesystem related error */
#define EA_MAKE_UNRUNNABLE	0x20	/* Make policy unrunnable */


/*
 *	Error location
 */
#define EL_SIQ_SOURCE		1
#define EL_SIQ_DEST		2


/*      SyncIQ error mapping 
 *
 * This will be used to associate siq err with their attributes.
 * This will be used at different places in code to decide 
 * the action to be taken for specific error.
 */
struct siq_err_map_entry
{
	int siqerr;		/* SyncIQ error number */
	uint64_t attr;		/* Error attributes */
	int siq_alert_type;	/* Alert mapping */
};


#endif /* __SIQ_ERROR_H__ */
