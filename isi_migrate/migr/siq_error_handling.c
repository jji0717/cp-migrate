#include "isirep.h"
#define ISI_ERROR_PROTECTED
#include "isi_migrate/siq_error_protected.h"
#include "isi_migrate/siq_error.h"

void
siq_construct_msg(struct generic_msg *mp,
    char *node,
    struct isi_error *e,
    int errloc)
{
	struct siq_err_msg *siqp;
	struct isi_siq_error *siqe = (struct isi_siq_error *)e;
	struct isi_siq_fs_error *siqfse = (struct isi_siq_fs_error *)e;

	ASSERT(mp != NULL);
	ASSERT(node != NULL);
	memset(mp, 0, sizeof(struct generic_msg));
	siqp = &(mp->body.siq_err);
	mp->head.type = SIQ_ERROR_MSG;
	siqp->siqerr = isi_siq_error_get_siqerr(siqe);
	siqp->errstr = isi_siq_error_get_errstr(siqe);
	siqp->errloc = errloc;

	/* Get nodename */
	siqp->nodename=node;

	if (isi_error_is_a(e, ISI_SIQ_FS_ERROR_CLASS)) {
		siqp->error = isi_siq_fs_error_get_unix_error(siqfse);
		siqp->file = isi_siq_fs_error_get_file(siqfse);
		siqp->enc = isi_siq_fs_error_get_enc(siqfse);
		siqp->filelin = isi_siq_fs_error_get_filelin(siqfse);
		siqp->dir = isi_siq_fs_error_get_dir(siqfse);
		siqp->dirlin = isi_siq_fs_error_get_dirlin(siqfse);
		siqp->eveclen = isi_siq_fs_error_get_eveclen(siqfse);
		siqp->evec = isi_siq_fs_error_get_evec(siqfse);
	}
}

void
siq_send_error(int fd, struct isi_siq_error *siq_error, int errloc)
{
	struct generic_msg m = {{0}};
	char node[MAXHOSTNAMELEN];
	struct isi_error *error = (struct isi_error *) siq_error;

	ASSERT(isi_error_is_a(error, ISI_SIQ_ERROR_CLASS));
	ASSERT(isi_siq_error_get_msgsent(siq_error) <= 0);
	gethostname(node, MAXHOSTNAMELEN);
	siq_construct_msg(&m, node, error, errloc);
	msg_send(fd, &m);
	isi_siq_error_set_msgsent(siq_error, true);
}

void
handle_stf_error_fatal(int fd, struct isi_error *error, int siq_err, int errloc,
    uint64_t src_dirlin, uint64_t src_filelin)
{

	char *error_str;
	struct isi_error *src_error = NULL;
	struct fmt FMT_INIT_CLEAN(fmt);
	int tmp_error;


	if (error == NULL)
		return;

	error_str = isi_error_get_detailed_message(error);
	if (isi_error_is_a(error, ISI_SYSTEM_ERROR_CLASS))
		tmp_error = isi_system_error_get_error(
		    (const struct isi_system_error *)error);
	else
		tmp_error = EIO;

	/* Create SyncIQ application error */
	src_error = isi_siq_fs_error_new(siq_err, tmp_error, NULL, src_dirlin,
	    0, NULL, NULL, 0 , src_filelin, "%s", error_str);

	/* Send the error */
	siq_send_error(fd,
	    (struct isi_siq_error *)src_error, errloc);


	fmt_print(&fmt, "%#{}", isi_error_fmt(error));
	free(error_str);

	/* Log the local error */
	log(FATAL, "Error : %s", fmt_string(&fmt));
	fmt_clean(&fmt);
	isi_error_free(src_error);
	isi_error_free(error);
}
