#ifndef __SIQ_ERROR_PROTECTEDH__
#define __SIQ_ERROR_PROTECTEDH__
#include <isi_util/enc.h>
#include <isi_util/isi_error_protected.h>

/*
 * 	isi_siq_error 
 */
struct isi_siq_error {
	struct isi_error		super;
	int				siqerr;
	char 				*errstr;
	/* used for debugging purposes */
	bool				msgsent;
};

void isi_siq_error_init(struct isi_siq_error *,
	const struct isi_error_class *, int siqerr,
	const char *function, const char *file, int line, 
	const char *, va_list) __nonnull();

void isi_siq_error_free_impl(struct isi_error *) __nonnull();
void isi_siq_error_format_impl(struct isi_error *, const char *format,
	va_list) __nonnull();


/*
 * 	isi_siq_fs_error is subtype of isi_siq_error
 *	Used for all filesystem errors
 */
struct isi_siq_fs_error {
	struct isi_siq_error		super;
	/* local error which is errno */
	int				error;
	/* directory path */
	char				*dir;
	u_int64_t			dirlin;
	unsigned			eveclen;
	enc_t				*evec;
	/* file */
	char				*filename;
	unsigned int			enc;
	u_int64_t			filelin;
	/* Do we need ADS information, what info ads dir, attr name*/
};

void isi_siq_fs_error_free_impl(struct isi_error *) __nonnull();
void isi_siq_fs_error_format_impl(struct isi_error *, const char *format,
	va_list) __nonnull();

#endif 
