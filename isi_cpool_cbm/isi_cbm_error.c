#include <isi_util/isi_error.h>

#include "isi_cbm_error.h"

#define ISI_CBM_ERROR_PROTECTED
#include "isi_cbm_error_protected.h"
#undef ISI_CBM_ERROR_PROTECTED

struct isi_error *
_cbm_error_new(const char *function, const char *file, int line,
    cbm_error_type error_type, const char *fmt, ...)
{
	ASSERT(error_type != MAX_CBM_ERROR_TYPE);

	struct isi_cbm_error *cbm_error =
	    calloc(1, sizeof(struct isi_cbm_error));
	ASSERT(cbm_error != NULL);

	cbm_error->error_type_ = error_type;

	va_list ap;
	va_start(ap, fmt);
	isi_error_init(&cbm_error->super_, CBM_ERROR_CLASS, function, file,
	    line, fmt, ap);
	va_end(ap);

	return &cbm_error->super_;
}

static void
cbm_error_free_impl(struct isi_error *error)
{
	isi_error_free_impl(error);
}

static void
cbm_error_format_impl(struct isi_error *error, const char *fmt, va_list ap)
{
	ASSERT(isi_error_is_a(error, CBM_ERROR_CLASS));
	const struct isi_cbm_error *cbm_error = (void *)error;

	isi_error_format_impl(error, fmt, ap);

	fmt_print(&error->fmt, ": [error code: %#{}]",
	    cbm_error_type_fmt(cbm_error->error_type_));
}

static void
cbm_error_encode_impl(const struct isi_error *error, struct isi_ostream *os)
{
	ASSERT(isi_error_is_a(error, CBM_ERROR_CLASS));
	const struct isi_cbm_error *cbm_error = (void *)error;

	isi_error_encode_impl(&cbm_error->super_, os);
	isi_ostream_put_int(os, cbm_error->error_type_);
}

static struct isi_error *
cbm_error_decode_impl(struct isi_istream *is)
{
	struct isi_cbm_error *cbm_error =
	    calloc(1, sizeof(struct isi_cbm_error));
	ASSERT(cbm_error != NULL);

	int error_type_as_int;

	if (!_isi_error_decode_impl(&cbm_error->super_, is))
		goto err;
	if (!isi_istream_get_int(is, &error_type_as_int))
		goto err;
	if (error_type_as_int < 0 || error_type_as_int >= MAX_CBM_ERROR_TYPE)
		goto err;

	cbm_error->error_type_ = (cbm_error_type)error_type_as_int;

	return (struct isi_error *)cbm_error;

 err:
	free(cbm_error);

	return NULL;
}

ISI_ERROR_CLASS_DEFINE_ED(cbm_error, CBM_ERROR_CLASS, ISI_ERROR_CLASS);

cbm_error_type
isi_cbm_error_get_type(const struct isi_error *error)
{
	ASSERT(error != NULL);

	if (isi_error_is_a(error, CBM_ERROR_CLASS))
	    return ((const struct isi_cbm_error*)error)->error_type_;

	return -1;
}
