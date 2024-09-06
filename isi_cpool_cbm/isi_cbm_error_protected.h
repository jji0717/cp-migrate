#ifndef __ISI_CBM_ERROR_PROTECTED_H__
#define __ISI_CBM_ERROR_PROTECTED_H__

#ifndef ISI_CBM_ERROR_PROTECTED
#error "Don't include isi_cbm_error_protected.h unless you know what you're "\
  "doing, include isi_cbm_error.h instead."
#endif
#undef ISI_CBM_ERROR_PROTECTED

#define ISI_ERROR_PROTECTED
#include <isi_util/isi_error_protected.h>
#undef ISI_ERROR_PROTECTED

#include "isi_cbm_error.h"

__BEGIN_DECLS

struct isi_cbm_error {
	struct isi_error super_;

	cbm_error_type error_type_;
};

struct isi_error *
_cbm_error_new(const char *function, const char *file, int line,
    cbm_error_type error_type, const char *fmt, ...) __fmt_print_like(5, 6);

#define isi_cbm_error_new(error_type, fmt, args...) \
	_cbm_error_new(__FUNCTION__, __FILE__, __LINE__, \
	    error_type, fmt, ##args)

__END_DECLS

#endif // __ISI_CBM_ERROR_PROTECTED_H__
