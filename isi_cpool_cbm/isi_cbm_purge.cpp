#include <isi_cpool_cbm/isi_cbm_scoped_flock.h>
#include <isi_cpool_cbm/isi_cbm_file.h>

void
isi_cbm_purge(int fd, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct isi_cbm_file *file = NULL;
	scoped_sync_lock sync_lock;

	ASSERT(fd > 0 && !(*error_out));

	file = isi_cbm_file_get(fd, &error);
	ON_ISI_ERROR_GOTO(out, error);

	sync_lock.lock_ex(fd, true, &error);
	ON_ISI_ERROR_GOTO(out, error);

	// purge stub information
	isi_cbm_file_stub_purge(file, true, true, true, &error);

	ON_ISI_ERROR_GOTO(out, error);
 out:
	if (file) {
		struct isi_error *error2 = NULL;

		sync_lock.unlock();

		isi_cbm_file_close(file, &error2);

		if (error2 && error == NULL)
			error = error2;
		else if (error2)
			isi_error_free(error2);
	}

	isi_error_handle(error, error_out);
}
