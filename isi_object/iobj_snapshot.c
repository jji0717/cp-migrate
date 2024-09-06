#include <unistd.h>
#include <isi_util/isi_printf.h>
#include <isi_config/array.h>

#include "iobj_snapshot.h"
#include <sys/isi_privilege.h>

#define	TEMP_SNAP_SUFFIX	"temp-"


/**
 * This routine creates a temporary snapshot. The snapshot name is
 * (CONST_SUFFIX + randomly generated guid).
 */

static struct isi_error *
create_temp_snapshot(struct isi_str *s_name_p, const char *store_name,
	const char *src_path, ifs_snapid_t *snap_id_p)
{
	ASSERT(s_name_p && store_name && src_path && snap_id_p);

	struct isi_error	*error = NULL;
	struct isi_str	        s_path;
	struct isi_str          *s_paths[2]   = { &s_path,  NULL };

	char		src_abs_path[MAXPATHLEN + 1];
	char		snapshot_dir[MAXPATHLEN + 1];
	struct stat     path_stat;
	int sfd = -1;

#if 0
	if (isi_licensing_module_status(ISI_LICENSING_SNAPSHOTS) != ISI_LICENSING_LICENSED) {
		error = isi_system_error_new(errno,
			    "Invalid snapshot license");
		goto out;
	}
#endif
	// Use suffix+guid as temporary snapshot name
        // to avoid possible snapshot name collision
	unsigned char guid[GUID_SIZE];
	isi_guid_generate(&guid, GUID_SIZE);
	char temp_snap_name[sizeof(TEMP_SNAP_SUFFIX)+sizeof(guid)+1];

	isi_snprintf(temp_snap_name, sizeof(temp_snap_name), "%s%{}",
		TEMP_SNAP_SUFFIX, arr_guid_fmt(guid));

	isi_str_init(s_name_p, temp_snap_name, sizeof(temp_snap_name),
		ENC_DEFAULT, ISISTR_STR_MALLOC);

	snprintf(src_abs_path, MAXPATHLEN + 1, "/%s%s",
		store_name, src_path);


	if (stat(src_abs_path, &path_stat) < 0) {
		error = isi_system_error_new(errno,
			    "Invalid source path %s", src_abs_path);
		goto out;
	}

	if (S_ISDIR(path_stat.st_mode) == 0) {
                // get the parent directory path
		char *pos = strrchr(src_abs_path, '/');
		int len = pos - src_abs_path + 1;
		ASSERT(len > 0 && len <= strlen(src_abs_path));
		memcpy(snapshot_dir, src_abs_path, len);
		snapshot_dir[len] = '\0';
	} else {
		// Not supported currently
		//memcpy(snapshot_dir, src_abs_path, strlen(src_abs_path)+1);
		error = isi_system_error_new(EISDIR,
			    "source is a directory, not supported");
		goto out;
	}

	isi_str_init(&s_path, snapshot_dir, sizeof(snapshot_dir),
		ENC_DEFAULT, ISI_STR_NO_MALLOC);
	validate_path(ISI_STR_GET_STR(&s_path), &error);

	if (error)
		goto out;
get_snap:
	*snap_id_p = snapshot_create(s_name_p, 0, s_paths, NULL, &error);

	if (error) {
		if (sfd == -1 && isi_system_error_is_a(error, EPERM)) {
			sfd = gettcred(NULL, true);
			if (sfd >= 0 && ipriv_check_cred(sfd, ISI_PRIV_SNAPSHOT,
			    ISI_PRIV_FLAG_NONE) == 0 && reverttcred() == 0) {
				isi_error_free(error);
				error = NULL;
				goto get_snap;
			}
		}
		goto out;
	}

out:
	if (error)
		isi_str_free(s_name_p);

	if (sfd >= 0) {
		settcred(sfd, TCRED_FLAG_THREAD, NULL);
		close(sfd);
	}

	return error;

}

/**
 * Routine to generate source path under snapshot
 */

static void
get_snap_src_path(char *snap_src_path, struct isi_str *s_name_p,
	const char *src_path)
{
	ASSERT(s_name_p && snap_src_path && src_path);
	snprintf(snap_src_path, MAXPATHLEN + 1, "/.snapshot/%s%s",
		ISI_STR_GET_STR(s_name_p), src_path);
}


/*
 * This routine initializes the snapshot name. It creates temporary snapshot
 * if snapshot name is not given in the input.
 */

ifs_snapid_t
iobj_init_clone_snap(char *snapshot_name, struct isi_str *s_name_p,
	char *snap_src_path, const char *store_name,const char *src_path,
	struct isi_error **err_out)
{
        ASSERT(snapshot_name && s_name_p  && snap_src_path && store_name &&
		src_path && *err_out == NULL);

	struct isi_error *error = NULL;
	ifs_snapid_t snapid = INVALID_SNAPID;

        if (strlen(snapshot_name) == 0) {
                // create a snapshot and delete after cloning
		if ((error = create_temp_snapshot(s_name_p, store_name,
					src_path, &snapid)))
			goto out;
        } else {
		isi_str_init(s_name_p, snapshot_name, strlen(snapshot_name)+1,
			ENC_DEFAULT, ISI_STR_NO_MALLOC);
	}
	get_snap_src_path(snap_src_path, s_name_p, src_path);

out:
	isi_error_handle(error, err_out);
	return snapid;
}

/**
 *  This routine deletes snapshot if it is created temporarily for cloning,
 *  and frees memory allocated to snapshot name
 */

struct isi_error *
iobj_free_snapshot(struct isi_str *s_name_p, ifs_snapid_t *snap_id_p)
{
	ASSERT(s_name_p && snap_id_p);
	int sfd = -1;
	struct isi_error *error = NULL;
delete_snap:
	if (*snap_id_p != INVALID_SNAPID)
		snapshot_destroy(s_name_p, &error);

	if (error) {
		if (sfd == -1 && isi_system_error_is_a(error, EPERM)) {
			sfd = gettcred(NULL, true);
			if (sfd >= 0 && ipriv_check_cred(sfd, ISI_PRIV_SNAPSHOT,
			    ISI_PRIV_FLAG_NONE) == 0 && reverttcred() == 0) {
				isi_error_free(error);
				error = NULL;
				goto delete_snap;
			}
		}
		goto out;
	}


out:
	isi_str_free(s_name_p);

	if (sfd >= 0) {
		settcred(sfd, TCRED_FLAG_THREAD, NULL);
		close(sfd);
	}

	return error;
}
