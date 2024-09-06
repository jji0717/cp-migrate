#ifndef __OAPI_SNAPSHOT_H__
#define __OAPI_SNAPSHOT_H__

#include <isi_util_cpp/isi_str_cpp.h>
#include <isi_snapshot/snapshot.h>
#include <isi_snapshot/snapshot_utils.h>
#include <isi_snapshot/snapshot_error.h>
#include <isi_object/iobj_snapshot.h>

class oapi_snapshot {
public:
        oapi_snapshot(std::string &snapshot_name, std::string &store_name,
                std::string &src_path, struct isi_error **error);
        ~oapi_snapshot();

        char            src_path_in_snapshot[MAXPATHLEN + 1];
private:
        ifs_snapid_t    new_snapid; // Valid only if new snapshot is created
        isi_str         s_name;
};

#endif /* __OAPI_SNAPSHOT_H__ */
