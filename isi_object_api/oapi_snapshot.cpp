#include "oapi_snapshot.h"

oapi_snapshot::oapi_snapshot(std::string &snapshot_name, std::string &store_n,
    std::string &src_path, struct isi_error **error)
{
	new_snapid = iobj_init_clone_snap(const_cast<char*> (snapshot_name.c_str()),
	    		&s_name, src_path_in_snapshot, store_n.c_str(),
			src_path.c_str(), error);
}

oapi_snapshot::~oapi_snapshot()
{
        iobj_free_snapshot(&s_name, &new_snapid);
}

