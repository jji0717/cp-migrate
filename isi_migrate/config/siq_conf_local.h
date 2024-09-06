#ifndef __SIQ_CONF_LOCAL_H__
#define __SIQ_CONF_LOCAL_H__

#include "siq_conf.h"
#include <isi_xml/merge_xml.h>

int siq_conf_coord_perf_load(siq_coord_perf_t *coord_perf,
	struct MXD_Node *perf_node, struct isi_error **error_out);

int siq_src_paths_load(struct siq_policy_path_head *paths_ptr,
	struct MXD_Node *paths_node, struct isi_error **error_out);
int siq_src_paths_save(struct siq_policy_path_head *paths,
	struct MXD_Node *paths_node, struct isi_error **error_out);

#endif /* __SIQ_CONF_LOCAL_H__ */
