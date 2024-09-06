#ifndef SIQ_BTREE_TYPE_H
#define SIQ_BTREE_TYPE_H

__BEGIN_DECLS

enum siq_btree_type
{
	siq_btree_repstate = 1,
	siq_btree_summ_stf = 2,
	siq_btree_worklist = 3,
	siq_btree_linmap = 4,
	siq_btree_cpools_sync_state = 5,
	siq_btree_lin_list = 6,
	siq_btree_compliance_map = 7,
};

int siq_btree_open_dir(enum siq_btree_type bt, const char *policy,
    struct isi_error **error_out);
void siq_btree_create(enum siq_btree_type bt, const char *policy, char *name,
    bool exist_ok, struct isi_error **error_out);
int siq_btree_open(enum siq_btree_type bt, const char *policy, char *name,
    struct isi_error **error_out);
void siq_btree_remove(enum siq_btree_type bt, const char *policy, char *name,
    bool missing_ok, struct isi_error **error_out);
void siq_btree_prune_old(enum siq_btree_type bt, const char *policy,
    char *name_root, ifs_snapid_t *snap_kept, int num_kept,
    struct isi_error **error_out);
void
siq_safe_btree_rename(enum siq_btree_type bt, const char *src_policy,
    const char *src_name, const char *dest_policy, const char *dest_name,
    struct isi_error **error_out);



static inline const char *siq_btree_printable_type(enum siq_btree_type bt)
{
	switch (bt) {
	case siq_btree_repstate:
		return "repstate";
		break;
	case siq_btree_summ_stf:
		return "summary stf";
		break;
	case siq_btree_worklist:
		return "worklist";
		break;
	case siq_btree_linmap:
		return "linmap";
		break;
	case siq_btree_cpools_sync_state:
		return "cpools sync state";
		break;
	case siq_btree_lin_list:
		return "lin list";
		break;
	case siq_btree_compliance_map:
		return "compliance map";
		break;
	default:
		ASSERT(false, "Bad btree type %d", bt);
	}
}

static inline enum sbt_key_size siq_btree_keysize(enum siq_btree_type bt)
{
	switch (bt) {
	case siq_btree_repstate:
	case siq_btree_summ_stf:
	case siq_btree_linmap:
	case siq_btree_cpools_sync_state:
	case siq_btree_lin_list:
	case siq_btree_compliance_map:
		return SBT_64BIT;

	case siq_btree_worklist:
		return SBT_128BIT;
		break;
	default:
		ASSERT(false, "Bad btree type %d", bt);
		return SBT_64BIT;
	}
}

__END_DECLS

#endif /* SIQ_BTREE_TYPE_H */
