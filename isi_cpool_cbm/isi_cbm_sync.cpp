// 
#include "isi_cbm_sync.h"

#include <boost/utility.hpp>
#include <ifs/ifs_adt.h>
#include <ifs/ifs_types.h>
#include <ifs/ifs_lin_open.h>
#include <isi_ilog/ilog.h>
#include <isi_cloud_common/isi_cpool_version.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_d_common/isi_cloud_object_id.h>
#include <isi_snapshot/snapshot_utils.h>
#include <isi_snapshot/snapshot.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/isi_error.h>
#include <list>
#include <memory>
#include <set>

#include "isi_cbm_account_util.h"
#include "isi_cbm_cache.h"
#include "isi_cbm_cache_iterator.h"
#include "isi_cbm_coi.h"
#include "isi_cbm_data_stream.h"
#include "isi_cbm_error.h"
#include "isi_cbm_error_util.h"
#include "isi_cbm_file.h"
#include "isi_cbm_mapper.h"
#include "isi_cbm_read.h"
#include "isi_cbm_snapshot.h"
#include "isi_cbm_scoped_coid_lock.h"
#include "isi_cbm_scoped_flock.h"
#include "isi_cbm_policyinfo.h"
#include "isi_cbm_scoped_ppi.h"
#include "isi_cbm_write.h"
#include "isi_cbm_stream.h"
#include "isi_cbm_gc.h"
#include "isi_cbm_util.h"
#include "io_helper/isi_cbm_ioh_base.h"
#include "io_helper/isi_cbm_ioh_creator.h"
#include "io_helper/encrypt_ctx.h"

namespace {

// macros for for patterns repeated too often.
/**
 * The sync pre-fail point
 * @param name the name of the fail point
 */
#define SYNC_FAIL_POINT_PRE(name)					\
	UFAIL_POINT_CODE(pre_##name,					\
		error = isi_system_error_new(RETURN_VALUE,		\
		    "Fail point pre_" #name " triggered");		\
		);							\
	ON_ISI_ERROR_GOTO(out, error);

/**
 * The sync post-fail point
 * @param name the name of the fail point
 */
#define SYNC_FAIL_POINT_POS(name)					\
	if (!error) {							\
		UFAIL_POINT_CODE(pos_##name,				\
			error = isi_system_error_new(RETURN_VALUE,	\
			    "Fail point pos_" #name " triggered"););	\
	}

/**
 * A sync fail point to change the file size
 * @param name the name of the fail point
 * @param lin  the lin of the file whose size is being changed
 */
#define SYNC_FAIL_POINT_CHANGE_SIZE(name, lin)				\
	UFAIL_POINT_CODE(name,						\
	{								\
		size_t sz = RETURN_VALUE;				\
		isi_cbm_file_truncate(lin, HEAD_SNAPID, sz, &error);	\
		ASSERT(!error, "truncate failed lin %lu", lin);		\
	})

/**
 * Pre log action macro
 * @param action: the action of the log
 * @param index: the index of the object
 * @param error: the error
 * @param file: the cbm file
 */
#define SYNC_LOG_PRE(action, index, error, file)			\
	log.pre_log_##action(index, log_context, &(error));		\
	ilog(IL_DEBUG, "pre_"#action" idx: %d for %{}, error: %#{}",	\
	    index, isi_cbm_file_fmt(file), isi_error_fmt(error));	\
	if ((error)) {							\
		isi_error_add_context((error),				\
		    "Failed to pre_log_"#action"for %{}. Index: %d",	\
		    isi_cbm_file_fmt(file), index);			\
		goto out;						\
	}

/**
 * post log action macro
 * @param action: the action of the log
 * @param index: the index of the object
 * @param error: the error
 * @param file: the cbm file
 */
#define SYNC_LOG_POS(action, index, error, file)			\
	ilog(IL_DEBUG, "pos_"#action" idx: %d for %{}, error: %#{}",	\
	    index, isi_cbm_file_fmt(file), isi_error_fmt(error));	\
	if ((error)) {							\
		log.pos_log_##action(index, log_context,		\
		    get_sync_error((error)),				\
		    isi_error_suppress());				\
	} else {							\
		log.pos_log_##action(index, log_context, 0,		\
		    &(error));						\
	}								\
	if ((error))							\
		goto out;

/**
 * Module for doing write-back
 */

enum sync_op {
	SO_INVALID = -1,
	SO_MOD = 0,
	SO_NEW = 1,
	SO_DEL = 2,
	SO_SNAP = 3,
	SO_CLONE = 4,
	SO_MOVE = 5,
	SO_DONE = 6, // signaling the maximum operation has been done.
};

class stub_syncer;
class master_cdo_syncer;
class cow_helper;

// class describing a CDO being synchronized
class cdo_syncer : boost::noncopyable {
public:
	cdo_syncer(stub_syncer *syncer, cow_helper *cower) :
	    syncer_(syncer), cower_(cower)
	{
		clear();
	}

	/**
	 * initialize/re-initialize the CDO synchronizer.
	 * This function hold a direct reference to the mapping entry. It is an
	 * error to delete the map_entry while this object is in existence.
	 */
	void init(master_cdo_syncer &master_cdo_syncer,
	    const isi_cfm_mapentry &map_entry, off_t chunksize, int index,
	    off_t length, sync_op sop);

	/**
	 * add range to write
	 */
	void add_range(isi_cbm_cache_record_info &record);

	/**
	 * fill the cache if necessary for modified CDO
	 */
	void fetch_uncached(struct isi_error **error_out);


	/**
	 * get the sync operation
	 */
	sync_op sop() { return sync_op_; }

	/**
	 * clear the state of the cdo_syncer
	 */
	void clear();

	/**
	 * check if the CDO is initialized. To ease the operation
	 * using the object on stack.
	 */
	bool is_initialized() { return initialized_; }

	int index() { return index_; }

	/**
	 * perform the cdo synchronization
	 */
	void do_sync(struct isi_error **error_out);

private:

	/**
	 * Modify or create a CDO this syncer is assigned
	 */
	void modify_or_create_cdo(struct isi_error **error_out);

	/**
	 * Remove a CDO this syncer is assigned.
	 */
	void remove_cdo(struct isi_error **error_out);

private:
	stub_syncer *syncer_;

	/**
	 * pointer to the cloud cower
	 */
	cow_helper *cower_;

	master_cdo_syncer *master_cdo_syncer_;
	isi_cfm_mapentry map_entry_; // the mapping entry
	off_t chunksize_; // the chunk size
	std::list<isi_cbm_cache_record_info> ranges_; // modified ranges
	off_t length_;
	int index_;
	sync_op sync_op_;
	bool initialized_;
};

/**
 * Synchronizer for the master CDO
 */
class master_cdo_syncer : boost::noncopyable {

public:

	/**
	 * instantiate a cdo synchronizer
	 */
	master_cdo_syncer(stub_syncer *syncer, cow_helper *cower) :
	    syncer_(syncer), cower_(cower)
	{
		clear();
	}

	/**
	 * d-tor
	 */
	~master_cdo_syncer() { clear(); syncer_ = NULL; }


	/**
	 * perform master cdo synchronization
	 */
	void do_sync(struct isi_error **error_out);

	/**
	 * perform cdo synchronization
	 */
	void sync_sub_cdo(cdo_syncer &cdo, struct isi_error **error_out);

	/**
	 * check if the CDO is initialized. To ease the operation
	 * using the object on stack.
	 */
	bool is_initialized() { return initialized_; }

	/**
	 * initialize/re-initialize the CDO synchronizer.
	 * This function holds a direct reference to the mapping entry. It is
	 * an error to delete the map_entry while this object in existence.
	 */
	void init(isi_cpool_master_cdo &master_cdo,
	    const isi_cfm_mapentry &map_entry,
	    sync_op sop, bool master_created,
	    struct isi_error **error_out);

	/**
	 * clear the state of the cdo_syncer
	 */
	void clear();

	/**
	 * get the sync operation
	 */
	sync_op sop() const { return sync_op_; }

	/**
	 * get the master cdo_syncer
	 */
	const isi_cpool_master_cdo &get_master_cdo() const
	{
		return master_cdo_;
	}

	/**
	 * Get the IO helper for this master CDO
	 */
	isi_cbm_ioh_base_sptr get_io_helper(struct isi_error **error_out);

	/**
	 * Get the mapping entry for the master CDO
	 */
	const isi_cfm_mapentry &get_map_entry() const { return map_entry_; }

	/**
	 * get the modifiable reference of the map_entry
	 */
	isi_cfm_mapentry &map_entry() { return map_entry_; }

	/**
	 * remove the CDOs in the master CDO from the set offset to the end
	 * @param offset[in]: the logical offset of the range to be truncated
	 *	  in the file
	 */
	void remove_cdos(off_t offset, struct isi_error **error_out);

	/**
	 * update the map_entry.
	 * During a sync process, the entry for this master CDO can be
	 * changed (for example, extended). We need to update it
	 */
	void update_map_entry(const isi_cfm_mapentry &mapentry) {
		map_entry_ = mapentry;
	}
private:

	/**
	 * Remove this master CDO
	 */
	void remove_master_cdo(struct isi_error **error_out);

	/**
	 * Truncate the master CDO, data with offset >= the given offset
	 * is truncated.
	 * @param offset[in]: the logical offset of the range to be truncated
	 *	  in the file
	 */
	void truncate(uint64_t offset, struct isi_error **error_out);

private:

	/**
	 * pointer to the syncer for the stub
	 */
	stub_syncer *syncer_;

	/**
	 * pointer to the cloud cower
	 */
	cow_helper *cower_;

	/**
	 * Indicate if the syncer has been initialized
	 */
	bool initialized_;

	/**
	 * Sync operation type
	 */
	sync_op sync_op_;

	/**
	 * The master CDO this syncer is assigned to work on
	 */
	isi_cpool_master_cdo master_cdo_;

	/**
	 * The mapping entry for the master CDO this syncer is working on
	 */
	isi_cfm_mapentry map_entry_;

	/**
	 * indicate if the master has been created, used in the case
	 * sync_op_ is SO_NEW
	 */
	bool master_created_;

	/**
	 * indicate if the master_cdo has been modified
	 */

	bool modified_;
};

/**
 * class encapsulating the cloud COWing aspect
 */
class cow_helper : boost::noncopyable {
public:
	/**
	 * Instantiate a cloud cower
	 */
	explicit cow_helper(stub_syncer *syncer) : syncer_(syncer),
	    referenced_(false), initialized_(false) { }

	/**
	 * Get the cow name
	 */

	const cl_object_name *get_cow_name(struct isi_error **error_out);

	/**
	 * check if the cower is initialized
	 */
	bool is_initialized() const { return initialized_; }

	/**
	 * initiate the cloud cowing.
	 * and populate the entry with the new cloud COWING name
	 */
	void initiate_cloud_cow(struct isi_error **error_out);

	/**
	 * Clone a master_cdo to the target. The target will have the
	 * the cloned names. This only sets up the target master_cdo object
	 * in memory but does not write to the cloud yet.
	 */
	void clone_master_cdo_for_cow(const isi_cpool_master_cdo &src,
	    isi_cpool_master_cdo &tgt, struct isi_error **error_out);

	/**
	 * mark cloned
	 */
	void mark_cdo_cloned(int index, struct isi_error **error_out);

	/**
	 * mark the end of write of the master CDO
	 * @param master_cdo[in] the offset of the master CDO
	 */
	void mark_master_cdo_snapped(off_t offset);

	/**
	 * Do COWing
	 * This function handles all the aftermath of COWing of the
	 * modified objects. It can either cow the rest of the unmodified
	 * objects or undo the COWing of the modified objects.
	 */
	void cow_cdos(int start_index, struct isi_error **error_out);

	/**
	 * Do cowing if necessary for cmo
	 */
	void update_or_cow_cmo(sync_op start_op,
	    struct isi_error **error_out);

	bool is_referenced() const { return referenced_; }

	/**
	 * Mark the original object as referenced
	 */
	void set_referenced(bool referenced) { referenced_ = referenced; }

private:

	/**
	 * private structure furnishing COWing of objects represented by
	 * a mapping entry.
	 */
	struct cow_context {

		/**
		 * The original mapping info.
		 */
		const isi_cfm_mapinfo &mapinfo;

		/**
		 * The CBM file
		 */
		struct isi_cbm_file *file;

		/**
		 * the file size when we peek at it at the start of a
		 * synchronization
		 */
		off_t filesize;

		/**
		 * the chunk size
		 */
		off_t chunksize;

		/**
		 * the mapping entry under COW
		 */
		const isi_cfm_mapentry &map_entry;

		/**
		 * The IO helper for this mapping entry. The source and target
		 * for the COW is in one account and hence one IOH
		 */
		isi_cbm_ioh_base_sptr ioh;

		/**
		 * The new mapping information
		 */
		isi_cfm_mapinfo &wip_mapinfo;

		/**
		 * The latest CMO
		 */
		const isi_cfm_mapinfo &latest_cmo;

		/**
		 * start index - used for resuming
		 */
		int start_index;
	};

	/** Check if the cloud object the stub currently references
	 * is used by other lins
	 */
	bool used_by_other_lins(uint64_t lin, const isi_cfm_mapinfo *mapinfo,
	    struct isi_error **error_out);

	/**
	 * Check if the cloud object is also used by other snaps of this
	 * LIN
	 */
	bool used_by_other_snaps(const isi_cfm_mapinfo *mapinfo,
	    struct isi_error **error_out);

	/**
	 * Check if the cloud object has been marked as backed up
	 */
	bool used_by_backups(uint64_t lin, const isi_cfm_mapinfo *mapinfo,
	    struct isi_error **error_out);

	/**
	 * COW the rest of the objects
	 * @param start_index[in] the start index for unmodified objects
	 */
	void cow_unmodified_objects(int start_index,
	    const isi_cfm_mapinfo &latest_cmo,
	    struct isi_error **error_out);

	/**
	 * rename the objects COWed for the modifed
	 * @param start_index[in] the start index for modified objects
	 */
	void move_modified_objects(int start_index,
	    struct isi_error **error_out);

	/**
	 * check if the specified CDO has been COWed
	 * @param index[in] the logical index of the CDO.
	 */
	bool is_cowed(int index, struct isi_error **error_out);

	/**
	 * clone unmodified objects
	 */
	void clone_unmodified_objects(cow_context &cow_arg,
	    struct isi_error **error_out);

	/**
	 * COW the CMO
	 * @param start_op[in]: the start operation (invalid, mod, snap)
	 */
	void cow_cmo(sync_op start_op, struct isi_error **error_out);

	/**
	 * Update the CMO. This updates the same CMO
	 * @param start_op[in]: the start operation (invalid, mod, snap)
	 */
	void update_cmo(sync_op start_op, isi_cbm_ioh_base *ioh,
	    struct isi_error **error_out);

	/**
	 * clone CMO. This write the CMO to another object
	 * @param start_op[in]: the start operation (invalid, mod, snap)
	 */
	void clone_cmo(sync_op start_op, isi_cbm_ioh_base *ioh,
	    struct isi_error **error_out);

private:
	/**
	 * The stub synchronizer
	 */
	stub_syncer *syncer_;

	/**
	 * The COWed name
	 */
	cl_object_name cow_name_;

	/**
	 * set of CDOs written to the destination
	 * TBD-must: This need to be persisted to the disk when we have the
	 * checking point with the sync_log.
	 */
	std::set<int> cloned_cdos_;

	/**
	 * set of cloned master CDO. They keys are the offset of the master
	 * CDOs.
	 */
	std::set<off_t> cloned_masters_;

	/**
	 * indicate if the cloud object is also referenced by others
	 */
	bool referenced_;
	/**
	 * indicate if this cower has been initialized
	 */
	bool initialized_;
};

/**
 * TBD-must fix this once isi_cbm_error is available.
 */
int
get_sync_error(isi_error *error)
{
	if (!error)
		return 0;

	if (isi_error_is_a(error, ISI_SYSTEM_ERROR_CLASS))
		return isi_error_to_int(error);

	return -1; // unknown error type
}

/**
 * A context intialized when written to the log
 */
class sync_log_context {
	off_t offset;

friend class sync_log;
};

/**
 * The synchronization stages.
 * They are sequential.
 */
enum sync_stage {
	SS_INVALID = -1,
	SS_SYNC_MODIFIED = 0,
	SS_COW_CDOS = 1,
	SS_UPDATE_CMO = 2,
	SS_PERSIST_MAP = 3,
	SS_TRUNCATION = 4,
	SS_SYNC_START = SS_SYNC_MODIFIED,
	SS_MAX_STAGES = SS_TRUNCATION,
};

enum sync_stage_status {
	SSS_INVALID = 0,
	SSS_STARTED = 1, // the stage has started
	SSS_COMPLETED = 2, // the stage has successfully completed
	SSS_FAILED = 3, // the stage has failed
};

/**
 * The sync state summary record.
 */
struct sync_state_record {
	/**
	 * The current stage
	 */
	sync_stage stage_ : 4;
	/**
	 * The stage status
	 */
	sync_stage_status status_ : 4;
	/**
	 * The error code, if failed
	 */
	int error_code_;
}__packed;

/**
 * The cow reference state
 */
enum sync_ref_state {
	SRS_UNKNOWN = 0, // has not check yet
	SRS_REF = 1, // referenced
	SRS_UNREF = 2 // not referenced
};

const unsigned int SYNC_MAGIC = 0x53594E43; // 'sync'

/**
 * The object ID for cowing
 */
struct sync_cow_name_id {
	uint64_t high_;
	uint64_t low_;
}__packed;

/**
 * The sync log header
 */
struct sync_log_header {
	/**
	 * the magic number of the sync log
	 */
	unsigned int magic_;
	/**
	 * the version number for the sync log format
	 */
	unsigned int version_;

	/**
	 * The stats we took a snap of
	 */
	struct stat stat_;

	/**
	 * The file rev snapshot
	 */
	uint64_t filerev_;

	/**
	 * the valid length of the sync log
	 */
	size_t length_;

	/**
	 * indicate if there is any modification
	 */
	bool modified_ : 4;

	/**
	 * syncer id
	 */
	uint64_t syncer_id_;

	/**
	 * The state record
	 */
	sync_state_record ss_record_;

	/**
	 * The cow ID
	 */
	sync_cow_name_id cow_id_;

	/**
	 * the reference state
	 */
	sync_ref_state ref_state_:4;

	/**
	 * section offsets
	 */
	off_t section_offsets_[SS_MAX_STAGES + 1];
}__packed;

/**
 * header for the modification section
 */
struct sync_section_header {
	/**
	 * The stage this section signals
	 */
	sync_stage stage_;
	/**
	 * The record count
	 */
	int record_cnt_;

	/**
	 * The record size
	 */
	size_t record_size_;
}__packed;

enum sync_object_type {
	SOT_INVALID = 0,
	SOT_MASTER_CDO = 1,
	SOT_CDO = 2,
	SOT_CMO = 3,
	SOT_MAP = 4,
};

/**
 * sync modification record for a cloud object
 */
struct sync_record {
	/**
	 * the logical index of the object.
	 * in case of MCDO, it is the map entry index. For CDO it is the
	 * CDO logical index -- starting from 0.
	 */
	int idx_;
	/**
	 * The operation type
	 */
	sync_op op_ : 8;

	/**
	 * The object type
	 */
	sync_object_type sot_ : 8;

	/**
	 * the status of this mod record
	 */
	sync_stage_status status_ : 8;

	/**
	 * error code if any
	 */
	int error_code_;

	sync_record() : idx_(0), op_(SO_INVALID), sot_(SOT_INVALID),
	    status_(SSS_INVALID), error_code_(0) {}

	sync_record(int idx, sync_op op, sync_object_type sot,
	    sync_stage_status status, int errcode)
	    : idx_(idx), op_(op), sot_(sot),
	    status_(status), error_code_(errcode) {}

}__packed;

const size_t SYNC_HEADER_SIZE = sizeof(struct sync_log_header);
const size_t SYNC_MAPINFO_SIZE = 8192; // 8k of mapping info always
const size_t SYNC_SECTION_HEADER_SIZE = sizeof(sync_section_header);
const size_t SYNC_REC_SIZE = sizeof(sync_record);
const off_t SYNC_HEADER_OFFSET = 0;
const off_t SYNC_MAPINFO_OFFSET = SYNC_HEADER_OFFSET + SYNC_HEADER_SIZE;
const off_t SYNC_MOD_HEADER_OFFSET = SYNC_MAPINFO_OFFSET + SYNC_MAPINFO_SIZE;
const off_t SYNC_MOD_REC_OFFSET = SYNC_MOD_HEADER_OFFSET +
    SYNC_SECTION_HEADER_SIZE;

/**
 * class encapsulating the actions taken for file during synchronization
 */
class sync_log {

public:
	sync_log(stub_syncer *syncer,
	    get_ckpt_data_func_t get_ckpt_cb,
	    set_ckpt_data_func_t set_ckpt_cb,
	    void *ckpt_ctx)
	    : syncer_(syncer), modified_(false), get_ckpt_cb_(get_ckpt_cb)
	    , set_ckpt_cb_(set_ckpt_cb), ckpt_ctx_(ckpt_ctx)
	    , header_written_(false), mapping_written_(false)
	    , log_loaded_(false)
	{
		memset(&log_header_, 0, sizeof(log_header_));
		log_header_.ss_record_.stage_ = SS_INVALID;
		for (int ss = SS_SYNC_START; ss <= SS_MAX_STAGES; ++ss) {
			header_loaded_[ss] = false;
			sync_headers_[ss].record_cnt_ = 0;
			sync_headers_[ss].record_size_ = SYNC_REC_SIZE;
			sync_headers_[ss].stage_ = (sync_stage)ss;
		}
	}

	/**
	 * Log the action taken to the CDO.
	 */
	void log(cdo_syncer &cdo);

	bool get_modified() const { return modified_; }

	/**
	 * Load the sync log from the checkpointing system
	 * @param error_out[out] error if any
	 * @return true if log loaded, false if no log found
	 */
	bool load_log(isi_error **error_out);

	void log_stage_completion(sync_stage stage, isi_error *&error);

	/**
	 * set the stage status
	 */
	void set_state_record(sync_state_record &ss_record,
	    isi_error **error_out);

	/**
	 * get the sync stage from the log
	 * Pre-condition, the log must have been successfully loaded
	 */
	void get_ss_record(sync_state_record &ss_record) const
	{
		ss_record = log_header_.ss_record_;
	}

	/**
	 * Get the current stage
	 */
	sync_stage get_current_stage() const
	{
		return log_header_.ss_record_.stage_;
	}

	/**
	 * set mapping information
	 */
	void set_mapping_info(const isi_cfm_mapinfo &mapinfo,
	    isi_error **error_out);

	/**
	 * get mapping information
	 */
	bool get_mapping_info(isi_cfm_mapinfo &mapinfo,
	    isi_error **error_out);

	/**
	 * pre-log the creation of the master_cdo
	 */
	void pre_log_create_master(int index,
	    sync_log_context &context, isi_error **error_out);

	/**
	 * post-log the creation of the master_cdo
	 */
	void pos_log_create_master(int index,
	    const sync_log_context &context, int errcode,
	    isi_error **error_out);

	/**
	 * pre-log the creation of the cdo
	 */
	void pre_log_create_cdo(int index,
	    sync_log_context &context, isi_error **error_out);

	/**
	 * post-log the creation of the cdo
	 */
	void pos_log_create_cdo(int index,
	    const sync_log_context &context, int errcode,
	    isi_error **error_out);

	/**
	 * pre-log the update of the cdo.
	 */
	void pre_log_update_cdo(int index, sync_log_context &context,
	    isi_error **error_out);

	/**
	 * post-log the update of the cdo.
	 */
	void pos_log_update_cdo(int index,
	    const sync_log_context &context, int errcode,
	    isi_error **error_out);

	/**
	 * pre-log the move of the cdo.
	 */
	void pre_log_move_cdo(int index, sync_log_context &context,
	    isi_error **error_out);

	/**
	 * post-log the move of the cdo.
	 */
	void pos_log_move_cdo(int index,
	    const sync_log_context &context, int errcode,
	    isi_error **error_out);

	/**
	 * pre-log the remove of the cdo.
	 */
	void pre_log_remove_cdo(int index, sync_log_context &context,
	    isi_error **error_out);

	/**
	 * post-log the remove of the cdo.
	 */
	void pos_log_remove_cdo(int index,
	    const sync_log_context &context, int errcode,
	    isi_error **error_out);

	/**
	 * pre-log the clone of the cdo.
	 */
	void pre_log_clone_cdo(int index, sync_log_context &context,
	    isi_error **error_out);

	/**
	 * post-log the clone of the cdo.
	 */
	void pos_log_clone_cdo(int index,
	    const sync_log_context &context, int errcode,
	    isi_error **error_out);

	/**
	 * pre-log the update of the cmo.
	 */
	void pre_log_update_cmo(int index, sync_log_context &context,
	    isi_error **error_out);

	/**
	 * post-log the update of the cmo.
	 */
	void pos_log_update_cmo(int index,
	    const sync_log_context &context, int errcode,
	    isi_error **error_out);

	/**
	 * pre-log the clone of the cmo.
	 */
	void pre_log_clone_cmo(int index, sync_log_context &context,
	    isi_error **error_out);

	/**
	 * post-log the clone of the cmo.
	 */
	void pos_log_clone_cmo(int index,
	    const sync_log_context &context, int errcode,
	    isi_error **error_out);


	// Log the cloud snapid being replaced before doing so
	void pre_log_replace_map(uint64_t old_csnapid,
	    sync_log_context &context, isi_error **error_out);

	/**
	 * load mod header
	 */
	bool get_mod_header(sync_section_header &mod_header,
	    sync_record &last_mod_rec, isi_error **error_out);

	/**
	 * load cow header
	 */
	bool get_cow_header(sync_section_header &mod_header,
	    sync_record &last_cow_rec,
	    isi_error **error_out);

	/**
	 * load cmo header
	 */
	bool get_cmo_header(sync_section_header &cmo_header,
	    sync_record &last_cmo_rec,
	    isi_error **error_out);

	/**
	 * load trunc header
	 */
	bool get_trunc_header(sync_section_header &trunc_header,
	    sync_record &last_trunc_rec,
	    isi_error **error_out);

	/**
	 * load persist header
	 */
	bool get_persist_header(sync_section_header &header,
	    sync_record &record, isi_error **error_out);

	/**
	 * Get the file rev info from the log.
	 * Pre-condition, the log must have been successfully loaded
	 */
	uint64_t get_filerev() const { return log_header_.filerev_; }

	/**
	 * Get the stats info from the log.
	 * Pre-condition, the log must have been successfully loaded
	 */
	struct stat get_stats() const { return log_header_.stat_; }

	/**
	 * Get the syncer id from the log.
	 * Pre-condition, the log must have been successfully loaded
	 */
	uint64_t get_syncer_id() const { return log_header_.syncer_id_; }

	/**
	 * check if we need to perform the stage
	 */
	bool need_to_do_stage(sync_stage stage);

	/**
	 * check if log header has been written
	 */
	bool is_header_written() const { return header_written_; }

	/**
	 * check if mapping info has been written
	 */
	bool is_mapping_written() const { return mapping_written_; }

	/**
	 * create and persist the log header
	 */
	void create_log_header(const struct stat &stat,
	    uint64_t filerev_, uint64_t syncer_id,
	    const sync_state_record &ss_record,
	    isi_error **error_out);

	/**
	 * update the log header
	 */
	void write_log_header(isi_error **error_out);

	/**
	 * log the mapping info
	 */
	void log_mapinfo(const isi_cfm_mapinfo &mapinfo,
	    isi_error **error_out);

	/**
	 * intialize the log header. Used in creating a new log.
	 * This does not commit the header change to the disk
	 */
	void initialize_log_header(const struct stat &stat,
	    uint64_t filerev, uint64_t syncer_id,
	    const sync_state_record &ss_record);

	/**
	 * check if a log has been loaded
	 */
	bool is_log_loaded() const { return log_loaded_; }

	/**
	 * load the mod record for the specified record index
	 */
	void load_mod_record(int index, sync_record &mod_record,
	    isi_error **error_out);

	/**
	 * load the cow record for the specified record index
	 */
	void load_cow_record(int index, sync_record &mod_record,
	    isi_error **error_out);

	/**
	 * set the cow info
	 */
	void set_cow_info(sync_cow_name_id &cow_id);

	/**
	 * get the cow info
	 */
	const sync_cow_name_id &get_cow_info() const
	{
		return log_header_.cow_id_;
	}

	/**
	 * conditionally load clone information from the log
	 * This informatoin is loaded only when the cow stage is
	 * not completed
	 */
	void load_clone_info_cond(cow_helper &cower,
	    isi_error **error_out);

	/**
	 * get the ref state
	 */
	sync_ref_state get_ref_state() { return log_header_.ref_state_; }

	/**
	 * set the ref state
	 */
	void set_ref_state(sync_ref_state ref_state)
	{
		log_header_.ref_state_ = ref_state;
	}

	void set_modified() { modified_ = true; }

	bool log_supported() const { return set_ckpt_cb_ != NULL; }

private:

	stub_syncer *syncer_;
	bool modified_;
	get_ckpt_data_func_t get_ckpt_cb_;
	set_ckpt_data_func_t set_ckpt_cb_;
	void *ckpt_ctx_;
	sync_log_header log_header_;
	bool header_written_;
	bool mapping_written_;
	bool log_loaded_;

	/**
	 * Indicate if the header has been loaded
	 */
	bool header_loaded_[SS_MAX_STAGES + 1];

	/**
	 * sync stage header records
	 */
	sync_section_header sync_headers_[SS_MAX_STAGES + 1];

	/**
	 * last sync_record for each section as a cache
	 */
	sync_record sync_records_[SS_MAX_STAGES + 1];

	/**
	 * common pre-log sync record
	 */
	void pre_log_sync_record(sync_stage stage,
	    int index, sync_op sop, sync_object_type sot,
	    sync_log_context &context, isi_error **error_out);

	/**
	 * common post-log sync-record
	 */
	void pos_log_sync_record(int index, sync_op sop,
	    sync_object_type sot, const sync_log_context &context,
	    int errcode, isi_error **error_out);

	/**
	 * Get the section header for the given stage
	 */
	bool get_section_header(sync_stage stage,
	    sync_section_header &header,
	    sync_record &last_rec,
	    isi_error **error_out);

	/**
	 * load the sync record for the given stage and index
	 */
	void load_sync_record(sync_stage stage, int index,
	    sync_record &record, isi_error **error_out);
};

/**
 * a class encapsulating the operation to synchronize a stub file to the
 * cloud storage.
 */
class stub_syncer : boost::noncopyable {
public:

	/**
	 * c-tor to construct a synchronizer for a stub
	 */
	stub_syncer(uint64_t lin, uint64_t snapid,
	    get_ckpt_data_func_t get_ckpt_cb,
	    set_ckpt_data_func_t set_ckpt_cb,
	    void *ctx, isi_cbm_sync_option &opt);

	~stub_syncer();
	/**
	 * perform the synchronization
	 */
	void do_sync(struct isi_error **error_out);

	/**
	 * perform the ckpt rollback
	 */
	void do_rollback(struct isi_error **error_out);


	/**
	 * get the CBM file for the stub being synced
	 */
	struct isi_cbm_file *file() { return file_; }

	/**
	 * get the map info for the stub being synced
	 */
	const isi_cfm_mapinfo &mapinfo() { return mapinfo_; }

	/**
	 * get the work-in-progress map info
	 */
	isi_cfm_mapinfo &wip_mapinfo() { return wip_mapinfo_; }

	off_t filesize() const { return stat_.st_size; }

	uint64_t filerev() const { return filerev_; }

	const struct stat &stat() const { return stat_; }

	/**
	 * get the CMO iohelper
	 */
	isi_cbm_ioh_base_sptr get_cmo_io_helper(struct isi_error **error_out);

	/**
	 * get the COI
	 */
	coi &get_coi() { return *(coi_.get()); }

	/**
	 * get the sync log
	 */
	sync_log &get_log() { return log_; }


	/**
	 * check if it is a resume operation
	 */
	bool is_resume() const { return is_resume_; }

	/**
	 * Check if the source CMO being synced is the latest
	 */
	bool is_latest() { return is_latest_; }

	/**
	 * Get the latest version object Id
	 */
	const isi_cloud_object_id &get_last_ver() { return last_ver_; }

	/**
	 * Get the COI entry for the latest version. The pointer ownership
	 * still belong to this class. The caller shall not free it.
	 */
	coi_entry *get_last_entry() { return last_entry_.get(); }

	uint64_t get_lin() const { return lin_; }
	uint64_t get_snapid() const { return snapid_; }

	// private methods:
private:

	/**
	 * routine to help sync mod to get the resumption context
	 */
	void get_sync_mod_resume_context(sync_log &log, off_t &initial_offset,
	    isi_cpool_master_cdo &master_cdo, master_cdo_syncer &master,
	    cdo_syncer &syncer, struct isi_error **error_out);

	/**
	 * internal routine to sync the modified regions
	 */
	void sync_modified_regions(struct isi_error **error_out);

        /**
         * mark cache region as ISI_CPOOL_CACHE_INPROGRESS
         */
        void mark_in_progress(size_t region, bool error_on_clean,
            struct isi_error **error_out);

	/**
	 * get the resumption context for cow CDOs operation
	 */
	void get_cow_cdo_resume_context(sync_log &log, int &start_index,
	     struct isi_error **error_out);

	/**
	 * internal routine to cow regions
	 */
	void handle_cow_cdos(struct isi_error **error_out);


	/**
	 * get the resumption context for cow CDOs operation
	 */
	void get_update_cmo_resume_context(sync_log &log, sync_op &start_op,
	     struct isi_error **error_out);

	/**
	 * internal routine to cow regions
	 */
	void handle_update_cmo(struct isi_error **error_out);

	/**
	 * get the resumption context for truncation operation
	 */
	void get_truncation_resume_context(sync_log &log, int &start_index,
	     struct isi_error **error_out);

	/**
	 * Load check point information from log
	 * @param error_out[out] error if any
	 */
	void load_ckpt_info(struct isi_error **error_out);

	/**
	 * establish sync environment either from the log or the file
	 * directly.
	 *
	 */
	void setup_sync_context(struct isi_error **error_out);
	/**
	 * Internal synchronization logic.
	 */
	void sync_internal(struct isi_error **error_out);

	/**
	 * persist the mapping info
	 */
	void persist_mapping_info(struct isi_error **error_out);

	/**
	 * garbage collect the CMO
	 */
	void collect_cloud_objects(const isi_cloud_object_id &obj,
	    struct isi_error **error_out);

	/**
	 * mark cache regions
	 */
	void mark_synced_cache_regions(struct isi_error **error_out);

	void allocate_new_map_entry(
	    isi_cfm_mapinfo &mapinfo,
	    isi_cpool_master_cdo &new_master_cdo,
	    isi_cfm_mapinfo::iterator &it,
	    const isi_cfm_mapentry *last_entry,
	    off_t offset, off_t len,
	    struct isi_error **error_out);


	void get_or_create_map_entry(
	    isi_cfm_mapinfo &mapinfo,
	    isi_cfm_mapentry &current_entry,
	    isi_cfm_mapinfo::iterator &it,
	    isi_cpool_master_cdo &new_master_cdo,
	    int index, size_t chunk_len, sync_op sop,
	    bool &created,
	    struct isi_error **error_out);

	/**
	 * This marks the sync is in progress in the cache header
	 */
	void mark_sync_in_progress(isi_error **error_out);

	/**
	 * This marks the completetion in the cache header
	 */
	void mark_sync_done_progress(isi_error **error_out);
	/**
	Mark cache state when wb fails as cluster is not part of permit list
	**/
	void mark_sync_done_not_permit_list (isi_error **error_out);


	/**
	 * Clear the sync status
	 * @param obj[in] the object id
	 * @param error_out[out] error if any
	 */
	void clear_sync_status(const isi_cloud_object_id &obj,
	    struct isi_error **error_out);

public:
	const isi_cfm_mapentry *get_map_entry_by_index(
	    isi_cfm_mapinfo &mapinfo, int index,
	    struct isi_error **error_out);

	cow_helper &get_cow_helper() { return cower_; }
private:
	struct isi_cbm_file *file_;
	uint64_t lin_;
	uint64_t snapid_;
	uint64_t filerev_;
	struct stat stat_;

	// pre-exising mapinfo
	isi_cfm_mapinfo mapinfo_;

	/**
	 * work-in-progress mapinfo
	 */
	isi_cfm_mapinfo wip_mapinfo_;

	/**
	 * The cloud object index
	 */
	isi_cbm_coi_sptr coi_;

	/**
	 * the sync log
	 */
	sync_log log_;

	/**
	 * indicate if the stub has been changed.
	 */
	bool change_detected_;

	/**
	 * indicate if the sync is resuming a failed or paused
	 * operation
	 */
	bool is_resume_;

	/**
	 * the cow helper
	 */
	cow_helper cower_;

	/**
	 * sync options
	 */
	isi_cbm_sync_option opt_;

	bool is_latest_; // is the version being synchronized the latest CMO
	isi_cloud_object_id last_ver_; // last version
	std::auto_ptr<coi_entry> last_entry_;
	bool updated_sync_status_;
	uint64_t syncer_id_;
	bool wipmapinfo_loaded_; // a CKPT may not have wip_mapinfo
};

uint64_t get_map_new_entry_len(const isi_cfm_mapentry &map_entry,
    off_t filesize);
inline off_t
get_chunk_len(off_t length, int index, off_t chunksize)
{
	return	MIN(length - index * chunksize, chunksize);
}

const char *
get_sync_stage_str(sync_stage stage)
{
	const char *st = NULL;
	switch (stage) {
	case SS_SYNC_MODIFIED:
		st = "SS_SYNC_MODIFIED";
		break;
	case SS_COW_CDOS:
		st = "SS_COW_CDOS";
		break;
	case SS_UPDATE_CMO:
		st = "SS_UPDATE_CMO";
		break;
	case SS_PERSIST_MAP:
		st = "SS_PERSIST_MAP";
		break;
	case SS_TRUNCATION:
		st = "SS_TRUNCATION";
		break;
	default:
		st = "SS_INVALID";
		break;
	}
	return st;
}

const char *
get_stage_status_str(sync_stage_status status)
{
	const char *st = NULL;
	switch (status) {
	case SSS_STARTED:
		st = "SSS_STARTED";
		break;
	case SSS_COMPLETED:
		st = "SSS_COMPLETED";
		break;
	case SSS_FAILED:
		st = "SSS_FAILED";
		break;
	default:
		st = "SSS_INVALID";
		break;
	}
	return st;
}

const char *
get_sync_op_str(sync_op op)
{
	const char *st = NULL;
	switch (op) {
	case SO_MOD:
		st = "SO_MOD";
		break;
	case SO_NEW:
		st = "SO_NEW";
		break;
	case SO_DEL:
		st = "SO_DEL";
		break;
	case SO_SNAP:
		st = "SO_SNAP";
		break;
	case SO_CLONE:
		st = "SO_CLONE";
		break;
	case SO_MOVE:
		st = "SO_MOVE";
		break;
	default:
		st = "SO_INVALID";
		break;
	}
	return st;
}

void
sync_log::log(cdo_syncer &cdo)
{
	set_modified();
}

bool
sync_log::load_log(isi_error **error_out)
{
	if (!get_ckpt_cb_)
		return false;

	void *data = NULL;
	size_t header_size = SYNC_HEADER_SIZE;
	djob_op_job_task_state t_state = DJOB_OJT_NONE;
	sync_log_header *header = NULL;
	bool ok = get_ckpt_cb_(ckpt_ctx_, &data, &header_size,
	    &t_state, SYNC_HEADER_OFFSET);

	if (data == NULL) {
		ilog(IL_DEBUG, "File %{} has no checkpointing information.",
		    lin_snapid_fmt(syncer_->get_lin(), syncer_->get_snapid()));
		return false;
	}

	if (header_size != SYNC_HEADER_SIZE) {
		ilog(IL_NOTICE, "File %{} has wrong checkpointing header "
		    "information, size: %ld, expected: %ld.",
		    lin_snapid_fmt(syncer_->get_lin(), syncer_->get_snapid()),
		    header_size,
		    SYNC_HEADER_SIZE);
		ok = false;
		goto out;
	}

	if (t_state == DJOB_OJT_PAUSED) {
		*error_out = isi_cbm_error_new(CBM_FILE_PAUSED,
		    "Sync Paused for file %{}",
		    lin_snapid_fmt(syncer_->get_lin(), syncer_->get_snapid()));
		goto out;
	}
	if (t_state == DJOB_OJT_STOPPED) {
		*error_out = isi_cbm_error_new(CBM_FILE_STOPPED,
		    "Sync Stopped for file %{}",
		    lin_snapid_fmt(syncer_->get_lin(), syncer_->get_snapid()));
		goto out;
	}

	ASSERT(ok == true); // must be true
	header = (sync_log_header *)data;
	if (header->magic_ != SYNC_MAGIC) {
		ilog(IL_NOTICE, "File %{} has wrong checkpointing header "
		    "information, magic: %x, expected: %x.",
		    lin_snapid_fmt(syncer_->get_lin(), syncer_->get_snapid()),
		    header->magic_,
		    SYNC_MAGIC);
		ok = false;
		goto out;
	}

	if (CPOOL_CBM_SYNC_VERSION != header->version_) {
		ilog(IL_NOTICE, "File %{} has wrong checkpointing header "
		    "information, expected version: %x, found version: %x.",
		    lin_snapid_fmt(syncer_->get_lin(), syncer_->get_snapid()),
		    CPOOL_CBM_SYNC_VERSION,
		    header->version_);
		ok = false;
		goto out;
	}

	memcpy(&log_header_, header, SYNC_HEADER_SIZE);

	modified_ = log_header_.modified_;
	log_loaded_ = true;
	header_written_ = true; // we loaded the header

 out:
	if (data) {
		free((void *)data);
		data = NULL;
	}

	return ok;
}

bool
sync_log::get_mapping_info(isi_cfm_mapinfo &mapinfo, isi_error **error_out)
{
	isi_error *error = NULL;
	bool ok = false;
	djob_op_job_task_state t_state = DJOB_OJT_NONE;
	void *data = NULL;
	size_t mapping_size = SYNC_MAPINFO_SIZE;

	if (log_header_.length_ < (
	    SYNC_HEADER_SIZE + SYNC_MAPINFO_SIZE))
		goto out;

	ok = get_ckpt_cb_(ckpt_ctx_, &data, &mapping_size,
	    &t_state, SYNC_MAPINFO_OFFSET);

	if (data == NULL) {
		error = isi_cbm_error_new(CBM_MAP_INFO_ERROR,
		    "Failed to load mapping info for %{}.",
		    lin_snapid_fmt(syncer_->get_lin(), syncer_->get_snapid()));
		goto out;
	}

	if (mapping_size <= 0) {
		error = isi_cbm_error_new(CBM_MAP_INFO_ERROR,
		    "Failed to load mapping info for %{} from the log. "
		    "Mapping size mismatch: size: %ld, expected: %ld.",
		    lin_snapid_fmt(syncer_->get_lin(), syncer_->get_snapid()),
		    mapping_size, SYNC_MAPINFO_SIZE);
		goto out;
	}

	if (t_state == DJOB_OJT_PAUSED) {
		error = isi_cbm_error_new(CBM_FILE_PAUSED,
		    "Sync Paused for file %{}",
		    lin_snapid_fmt(syncer_->get_lin(), syncer_->get_snapid()));
		goto out;
	}
	if (t_state == DJOB_OJT_STOPPED) {
		error = isi_cbm_error_new(CBM_FILE_STOPPED,
		    "Sync Stopped for file %{}",
		    lin_snapid_fmt(syncer_->get_lin(), syncer_->get_snapid()));
		goto out;
	}

	ASSERT(ok == true); // must be true
	mapinfo.unpack((void *)data, mapping_size, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (mapinfo.is_overflow()) {
		mapinfo.open_store(false, false, NULL, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	mapping_written_ = true;
 out:
	isi_error_handle(error, error_out);
	if (data) {
		free((void *)data);
		data = NULL;
	}
	return ok;
}

bool
sync_log::need_to_do_stage(sync_stage stage)
{
	sync_state_record &ss_record = log_header_.ss_record_;

	if (ss_record.stage_ == SS_INVALID)
		return true;

	if (ss_record.stage_ < stage)
		return true;

	if (ss_record.stage_ > stage)
		return false;

	// it is in the same stage, completed
	if (ss_record.status_ == SSS_COMPLETED)
		return false;

	// for everything else, do the stage
	return true;
}

void
sync_log::create_log_header(const struct stat &stat,
    uint64_t filerev, uint64_t syncer_id,
    const sync_state_record &ss_record,
    isi_error **error_out)
{
	if (!set_ckpt_cb_)
		return;

	initialize_log_header(stat, filerev, syncer_id, ss_record);

	write_log_header(error_out);

	if (*error_out) {
		isi_error_add_context(*error_out, "Could not "
		    "update log header for %{}",
		    isi_cbm_file_fmt(syncer_->file()));
		return;
	}

	header_written_ = true;
}

void
sync_log::initialize_log_header(const struct stat &stat,
    uint64_t filerev, uint64_t syncer_id, const sync_state_record &ss_record)
{
	log_header_.magic_ = SYNC_MAGIC;
	log_header_.version_ = CPOOL_CBM_SYNC_VERSION;
	log_header_.stat_ = stat;
	log_header_.filerev_ = filerev;
	log_header_.length_ = SYNC_HEADER_SIZE; // the current length
	log_header_.modified_ = false;
	log_header_.ss_record_ = ss_record;
	log_header_.syncer_id_ = syncer_id;
}

void
sync_log::write_log_header(isi_error **error_out)
{
	if (!set_ckpt_cb_)
		return;

	djob_op_job_task_state t_state = DJOB_OJT_NONE;

	bool ok = set_ckpt_cb_(ckpt_ctx_, &log_header_, SYNC_HEADER_SIZE,
	    &t_state, SYNC_HEADER_OFFSET);

	if (!ok) {
		*error_out = isi_system_error_new(errno,
		    "Failed to write log header for %{}",
		    isi_cbm_file_fmt(syncer_->file()));
		return;
	}

	if (t_state == DJOB_OJT_PAUSED) {
		*error_out = isi_cbm_error_new(CBM_FILE_PAUSED,
		    "Sync Paused for file %{}",
		    lin_fmt(syncer_->file()->lin));
		return;
	}
	if (t_state == DJOB_OJT_STOPPED) {
		*error_out = isi_cbm_error_new(CBM_FILE_STOPPED,
		    "Sync Stopped for file %{}",
		    lin_fmt(syncer_->file()->lin));
		return;
	}
}

void
sync_log::log_mapinfo(const isi_cfm_mapinfo &mapinfo,
    isi_error **error_out)
{
	if (!set_ckpt_cb_)
		return;

	djob_op_job_task_state t_state = DJOB_OJT_NONE;

	int packed_size = 0;
	void *data = NULL;
	struct isi_error *error = NULL;
	bool ok = false;

	mapinfo.pack(packed_size, &data, &error);
	ON_ISI_ERROR_GOTO(out, error, "Failed to pack the mapinfo");

	ok = set_ckpt_cb_(ckpt_ctx_, data, packed_size,
	    &t_state, SYNC_MAPINFO_OFFSET);

	if (!ok) {
		error = isi_system_error_new(errno,
		    "Failed to log mapinfo for %{}, sz: %d",
		    isi_cbm_file_fmt(syncer_->file()), packed_size);
		goto out;
	}

	if (log_header_.length_ < SYNC_HEADER_SIZE + SYNC_MAPINFO_SIZE) {
		// we need to update the log length
		log_header_.length_ =  SYNC_HEADER_SIZE + SYNC_MAPINFO_SIZE;
		write_log_header(&error);

		ON_ISI_ERROR_GOTO(out, error,
		    "Could not update log header for %{}",
		    isi_cbm_file_fmt(syncer_->file()));
	}

	if (t_state == DJOB_OJT_PAUSED) {
		error = isi_cbm_error_new(CBM_FILE_PAUSED,
		    "Sync Paused for file %{}",
		    lin_fmt(syncer_->file()->lin));
		goto out;
	}
	if (t_state == DJOB_OJT_STOPPED) {
		error = isi_cbm_error_new(CBM_FILE_STOPPED,
		    "Sync Stopped for file %{}",
		    lin_fmt(syncer_->file()->lin));
		goto out;
	}

 out:
	// free the buffer, no longer needed
	if (data)
		free(data);

	isi_error_handle(error, error_out);
}

void
sync_log::pre_log_create_master(int index,
    sync_log_context &context, isi_error **error_out)
{
	pre_log_sync_record(SS_SYNC_MODIFIED, index, SO_NEW,
	    SOT_MASTER_CDO, context, error_out);
}

void
sync_log::pos_log_create_master(int index,
    const sync_log_context &context, int errcode, isi_error **error_out)
{
	pos_log_sync_record(index, SO_NEW, SOT_MASTER_CDO, context,
	    errcode, error_out);
}

void
sync_log::pre_log_create_cdo(int index,
    sync_log_context &context, isi_error **error_out)
{
	pre_log_sync_record(SS_SYNC_MODIFIED, index, SO_NEW, SOT_CDO,
	    context, error_out);
}

void
sync_log::pos_log_create_cdo(int index,
    const sync_log_context &context, int errcode, isi_error **error_out)
{
	pos_log_sync_record(index, SO_NEW, SOT_CDO, context,
	    errcode, error_out);
}

void
sync_log::pre_log_update_cdo(int index, sync_log_context &context,
    isi_error **error_out)
{
	pre_log_sync_record(SS_SYNC_MODIFIED, index, SO_MOD, SOT_CDO,
	    context, error_out);
}

void
sync_log::pos_log_update_cdo(int index,
    const sync_log_context &context, int errcode, isi_error **error_out)
{
	pos_log_sync_record(index, SO_MOD, SOT_CDO, context,
	    errcode, error_out);
}

void
sync_log::pre_log_move_cdo(int index, sync_log_context &context,
    isi_error **error_out)
{
	log_header_.ref_state_ = SRS_UNREF;
	pre_log_sync_record(SS_COW_CDOS, index, SO_MOVE, SOT_CDO,
	    context, error_out);
}

void
sync_log::pos_log_move_cdo(int index,
    const sync_log_context &context, int errcode, isi_error **error_out)
{
	pos_log_sync_record(index, SO_MOVE, SOT_CDO, context,
	    errcode, error_out);
}

void
sync_log::pre_log_remove_cdo(int index, sync_log_context &context,
    isi_error **error_out)
{
	pre_log_sync_record(SS_TRUNCATION, index, SO_DEL, SOT_CDO,
	    context, error_out);
}

void
sync_log::pos_log_remove_cdo(int index,
    const sync_log_context &context, int errcode, isi_error **error_out)
{
	pos_log_sync_record(index, SO_DEL, SOT_CDO, context,
	    errcode, error_out);
}

void
sync_log::pre_log_update_cmo(int index, sync_log_context &context,
    isi_error **error_out)
{
	pre_log_sync_record(SS_UPDATE_CMO, index, SO_MOD, SOT_CMO,
	    context, error_out);
}

void
sync_log::pos_log_update_cmo(int index,
    const sync_log_context &context, int errcode, isi_error **error_out)
{
	pos_log_sync_record(index, SO_MOD, SOT_CMO, context,
	    errcode, error_out);
}

void
sync_log::pre_log_clone_cmo(int index, sync_log_context &context,
    isi_error **error_out)
{
	pre_log_sync_record(SS_UPDATE_CMO, index, SO_CLONE, SOT_CMO,
	    context, error_out);
}

void
sync_log::pos_log_clone_cmo(int index,
    const sync_log_context &context, int errcode, isi_error **error_out)
{
	pos_log_sync_record(index, SO_CLONE, SOT_CMO, context,
	    errcode, error_out);
}

void
sync_log::pre_log_replace_map(uint64_t old_csnapid, sync_log_context &context,
    isi_error **error_out)
{
	ASSERT(old_csnapid <= INT_MAX);
	pre_log_sync_record(SS_PERSIST_MAP, old_csnapid, SO_MOD, SOT_MAP,
	    context, error_out);
}

bool
sync_log::get_mod_header(sync_section_header &mod_header,
    sync_record &last_mod_rec, isi_error **error_out)
{
	bool ok = true;
	ok = get_section_header(SS_SYNC_MODIFIED, mod_header, last_mod_rec,
	    error_out);
	if (ok && mod_header.record_cnt_ > 0)
		set_modified();

	return ok;
}

bool
sync_log::get_cow_header(sync_section_header &cow_header,
    sync_record &last_cow_rec, isi_error **error_out)
{
	return get_section_header(SS_COW_CDOS, cow_header, last_cow_rec,
	    error_out);
}

bool
sync_log::get_cmo_header(sync_section_header &cmo_header,
    sync_record &last_cmo_rec, isi_error **error_out)
{
	return get_section_header(SS_UPDATE_CMO, cmo_header, last_cmo_rec,
	    error_out);
}

bool
sync_log::get_trunc_header(sync_section_header &trunc_header,
    sync_record &last_trunc_rec, isi_error **error_out)
{
	return get_section_header(SS_TRUNCATION, trunc_header,
	    last_trunc_rec, error_out);
}

bool
sync_log::get_persist_header(sync_section_header &header,
    sync_record &record, isi_error **error_out)
{
	return get_section_header(SS_PERSIST_MAP, header, record,
	    error_out);
}

bool
sync_log::get_section_header(sync_stage stage, sync_section_header &header,
    sync_record &last_rec, isi_error **error_out)
{
	djob_op_job_task_state t_state = DJOB_OJT_NONE;
	void *data = NULL;
	size_t header_size = SYNC_SECTION_HEADER_SIZE;
	bool ok = false;

	if (!get_ckpt_cb_)
		return false;

	if (header_loaded_[stage]) {
		header = sync_headers_[stage];
		last_rec = sync_records_[stage];
		return true;
	}
	sync_stage current_stage = get_current_stage();
	off_t offset = log_header_.section_offsets_[stage];

	if (current_stage < stage || offset == 0) {
		ilog(IL_DEBUG, "File %{} has no header "
		    "information for stage: %d, max stage: %d, offset: %ld.",
		    isi_cbm_file_fmt(syncer_->file()), stage,
		    current_stage, offset);
		return false;
	}

	ok = get_ckpt_cb_(ckpt_ctx_, &data, &header_size,
	    &t_state, offset);

	if (!ok || header_size != SYNC_SECTION_HEADER_SIZE || !data) {
		ilog(IL_DEBUG, "File %{} has no/invalid header "
		    "information for stage %d, size: %ld, got: %ld, data: %p.",
		    isi_cbm_file_fmt(syncer_->file()),
		    stage, SYNC_SECTION_HEADER_SIZE, header_size, data);
		ok = false;
		goto out;
	}

	if (t_state == DJOB_OJT_PAUSED) {
		*error_out = isi_cbm_error_new(CBM_FILE_PAUSED,
		    "Sync Paused for file %{}",
		    lin_fmt(syncer_->file()->lin));
		goto out;
	}
	if (t_state == DJOB_OJT_STOPPED) {
		*error_out = isi_cbm_error_new(CBM_FILE_STOPPED,
		    "Sync Stopped for file %{}",
		    lin_fmt(syncer_->file()->lin));
		goto out;
	}

	if (sync_headers_[stage].stage_ != stage) {
		*error_out = isi_cbm_error_new(CBM_SECT_HEAD_ERROR,
		    "Got corrupted section header for %{}. stage: %d, got: %d",
		    isi_cbm_file_fmt(syncer_->file()), stage,
		    sync_headers_[stage].stage_);
		ok = false;
		goto out;
	}

	memcpy(&sync_headers_[stage], data, SYNC_SECTION_HEADER_SIZE);
	free((void *)data);
	data = NULL;

	if (sync_headers_[stage].record_cnt_ <= 0) {
		*error_out = isi_cbm_error_new(CBM_SECT_HEAD_ERROR,
		    "Section header found with invalid record count "
		    "for %{}. stage: %d, record_count: %d",
		    isi_cbm_file_fmt(syncer_->file()), stage,
		    sync_headers_[stage].record_cnt_);
		ok = false;
		goto out;
	}

	load_sync_record(stage, sync_headers_[stage].record_cnt_ - 1,
	    sync_records_[stage], error_out);
	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to load_sync_record for %{}. stage: %d, "
		    "index: %d",
		    isi_cbm_file_fmt(syncer_->file()), stage,
		    sync_headers_[stage].record_cnt_ - 1);
		ok = false;
		goto out;
	}

	if (sync_records_[stage].status_ != SSS_COMPLETED) {
		// retry this record
		sync_headers_[stage].record_cnt_--;
	}

	header = sync_headers_[stage];
	last_rec = sync_records_[stage];
	ok = true;
	header_loaded_[stage] = true;
 out:
	if (data) {
		free((void *)data);
		data = NULL;
	}
	return ok;
}


void
sync_log::load_mod_record(int index, sync_record &mod_record,
    isi_error **error_out)
{
	load_sync_record(SS_SYNC_MODIFIED, index, mod_record, error_out);

}

void
sync_log::load_cow_record(int index, sync_record &cow_record,
    isi_error **error_out)
{
	load_sync_record(SS_COW_CDOS, index, cow_record, error_out);
}

void
sync_log::load_sync_record(sync_stage stage, int index,
    sync_record &record, isi_error **error_out)
{
	if (!get_ckpt_cb_)
		return;

	djob_op_job_task_state t_state = DJOB_OJT_NONE;
	void *data = NULL;
	size_t rec_size = SYNC_REC_SIZE;

	ASSERT((index >= 0 && index < (sync_headers_[stage].record_cnt_)),
	    "Wrong record index is requested: %d. valid [0~%d] for stage: %d",
	    index, sync_headers_[stage].record_cnt_ - 1, stage);

	off_t rec_off = log_header_.section_offsets_[stage] +
	    SYNC_SECTION_HEADER_SIZE + (index * SYNC_REC_SIZE);

	bool ok = get_ckpt_cb_(ckpt_ctx_, &data, &rec_size,
	    &t_state, rec_off);

	if (!ok || data == NULL || rec_size != SYNC_REC_SIZE) {
		*error_out = isi_cbm_error_new(CBM_SYNC_REC_ERROR,
		    "Failed to load the mod record information for %{}, "
		    "size: %ld, got size: %ld, index: %d.",
		    isi_cbm_file_fmt(syncer_->file()), SYNC_REC_SIZE,
		    rec_size, index);
		goto out;
	}

	if (t_state == DJOB_OJT_PAUSED) {
		*error_out = isi_cbm_error_new(CBM_FILE_PAUSED,
		    "Sync Paused for file %{}",
		    lin_fmt(syncer_->file()->lin));
		goto out;
	}
	if (t_state == DJOB_OJT_STOPPED) {
		*error_out = isi_cbm_error_new(CBM_FILE_STOPPED,
		    "Sync Stopped for file %{}",
		    lin_fmt(syncer_->file()->lin));
		goto out;
	}

	memcpy(&record, data, SYNC_REC_SIZE);

 out:
	if (data) {
		free((void *)data);
		data = NULL;
	}
}


void
sync_log::set_cow_info(sync_cow_name_id &cow_id)
{
	log_header_.cow_id_ = cow_id;
}

void
sync_log::log_stage_completion(sync_stage stage, isi_error *&error)
{
	// mark the completion of ths sync mod stage
	sync_state_record ss_record = {
	    .stage_ = stage,
	    .status_ = error ? SSS_FAILED : SSS_COMPLETED,
	    .error_code_ = get_sync_error(error)
	};
	if (error)
		set_state_record(ss_record, isi_error_suppress());
	else {
		set_state_record(ss_record, &error);
		if (error) {
			isi_error_add_context(error,
			    "Failed to set_state_record for %{}",
			    isi_cbm_file_fmt(syncer_->file()));
		}
	}
}

void
sync_log::set_state_record(sync_state_record &ss_record,
    isi_error **error_out)
{
	if (!set_ckpt_cb_)
		return;

	log_header_.ss_record_ = ss_record;

	write_log_header(error_out);
	if (*error_out) {
		isi_error_add_context(*error_out, "Could not "
		    "write_log_header for %{}",
		    isi_cbm_file_fmt(syncer_->file()));
		return;
	}
}

void
sync_log::pre_log_clone_cdo(int index,
    sync_log_context &context, isi_error **error_out)
{
	log_header_.ref_state_ = SRS_REF;
	pre_log_sync_record(SS_COW_CDOS, index, SO_CLONE, SOT_CDO,
	    context, error_out);
}

void
sync_log::pos_log_clone_cdo(int index,
    const sync_log_context &context, int errcode, isi_error **error_out)
{
	pos_log_sync_record(index, SO_CLONE, SOT_CDO, context,
	    errcode, error_out);
}

void
sync_log::load_clone_info_cond(cow_helper &cower, isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (!get_ckpt_cb_)
		return;

	if (!need_to_do_stage(SS_TRUNCATION))
		return;

	sync_section_header mod_header;
	sync_record last_mod_rec;
	const int max_rec_count = 1024;
	int rec_to_process = 0;
	int rec_count = 0;

	void *data = NULL;
	size_t data_size, requested_size;
	off_t offset = SYNC_MOD_REC_OFFSET;
	bool ok = false;

	bool mod_header_loaded =
	    get_mod_header(mod_header, last_mod_rec, &error);

	ON_ISI_ERROR_GOTO(out, error, "Failed to get_mod_header for %{}",
	    isi_cbm_file_fmt(syncer_->file()));

	if (!mod_header_loaded)
		return;

	cower.get_cow_name(&error);
	ON_ISI_ERROR_GOTO(out, error, "Failed to get COW name for " "file %{}",
	    isi_cbm_file_fmt(syncer_->file()));

	if (!need_to_do_stage(SS_COW_CDOS))
		return;

	rec_to_process = mod_header.record_cnt_;
	rec_count = MIN(max_rec_count, rec_to_process);

	djob_op_job_task_state t_state;
	while (rec_to_process > 0) {
		requested_size = rec_count * SYNC_REC_SIZE;
		data_size = requested_size;
		ok = get_ckpt_cb_(ckpt_ctx_, &data, &data_size,
		    &t_state, offset);

		if (!ok || data_size != requested_size || !data) {
			error = isi_cbm_error_new(CBM_MOD_REC_ERROR,
			    "Failed to load the mod record information "
			    " for %{}, offset: %ld. "
			    "Requested size: %ld, got: %ld, data: %p",
			    isi_cbm_file_fmt(syncer_->file()),
			    offset, requested_size, data_size, data);
			goto out;
		}

		if (t_state == DJOB_OJT_PAUSED) {
			error = isi_cbm_error_new(CBM_FILE_PAUSED,
			    "Sync Paused for file %{}",
			    lin_fmt(syncer_->file()->lin));
			goto out;
		}
		if (t_state == DJOB_OJT_STOPPED) {
			error = isi_cbm_error_new(CBM_FILE_STOPPED,
			    "Sync Stopped for file %{}",
			    lin_fmt(syncer_->file()->lin));
			goto out;
		}

		// to simplify the operation fr cow, we need to load indices
		// of the objects we already touched
		for (int i = 0; i < rec_count; ++i) {
			const sync_record *mod_rec =
			    (const sync_record *)data + i;

			if (mod_rec->sot_ == SOT_CDO && (
			    mod_rec->op_ == SO_MOD ||
			    mod_rec->op_ == SO_NEW) &&
			    mod_rec->status_ == SSS_COMPLETED) {
				cower.mark_cdo_cloned(mod_rec->idx_, &error);

				ON_ISI_ERROR_GOTO(out, error,
				    "Failed to mark_cdo_cloned for %{} idx : %d",
				    isi_cbm_file_fmt(syncer_->file()),
				    mod_rec->idx_);
			} else if(mod_rec->sot_ == SOT_MASTER_CDO &&
			    mod_rec->op_ == SO_SNAP &&
			    mod_rec->status_ == SSS_COMPLETED) {
				const isi_cfm_mapentry *entry =
				    syncer_->get_map_entry_by_index(
				    syncer_->wip_mapinfo(),
				    mod_rec->idx_, &error);

				ON_ISI_ERROR_GOTO(out, error,
				    "Failed to get mapentry by index for %{}",
				    isi_cbm_file_fmt(syncer_->file()));

				if (entry == NULL) {
					error = isi_cbm_error_new(
					    CBM_MAP_INFO_ERROR,
					    "Corrupted mapping info from log "
					    "could not find mapping entry for "
					    "index %d, for %{}.",
					    mod_rec->idx_,
					    isi_cbm_file_fmt(syncer_->file()));
					goto out;
				}
				cower.mark_master_cdo_snapped(
				    entry->get_offset());
			}
		}

		offset += (rec_count * SYNC_REC_SIZE);
		rec_to_process -= rec_count;
		rec_count = MIN(max_rec_count, rec_to_process);
		free(data);
		data = NULL;
	}

 out:
	if (data)
		free(data);

	isi_error_handle(error, error_out);
}

void
sync_log::pre_log_sync_record(
    sync_stage stage,
    int index, sync_op sop,
    sync_object_type sot,
    sync_log_context &context,
    isi_error **error_out)
{
	if (!set_ckpt_cb_)
		return;

	ASSERT(stage >= SS_SYNC_START && stage <= SS_MAX_STAGES, "Invalid "
	    "stage: %d, valid: %d-%d", stage, SS_SYNC_START,
	    SS_MAX_STAGES);
	bool update_header = false;

	off_t new_section_offset;

	if (log_header_.section_offsets_[stage] == 0) {
		// first calculate the offset for the section header
		// previous stage's section header offset +
		// section header size + bytes used by the records

		// some stages can be skipped, so find the most
		// nearest previous stage
		int ss = stage - 1;
		for (; ss >= 0; --ss) {
			if (log_header_.section_offsets_[ss] != 0)
				break;
		}

		// The log_header_ will be written to the checkpoint even if
		// we return an error, so we can't update section_offsets_
		// until the new section has been written to the checkpoint

		if (ss != SS_INVALID) {
			new_section_offset =
			    log_header_.section_offsets_[ss] +
			    SYNC_SECTION_HEADER_SIZE +
			    sync_headers_[ss].record_cnt_ *
			    SYNC_REC_SIZE;
		} else {
			// this is the first section: modification section
			new_section_offset =
			    SYNC_MOD_HEADER_OFFSET;
		}

		log_header_.ss_record_.stage_ = stage;
		log_header_.ss_record_.status_ = SSS_STARTED;
		log_header_.ss_record_.error_code_ = 0;

		update_header = true;
	} else {
		new_section_offset = log_header_.section_offsets_[stage];
	}

	context.offset = new_section_offset + SYNC_SECTION_HEADER_SIZE +
	    (sync_headers_[stage].record_cnt_ * SYNC_REC_SIZE);

	sync_record record(index, sop, sot, SSS_STARTED, 0);

	djob_op_job_task_state t_state = DJOB_OJT_NONE;
	bool ok = set_ckpt_cb_(ckpt_ctx_, &record, SYNC_REC_SIZE,
	    &t_state, context.offset);

	if (!ok) {
		*error_out = isi_system_error_new(errno,
		    "Failed to pre-log move CDO for %{}, sz: %ld",
		    isi_cbm_file_fmt(syncer_->file()), SYNC_REC_SIZE);
		return;
	}

	UFAIL_POINT_CODE(sync_interrupted_checkpoint1,
	    *error_out = isi_system_error_new(RETURN_VALUE,
		"Fail point sync_interrupted_checkpoint1 triggered");
	    return;
	    );

	if (t_state == DJOB_OJT_PAUSED) {
		*error_out = isi_cbm_error_new(CBM_FILE_PAUSED,
		    "Sync Paused for file %{}",
		    lin_fmt(syncer_->file()->lin));
		return;
	}
	if (t_state == DJOB_OJT_STOPPED) {
		*error_out = isi_cbm_error_new(CBM_FILE_STOPPED,
		    "Sync Stopped for file %{}",
		    lin_fmt(syncer_->file()->lin));
		return;
	}

	++sync_headers_[stage].record_cnt_;
	ok = set_ckpt_cb_(ckpt_ctx_, &sync_headers_[stage],
	    SYNC_SECTION_HEADER_SIZE,
	    &t_state, new_section_offset);

	if (!ok) {
		*error_out = isi_system_error_new(errno,
		    "Failed to write section header of %d for %{}, sz: %ld",
		    stage,
		    isi_cbm_file_fmt(syncer_->file()),
		    SYNC_SECTION_HEADER_SIZE);
		return;
	}

	UFAIL_POINT_CODE(sync_interrupted_checkpoint2,
	    *error_out = isi_system_error_new(RETURN_VALUE,
		"Fail point sync_interrupted_checkpoint2 triggered");
	    return;
	    );

	if (t_state == DJOB_OJT_PAUSED) {
		*error_out = isi_cbm_error_new(CBM_FILE_PAUSED,
		    "Sync Paused for file %{}",
		    lin_fmt(syncer_->file()->lin));
		return;
	}
	if (t_state == DJOB_OJT_STOPPED) {
		*error_out = isi_cbm_error_new(CBM_FILE_STOPPED,
		    "Sync Stopped for file %{}",
		    lin_fmt(syncer_->file()->lin));
		return;
	}

	if (update_header) {
		log_header_.section_offsets_[stage] = new_section_offset;

		write_log_header(error_out);

		if (*error_out) {
			isi_error_add_context(*error_out, "Could not "
			    "update log header for %{}",
			    isi_cbm_file_fmt(syncer_->file()));
			return;
		}
	}
}

void
sync_log::pos_log_sync_record(int index, sync_op sop,
    sync_object_type sot, const sync_log_context &context,
    int errcode, isi_error **error_out)
{
	if (!set_ckpt_cb_)
		return;

	sync_record record(index, sop, sot,
	    errcode == 0 ? SSS_COMPLETED : SSS_FAILED, errcode);

	djob_op_job_task_state t_state = DJOB_OJT_NONE;
	bool ok = set_ckpt_cb_(ckpt_ctx_, &record, SYNC_REC_SIZE,
	    &t_state, context.offset);

	if (!ok) {
		*error_out = isi_system_error_new(errno,
		    "Failed to post-log update CDO for %{}, sz: %ld",
		    isi_cbm_file_fmt(syncer_->file()), SYNC_REC_SIZE);
		return;
	}

        if (t_state == DJOB_OJT_PAUSED) {
		*error_out = isi_cbm_error_new(CBM_FILE_PAUSED,
		    "Sync Paused for file %{}",
		    lin_fmt(syncer_->file()->lin));
		return;
	}
	if (t_state == DJOB_OJT_STOPPED) {
		*error_out = isi_cbm_error_new(CBM_FILE_STOPPED,
		    "Sync Stopped for file %{}",
		    lin_fmt(syncer_->file()->lin));
		return;
	}
}

void
cdo_syncer::init(master_cdo_syncer &master_cdo_syncer,
    const isi_cfm_mapentry &map_entry, off_t chunksize, int index,
    off_t length, sync_op sop)
{
	clear();
	master_cdo_syncer_ = &master_cdo_syncer;
	map_entry_ = map_entry;
	chunksize_ = chunksize;
	index_ = index;
	length_ = length;
	sync_op_ = sop;
	initialized_ = true;
}

void
cdo_syncer::clear()
{
	master_cdo_syncer_ = NULL;
	chunksize_ = 0;
	index_ = -1;
	sync_op_ = SO_INVALID;
	ranges_.clear();
	initialized_ = false;
	length_ = 0;
}

/**
 * All the ranges MUST be in one CDO.
 */
void
cdo_syncer::add_range(isi_cbm_cache_record_info &record)
{
	ranges_.push_back(record);
}

struct file_range {
	off_t offset;
	off_t length;
};

bool
operator ==(const file_range &a, const file_range &b)
{
	return a.offset == b.offset && a.length == b.length;
}

bool operator !=(const file_range &a, const file_range &b)
{
	return !(a == b);
}

void
cdo_syncer::fetch_uncached(struct isi_error **error_out)
{
	struct isi_cbm_file *file = syncer_->file();
	isi_cbm_ioh_base_sptr io_helper = isi_cpool_get_io_helper(
	    map_entry_.get_account_id(), error_out);
	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to call isi_cpool_get_io_helper "
		    "for %{}",
		    isi_cbm_file_fmt(file));
		return;
	}

	off_t chunk_len = get_chunk_len(syncer_->filesize(),
	    index_, chunksize_);
	off_t begin = index_ * chunksize_;
	off_t end = begin + chunk_len;
	// now fetch the non-cached part
	cbm_cache_iterator cache_iter(syncer_->file(), begin, end,
	    ISI_CPOOL_CACHE_MASK_NOTCACHED);
	isi_cbm_cache_record_info record;

	// do not do the sync and invalidation in the same time.
	// TBD-nice change isi_cbm_cache_fill_region to handle filling
	// multiple regions.
	std::string buffer;
	while (cache_iter.next(&record, error_out)) {
		size_t readsize = syncer_->mapinfo().get_readsize();
		if (buffer.size() != readsize)
			buffer.resize(readsize);

		int region = record.offset / readsize;
		isi_cbm_file_read(file, (char *)buffer.data(), record.offset,
		    readsize, CO_CACHE, error_out);

		if (*error_out) {
			isi_error_add_context(*error_out,
			    "Failed to isi_cbm_file_read "
			    "for %{}, region: %d",
			    isi_cbm_file_fmt(file), region);
			return;
		}
	}

}

void
cdo_syncer::modify_or_create_cdo(struct isi_error **error_out)
{

	cl_object_name cloud_object_name;
	std::string entityname;

	isi_error *error = NULL;
	if (sync_op_ == SO_MOD) {
		fetch_uncached(error_out);
		if (*error_out) {
			isi_error_add_context(*error_out,
			    "Failed to fetch_uncached "
			    "for %{}",
			    isi_cbm_file_fmt(syncer_->file()));
			return;
		}
	}

	// Now we can write out this CDO.
	isi_cbm_ioh_base_sptr ioh =
	    master_cdo_syncer_->get_io_helper(error_out);

	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to get the IO helper "
		    "for %{}",
		    isi_cbm_file_fmt(syncer_->file()));
		return;
	}

	const isi_cpool_master_cdo &master_cdo =
	    master_cdo_syncer_->get_master_cdo();

	struct isi_cbm_file *file = syncer_->file();
	int fd = file->fd;
	sync_log_context log_context;
	// Now we write the object for the modified ranges
	// in this case, the COWing is handled on the server side
	std::list<isi_cbm_cache_record_info>::iterator it;

	data_istream strm(fd);

	const isi_cfm_mapinfo &mapinfo = syncer_->mapinfo();

	bool compress = mapinfo.is_compressed();
	bool checksum = mapinfo.has_checksum();
	bool encrypt = mapinfo.is_encrypted();
	sync_log &log = syncer_->get_log();
	encrypt_ctx ectx;

	if (encrypt) {
		mapinfo.get_encryption_ctx(ectx, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	// fail point to simulate an aborted upload -- can happen
	// if the file is truncated later
	UFAIL_POINT_CODE(sync_abort_upload,
		if (RETURN_VALUE == index_) {
			error = isi_cbm_error_new(
			    CBM_CLAPI_ABORTED_BY_CALLBACK,
			    "Fail point sync_abort_upload triggered for %{}",
			    isi_cbm_file_fmt(file));
		}
	);
	ON_ISI_ERROR_GOTO(out, error);

	{
		isi_cpool_master_cdo clone_master;
		// It does not support random_io, or new object, write out all

		// TBD-nice: make chunkinfo part of cdo_syncer
		isi_cbm_chunkinfo chunkinfo = {
			.fd = fd,
			.index = index_ + 1,
			.file_offset = index_ * chunksize_, // redundant
			.object_offset = 0,
			.len = length_,
			.compress = compress,
			.checksum = checksum,
			.ectx = (encrypt ? &ectx : NULL)
		};

		cower_->clone_master_cdo_for_cow(
		    master_cdo, clone_master, &error);
		ON_ISI_ERROR_GOTO(out, error,
		    "Failed to clone CDO for %{}",
		    isi_cbm_file_fmt(file));
		SYNC_LOG_PRE(create_cdo, index_, error, file);

		isi_cph_write_data_blob(fd, index_ * chunksize_,
		    ioh.get(), clone_master, chunkinfo, &error);
		if (error) {
			isi_error_add_context(error,
			    "Failed to isi_cph_write_data_blob "
			    "for %{}, index: %d, length: %ld "
			    "comp: %d, chksum: %d enc: %d "
			    "mcdo idx: %d mcdo plen: %ld, mdco off: %ld",
			    isi_cbm_file_fmt(file), index_, length_,
			    compress, checksum, encrypt,
			    clone_master.get_index(),
			    clone_master.get_physical_length(),
			    clone_master.get_offset()
			);
		}

		SYNC_LOG_POS(create_cdo, index_, error, file);

		cower_->mark_cdo_cloned(index_, &error);
		if (error) {
			isi_error_add_context(error,
			    "Failed to mark_cdo_cloned "
			    "for %{}",
			    isi_cbm_file_fmt(syncer_->file()));
		}

	}

 out:
	isi_error_handle(error, error_out);
}

void
cdo_syncer::remove_cdo(struct isi_error **error_out)
{
	ifs_lin_t lin;
	off_t offset;
	cl_object_name cloud_object_name;
	std::string entityname;
	cpool_events_attrs *cev_attrs = NULL;

	isi_cbm_ioh_base_sptr ioh =
	    master_cdo_syncer_->get_io_helper(error_out);

	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to get the IO helper "
		    "for %{}",
		    isi_cbm_file_fmt(syncer_->file()));
		return;
	}

	const isi_cpool_master_cdo &master_cdo =
	    master_cdo_syncer_->get_master_cdo();

	cev_attrs = new cpool_events_attrs;

	try {
		struct isi_cbm_file *file = syncer_->file();
		lin = file->lin;

		offset = master_cdo.get_offset();

		cloud_object_name = master_cdo.get_cloud_object_name();
		entityname.clear();
		entityname = get_entityname(cloud_object_name.container_cdo,
		    ioh->get_cdo_name(cloud_object_name.obj_base_name,
		    index_ + 1));

		set_cpool_events_attrs(cev_attrs, lin, offset,
		    ioh->get_account().account_id, entityname);

		ioh->delete_cdo(master_cdo.get_cloud_object_name(), index_ + 1,
		    master_cdo.get_snapid());

		// This call will trigger fail point and
		// generate error for the generation of
		// cloudpool_event.
		trigger_failpoint_cloudpool_events();
	}
	CATCH_CLAPI_EXCEPTION(cev_attrs, *error_out, true)
	delete cev_attrs;
}

/**
 * For non-mastered CDOs this routine handles DEL, MOD and NEW
 * For mastered CDOs, this routine only handle MOD and NEW.
 *
 */
void
cdo_syncer::do_sync(struct isi_error **error_out)
{
	switch (sync_op_) {
	case SO_DEL:
		remove_cdo(error_out);
		break;
	case SO_MOD:
	case SO_NEW:
		modify_or_create_cdo(error_out);
		break;
	default:
		break;
	}
}

void
master_cdo_syncer::do_sync(struct isi_error **error_out)
{
}

void
master_cdo_syncer::remove_cdos(off_t offset, struct isi_error **error_out)
{
	isi_error *error = NULL;
	struct isi_cbm_file *file = syncer_->file();
	sync_log &log = syncer_->get_log();
	sync_log_context log_context;

	if (master_cdo_.is_concrete() && sop() == SO_DEL) {
		remove_master_cdo(error_out);
		return;
	}

	if (master_cdo_.is_concrete()) {
		// we can do this in one call.
		truncate(offset, error_out);
		return;
	}

	// This is for the case non-concrete master, truely delete the CDOs:
	cdo_syncer syncer(syncer_, cower_);

	off_t chunksize = master_cdo_.get_chunksize();
	off_t length = master_cdo_.get_length();
	off_t master_offset = master_cdo_.get_offset();

	// advance in the map for the CDOs to delete
	for (off_t t_off = offset; t_off < master_offset + length;
	    t_off += chunksize) {

		int index = t_off / chunksize;
		off_t chunk_len = get_chunk_len(master_offset + length, index,
		    chunksize);
		syncer.init(*this, map_entry_, chunksize,
		    index, chunk_len, SO_DEL);

		SYNC_LOG_PRE(remove_cdo, index, error, file);
		SYNC_FAIL_POINT_PRE(sync_remove_cdo_err);

		sync_sub_cdo(syncer, &error);
		if (error) {
			isi_error_add_context(error,
			    "Failed to delete CDO for %{}",
			    isi_cbm_file_fmt(file));
		}

		SYNC_LOG_POS(remove_cdo, index, error, file);
		SYNC_FAIL_POINT_POS(sync_remove_cdo_err);

		if (error)
			goto out;
	}
 out:
	isi_error_handle(error, error_out);
}

void
master_cdo_syncer::sync_sub_cdo(cdo_syncer &cdo,
    struct isi_error **error_out)
{
	ifs_lin_t lin;
	off_t offset;

	cl_object_name cloud_object_name;
	std::string entityname;
	cpool_events_attrs *cev_attrs = NULL;

	isi_error *error = NULL;
	struct isi_cbm_file *file = syncer_->file();
	sync_log &log = syncer_->get_log();
	if (!log.is_mapping_written()) {
		log.log_mapinfo(syncer_->wip_mapinfo(), &error);

		if (error) {
			isi_error_add_context(error,
			"Failed to log_mapinfo for %{}",
			isi_cbm_file_fmt(syncer_->file()));
			goto out;
		}
	}

	log.log(cdo);
	if (sync_op_ == SO_NEW && !master_created_) {
		// first create the master CDO
		isi_cbm_ioh_base_sptr ioh = get_io_helper(&error);

		if (error) {
			isi_error_add_context(error,
			"Failed to get the IO helper "
			"for %{}",
			isi_cbm_file_fmt(syncer_->file()));
			goto out;
		}
		sync_log_context log_context;

		SYNC_LOG_PRE(create_master, map_entry_.get_index(),
		    error, file);

		cev_attrs = new cpool_events_attrs;

		try {

			struct isi_cbm_file *file = syncer_->file();
			lin = file->lin;

			offset = master_cdo_.get_offset();

			cloud_object_name =
			    master_cdo_.get_cloud_object_name();
			entityname.clear();
			entityname = get_entityname(cloud_object_name.container_cdo,
			    ioh->get_cdo_name(cloud_object_name.obj_base_name,
			    map_entry_.get_index()));

			set_cpool_events_attrs(cev_attrs, lin, offset,
			    ioh->get_account().account_id, entityname);

			ioh->start_write_master_cdo(master_cdo_);

			// This call will trigger fail point and
			// generate error for the generation of
			// cloudpool_event.
			trigger_failpoint_cloudpool_events();
		}
		CATCH_CLAPI_EXCEPTION(cev_attrs, error, true)
		delete cev_attrs;

		SYNC_LOG_POS(create_master, map_entry_.get_index(),
		    error, file);
	}

	master_created_ = true;
	cdo.do_sync(&error);
	if (error) {
		isi_error_add_context(error,
		    "Failed to synchronize the CDO "
		    "for %{}",
		    isi_cbm_file_fmt(file));
		goto out;
	}
	modified_ = true;
 out:
	isi_error_handle(error, error_out);
}


void
master_cdo_syncer::init(isi_cpool_master_cdo &master_cdo,
    const isi_cfm_mapentry &map_entry, sync_op sop,
    bool master_created, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	clear();
	master_cdo_.isi_cpool_copy_master_cdo(master_cdo, &error);
	if (error) {
		goto out;
	}
	sync_op_ = sop;
	map_entry_ = map_entry;
	initialized_ = true;
	master_created_ = master_created;
out:
       isi_error_handle(error, error_out);
}

void
master_cdo_syncer::clear()
{
	sync_op_ = SO_INVALID;
	initialized_ = false;
	master_created_ = false;
	modified_ = false;
}

void
master_cdo_syncer::remove_master_cdo(struct isi_error **error_out)
{
	isi_cbm_ioh_base_sptr ioh = get_io_helper(error_out);
	cpool_events_attrs *cev_attrs = NULL;

	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to get the IO helper "
		    "for %{}",
		    isi_cbm_file_fmt(syncer_->file()));
		return;
	}

	const isi_cpool_master_cdo &master_cdo = get_master_cdo();

	struct isi_cbm_file *file = syncer_->file();
	ifs_lin_t lin = file->lin;

	off_t offset = master_cdo.get_offset();
	cl_object_name cloud_object_name;
	std::string entityname;

	cev_attrs = new cpool_events_attrs;

	try {
		cloud_object_name = master_cdo.get_cloud_object_name();
		entityname.clear();
		entityname = get_entityname(cloud_object_name.container_cdo,
		    ioh->get_cdo_name(cloud_object_name.obj_base_name,
		    master_cdo.get_index()));

		set_cpool_events_attrs(cev_attrs, lin, offset,
		    ioh->get_account().account_id, entityname);

		ioh->delete_cdo(master_cdo.get_cloud_object_name(),
		    master_cdo.get_index(), master_cdo.get_snapid());
		trigger_failpoint_cloudpool_events();
	}
	CATCH_CLAPI_EXCEPTION(cev_attrs, *error_out, true)
	delete cev_attrs;
}

void
master_cdo_syncer::truncate(uint64_t offset, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	cpool_events_attrs *cev_attrs = new cpool_events_attrs;
	isi_cbm_ioh_base_sptr ioh = get_io_helper(&error);

	const isi_cfm_mapinfo &mapinfo = syncer_->mapinfo();

	bool compress = mapinfo.is_compressed();
	bool checksum = mapinfo.has_checksum();
	bool encrypt = mapinfo.is_encrypted();
	encrypt_ctx ectx;
	ifs_lin_t lin;
	cl_object_name cloud_object_name;
	std::string entityname;

	int index = offset / mapinfo.get_chunksize() + 1;
	off_t begin = offset % mapinfo.get_chunksize();
	cl_data_range data_range = {begin, 0};

	ON_ISI_ERROR_GOTO(out, error);
	if (encrypt) {
		mapinfo.get_encryption_ctx(ectx, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	try {
		struct isi_cbm_file *file = syncer_->file();
		lin = file->lin;

		cloud_object_name = master_cdo_.get_cloud_object_name();
		entityname.clear();
		entityname = get_entityname(cloud_object_name.container_cdo,
		    ioh->get_cdo_name(cloud_object_name.obj_base_name, index));

		set_cpool_events_attrs(cev_attrs, lin, offset,
		    ioh->get_account().account_id, entityname);

		ioh->update_cdo(master_cdo_, index, data_range,
		    NULL, compress, checksum, encrypt? &ectx: NULL);

		// This call will trigger fail point and
		// generate error for the generation of
		// cloudpool_event.
		trigger_failpoint_cloudpool_events();
	}
	CATCH_CLAPI_EXCEPTION(cev_attrs, error, true)

 out:
	delete cev_attrs;
	isi_error_handle(error, error_out);
}

isi_cbm_ioh_base_sptr
master_cdo_syncer::get_io_helper(struct isi_error **error_out)
{
	struct isi_cbm_file *file = syncer_->file();

	isi_cbm_ioh_base_sptr io_helper = isi_cpool_get_io_helper(
	    map_entry_.get_account_id(), error_out);

	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to call isi_cpool_get_io_helper "
		    "for %{}",
		    isi_cbm_file_fmt(file));
		return io_helper;
	}

	return io_helper;
}
/**
 *
 *
 * @author ssasson (9/25/2013)
 *
 * The function looks at the mapinfo and determine if it is
 * a simple add or a case of truncate or extend.
 * It determines that by seeing if there is an entry with the
 * same exact off set. If there is than it is either extend or
 * truncate and the entry in the mapinfo is replaced with this
 * entry.
 * @param mapinfo --- The current map info we are working on
 * @param entry --- The entry that is about to be added.
 */
void
add_or_replace_entry(isi_cfm_mapinfo &mapinfo, isi_cfm_mapentry &entry,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	isi_cfm_mapinfo::iterator it;
	// If this is the normal case (no entry with the same offset found
	// just add the entry.
	mapinfo.get_containing_map_iterator_for_offset(
	    it, entry.get_offset(), &error);
	ON_ISI_ERROR_GOTO(out, error, "Failed to find entry by offset");

	if (it == mapinfo.end()) {
		mapinfo.add(entry, &error);
		ON_ISI_ERROR_GOTO(out, error, "Failed to add entry");
	}
	// If an entry with the same offset is found replace it.
	if (it->first == entry.get_offset()) {
		mapinfo.erase(it->first, &error);
		ON_ISI_ERROR_GOTO(out, error, "Failed to remove entry");

		mapinfo.add(entry, &error);
		ON_ISI_ERROR_GOTO(out, error, "Failed to add entry");
		return;
	}
	mapinfo.add(entry, &error);
	ON_ISI_ERROR_GOTO(out, error, "Failed to add entry");

 out:
	isi_error_handle(error, error_out);
}
/**
 * Allocate a new mapping entry for the offset and length
 *
 * This routine either reallocate a new entry or extend the existing
 * mapping entry.
 */
void
stub_syncer::allocate_new_map_entry(isi_cfm_mapinfo &mapinfo,
    isi_cpool_master_cdo &new_master_cdo,
    isi_cfm_mapinfo::iterator &it,
    const isi_cfm_mapentry *last_entry,
    off_t offset, off_t len,
    struct isi_error **error_out)
{
	isi_error *error = NULL;
	bool random_io = false;
	isi_cfm_mapentry map_entry;
	isi_cbm_account account;
	isi_cpool_blobinfo blobinfo;
	isi_cbm_ioh_base_sptr io_helper;
	struct isi_cbm_file *file = file_;

	size_t master_cdo_size = 0;
	std::map<std::string, std::string> attr_map;
	isi_cpool_master_cdo prev_mcdo;

	{
		scoped_ppi_reader rdr;

		const isi_cfm_policy_provider *cbm_cfg = rdr.get_ppi(&error);
		ON_ISI_ERROR_GOTO(out, error);

		isi_cfm_select_writable_account(
		    (isi_cfm_policy_provider *)cbm_cfg,
		    mapinfo.get_provider_id(), mapinfo.get_policy_id(),
		    size_constants::SIZE_1GB, // TBD-nice
		    account, &error);

		if (error) {
			isi_error_add_context(error,
			    "Failed to get an account "
			    "for %{}",
			    isi_cbm_file_fmt(file));
			goto out;
		}
	}

	io_helper.reset(
	    isi_cbm_ioh_creator::get_io_helper(account.type, account));
	if (!io_helper) {
		error = isi_cbm_error_new(CBM_INVALID_ACCOUNT,
		    "Unsupported account type %d", account.type);
		goto out;
	}

	// TBD-nice: to refactor this code, the name is
	// too generic.

	isi_cpool_api_get_info(io_helper.get(), account.account_id,
	    file->lin, random_io, blobinfo, &error);
	if (error != NULL) {
		isi_error_add_context(error);
		goto out;
	}

	// we want to use the existing object's prefixes
	blobinfo.cloud_name.obj_base_name = mapinfo.get_object_id();

	if (account.master_cdo_size > 0) {
		master_cdo_size = MAX(account.master_cdo_size,
		    g_minimum_chunksize);
		master_cdo_size = (master_cdo_size / g_minimum_chunksize) *
		    g_minimum_chunksize;
	}

	if (last_entry)
		master_cdo_from_map_entry(prev_mcdo, mapinfo, last_entry);

	io_helper.get()->get_new_master_cdo(offset, prev_mcdo,
	    blobinfo.cloud_name,
	    mapinfo.is_compressed(),
	    mapinfo.has_checksum(),
	    mapinfo.get_chunksize(),
	    mapinfo.get_readsize(),
	    mapinfo.get_sparse_resolution(),
	    attr_map,
	    new_master_cdo,
	    master_cdo_size);

	map_entry.setentry(CPOOL_CFM_MAP_VERSION, account.account_id,
	    offset, len, blobinfo.cloud_name.container_cdo,
	    blobinfo.cloud_name.obj_base_name);
	map_entry.set_index(new_master_cdo.get_index());
	map_entry.set_plength(new_master_cdo.get_physical_length());

	mapinfo.add(map_entry, &error);
	ON_ISI_ERROR_GOTO(out, error);

	get_log().log_mapinfo(mapinfo, &error);
	ON_ISI_ERROR_GOTO(out, error);

	mapinfo.get_containing_map_iterator_for_offset(it, offset, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);
}

/**
 * Check if the account used for this map_entry has sufficient space
 */
bool
has_sufficient_space(const isi_cfm_mapentry *map_entry)
{
	// TBD-need: there is a lot of logic to hash out for this
	return true;
}

/**
 * Check if we need to allocate a new mapping entry
 * @param mapinfo[in] the mapping info
 * @param map_entry[in] the current mapping entry
 * @param chunksize[in] the CDO chunksize
 * @param error_out[out] error if any
 */
bool
need_new_entry(const isi_cfm_mapinfo &mapinfo,
    const isi_cfm_mapentry *map_entry, off_t offset,
    off_t chunksize, isi_error **error_out)
{
	isi_error *error = NULL;
	bool need_new = false;
	if (map_entry == NULL) {
		need_new = true;
		goto out;
	}

	if (has_sufficient_space(map_entry))
		goto out;

	// otherwise need new
	need_new = true;
 out:
	isi_error_handle(error, error_out);
	return need_new;
}

/**
 * use the current mapping entry or create a new one with concrete master
 * CDO
 */
void
stub_syncer::get_or_create_map_entry(isi_cfm_mapinfo &mapinfo,
    isi_cfm_mapentry &current_entry,
    isi_cfm_mapinfo::iterator &it,
    isi_cpool_master_cdo &new_master_cdo,
    int index, size_t chunk_len, sync_op sop, bool &created,
    struct isi_error **error_out)
{
	const isi_cfm_mapentry *entry = NULL;
	off_t chunksize = mapinfo.get_chunksize();
	off_t offset = index * chunksize;
	bool to_create = false;
	struct isi_cbm_file *file = file_;
	struct isi_error *error = NULL;

	created = false;

	mapinfo.get_containing_map_iterator_for_offset(it, offset, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (it != mapinfo.end())
		entry = &it->second;

	if (sop == SO_MOD || sop == SO_DEL) {
		if (!entry) {
			// this sounds a corrupted stub mapping
			*error_out = isi_cbm_error_new(CBM_MAP_INFO_ERROR,
			    "Corrupted mapping information "
			    "for %{}. Offset: %ld",
			    isi_cbm_file_fmt(file), offset);
			goto out;
		}

		if (sop == SO_DEL)
			goto out;
	}

	if (!entry) {
		it = mapinfo.last(&error);
		ON_ISI_ERROR_GOTO(out, error);

		if (it != mapinfo.end())
			entry = &it->second;
	}

	to_create = need_new_entry(mapinfo, entry, offset, chunksize,
	    &error);
	if (error) {
		isi_error_add_context(error,
			"Failed to need_new_entry for %{}",
			isi_cbm_file_fmt(file));
		goto out;
	}

	if (to_create) {
		// allocate new entry
		allocate_new_map_entry(mapinfo, new_master_cdo, it, entry,
		    offset, chunk_len, &error);

		if (error) {
			isi_error_add_context(error,
			    "Failed to allocate_new_map_entry for "
			    "file %{}",
			    isi_cbm_file_fmt(file));
			it = mapinfo.end();
			goto out;
		}

		created = true;
	}
	else {
		// extend the mapping entry
		isi_cfm_mapentry update_entry;
		update_entry.setentry(CPOOL_CFM_MAP_VERSION, entry->get_account_id(),
		    offset,
		    chunk_len,
		    entry->get_container(),
		    wip_mapinfo_.get_object_id());

		update_entry.set_index(entry->get_index());
		update_entry.set_plength(entry->get_plength());

		update_entry.set_length(get_map_new_entry_len(update_entry,
		    stat_.st_size));

		mapinfo.add(update_entry, &error);
		ON_ISI_ERROR_GOTO(out, error);

		log_.log_mapinfo(mapinfo, &error);
		ON_ISI_ERROR_GOTO(out, error);

		mapinfo.get_containing_map_iterator_for_offset(
		    it, offset, &error);
		ON_ISI_ERROR_GOTO(out, error);

		current_entry = it->second; // current entry recreated
	}

 out:
	if (error == NULL) {
		 if (it != mapinfo.end())
			 master_cdo_from_map_entry(new_master_cdo, mapinfo, &it->second);
		 if (current_entry.get_offset() == -1 && it != mapinfo.end())
			 current_entry = it->second;
	}
	isi_error_handle(error, error_out);
	return;
}


const isi_cfm_mapentry *
stub_syncer::get_map_entry_by_index(isi_cfm_mapinfo &mapinfo, int index,
    struct isi_error **error_out)
{
	isi_cfm_mapinfo::iterator end = mapinfo.end();
	isi_cfm_mapinfo::iterator mit = mapinfo.begin(error_out);

	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to start the mapinfo iterator");
		return NULL;
	}

	for(; mit != end; mit.next(error_out)) {
		if (*error_out)
			break;

		if (mit->second.get_index() == index)
			return &mit->second;
	}

	return NULL;
}

const cl_object_name *
cow_helper::get_cow_name(struct isi_error **error_out)
{
	struct isi_cbm_file *file = syncer_->file();

	if (!initialized_) {
		initiate_cloud_cow(error_out);
		if (*error_out) {
			isi_error_add_context(*error_out,
			    "Failed to get stub mapinfo for %{}",
			    isi_cbm_file_fmt(file));
			return NULL;
		}
	}

	return &cow_name_;
}

void
cow_helper::initiate_cloud_cow(struct isi_error **error_out)
{
	cow_name_.obj_base_name = syncer_->wip_mapinfo().get_object_id();
	initialized_ = true;
}

void
cow_helper::clone_master_cdo_for_cow(const isi_cpool_master_cdo &src,
    isi_cpool_master_cdo &tgt, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	tgt.isi_cpool_copy_master_cdo(src, &error);
	if (error) {
		goto out;
	}

	// same object prefix, with different versions:
	tgt.set_snapid(syncer_->wip_mapinfo().get_object_id().
	    get_snapid());

out:
	isi_error_handle(error, error_out);
	return;
}

void
cow_helper::mark_cdo_cloned(int index, struct isi_error **error_out)
{
	cloned_cdos_.insert(index);
}

void
cow_helper::mark_master_cdo_snapped(off_t offset)
{
	cloned_masters_.insert(offset);
}

bool
cow_helper::used_by_other_lins(uint64_t lin, const isi_cfm_mapinfo *mapinfo,
    struct isi_error **error_out)
{
	struct isi_cbm_file *file = syncer_->file();
	coi &coi_helper  = syncer_->get_coi();

	const isi_cloud_object_id &object_id = mapinfo->get_object_id();

	std::set<ifs_lin_t> lins;
	coi_helper.get_references(object_id, lins, error_out);

	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to coi::get_references "
		    "for %{} object_id: (%s %ld)",
		    isi_cbm_file_fmt(file), object_id.to_c_string(),
		    object_id.get_snapid());
		return false;
	}

	// more than one entries, must be referenced by others
	if (lins.size() > 1)
		return true;

	// no entry, no reference
	if (lins.size() == 0)
		return false;

	// only one entry, and it is not the input lin, yes
	return lins.find(lin) == lins.end();
}

bool
cow_helper::used_by_other_snaps(const isi_cfm_mapinfo *mapinfo,
    struct isi_error **error_out)
{
	isi_error *error = NULL;
	struct isi_cbm_file *file = syncer_->file();
	bool used = false;
	used = cbm_snap_used_by_other_snaps(file, mapinfo, &error);
	ON_ISI_ERROR_GOTO(out, error);
out:
	isi_error_handle(error, error_out);
	return used;
}

bool
cow_helper::used_by_backups(uint64_t lin, const isi_cfm_mapinfo *mapinfo,
    struct isi_error **error_out)
{
	isi_error *error = NULL;
	bool used = false;
	coi &coi = syncer_->get_coi();
	const isi_cloud_object_id &object_id = mapinfo->get_object_id();
	std::auto_ptr<coi_entry> entry(coi.get_entry(object_id, &error));
	ON_ISI_ERROR_GOTO(out, error);

	if (entry->is_backed()) {
		struct timeval tv;
		struct timeval dod = entry->get_co_date_of_death();
		gettimeofday(&tv, NULL);
		if (timercmp(&dod, &tv, >)) {
			// need to live longer
			used = true;
		} else {
			used = false;
		}
	}

 out:
	isi_error_handle(error, error_out);
	return used;
}

void
cow_helper::cow_cmo(sync_op start_op, struct isi_error **error_out)
{
	struct isi_cbm_file *file = syncer_->file();

	isi_cbm_ioh_base_sptr ioh = syncer_->get_cmo_io_helper(error_out);
	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to get_cmo_io_helper "
		    "for %{}",
		    isi_cbm_file_fmt(file));
		return;
	}

	clone_cmo(start_op, ioh.get(), error_out);
	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to clone_cmo for %{}",
		    isi_cbm_file_fmt(file));
		return;
	}
}

// TBD-nice: refactor the following two functions:
void
cow_helper::update_cmo(sync_op start_op, isi_cbm_ioh_base *ioh,
    struct isi_error **error_out)
{
	if (start_op == SO_SNAP) {
		// we are past that
		return;
	}

	isi_error *error = NULL;
	const isi_cfm_mapinfo &mapinfo = syncer_->wip_mapinfo();
	struct isi_cbm_file *file = syncer_->file();
	sync_log &log = syncer_->get_log();
	sync_log_context log_context;

	cl_object_name tgt_name = {
		.container_cmo = mapinfo.get_container(),
		.container_cdo = "", // we do not need this info
		.obj_base_name = mapinfo.get_object_id()
	};

	std::map<std::string, std::string> attr_map;

	// TBD-nice: this is the unwieldy way to do an update, we do not need all
	// of the information here:
	isi_cpool_blobinfo blobinfo = {
		.chunksize = mapinfo.get_chunksize(),
		.readsize = mapinfo.get_readsize(),
		.lin = file->lin,
		.account_id = mapinfo.get_account(),
		.cloud_name = tgt_name,
		.index = 0,
		.attr_map = attr_map,
		.sparse_resolution = mapinfo.get_sparse_resolution()
	};

	std::string object_name;

	isi_cmo_info cmo_info;
	cmo_info.version = CPOOL_CMO_VERSION;
	cmo_info.lin = file->lin;
	cmo_info.stats = &syncer_->stat();
	cmo_info.mapinfo = &syncer_->wip_mapinfo();

	SYNC_LOG_PRE(update_cmo, 0, error, file);

	SYNC_FAIL_POINT_PRE(sync_update_cmo_err);
	// write the CMO to a cloned object
	// The following function badly needs refactoring, too much
	// duplicate info and unnecessary info in the arguments
	ilog(IL_DEBUG, "Writing CMO: (%s, %ld) for %{}",
		blobinfo.cloud_name.obj_base_name.to_c_string(),
		blobinfo.cloud_name.obj_base_name.get_snapid(),
		isi_cbm_file_fmt(file));
	isi_cph_write_md_blob(file->fd, ioh, blobinfo,
	    cmo_info,
	    object_name, // TBD-nice: fix this, useless parameter
	    &error);

	if (error) {
		isi_error_add_context(error,
		    "Failed to clone_cmo for %{}",
		    isi_cbm_file_fmt(file));
	}

	SYNC_LOG_POS(update_cmo, 0, error, file);

	SYNC_FAIL_POINT_POS(sync_update_cmo_err);

 out:
	isi_error_handle(error, error_out);
}

/**
 * write the CMO to a clone
 */
void
cow_helper::clone_cmo(sync_op start_op, isi_cbm_ioh_base *ioh,
    struct isi_error **error_out)
{
	isi_error *error = NULL;
	if (start_op == SO_SNAP) {
		// we are past that
		return;
	}

	// generate the new cloud object name if not already done:
	get_cow_name(error_out);
	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to get COW name for "
		    "file %{}",
		    isi_cbm_file_fmt(syncer_->file()));
		return;
	}

	const isi_cfm_mapinfo &mapinfo = syncer_->mapinfo();
	struct isi_cbm_file *file = syncer_->file();

	cl_object_name tgt_name = {
		.container_cmo = mapinfo.get_container(),
		.container_cdo = "", // we do not need this info
		.obj_base_name = cow_name_.obj_base_name
	};

	std::map<std::string, std::string> attr_map;

	isi_cpool_blobinfo blobinfo = {
		.chunksize = mapinfo.get_chunksize(),
		.readsize = mapinfo.get_readsize(),
		.lin = file->lin,
		.account_id = mapinfo.get_account(),
		.cloud_name = tgt_name,
		.index = 0,
		.attr_map = attr_map,
		.sparse_resolution = mapinfo.get_sparse_resolution()
	};

	sync_log &log = syncer_->get_log();
	sync_log_context log_context;
	syncer_->wip_mapinfo().set_object_id(cow_name_.obj_base_name);
	std::string object_name;

	isi_cmo_info cmo_info;
	cmo_info.version = CPOOL_CMO_VERSION;
	cmo_info.lin = file->lin;
	cmo_info.stats = &syncer_->stat();
	cmo_info.mapinfo = &syncer_->wip_mapinfo();

	SYNC_LOG_PRE(clone_cmo, 0, error, file);
	SYNC_FAIL_POINT_PRE(sync_clone_cmo_err);

	// write the CMO to a cloned object
	// The following function badly needs refactoring, too much
	// duplicate info and unnecessary info in the arguments
	isi_cph_write_md_blob(file->fd, ioh, blobinfo,
	    cmo_info,
	    object_name,
	    &error);

	if (error) {
		isi_error_add_context(error,
		    "Failed to clone_cmo for %{}",
		    isi_cbm_file_fmt(file));
	}

	SYNC_LOG_POS(clone_cmo, 0, error, file);
	SYNC_FAIL_POINT_POS(sync_clone_cmo_err);

 out:
	isi_error_handle(error, error_out);
}

void
cow_helper::cow_cdos(int start_index, struct isi_error **error_out)
{
	isi_error *error = NULL;
	// no need to do any special handling, COW for modified objects
	// have already been done.
	const isi_cfm_mapinfo *latest_cmo = NULL;
	cl_cmo_ostream out_stream;
	bool send_celog_event = true;

	struct isi_cbm_file *file = syncer_->file();

	{
		if (syncer_->is_latest())
			goto out;
		// TBD-nice The following code and similar code in cloud GC
		// should be refactored to share the same CMO stream down
		// code.
		coi_entry *coi_entry = syncer_->get_last_entry();
		isi_cbm_id cmo_account_id;
		struct isi_cbm_account cmo_account;
		struct cl_object_name cloud_object_name;
		isi_cloud_object_id object_id = syncer_->get_last_ver();

		cmo_account_id = coi_entry->get_archive_account_id();

		// Access config under read lock
		{
			scoped_ppi_reader ppi_reader;

			struct isi_cfm_policy_provider *ppi;

			ppi = (struct isi_cfm_policy_provider *)
			    ppi_reader.get_ppi(&error);
			ON_ISI_ERROR_GOTO(out, error, "failed to get ppi");

			isi_cfm_get_account((isi_cfm_policy_provider *) ppi,
			    cmo_account_id, cmo_account, &error);
			ON_ISI_ERROR_GOTO(out, error, "failed to "
			    "isi_cfm_get_account ppi");
		}

		isi_cbm_ioh_base_sptr cmo_io_helper =
		    isi_cpool_get_io_helper(cmo_account, &error);
		ON_ISI_ERROR_GOTO(out, error, "failed to "
		    "isi_cpool_get_io_helper");


		cloud_object_name.container_cmo = coi_entry->get_cmo_container();
		cloud_object_name.container_cdo = "unused";
		cloud_object_name.obj_base_name = object_id;
		isi_cph_read_md_blob(out_stream, -1, cloud_object_name,
		    coi_entry->is_compressed(), coi_entry->has_checksum(),
		    cmo_io_helper.get(), send_celog_event, &error);
		if (error != NULL) {
			isi_error_add_context(error, "failed to read CMO "
			    "while attempting to retrieve stub map "
			    "(cloud object id: %s)", object_id.to_c_string());
			goto out;
		}
		latest_cmo = &out_stream.get_mapinfo();
	}

	cow_unmodified_objects(start_index, *latest_cmo, &error);
	if (error) {
		isi_error_add_context(error,
		    "Failed to cow_unmodified_objects for %{}",
		    isi_cbm_file_fmt(file));
		goto out;
	}
 out:
	isi_error_handle(error, error_out);
}

void
cow_helper::update_or_cow_cmo(sync_op start_op, struct isi_error **error_out)
{
	struct isi_cbm_file *file = syncer_->file();

	isi_cbm_ioh_base_sptr ioh =
	    syncer_->get_cmo_io_helper(error_out);
	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to get_cmo_io_helper "
		    "for %{}",
		    isi_cbm_file_fmt(file));
		return;
	}

	update_cmo(start_op, ioh.get(), error_out);
	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to update_cmo for %{}",
		    isi_cbm_file_fmt(file));
		return;
	}
}

bool
cow_helper::is_cowed(int index, struct isi_error **error_out)
{
	// We need to search the information in the persistent store
	return cloned_cdos_.find(index) != cloned_cdos_.end();
}

void
cow_helper::move_modified_objects(int start_index,
    struct isi_error **error_out)
{
	isi_error *error = NULL;
	const isi_cfm_mapinfo &mapinfo = syncer_->wip_mapinfo();
	isi_cfm_mapinfo::iterator end = mapinfo.end();
	struct isi_cbm_file *file = syncer_->file();

	cpool_events_attrs *cev_attrs = NULL;
	ifs_lin_t lin = file->lin;

	isi_cfm_mapinfo::map_iterator mit;
	sync_log &log = syncer_->get_log();
	sync_log_context log_context;

	for (std::set<int>::iterator it =
	    cloned_cdos_.lower_bound(start_index);
	    it != cloned_cdos_.end(); ++it) {
		int index = *it;
		off_t offset = index * mapinfo.get_chunksize();
		mapinfo.get_map_iterator_for_offset(mit, offset, &error);

		ON_ISI_ERROR_GOTO(out, error);

		if (mit == end)
			continue;

		const isi_cfm_mapentry *entry = &mit->second;
		isi_cbm_ioh_base_sptr ioh = isi_cpool_get_io_helper(
		    entry->get_account_id(), &error);
		if (error) {
			isi_error_add_context(error,
			    "Failed to call isi_cpool_get_io_helper "
			    "for %{}",
			    isi_cbm_file_fmt(file));
			goto out;
		}
		cl_object_name tgt_name = {
			.container_cmo = mapinfo.get_container(),
			.container_cdo = entry->get_container(),
			.obj_base_name = entry->get_object_id()
		};

		cl_object_name src_name = {
			.container_cmo = mapinfo.get_container(),
			.container_cdo = entry->get_container(),
			.obj_base_name = cow_name_.obj_base_name
		};

		SYNC_LOG_PRE(move_cdo, index, error, file);

		SYNC_FAIL_POINT_PRE(sync_move_cdo_err);

		cl_object_name cloud_object_name;
		std::string entityname;
		cev_attrs = new cpool_events_attrs;

		try {
			entityname.append("src: ");
			entityname.append(get_entityname(src_name.container_cdo,
			    ioh->get_cdo_name(src_name.obj_base_name, index + 1)));
			entityname.append(" move to dest: ");
			entityname.append(get_entityname(tgt_name.container_cdo,
			    ioh->get_cdo_name(tgt_name.obj_base_name, index + 1)));

			set_cpool_events_attrs(cev_attrs, lin, offset,
			    ioh->get_account().account_id, entityname);

			ioh->move_cdo(src_name, tgt_name,
			    index + 1, // the CDO index
			    true);

			// This call will trigger fail point and
			// generate error for the generation of
			// cloudpool_event.
			trigger_failpoint_cloudpool_events();
		}
		CATCH_CLAPI_EXCEPTION(cev_attrs, error, true)
 	        delete cev_attrs;

		if (error) {
			isi_error_add_context(error,
			    "Failed to move_cdo for %{}. "
			    "src: %s, dst: %s, container: %s, idx: %d "
			    "account: (%d, %s)",
			    isi_cbm_file_fmt(syncer_->file()),
			    src_name.obj_base_name.to_string().c_str(),
			    tgt_name.obj_base_name.to_string().c_str(),
			    src_name.container_cdo.c_str(),
			    index,
			    entry->get_account_id().get_id(),
			    entry->get_account_id().get_cluster_id());
		}

		SYNC_LOG_POS(move_cdo, index, error, file);

		SYNC_FAIL_POINT_POS(sync_move_cdo_err);

		if (error)
			break;
	}

 out:
	 isi_error_handle(error, error_out);
}

/*
 * This handles the case of COWing the objects not modified.
 * Depending on the COW mode, and update the WIP mapping info
 */
void
cow_helper::cow_unmodified_objects(int start_index,
    const isi_cfm_mapinfo &latest_cmo,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	const isi_cfm_mapinfo &mapinfo = syncer_->mapinfo();
	struct isi_cbm_file *file = syncer_->file();

	off_t filesize = syncer_->filesize();
	off_t chunksize = mapinfo.get_chunksize();

	isi_cfm_mapinfo::iterator it = mapinfo.begin(&error);

	ON_ISI_ERROR_GOTO(out, error, "Failed to get the mapinfo iterator");

	for (; it != mapinfo.end(); it.next(&error)) {
		ON_ISI_ERROR_GOTO(out, error);

		const isi_cfm_mapentry &entry = it->second;

		off_t end_index =
		    (entry.get_offset() + entry.get_length() - 1) /
		    mapinfo.get_chunksize();
		if (end_index < start_index)
			continue;

		off_t offset = entry.get_offset();

		if ( offset >= filesize) {
			// we do not need these entries in the new map
			// remove it.
			syncer_->wip_mapinfo().remove(entry, &error);

			ON_ISI_ERROR_GOTO(out, error, "Failed to remove entry");
			break;
		}
		// Now do the cloning
		isi_cbm_ioh_base_sptr ioh = isi_cpool_get_io_helper(
		    entry.get_account_id(), &error);

		ON_ISI_ERROR_GOTO(out, error, "Failed to get io helper for "
		    "file: %{}", isi_cbm_file_fmt(file));

		cow_context cow_arg = {
			.mapinfo = mapinfo,
			.file = file,
			.filesize = filesize,
			.chunksize = chunksize,
			.map_entry = entry,
			.ioh = ioh,
			.wip_mapinfo = syncer_->wip_mapinfo(),
			.latest_cmo = latest_cmo,
			.start_index = start_index
		};

		clone_unmodified_objects(cow_arg, &error);

		ON_ISI_ERROR_GOTO(out, error,
		    "Failed to clone_unmodified_objects for file: %{}",
		    isi_cbm_file_fmt(file));
	}

 out:
	isi_error_handle(error, error_out);
}

/**
 * utility to get the current map_entry's correct length
 * after factoring in the current file length
 */
uint64_t
get_map_new_entry_len(const isi_cfm_mapentry &map_entry,
    off_t filesize)
{
	off_t end = map_entry.get_offset() +
	    map_entry.get_length();

	return end < filesize ?
	    map_entry.get_length() :
	    filesize - map_entry.get_offset();
}

/*
 * This handles the case of cloning the objects not modified.
 * This works on a particular mapping entry and handle all the objects
 * modelled by the entry.
 */
void
cow_helper::clone_unmodified_objects(cow_context &cow_arg,
    struct isi_error **error_out)
{
	isi_error *error = NULL;
	off_t offset = cow_arg.map_entry.get_offset();
	off_t end = offset + cow_arg.map_entry.get_length();
	// Now do the cloning
	isi_cbm_ioh_base_sptr ioh = cow_arg.ioh;
	sync_log &log = syncer_->get_log();
	sync_log_context log_context;
	isi_cfm_mapinfo::iterator it;
	off_t chunksize = cow_arg.wip_mapinfo.get_chunksize();
	std::string entityname;

	struct isi_cbm_file *file = syncer_->file();
	cpool_events_attrs *cev_attrs = NULL;

	ifs_lin_t lin = file->lin;

	int index = offset / chunksize;
	if (cow_arg.start_index > index)
		offset = cow_arg.start_index * chunksize;

	if (syncer_->is_latest()) {
		// syncing the latest, nothing need to be done.
		return;
	}

	// iterate through the chunks modeled by this entry:
	for (; offset < end && offset < cow_arg.filesize;
	    offset += cow_arg.chunksize) {
		int index = offset / cow_arg.chunksize; // CDO index
		if (is_cowed(index, &error))
			continue;
		if (error) {
			isi_error_add_context(error,
			    "Failed to check is_cowed for "
			    "file %{}",
			    isi_cbm_file_fmt(cow_arg.file));
			goto out;
		}

		// make sure cow name is generated, we can be the first
		// in a truncation case.
		get_cow_name(&error);
		if (error) {
			isi_error_add_context(error,
			    "Failed to get COW name for "
			    "file %{}",
			    isi_cbm_file_fmt(cow_arg.file));
			goto out;
		}

		bool contained = cow_arg.latest_cmo.contains(offset,
		    cow_arg.map_entry.get_object_id(), &error);

		ON_ISI_ERROR_GOTO(out, error);

		if (contained) {
			// referenced by the latest, skip
			continue;
		}
		// otherwise COW to enforce the invariant.

		cl_object_name src_name = {
			.container_cmo = cow_arg.mapinfo.get_container(),
			.container_cdo = cow_arg.map_entry.get_container(),
			.obj_base_name = cow_arg.map_entry.get_object_id()
		};

		cl_object_name tgt_name = {
			.container_cmo = cow_arg.mapinfo.get_container(),
			.container_cdo = cow_arg.map_entry.get_container(),
			.obj_base_name = cow_name_.obj_base_name
		};

		SYNC_LOG_PRE(clone_cdo, index, error, cow_arg.file);

		SYNC_FAIL_POINT_PRE(sync_clone_cdo_err);

		cev_attrs = new cpool_events_attrs;

		try {
			entityname.clear();
			entityname.append("src: ");
			entityname.append(get_entityname(src_name.container_cdo,
			    ioh->get_cdo_name(src_name.obj_base_name, index + 1)));
			entityname.append(" move to dest: ");
			entityname.append(get_entityname(tgt_name.container_cdo,
			    ioh->get_cdo_name(tgt_name.obj_base_name, index + 1)));

			set_cpool_events_attrs(cev_attrs, lin, offset,
			    ioh->get_account().account_id, entityname);

			cow_arg.wip_mapinfo
			    .get_containing_map_iterator_for_offset(
				it, offset, &error);
			ON_ISI_ERROR_GOTO(out, error);

			isi_cfm_mapentry map_entry = it->second;
			map_entry.set_object_id(cow_name_.obj_base_name);
			off_t chunk_len = get_chunk_len(
			    syncer_->filesize(), index,
			    cow_arg.mapinfo.get_chunksize());
			map_entry.update(offset, chunk_len);

			cow_arg.wip_mapinfo.add(map_entry, &error);
			ON_ISI_ERROR_GOTO(out, error);

			syncer_->get_log().log_mapinfo(
			    cow_arg.wip_mapinfo, &error);
			ON_ISI_ERROR_GOTO(out, error);

			ioh->clone_object(src_name, tgt_name, index + 1);
			// This call will trigger fail point and
			// generate error for the generation of
			// cloudpool_event.
			trigger_failpoint_cloudpool_events();
		}
		CATCH_CLAPI_EXCEPTION(cev_attrs, error, true)
		delete cev_attrs;
		cev_attrs = NULL;
		SYNC_LOG_POS(clone_cdo, index, error, cow_arg.file);

		SYNC_FAIL_POINT_POS(sync_clone_cdo_err);

		if (error)
			goto out;
	}

 out:
	if (cev_attrs != NULL)
		delete cev_attrs;

	isi_error_handle(error, error_out);
}


pre_persist_map_callback g_pre_save_map_callback = NULL;

void
stub_syncer::persist_mapping_info(struct isi_error **error_out)
{
	isi_error *error = NULL;
	sync_log &log = get_log();
	bool tried = false;
	int map_type;

	uint64_t replaced_cloud_snapid;


	if (!log.need_to_do_stage(SS_PERSIST_MAP)) {
		ilog(IL_DEBUG, "Log indicated that mapping info has already "
		    "been persisted. This will be skipped.\n");
		goto out;
	}

	// A trigger for the stubmap corruption seen in bug 201331
	SYNC_FAIL_POINT_PRE(sync_persist_mapping_info_err);

	if (!log.get_modified() && stat_.st_size ==
	    mapinfo_.get_filesize()) {
		// did not find any modified part nor the file size
		// shrink no deletion as for now. no change
		goto out;
	}


	// wip_mapinfo is ready for use; mark as such and
	// attempt to create the actual store from the wip, if not
	// already done
	map_type = wip_mapinfo_.get_type();
	if (map_type & ISI_CFM_MAP_TYPE_WIP) {
		wip_mapinfo_.set_type(map_type & ~ISI_CFM_MAP_TYPE_WIP);
		wip_mapinfo_.move_store_on_name_change(&error);

		if (error) {
			isi_error_add_context(error,
			    "Failed to isi_cfm_mapinfo::move_store_on_name_change()"
			    " for %{}", isi_cbm_file_fmt(file_));
			goto out;
		}
	}

	// Verify wip_mapinfo before proceeding further.
	// This handles the possibility where the CMO had already been
	// written before the fix for 201331 was applied and writeback
	// resumed at this stage.
	wip_mapinfo_.verify_map(&error);
	ON_ISI_ERROR_GOTO(out, error);

	log.set_modified();
	tried = true;
	log.log_mapinfo(wip_mapinfo(), &error);
	ON_ISI_ERROR_GOTO(out, error);

	UFAIL_POINT_CODE(pre_write_map_takesnap,
		if (g_pre_save_map_callback) {
			g_pre_save_map_callback(RETURN_VALUE);
		}
	);

	// record or retrieve the old cloud snapid
	{
		sync_section_header header;
		sync_record record;

		bool possibly_complete = log.get_persist_header(header, record,
		    &error);
		ON_ISI_ERROR_GOTO(out, error);

		if (possibly_complete) {
			ASSERT(record.status_ == SSS_STARTED,
			    "record.status_: %d", record.status_);
			ASSERT(record.sot_ == SOT_MAP,
			    "record.sot_: %d", record.sot_);

			replaced_cloud_snapid = record.idx_;
		} else {
			// we haven't replaced the file's mapinfo yet,
			// so the mapinfo_ variable is still reliable
			replaced_cloud_snapid = mapinfo_.get_object_id().get_snapid();

			sync_log_context context;
			log.pre_log_replace_map(replaced_cloud_snapid, context,
			    &error);
			ON_ISI_ERROR_GOTO(out, error);
		}
	}

	// store map content and mark the file stubbed
	{
		UFAIL_POINT_CODE(sync_stublock_contention,
		{
			error = isi_cbm_error_new(
			    CBM_DOMAIN_LOCK_CONTENTION,
			    "Fail point sync_synlock_contention triggered "
			    "for %{}", isi_cbm_file_fmt(file_));
		});

		ON_ISI_ERROR_GOTO(out, error);

		isi_cph_add_coi_entry(wip_mapinfo_, file_->lin, true,
		    &error);
		ON_ISI_ERROR_GOTO(out, error);

		scoped_stub_lock stub_lock;
		stub_lock.lock_ex(file_->fd, opt_.blocking, &error);
		ON_ISI_ERROR_GOTO(out, error);

		wip_mapinfo_.write_map(file_->fd, filerev_, true, false, &error);

		if (error) {
			isi_error_add_context(error,
			    "Failed to isi_cfm_mapinfo::write_map for %{}",
			    isi_cbm_file_fmt(file_));
			goto out;
		}
	}



	{
		// Remove a reference (if necessary) from the old objects
		SYNC_FAIL_POINT_PRE(sync_collect_objs_err);

		//the old id is the same as the new but with a different csnapid
		isi_cloud_object_id old_object(wip_mapinfo_.get_object_id());
		old_object.set_snapid(replaced_cloud_snapid);

		collect_cloud_objects(old_object, &error);

		ON_ISI_ERROR_GOTO(out, error,
		    "Failed to collect_cloud_objects for %{}",
		    isi_cbm_file_fmt(file_));

		SYNC_FAIL_POINT_POS(sync_collect_objs_err);
	}

 out:
	if(tried)
		log.log_stage_completion(SS_PERSIST_MAP, error);

	isi_error_handle(error, error_out);
}

isi_cbm_ioh_base_sptr
stub_syncer::get_cmo_io_helper(struct isi_error **error_out)
{
	isi_cbm_ioh_base_sptr ioh = isi_cpool_get_io_helper(
	    mapinfo_.get_account(), error_out);

	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to update_cmo for %{}",
		    isi_cbm_file_fmt(file_));
		ASSERT(!ioh);
		return ioh;
	}

	return ioh;
}

void
stub_syncer::get_sync_mod_resume_context(sync_log &log, off_t &initial_offset,
    isi_cpool_master_cdo &master_cdo, master_cdo_syncer &master,
    cdo_syncer &syncer, struct isi_error **error_out)
{
	sync_section_header mod_header;
	sync_record mod_rec;
	bool mod_header_loaded = false;
	off_t chunksize = mapinfo_.get_chunksize();
	initial_offset = 0;

	if (log.is_log_loaded()) {
		mod_header_loaded = log.get_mod_header(mod_header,
		    mod_rec, error_out);

		if (*error_out) {
			isi_error_add_context(*error_out,
			    "Failed to get_mod_header for %{}",
			    isi_cbm_file_fmt(file_));
			return;
		}
	} else
		return;

	if (!mod_header_loaded)
		return;

	if (mod_rec.sot_ != SOT_CDO) {
		*error_out = isi_cbm_error_new(CBM_SYNC_REC_ERROR,
		    "The sync record is not a CDO, corruption "
		    "type: %d, expected: %d for %{}.",
		    mod_rec.sot_, SOT_CDO,
		    isi_cbm_file_fmt(file_));
		return;
	}

	if (mod_rec.status_ == SSS_COMPLETED) {
		// this CDO has been completed
		// go to the next
		initial_offset = (mod_rec.idx_ + 1) *
		    chunksize;
	}
	else {
		// this CDO need to be retried
		initial_offset = mod_rec.idx_ * chunksize;
	}
}

void
stub_syncer::mark_in_progress(size_t region, bool error_on_clean,
    struct isi_error **error_out)
{
        bool changed = false;
        struct isi_error *error = NULL;
        isi_cbm_cache_status cs = ISI_CPOOL_CACHE_INVALID;

        cs = file_->cacheinfo->cond_mark_cache_state(region,
            ISI_CPOOL_CACHE_DIRTY, ISI_CPOOL_CACHE_INPROGRESS, changed, &error);
        if (error) {
                if (isi_cbm_error_is_a(error, CBM_CACHEINFO_SIZE_ERROR)) {
                        ilog(IL_DEBUG, "Failed cond_mark_cache_state for %{}, "
                            "due to truncation. Details: %#{}",
                            isi_cbm_file_fmt(file_), isi_error_fmt(error));
                        isi_error_free(error);
                        error = isi_cbm_error_new(CBM_FILE_TRUNCATED,
                            "The file %{} was truncated before "
                            "marking the cache state", isi_cbm_file_fmt(file_));
                } else {
                        isi_error_add_context(error, "Failed to mark cache "
                            "state for file %{}", isi_cbm_file_fmt(file_));
                }
                goto out;
        }

        if (error_on_clean && !changed && cs != ISI_CPOOL_CACHE_INPROGRESS) {
                error = isi_cbm_error_new(CBM_INVALID_CACHE,
                    "Corrupted cache range, the state changed under the syncer."
                    " region: %lld, state: %d for %{}", region, cs,
                    isi_cbm_file_fmt(file_));
                goto out;
        }

out:
        *error_out = error;
}

void
stub_syncer::sync_modified_regions(struct isi_error **error_out)
{
	isi_cpool_master_cdo master_cdo;
	isi_cpool_master_cdo new_master_cdo;
	cow_helper &cower = get_cow_helper();
	master_cdo_syncer master(this, &cower);
	cdo_syncer syncer(this, &cower);
	int index = 0;
        // It's safer to compute last_region based on the initial stat, since
        // file_->cacheinfo is refreshed if anyone tries to grab cache header
        // lock before this point
        size_t last_region = (stat_.st_size == 0) ? 0 :
                    (stat_.st_size - 1) / file_->cacheinfo->get_regionsize();

	isi_error *error = NULL;
	isi_cfm_mapentry current_entry;
	isi_cfm_mapentry new_entry;

	sync_op sop = SO_INVALID;
	isi_cbm_cache_record_info record;
	int end_index = 0;
	off_t chunksize = mapinfo_.get_chunksize();
	off_t old_fsize = mapinfo_.get_filesize();
	off_t last_chunk_end = (old_fsize / chunksize +
	    (old_fsize % chunksize != 0 ? 1 : 0)) * chunksize;
	sync_log &log = get_log();
	off_t initial_offset = 0;

	if (!log.need_to_do_stage(SS_SYNC_MODIFIED)) {
		ilog(IL_DEBUG, "Checkpoint indicated SS_SYNC_MODIFIED can "
		    "be skipped for %{}", isi_cbm_file_fmt(file_));
		return;
	}

	SYNC_FAIL_POINT_CHANGE_SIZE(sync_modified_regions_err_1, file_->lin);

	get_sync_mod_resume_context(log, initial_offset,
	    master_cdo, master, syncer, error_out);

	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to get_sync_mod_resume_context for %{}",
		    isi_cbm_file_fmt(file_));
		return;
	}

	// dirty cache iterator
	cbm_cache_iterator cache_iter(file_, initial_offset, stat_.st_size,
	    ISI_CPOOL_CACHE_MASK_DIRTY|ISI_CPOOL_CACHE_MASK_INPROGRESS);

	while (cache_iter.next(&record, &error)) {

                size_t region = record.offset / file_->cacheinfo->get_regionsize();

                // Since we initialize both cache and stat_ structure under the
                // same cacheheader exclusive lock, we must not iterate past the
                // length of the file
                ASSERT((record.offset + record.length) <= stat_.st_size);

		index = record.offset / chunksize;
		end_index = (record.offset + record.length - 1) / chunksize;

		// TBD-nice: relax the following requirement so that
		// the cache range can cross the CDOs.
		if (index != end_index) {
			error = isi_cbm_error_new(CBM_INVALID_CACHE,
			    "Corrupted cache range, it is not"
			    " aligned to the CDO boundary"
			    " offset: %lld len: %lld"
			    " for %{}",
			    record.offset, record.length,
			    isi_cbm_file_fmt(file_));
			goto out;
		}

                // The last region is set in progress in stub_syncer::do_sync()
                // atomically with grabbing the file size, to make sure we don't
                // miss writes  between stat() and mark_in_progress()
                if (region != last_region) {
                        mark_in_progress(region, true, &error);
                        ON_ISI_ERROR_GOTO(out, error);
                }

		off_t chunk_len = get_chunk_len(stat_.st_size, index, chunksize);

		if (record.offset < last_chunk_end)
			sop = SO_MOD;
		else
			sop = SO_NEW;


		cower.get_cow_name(&error);
		if (error) {
			isi_error_add_context(error,
			    "Failed to get COW name for "
			    "file %{}",
			    isi_cbm_file_fmt(file_));
			goto out;
		}

		bool created_entry = false;

		// use the current master CDO or allocate a new
		// master CDO.

		isi_cfm_mapinfo::iterator it;
		get_or_create_map_entry(wip_mapinfo_, current_entry, it,
		    new_master_cdo, index, chunk_len, sop,
		    created_entry, &error);
		if (error) {
			isi_error_add_context(error,
			    "Failed to get_or_create_map_entry for "
			    "file %{}",
			    isi_cbm_file_fmt(file_));
			goto out;
		}

		new_entry = it->second;
		if (master_cdo.get_index() == 0) {

			master_cdo.isi_cpool_copy_master_cdo(
			    new_master_cdo, &error);
			if (error) {
				isi_error_add_context(error,
				    "Failed to copy CDO for %{}",
				    isi_cbm_file_fmt(file_));
				goto out;
			}
		}

		if (!master.is_initialized()) {
			master.init(master_cdo, current_entry,
			    created_entry ? SO_NEW : SO_MOD, false, &error);
			if (error) {
				isi_error_add_context(error,
				    "Failed to init CDO syncer for "
				    "file %{}",
				    isi_cbm_file_fmt(file_));
				goto out;
			}
		}

		if (!syncer.is_initialized()) {
			syncer.init(master, current_entry, chunksize,
			    index, chunk_len, sop);
		}

		if (index > syncer.index()) {
			// now we have a new CDO, write
			// the change for the last CDO
			SYNC_FAIL_POINT_PRE(sync_sub_cdo_err_1);

			master.sync_sub_cdo(syncer, &error);

			SYNC_FAIL_POINT_POS(sync_sub_cdo_err_1);

			if (error) {
				isi_error_add_context(error,
				    "Failed to sync for %{}",
				    isi_cbm_file_fmt(file_));
				goto out;
			}

			current_entry = new_entry;
			// update the CDO syncer for a new CDO
			syncer.init(master, current_entry, chunksize,
			    index, chunk_len, sop);
		}

		if (master_cdo.get_offset() !=
		    new_master_cdo.get_offset()) {
			// we have a new master cdo, sync the current one

			SYNC_FAIL_POINT_PRE(sync_mcdo_err_1);
			master.do_sync(&error);
			SYNC_FAIL_POINT_POS(sync_mcdo_err_1);
			if (error) {
				isi_error_add_context(error,
				    "Failed to sync for %{}",
				    isi_cbm_file_fmt(file_));
				goto out;
			}

			master_cdo.isi_cpool_copy_master_cdo(
			    new_master_cdo, &error);
			if (error) {
				isi_error_add_context(error,
				    "Failed to copy CDO for %{}",
				    isi_cbm_file_fmt(file_));
				goto out;
			}

			// update the master syncer for new master cdo
			master.init(
			    master_cdo, current_entry, sop, false, &error);
			if (error) {
				isi_error_add_context(error,
				    "Failed to init CDO syncer for %{}",
				    isi_cbm_file_fmt(file_));
				goto out;
			}
		} else
			master.update_map_entry(current_entry);

		if (sop == SO_MOD)
			syncer.add_range(record);
	}
	if (error) {
		isi_error_add_context(error,
		    "Failed to get the cache range for %{}",
		    isi_cbm_file_fmt(file_));
		goto out;
	}

	if (syncer.is_initialized()) {
		ASSERT(master.is_initialized());
		// The following takes care of the last CDO to be written.

		SYNC_FAIL_POINT_PRE(sync_sub_cdo_err_2);

		master.sync_sub_cdo(syncer, &error);

		SYNC_FAIL_POINT_POS(sync_sub_cdo_err_2);

		if (error) {
			isi_error_add_context(error,
			    "Failed to sync for %{}",
			    isi_cbm_file_fmt(file_));
			goto out;
		}
	}
	if (master.is_initialized()) {
		master.do_sync(&error);
		if (error) {
			isi_error_add_context(error,
			    "Failed to sync for %{}",
			    isi_cbm_file_fmt(file_));
			goto out;
		}
	}
 out:

	if (log.get_modified())
		log.log_stage_completion(SS_SYNC_MODIFIED, error);

	if (error)
		isi_error_handle(error, error_out);
}

void
stub_syncer::get_cow_cdo_resume_context(sync_log &log, int &start_index,
    struct isi_error **error_out)
{
	sync_section_header cow_header;
	bool cow_header_loaded = false;
	sync_record cow_rec;

	start_index = -1;
	if (log.is_log_loaded()) {
		cow_header_loaded = log.get_cow_header(cow_header,
		    cow_rec, error_out);

		if (*error_out) {
			isi_error_add_context(*error_out,
			    "Failed to get_mod_header for %{}",
			    isi_cbm_file_fmt(file_));
			return;
		}
	}

	if (cow_header_loaded) {
		if (cow_rec.status_ == SSS_COMPLETED) {
			// this CDO has been completed
			// go to the next
			start_index = cow_rec.idx_ + 1;
		}
		else {
			// this CDO need to be retried
			start_index =  cow_rec.idx_;
		}
	}
}

/**
 * This handle the cowing the cdos
 */
void
stub_syncer::handle_cow_cdos(struct isi_error **error_out)
{
	sync_log &log = get_log();
	cow_helper &cower = get_cow_helper();
	isi_error *error = NULL;
	bool tried = false;

	if (log.need_to_do_stage(SS_COW_CDOS)) {
		if (!log.get_modified() && stat_.st_size ==
		    mapinfo_.get_filesize()) {
			// did not find any modified part nor the file size
			// shrink no deletion as for now. no change
			return;
		}

		int start_index = -1;

		get_cow_cdo_resume_context(log, start_index, &error);
		if (error) {
			isi_error_add_context(error,
			    "Failed to get_cow_cdo_resume_context for %{}",
			    isi_cbm_file_fmt(file_));
			goto out;
		}

		tried = true;
		// Do the cowing if necessary.
		// this finishing up the cloud CMO and its CDOs.
		cower.cow_cdos(start_index, &error);

		if (error) {
			isi_error_add_context(error,
			    "Failed to cow_cdos for %{}",
			    isi_cbm_file_fmt(file_));
			goto out;
		}
	} else {
		ilog(IL_DEBUG, "Checkpoint indicated SS_COW_CDOS can "
		    "be skipped for %{}", isi_cbm_file_fmt(file_));
	}
 out:
	if(tried)
		log.log_stage_completion(SS_COW_CDOS, error);
	isi_error_handle(error, error_out);
}

void
stub_syncer::get_update_cmo_resume_context(sync_log &log, sync_op &start_op,
    struct isi_error **error_out)
{
	sync_section_header cmo_header;
	bool cmo_header_loaded = false;
	sync_record cmo_rec;
	if (log.is_log_loaded()) {
		cmo_header_loaded = log.get_cmo_header(cmo_header,
		    cmo_rec, error_out);

		if (*error_out) {
			isi_error_add_context(*error_out,
			    "Failed to get_mod_header for %{}",
			    isi_cbm_file_fmt(file_));
			return;
		}
	}

	if (cmo_header_loaded) {
		if (cmo_rec.status_ == SSS_COMPLETED) {
			if (cmo_rec.op_ == SO_MOD || cmo_rec.op_ == SO_CLONE) {
				// this CDO has been completed
				// go to the next
				start_op = SO_SNAP;
			}
			else {
				// we are already done with snap. done
				start_op = SO_DONE;
			}
		}
		else {
			// resume that operation
			start_op = cmo_rec.op_;
		}
	}
}


/**
 * This handle update or clone the cmo
 */
void
stub_syncer::handle_update_cmo(struct isi_error **error_out)
{
	sync_log &log = get_log();
	cow_helper &cower = get_cow_helper();
	isi_error *error = NULL;
	bool tried = false;
	sync_op start_op = SO_INVALID;
	int map_type;
	size_t map_filesize = 0;
	isi_cfm_mapinfo::iterator it;

	if (log.need_to_do_stage(SS_UPDATE_CMO)) {
		if (!log.get_modified() && stat_.st_size ==
		    mapinfo_.get_filesize()) {
			// did not find any modified part nor the file size
			// shrink no deletion as for now. no change
			return;
		}
		// first make sure we truncate the mapping info
		wip_mapinfo().truncate(stat_.st_size, &error);
		ON_ISI_ERROR_GOTO(out, error);

		// Set the mapinfo file size
		it = wip_mapinfo_.last(&error);
		ON_ISI_ERROR_GOTO(out, error);
		if (it != wip_mapinfo_.end()) {
			const isi_cfm_mapentry &entry = it->second;
			map_filesize = entry.get_offset() + entry.get_length();
		}
		if (map_filesize != stat_.st_size) {
			ilog(IL_DEBUG, "Filesize changed after WB started "
			    "from %lu to %lu for %{}", stat_.st_size,
			    map_filesize, isi_cbm_file_fmt(file_));
		}
		wip_mapinfo_.set_filesize(map_filesize);

		// Check map content is well formed.
		// Do this here before we write the CMO; such a
		// write precedes the mapinfo write (to disk) in the next stage.
		// Even though it may still be marked as a wip_mapinfo,
		// mark it as non-wip so that a full verification can be done.
		map_type = wip_mapinfo_.get_type();
		wip_mapinfo_.set_type(map_type & ~ISI_CFM_MAP_TYPE_WIP);
		wip_mapinfo_.verify_map(&error);
		wip_mapinfo_.set_type(map_type);
		ON_ISI_ERROR_GOTO(out, error);

		// Save the verified mapping info before we write it
		log_.log_mapinfo(wip_mapinfo_, &error);
		ON_ISI_ERROR_GOTO(out, error);

		tried = true;

		get_update_cmo_resume_context(log, start_op, &error);
		ON_ISI_ERROR_GOTO(out, error);

		if (start_op == SO_DONE) {
			ilog(IL_DEBUG, "Sync records indicates the operation "
			    "has already been finished for %{}",
			    isi_cbm_file_fmt(file_));
			goto out;
		}

		// Do the cowing if necessary.
		// this finishing up the cloud CMO and its CDOs.
		cower.update_or_cow_cmo(start_op, &error);

		if (error) {
			isi_error_add_context(error,
			    "Failed to update_or_cow_cmo for %{}",
			    isi_cbm_file_fmt(file_));
			goto out;
		}
	} else {
		ilog(IL_DEBUG, "Checkpoint indicated SS_UPDATE_CMO can "
		    "be skipped for %{}", isi_cbm_file_fmt(file_));
	}
 out:
	if(tried)
		log.log_stage_completion(SS_UPDATE_CMO, error);

	isi_error_handle(error, error_out);
}

void
stub_syncer::get_truncation_resume_context(sync_log &log, int &start_index,
    struct isi_error **error_out)
{
	sync_section_header trunc_header;
	bool trunc_header_loaded = false;
	sync_record trunc_rec;
	start_index = -1;
	if (log.is_log_loaded()) {
		trunc_header_loaded = log.get_trunc_header(trunc_header,
		    trunc_rec, error_out);

		if (*error_out) {
			isi_error_add_context(*error_out,
			    "Failed to get_mod_header for %{}",
			    isi_cbm_file_fmt(file_));
			return;
		}
	}

	if (trunc_header_loaded) {
		if (trunc_rec.status_ == SSS_COMPLETED) {
			// this CDO has been completed
			// go to the next
			start_index = trunc_rec.idx_ + 1;
		}
		else {
			// this CDO need to be retried
			start_index =  trunc_rec.idx_;
		}
	}
}


void
stub_syncer::load_ckpt_info(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	bool log_ok = log_.load_log(&error);
	ON_ISI_ERROR_GOTO(out, error);

	if (log_ok) {
		wipmapinfo_loaded_ = log_.get_mapping_info(wip_mapinfo_, &error);
		ON_ISI_ERROR_GOTO(out, error);

		sync_state_record ss_record;
		log_.get_ss_record(ss_record);
		filerev_ = log_.get_filerev();

		stat_ = log_.get_stats();
		syncer_id_ = log_.get_syncer_id();

		ilog(IL_DEBUG, "Resuming from checkpoint for %{}. "
		    "sync stage: %s %u, status: %s %u err: %d filerev: %ld "
		    "syncer_id_ : %ld",
		    lin_snapid_fmt(lin_, snapid_),
		    get_sync_stage_str(ss_record.stage_), ss_record.stage_,
		    get_stage_status_str(ss_record.status_), ss_record.status_,
		    ss_record.error_code_, filerev_, syncer_id_);

		is_resume_ = true;
		if (log_.get_ref_state() != SRS_UNKNOWN) {
			cower_.set_referenced(log_.get_ref_state() ==
			    SRS_REF);
		}
	}
 out:
	isi_error_handle(error, error_out);
}

void
stub_syncer::clear_sync_status(const isi_cloud_object_id &obj,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	coi &coi = get_coi();
	scoped_coid_lock coid_lock(coi, obj);
	coid_lock.lock(true, &error);
	ON_ISI_ERROR_GOTO(out, error);

	// clear the under sync status
	coi.update_sync_status(obj, syncer_id_, false, &error);

	ON_ISI_ERROR_GOTO(out, error,
	    "Failed to update_sync_status for %{} "
	    "for cloud object: %#{}",
	    lin_snapid_fmt(lin_, snapid_),
	    isi_cloud_object_id_fmt(&obj));
 out:
	isi_error_handle(error, error_out);
}

void
stub_syncer::setup_sync_context(struct isi_error **error_out)
{
	isi_error *error = NULL;
	cow_helper &cower = get_cow_helper();

	// get the original mapping info
	isi_cph_get_stubmap(file_->fd, mapinfo_, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (!wipmapinfo_loaded_) {
		coi &coi = get_coi();

		isi_cloud_object_id new_coid(mapinfo_.get_object_id());
		int map_type;

		coi.get_last_version(mapinfo_.get_object_id(),
		    last_ver_, &error);
		ON_ISI_ERROR_GOTO(out, error);

		ilog(IL_DEBUG, "Current object: {%s, %ld} "
		    "latest: {%s, %ld}.",
		    mapinfo_.get_object_id().to_c_string(),
		    mapinfo_.get_object_id().get_snapid(),
		    last_ver_.to_c_string(),
		    last_ver_.get_snapid());

		last_entry_.reset(coi.get_entry(last_ver_, &error));
		ON_ISI_ERROR_GOTO(out, error);

		cmoi_entry cmoi_entry;
		coi.get_cmo(mapinfo_.get_object_id(), cmoi_entry,
		    &error);
		ON_ISI_ERROR_GOTO(out, error);

		uint64_t ver = cmoi_entry.get_highest_ver();
		++ver;

		// set the new object_id.
		new_coid.set_snapid(ver);
		if (mapinfo_.get_object_id() != last_ver_) {
			// a version other than the latest is modified
			// and being synced back.
			is_latest_ = false;
		}

		// get wip_mapinfo_ ready
		map_type = wip_mapinfo_.get_type();
		wip_mapinfo_.set_type(map_type | ISI_CFM_MAP_TYPE_WIP);
		wip_mapinfo_.set_object_id(new_coid);

		// TBD-must, check point it before copy.
		if (getfilerev(file_->fd, &filerev_)) {
			error = isi_system_error_new(errno,
			    "Getting initial filerev for %{}",
			    isi_cbm_file_fmt(file_));
			goto out;
		}

		if (!is_resume()) {
                        if ((stat_.st_flags & SF_FILE_STUBBED) == 0) {
				error = isi_system_error_new(EALREADY,
				    "file %{} is not stubbed",
				    isi_cbm_file_fmt(file_));
				goto out;
			}

			// contend with the durable lock
			{
				scoped_coid_lock coid_lock(coi,
				    mapinfo_.get_object_id());
				coid_lock.lock(true, &error);
				ON_ISI_ERROR_GOTO(out, error);
				coi.generate_sync_id(mapinfo_.get_object_id(),
				    syncer_id_, &error);
				ON_ISI_ERROR_GOTO(out, error);
			}

			sync_state_record ss_record = {
			    SS_SYNC_MODIFIED, SSS_STARTED, 0};

			// Now log the header
			log_.create_log_header(stat_, filerev_, syncer_id_,
			    ss_record, &error);
			ON_ISI_ERROR_GOTO(out, error);
		}

		// copy mapinfo + mapentry store (if overflow)
		wip_mapinfo_.copy(mapinfo_, &error);
		ON_ISI_ERROR_GOTO(out, error, "Failed to copy the mapinfo");
	}

	if (is_resume()) {
		log_.load_clone_info_cond(cower, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

 out:
	isi_error_handle(error, error_out);
}

/**
 * Workhorse for sync, traverse through the modified regions and
 * compare with the mapping information in order to achieve the
 * following:
 * 1. creating new CDOs/updating in the cloud
 * 2. COWing of existing CDOs
 * 3. deleting no longer referenced CDOs
 * 4. persisting mapping information
 * 5. marking the cache ranges
 */
void
stub_syncer::sync_internal(struct isi_error **error_out)
{
	isi_error *error = NULL;

	setup_sync_context(&error);
	ON_ISI_ERROR_GOTO(out, error);

	{
		coi &coi = get_coi();
		scoped_coid_lock coid_lock(coi,
		    mapinfo_.get_object_id());
		coid_lock.lock(true, &error);
		ON_ISI_ERROR_GOTO(out, error);

		coi.update_sync_status(mapinfo_.get_object_id(),
		    syncer_id_, true, &error);

		ON_ISI_ERROR_GOTO(out, error,
		    "Failed to update_sync_status for %{} "
		    "for cloud object: %#{}",
		    isi_cbm_file_fmt(file_),
		    isi_cloud_object_id_fmt(&mapinfo_.get_object_id()));

		updated_sync_status_ = true;

		sync_modified_regions(&error);
		ON_ISI_ERROR_GOTO(out, error);

		handle_cow_cdos(&error);
		ON_ISI_ERROR_GOTO(out, error);

		handle_update_cmo(&error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	persist_mapping_info(&error);
	ON_ISI_ERROR_GOTO(out, error);

	mark_synced_cache_regions(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	if (updated_sync_status_ && (log_.log_supported() == false ||
	    error == NULL)) {
		// if updated sync_status and no error or
		// if the caller does not supporting ckpt, clear out sync bit
		struct isi_error *error2 = NULL;
		clear_sync_status(mapinfo_.get_object_id(), &error2);

		if (error == NULL && error2 != NULL) {
			error = error2;
		} else if (error2) {
			ilog(IL_DEBUG, "Failed to clear_sync_status %#{}",
			    isi_error_fmt(error2));
			isi_error_free(error2);
			error2 = NULL;
		}
	}
	if (error)
		isi_error_handle(error, error_out);
}

void
stub_syncer::collect_cloud_objects(const isi_cloud_object_id &obj,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	struct timeval date_of_death = {0};
	gettimeofday(&date_of_death, NULL);

	// We do not need the timing part, the object can be
	// collected immediately.
	coi_->remove_ref(obj, file_->lin, INVALID_SNAPID, date_of_death,
	    NULL, &error);
	if (error != NULL) {
		if (isi_error_is_a(error, CBM_ERROR_CLASS) &&
		    isi_cbm_error_get_type(error) == CBM_LIN_DOES_NOT_EXIST) {
			ilog(IL_DEBUG, "%{} is not found in the COI for "
			    "object: %s. Maybe already removed by last sync. "
			    "error from lower level: %#{}",
			    isi_cbm_file_fmt(file_),
			    obj.to_c_string(),
			    isi_error_fmt(error));
			isi_error_free(error);
			error = NULL;
		} else {
			isi_error_add_context(error,
			    "Failed to coi::remove_ref from %s "
			    "for %{}",
			    obj.to_c_string(),
			    isi_cbm_file_fmt(file_));
		}
	}

	isi_error_handle(error, error_out);
}

void
stub_syncer::mark_synced_cache_regions(struct isi_error **error_out)
{
	isi_cbm_cache_record_info record;

	ilog(IL_DEBUG, "Marking sync completed for range %d, %ld for file %{}",
	    0, stat_.st_size, isi_cbm_file_fmt(file_));

	cbm_cache_iterator cache_iter(file_, 0, stat_.st_size,
	    ISI_CPOOL_CACHE_MASK_DIRTY|ISI_CPOOL_CACHE_MASK_INPROGRESS);

	// mark the region with WIP to cached
	while (cache_iter.next(&record, error_out)) {
		// mark the cache region as cached:
		bool changed = false;

		isi_cbm_cache_status cs = cache_iter.cache().
		    cond_mark_cache_state(
		    record.offset / mapinfo().get_readsize(),
		    ISI_CPOOL_CACHE_INPROGRESS, ISI_CPOOL_CACHE_CACHED,
		    changed, error_out);
		if (*error_out) {
			isi_error_add_context(*error_out,
			    "Failed to cond_mark_cache_state for %{}",
			    isi_cbm_file_fmt(file_));
			return;
		}

		if (!changed && cs != ISI_CPOOL_CACHE_DIRTY) {
			*error_out = isi_cbm_error_new(CBM_INVALID_CACHE,
			    "Corrupted cache range, the state changed under"
			    " the syncer."
			    " offset: %lld len: %lld, state: %d"
			    " for %{}",
			    record.offset, record.length, cs,
			    isi_cbm_file_fmt(file_));
			return;
		}

		if (!changed)
			change_detected_ = true;
	}
	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to get the cache range for %{}",
		    isi_cbm_file_fmt(file_));
		return;
	}
}

stub_syncer::stub_syncer(uint64_t lin, uint64_t snapid,
    get_ckpt_data_func_t get_ckpt_cb,
    set_ckpt_data_func_t set_ckpt_cb,
    void *ctx, isi_cbm_sync_option &opt)
    : file_(NULL), lin_(lin), snapid_(snapid), filerev_(0),
    log_(this, get_ckpt_cb, set_ckpt_cb, ctx), change_detected_(false),
    is_resume_(false), cower_(this), opt_(opt),
    is_latest_(true), updated_sync_status_(false), syncer_id_(0),
    wipmapinfo_loaded_(false)
{
	bzero(&stat_, sizeof(stat_));
}

stub_syncer::~stub_syncer()
{
	if (file_) {
		isi_cbm_file_close(file_, isi_error_suppress());
		file_ = NULL;
	}
}

void
stub_syncer::mark_sync_in_progress(isi_error **error_out)
{
	isi_error *error = NULL;
	bool ch_locked = false;

	cpool_cache &cacheinfo = *file_->cacheinfo;

	if (!cacheinfo.is_cache_open()) {
		cacheinfo.cache_open(file_->fd,
		    false, false, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	cacheinfo.cacheheader_lock(true, &error);
	ON_ISI_ERROR_GOTO(out, error);

	ch_locked = true;

	// clear the dirty bit and set the sync bit on
	cacheinfo.set_sync_state(false, true, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	if (ch_locked) {
		cacheinfo.cacheheader_unlock(isi_error_suppress());
	}

	isi_error_handle(error, error_out);
}

void
stub_syncer::mark_sync_done_progress(isi_error **error_out)
{
	isi_error *error = NULL;
	bool ch_locked = false;
	bool sync = true, dirty = false;

	cpool_cache &cacheinfo = *file_->cacheinfo;

	if (!cacheinfo.is_cache_open()) {
		cacheinfo.cache_open(file_->fd,
		    false, false, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	cacheinfo.cacheheader_lock(true, &error);
	ON_ISI_ERROR_GOTO(out, error);

	ch_locked = true;
	cacheinfo.get_sync_state(dirty, sync, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (dirty) {
		// need to queue the item, as the file has been
		// changed since we began working on it
		isi_cbm_file_queue_daemon(file_, ISI_CBM_CACHE_FLAG_DIRTY,
		    &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	// set the dirty bit and clear the sync bit on
	cacheinfo.set_sync_state(dirty, false, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	if (ch_locked) {
		cacheinfo.cacheheader_unlock(isi_error_suppress());
	}

	isi_error_handle(error, error_out);
}

void stub_syncer::mark_sync_done_not_permit_list(isi_error **error_out)
{
        isi_error *error = NULL;
        bool ch_locked = false;

        cpool_cache &cacheinfo = *file_->cacheinfo;

        if (!cacheinfo.is_cache_open()) {
                cacheinfo.cache_open(file_->fd,
                    false, false, &error);
                ON_ISI_ERROR_GOTO(out, error);
        }

        cacheinfo.cacheheader_lock(true, &error);
        ON_ISI_ERROR_GOTO(out, error);

        ch_locked = true;

        /*
        Cluster is not in permit list, WB failed and do not need
        to retry.When the cluster is readded and the next write
        comes in, the file will be WB to cloud
        */

        cacheinfo.set_sync_state(false, false, &error);
        ON_ISI_ERROR_GOTO(out, error);

 out:
        if (ch_locked) {
                cacheinfo.cacheheader_unlock(isi_error_suppress());
        }

        isi_error_handle(error, error_out);

}


void
stub_syncer::do_sync(struct isi_error **error_out)
{
	isi_error *error = NULL;
	scoped_sync_lock sync_lk;
	bool sync_marked = false;
	bool has_permit_error = false;
	struct timeval stime = {0};
	struct timeval now = {0};
	int flags = 0;
        bool cache_locked = false;
	int posix_error = 0;

        // Recovers stat_ structure in case of resume
	load_ckpt_info(&error);
	ON_ISI_ERROR_GOTO(out, error);

	coi_ = isi_cbm_get_coi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	file_ = isi_cbm_file_open(lin_, snapid_, flags, &error);
	if (error) {
		bool to_rollback = false;
		// stub file gone, try a rollback if there is ckpt
		if (isi_system_error_is_a(error, ENOENT) ||
		    isi_cbm_error_is_a(error, CBM_NOT_A_STUB)) {
			ilog(IL_DEBUG, "do_sync not a stub file, or not found: "
			 "%#{}", isi_error_fmt(error));
			isi_error_free(error);
			error = NULL;
			to_rollback = true;
		} else {
			isi_error_add_context(error, "Opening %{}",
			    lin_snapid_fmt(lin_, snapid_));
		}

		if (to_rollback && is_resume()) {
			do_rollback(&error);
			ON_ISI_ERROR_GOTO(out, error);

			// clear the sync status

			clear_sync_status(wip_mapinfo_.get_object_id(), &error);
			ON_ISI_ERROR_GOTO(out, error);
		}

		goto out;
	}

        // To handle extend of last region during write-back, do the following:
        // 1. Grab cache header lock shared, to ensure regions number does not change
        // 2. Mark last region INPROGRESS
        // 3. fstat() to get file size, which determines the size of the last region
        //
        // It is possible for the last region extend to happen between setting
        // INPROGRESS and fstat(), since cache header lock is taken exclusive
        // only when the number of regions changes.  However, it's not a problem,
        // since in this case file extend will re-set last region back to DIRTY
        // and it will be written back again later (OTOH if fstat() would happen
        // first, we would miss extend writes in-between fstat() and
        // mark_in_progress()).  See bugs 166905 and 128135.
        file_->cacheinfo->cacheheader_lock(false, &error);
        ON_ISI_ERROR_GOTO(out, error);
        cache_locked = true;

        file_->cacheinfo->cache_open(file_->fd, true, true, &error);
        if (error) {
                if (isi_system_error_is_a(error, ENOENT)) {
                        // The whole file is not cached.
                        isi_error_free(error);
                        error = NULL;
                        goto out;
                }
                ON_ISI_ERROR_GOTO(out, error, "Failed to call "
                    "cpool_cache::cache_open for %{}", isi_cbm_file_fmt(file_));
        }


	if (!is_resume() ||
	    (is_resume() && log_.need_to_do_stage(SS_SYNC_MODIFIED))) {
		// attempt to mark the last region as in-progress, if it's DIRTY
		mark_in_progress(file_->cacheinfo->get_last_region(), false,
		    &error);
		ON_ISI_ERROR_GOTO(out, error);

		posix_error = fstat(file_->fd, &stat_);
		if (posix_error) {
			error = isi_system_error_new(errno,
			    "Getting stat information for %{}",
			    isi_cbm_file_fmt(file_));
			goto out;
		}
	}

        file_->cacheinfo->cacheheader_unlock(&error);
        ON_ISI_ERROR_GOTO(out, error);
        cache_locked = false;

	/*
	 * 'true' means it is ok to sync when the guid is being removed
	 * from the permit list
	 */
	cluster_permission_check(file_, NULL, true, &error);

	if(error) {
		if (isi_cbm_error_is_a(error, CBM_NOT_IN_PERMIT_LIST)) {
			ilog(IL_DEBUG,"Writeback did not occur as cluster is not in "
                        "the permit list. File %{} is not added for sync back.",
                        lin_snapid_fmt(lin_, snapid_));
			/*
			Ensure that the file gets picked up again
			during the next modification
			*/
			has_permit_error = true;
		}
		goto out;
	}
	ilog(IL_DEBUG, "Syncing %{}", lin_snapid_fmt(lin_, snapid_));

	// First acquire the synclock. Try lock.
	// If in the unlikely event it is already locked, we should just
	// return. Normally this is covered in the job layer. But the CBM test
	// util or any other programs with the sync can contend.
	sync_lk.lock_ex(file_->fd, opt_.blocking, &error);
	if (error) {
		isi_error_add_context(error,
		    "Failed to acquire the sync lock for CBM file %{}",
		    isi_cbm_file_fmt(file_));
		goto out;
	}

        if (!opt_.skip_settle) {
		stime.tv_sec = stat_.st_mtime;
		isi_cbm_file_get_settle_time(file_->lin, file_->snap_id,
		    file_->mapinfo->get_policy_id(), STT_WRITEBACK,
		    stime, &error);
		if (error) {
			isi_error_add_context(error,
			    "Failed to acquire the sync lock for CBM file %{}",
			    isi_cbm_file_fmt(file_));
			goto out;
		}
		gettimeofday(&now, NULL);

		if (now.tv_sec < stime.tv_sec) {
			isi_cbm_wbi_sptr wb;
			wb = isi_cbm_get_wbi(&error);
			if (error) {
				isi_error_add_context(error,
				    "Cannot get WBI DB for %{}",
				    isi_cbm_file_fmt(file_));
				goto out;
			}

			/*
			 * settle time can be set to a later time due to more
			 * recent write on same file. In this case, we don't
			 * touch the WBI entry created by initial write.
			 * Instead, requeue a new WBI entry with new settle time
			 * (161589)
			 */
			wb->add_cached_file(file_->lin, file_->snap_id,
			    stime, &error);
			if (error) {
				isi_error_add_context(error,
				    "Failed to requeue WBI entry for %{}",
				    isi_cbm_file_fmt(file_));
				goto out;
			}

			static const char *TIME_FMT = "%Y-%m-%d %H:%M:%S GMT";
			time_t tr = stime.tv_sec;
			struct tm gmt;
			gmtime_r(&tr, &gmt);
			char timebuf[64];
			strftime(timebuf, sizeof(timebuf), TIME_FMT, &gmt);
			// the file has not settled yet, rescheduled for later
			ilog(IL_TRACE,
			    "The file has not settled yet, writeback requeued "
			    "for later than %s for %{}.",
			    timebuf, isi_cbm_file_fmt(file_));
			goto out;
		}
	}

	if (!is_resume()) {
		mark_sync_in_progress(&error);
		if (error) {
			if (isi_system_error_is_a(error, ENOENT)) {
				// the cache has not been setup yet, no changes
				ilog(IL_DEBUG, "File %{} has no cache information. "
				    "The sync operation is skipped. Error: %#{}",
				    isi_cbm_file_fmt(file_),
				    isi_error_fmt(error));
				isi_error_free(error);
				error = NULL;
			} else {
				isi_error_add_context(error,
				    "Failed to mark the sync state in the cache "
				    "for %{}",
				    isi_cbm_file_fmt(file_));
			}
			goto out;
		}
	}

	sync_marked = true;
	sync_internal(&error);
	if (error) {
		isi_error_add_context(error,
		    "Failed to sync the CBM file %{}",
		    isi_cbm_file_fmt(file_));
		goto out;
	}
out:
        if (cache_locked)
                file_->cacheinfo->cacheheader_unlock(isi_error_suppress());
        if (sync_marked) {
		isi_error *error2 = NULL;
		mark_sync_done_progress(&error2);
		if (error2) {
			isi_error_add_context(error2,
			    "Failed to mark the sync state in the cache "
			    "for %{}",
			    isi_cbm_file_fmt(file_));
		}

		if (!error)
			error = error2;
		else if (error2) {
			// the cache has not been setup yet, no changes
			ilog(IL_DEBUG, "Failed to update sync status for "
			    "file %{}. Error: %#{}",
			    isi_cbm_file_fmt(file_),
			    isi_error_fmt(error2));
			isi_error_free(error2);
			error2 = NULL;
		}
	}
	if (has_permit_error) {
		ASSERT(error != NULL,
		    "has_permit_error implies error is not null");
                isi_error *error2 = NULL;
                mark_sync_done_not_permit_list(&error2);
                if (error2) {
                        isi_error_add_context(error2,
                            "Failed to mark the sync state in the cache "
                            "for %{}",
                            isi_cbm_file_fmt(file_));

			// the cache has not been setup yet, no changes
                        ilog(IL_DEBUG, "Failed to update sync status for "
                            "file %{}. Error: %#{}",
                            isi_cbm_file_fmt(file_),
                            isi_error_fmt(error2));
                        isi_error_free(error2);
                        error2 = NULL;
                }
        }
	if (error) {
		ilog(IL_DEBUG, "Finished syncing %{}, error: %#{}",
		    lin_snapid_fmt(lin_, snapid_), isi_error_fmt(error));
		isi_error_handle(error, error_out);
	} else
		ilog(IL_DEBUG, "Finished syncing %{} successfully",
		    lin_snapid_fmt(lin_, snapid_));
}

void
stub_syncer::do_rollback(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	isi_cbm_ioh_base_sptr ioh;
	struct cl_object_name cloud_obj_name;
	isi_cbm_account acct;

	// exit if mapinfo is not loadable
	if (!log_.get_mapping_info(wip_mapinfo_, &error) || error)
		goto out;

	// get account by id
	{
		scoped_ppi_reader rdr;
		isi_cfm_policy_provider *ppi =
		    const_cast<isi_cfm_policy_provider *>(rdr.get_ppi(&error));
		ON_ISI_ERROR_GOTO(out, error);

		isi_cfm_get_account(ppi,
		    wip_mapinfo_.get_account(), acct, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	ioh.reset(isi_cbm_ioh_creator::get_io_helper(acct.type, acct));

	cloud_obj_name.container_cmo = wip_mapinfo_.get_container();
	cloud_obj_name.container_cdo = "unused";
	cloud_obj_name.obj_base_name = wip_mapinfo_.get_object_id();

	isi_cbm_remove_cloud_objects(wip_mapinfo_, NULL, NULL,
	    cloud_obj_name, ioh.get(), true, NULL, NULL, NULL, &error);

 out:
	isi_error_handle(error, error_out);
}

} // private namespace

void
cbm_sync_register_pre_persist_map_callback(pre_persist_map_callback callback)
{
	g_pre_save_map_callback = callback;
}

void isi_cbm_sync_opt(uint64_t lin, uint64_t snapid,
    get_ckpt_data_func_t get_ckpt_cb, set_ckpt_data_func_t set_ckpt_cb,
    void *ctx, isi_cbm_sync_option &opt, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	stub_syncer syncer(lin, snapid, get_ckpt_cb, set_ckpt_cb, ctx, opt);

	/*
	 * We currently can't writeback snapshots or write-restricted lins
	 */
	if (snapid != HEAD_SNAPID) {
		error = isi_cbm_error_new(CBM_PERM_ERROR,
		    "Invalid snap id (lin %{} snapid %{})",
		    lin_fmt(lin), snapid_fmt(snapid));
		goto out;
	}
	if (linsnap_is_write_restricted(lin, snapid)) {
		error = isi_cbm_error_new(CBM_RESTRICTED_MODIFY,
		    "Restricted file (lin %{} snapid %{})",
		    lin_fmt(lin), snapid_fmt(snapid));
		goto out;
	}

	syncer.do_sync(&error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);
}

/*
 * Interface for synchronizing modified data to the cloud storage.
 * @param lin[IN]: the LIN id of the stub
 * @param snapid[IN]: the snapid of the stub
 * @param get_ckpt_cb[IN]: the callback function for get the checkpoint data
 * @param set_ckpt_cb[IN]: the callback function for set the checkpoint data
 * @param ctx[IN]: the context pointer to pass to the callbacks
 * @param error_out[OUT]: errors if any, see above for detailed information.
 */
void
isi_cbm_sync(uint64_t lin, uint64_t snapid,
    get_ckpt_data_func_t get_ckpt_cb, set_ckpt_data_func_t set_ckpt_cb,
    void *ctx, struct isi_error **error_out)
{

	isi_cbm_sync_option opt = {blocking: false, skip_settle: false};
	isi_cbm_sync_opt(lin, snapid, get_ckpt_cb, set_ckpt_cb, ctx, opt,
	    error_out);
}
