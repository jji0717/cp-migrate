#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <signal.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#include <sys/isi_stats_protocol.h>
#include <sys/isi_stats_siq.h>

#include <isi_util/isi_assert.h>
#include "isirep.h"
#include "msg_internal.h"

ISI_HASH_DEFINE(H_SIQ_RIPT_MSG_FIELDS, "siq Riptide msg fields");
ISI_HASH_DEFINE(H_SIQ_RIPT_MSG_FIELD_VERS, "siq Riptide msg field versions");

#define ALIGNMENT_PTR   sizeof(char *)
#define ALIGN_BYTES(x)	(ALIGNMENT_PTR - ((uintptr_t)x % ALIGNMENT_PTR))
#define ALIGN_PTR(x)	(x += (uintptr_t)x % ALIGNMENT_PTR ? ALIGN_BYTES(x) : 0)

/* Only use this with arrays that are initialized
 * to prevent a divide by zero scenario. */
#define ARRAY_SIZE(x) (sizeof(x)/sizeof(x[0]))

struct timeval now = {0, 0};

static bool do_msg_dump = false;

static char *last_msg_from;

/* Keep log of SyncIQ RPC messages */
#define MSG_LOG_SIZE 100
struct msglog {
	unsigned      type;
	unsigned      len;
	unsigned long cnt;
};

struct msglog sendmsglog[MSG_LOG_SIZE];
struct msglog recvmsglog[MSG_LOG_SIZE];

/* Total number of RPC messages sent for this job */
unsigned long totmsgcnt = 0;
unsigned long sendmsgcnt = 0;
unsigned long recvmsgcnt = 0;

bool force_process_stop;

#define SEND_RPC_MSG_LOG(m) \
	    do { \
	        sendmsglog[sendmsgcnt % MSG_LOG_SIZE].type = m->head.type; \
	        sendmsglog[sendmsgcnt % MSG_LOG_SIZE].len = m->head.len; \
	        sendmsglog[sendmsgcnt % MSG_LOG_SIZE].cnt = totmsgcnt; \
	        sendmsgcnt++; \
		totmsgcnt++; \
	    } while (0);

#define RECV_RPC_MSG_LOG(m) \
	    do { \
	        recvmsglog[recvmsgcnt % MSG_LOG_SIZE].type = m.head.type; \
	        recvmsglog[recvmsgcnt % MSG_LOG_SIZE].len = m.head.len; \
	        recvmsglog[recvmsgcnt % MSG_LOG_SIZE].cnt = totmsgcnt; \
	        recvmsgcnt++; \
		totmsgcnt++; \
	    } while (0);

static struct mesgtab msgs[] = {
	{REP_MSG, "REP_MSG", "uu", {"len", "type"}, sizeof(struct rep_msg)},

	{NOOP_MSG, "NOOP_MSG",	""},

	{CONN_MSG, "CONN_MSG", 0},

	{DISCON_MSG, "DISCON_MSG"},

	{ERROR_MSG, "ERROR_MSG", "uus", {"error", "io", "str"},
	 sizeof(struct error_msg)},

	{GROUP_MSG, "GROUP_MSG", "u", {"group"}, sizeof(unsigned)},

	{OLD_CLUSTER_MSG, "OLD_CLUSTER_MSG", "sb", {"name", "devs"},
	 sizeof(struct old_cluster_msg)},

	{KILL_MSG, "KILL_MSG", "ss", {"config", "set"},
	 sizeof(struct kill_msg)},

	{AUTH_MSG, "AUTH_MSG", "us", {"salt", "pass"},
	 sizeof(struct auth_msg)},

	{OLD_DATA_MSG, "DATA_MSG", "d", {"size"}, sizeof(struct old_data_msg)},

	{DONE_MSG, "DONE_MSG", "u", {"count"}, sizeof(struct done_msg)},

	{OLD_WORK_INIT_MSG, "OLD_WORK_INIT_MSG",
	 "ssssssuuussuu", {"target",
			   "peer",
			   "path",
			   "src", 
			   "bw",
			   "predicate",
			   "last_runtime", 
			   "delete_secs",
			   "flags",
			   "pass", 
			   "jobid",
			   "log",
			   "action"}, sizeof(struct old_work_init_msg)},

	{WORK_REQ_MSG, "WORK_REQ_MSG", "uuuuUuuu", {"errors",
						    "dirs",
						    "files",
						    "transfered",
						    "net",
						    "deleted",
						    "purged",
						    "initial"},
	 sizeof(struct work_req_msg)},

	{BANDWIDTH_INIT_MSG, "BANDWIDTH_INIT_MSG", "suu", {"policy_name", 
							    "worker_count", 
							    "priority"}, 
	 sizeof(struct bandwidth_init_msg)},

	{THROTTLE_INIT_MSG, "THROTTLE_INIT_MSG", "suu", {"policy_name", 
							 "worker_count", 
							 "priority"}, 
	 sizeof(struct throttle_init_msg)},

	{BANDWIDTH_MSG, "BANDWIDTH_MSG", "u", {"ration"},
	 sizeof(struct bandwidth_msg)},

	{THROTTLE_MSG, "THROTTLE_MSG", "u", {"ration"},
	 sizeof(struct throttle_msg)},

	{BANDWIDTH_STAT_MSG, "BANDWIDTH_STAT_MSG", "su", {"name",
							 "bytes_sent"},
	 sizeof(struct bandwidth_stat_msg)},

	{THROTTLE_STAT_MSG, "THROTTLE_STAT_MSG", "su", {"name",
							"files_sent"},
	 sizeof(struct throttle_stat_msg)},

	{ADS_MSG, "ADS_MSG", "u", {"start"}, sizeof(struct ads_msg)},

	{OLD_DIR_MSG3, "OLD_DIR_MSG3", "suuuuuusbu", {"dir",
						      "mode",
						      "uid",
						      "gid",
						      "atime",
						      "mtime",
						      "reap",
						      "acl",
						      "encs",
						      "di_flags"},
	 sizeof(struct old_dir_msg3)},

	{DIR_MSG4, "DIR_MSG4", "suuuuuusbuUu", {"dir",
						"mode",
						"uid",
						"gid",
						"atime",
						"mtime",
						"reap",
						"acl",
						"encs",
						"di_flags",
						"dir_lin",
						"need_dir_ack"},
	 sizeof(struct dir_msg4)},

	{OLD_SWORKER_TW_STAT_MSG, "SWORKER_TW_STAT_MSG",
	 "uuuuuuuuuuuuUUUU", {"dirs_visited",
			      "dirs_dst_deleted",
			      "files_total",
			      "files_replicated_selected",
			      "files_replicated_transferred",
			      "files_deleted_src",
			      "files_deleted_dst",
			      "files_skipped_up_to_data",
			      "files_skipped_user_conflict",
			      "files_skipped_error_io",
			      "files_skipped_error_net",
			      "files_skipped_error_checksum",
			      "bytes_transferred",
			      "bytes_recoverable",
			      "bytes_recovered_src",
			      "bytes_recovered_dst"},
	 sizeof(struct old_sworker_tw_stat_msg)},

	{JOBCTL_MSG, "JOBCTL_MSG", "u", {"state"}, sizeof(struct jobctl_msg)},

	{LIST_MSG, "LIST_MSG", "ud", {"last", "used"},
	 sizeof(struct list_msg)},

	{LIST_MSG2, "LIST_MSG2", "uuuud", {"range_begin",
					   "range_end",
					   "continued", 
					   "last", 
					   "used"},
	 sizeof(struct list_msg2)},

	{SNAP_MSG, "SNAP_MSG", "sssu", {"name",
					"alias",
					"path",
					"expiration"},
	 sizeof(struct snap_msg)},

	{SNAP_RESP_MSG, "SNAP_RESP_MSG", "us", {"result", "msg"},
	 sizeof(struct snap_resp_msg)},

	{OLD_WORK_INIT_MSG4, "OLD_WORK_INIT_MSG4",
	 "sssssssUuuussuussuusuUusuu", {"target",
				    "peer",
				    "path",
				    "src",
				    "bw",
				    "predicate",
				    "sync_id",
				    "run_id",
				    "last_runtime",
				    "delete_secs",
				    "flags",
				    "pass",
				    "jobid",
				    "log",
				    "action",
				    "src2",
				    "basesrc",
				    "old_time",
				    "new_time",
				    "restrict_by",
				    "hash_type",
				    "min_hash_piece_len",
				    "hash_pieces_per_file",
				    "recovery_dir",
				    "skip_bb_hash",
				    "worker_id"},
	 sizeof(struct old_work_init_msg4)},

	{OLD_WORK_INIT_MSG5, "OLD_WORK_INIT_MSG5",
	 "sssssssUuussuuuusuUusuuUU", {"target",
				    "peer",
				    "path",
				    "src",
				    "bw",
				    "predicate",
				    "sync_id",
				    "run_id",
				    "last_runtime",
				    "flags",
				    "pass",
				    "jobid",
				    "log",
				    "action",
				    "old_time",
				    "new_time",
				    "restrict_by",
				    "hash_type",
				    "min_hash_piece_len",
				    "hash_pieces_per_file",
				    "recovery_dir",
				    "skip_bb_hash",
				    "worker_id",
				    "prev_snapid",
				    "cur_snapid"},
	 sizeof(struct old_work_init_msg5)},
	
	{OLD_WORK_INIT_MSG6, "OLD_WORK_INIT_MSG6",
	 "sssssssUuussuuuusuUusuuUUUu", {"target",
				    "peer",
				    "path",
				    "src",
				    "bw",
				    "predicate",
				    "sync_id",
				    "run_id",
				    "last_runtime",
				    "flags",
				    "pass",
				    "jobid",
				    "log",
				    "action",
				    "old_time",
				    "new_time",
				    "restrict_by",
				    "hash_type",
				    "min_hash_piece_len",
				    "hash_pieces_per_file",
				    "recovery_dir",
				    "skip_bb_hash",
				    "worker_id",
				    "prev_snapid",
				    "cur_snapid",
				    "domain_id",
				    "domain_generation"},
	 sizeof(struct old_work_init_msg6)},

	{WORK_RESP_MSG3, "WORK_RESP3", "U", {"wi_lin"},
	 sizeof(struct work_resp_msg3)},

	{SPLIT_REQ_MSG, "SPLIT_REQ", "U", {"split_wi_lin"},
	 sizeof(struct split_req_msg)},

	{SPLIT_RESP_MSG, "SPLIT_RESP", "u", {"succeeded"},
	 sizeof(struct split_resp_msg)},

	{OLD_FILE_MSG4, "FILE_MSG4", "uUuuuususuUUU", {"mode",
						   "size",
						   "uid",
						   "gid",
						   "atime",
						   "mtime",
						   "file",
						   "enc",
						   "acl",
						   "di_flags",
						   "rev",
						   "dir_lin",
						   "filelin"},
	 sizeof(struct old_file_msg4)},

	{OLD_SYMLINK_MSG4, "SYMLINK_MSG4", "uuuuussuuuUUU", {"mode",
							 "uid",
							 "gid",
							 "atime",
							 "mtime",
							 "name",
							 "target",
							 "nenc",
							 "tenc",
							 "di_flags",
							 "rev",
							 "dir_lin",
							 "filelin"},
	 sizeof(struct old_symlink_msg4)},

	{OLD_ACK_MSG2, "ACK_MSG2", "suUUUuuuuU", {"file",
						"enc",
						"rev",
						"filesize",
						"dir_lin",
						"result",
						"io_errors",
						"checksum_errors",
						"transfered",
						  "filelin"},
	 sizeof(struct old_ack_msg2)},

	{POSITION_MSG, "POSITION_MSG", "ussuUsbU", {"reason",
						    "comment",
						    "file",
						    "enc",
						    "filelin",
						    "dir",
						    "evec",
						    "dirlin"},
	 sizeof(struct position_msg)},

	{ACK_DIR_MSG, "ACK_DIR_MSG", "sbu", {"dir",
					     "dir_evec",
					     "is_new_dir"},
	 sizeof(struct ack_dir_msg)},

	{SHORT_HASH_RESP_MSG, "SHORT_HASH_RESP_MSG", "Uu", {"src_dkey",
							    "sync_state"},
	 sizeof(struct short_hash_resp_msg)},

	{FULL_HASH_DIR_REQ_MSG, "FULL_HASH_DIR_REQ_MSG",
	 "sbU", {"dir",
		 "evec",
		 "src_dlin"}, sizeof(struct full_hash_dir_req_msg)},

	{FULL_HASH_FILE_REQ_MSG, "FULL_HASH_FILE_REQ_MSG",
	 "suUUU", {"file",
		   "enc",
		   "size",
		   "src_dlin",
		   "src_dkey"}, sizeof(struct full_hash_file_req_msg)},

	{FULL_HASH_FILE_RESP_MSG, "FULL_HASH_FILE_RESP_MSG",
	 "suUUUUsuuu", {"file",
			"enc",
			"src_dlin",
			"src_dkey",
			"offset",
			"len",
			"hash_str",
			"type",
			"status",
			"origin"}, sizeof(struct full_hash_file_resp_msg)},

	{HASH_STOP_MSG, "HASH_STOP_MSG", "usuUU", {"stop_type",
						   "file",
						   "enc",
						   "src_dlin",
						   "src_dkey"},
	 sizeof(struct hash_stop_msg)},

	{HASH_ERROR_MSG, "HASH_ERROR_MSG", "u", {"error"},
	 sizeof(struct hash_error_msg)},
	{HASH_QUIT_MSG, "HASH_QUIT_MSG", 0},
	{FILE_DATA_MSG, "FILE_DATA_MSG", "UUsuUuuub", {"src_lin",
						       "src_dlin",
						       "fname",
						       "fenc",
						       "offset",
						       "data_type",
						       "logical_size",
						       "checksum",
						       "data"},
	 sizeof(struct file_data_msg)},

	{OLD_FILE_BEGIN_MSG, "OLD_FILE_BEGIN_MSG", "UUUUuuuuuuusususuub",
	 {"src_lin",
	  "src_dlin",
	  "src_dkey",
	  "size",
	  "file_type",
	  "di_flags",
	  "mode",
	  "uid",
	  "gid",
	  "atime",
	  "mtime",
	  "acl",
	  "full_transfer",
	  "fname",
	  "fenc",
	  "symlink",
	  "senc",
	  "sync_state",
	  "short_hashes"}, sizeof(struct old_file_begin_msg)},

	{OLD_FILE_BEGIN_MSG2, "OLD_FILE_BEGIN_MSG2", "UUUUuuuuuUUUUsususuubUu",
	 {"src_lin",
	  "src_dlin",
	  "src_dkey",
	  "size",
	  "file_type",
	  "di_flags",
	  "mode",
	  "uid",
	  "gid",
	  "atime_sec",
	  "atime_nsec",
	  "mtime_sec",
	  "mtime_nsec",
	  "acl",
	  "full_transfer",
	  "fname",
	  "fenc",
	  "symlink",
	  "senc",
	  "sync_state",
	  "short_hashes",
	  "prev_cookie",
	  "skip_bb_hash"}, sizeof(struct old_file_begin_msg2)},

	{FILE_BEGIN_MSG, "FILE_BEGIN_MSG", "UUUUuuuuuUUUUsususuubUuuu",
	 {"src_lin",
	  "src_dlin",
	  "src_dkey",
	  "size",
	  "file_type",
	  "di_flags",
	  "mode",
	  "uid",
	  "gid",
	  "atime_sec",
	  "atime_nsec",
	  "mtime_sec",
	  "mtime_nsec",
	  "acl",
	  "full_transfer",
	  "fname",
	  "fenc",
	  "symlink",
	  "senc",
	  "sync_state",
	  "short_hashes",
	  "prev_cookie",
	  "skip_bb_hash",
	  "worm_committed",
	  "worm_retention_date"}, sizeof(struct file_begin_msg)},

	{FILE_DONE_MSG, "FILE_DONE_MSG", "uUUsuuus",
	 {"success",
	  "src_lin",
	  "src_dlin",
	  "fname",
	  "fenc",
	  "sync_state",
	  "size_changed",
	  "hash_str"}, sizeof(struct file_done_msg)},

	{ACK_MSG3, "ACK_MSG3", "UUUsu", {"src_lin",
					"src_dlin",
					"src_dkey",
					"file",
					 "enc"}, sizeof(struct ack_msg3)},

	{LIN_ACK_MSG, "LIN_ACK_MSG", "UUuuuu", {"src_lin",
					"dst_lin",
					"oper_count",
					"result",
					"sys_err",
					"siq_err"}, sizeof(struct lin_ack_msg)},

	{SIQ_ERROR_MSG, "SIQ_ERROR_MSG", "ussuusuUsUb", {"siqerr",
					       "errstr",
					       "nodename",
					       "errloc",
					       "error",
					       "file",
					       "enc",
					       "filelin",
					       "dir",
					       "dirlin",
					       "encs"},
	 sizeof(struct siq_err_msg)},
	 
	 {OLD_PWORKER_TW_STAT_MSG, "PWORKER_TW_STAT_MSG",
	 	"uuuuuuuuuuuuuuuuuuuuuuUUUUUUUUUU", {
	 		      "dirs_visited",
			      "dirs_dst_deleted",
			      "files_total",
			      "files_replicated_selected",
			      "files_replicated_transferred",
			      "files_replicated_new_files",
			      "files_replicated_updated_files",
			      "files_replicated_files_with_ads",
			      "files_replicated_ads_streams",
			      "files_replicated_symlinks",
			      "files_replicated_block_specs",
			      "files_replicated_char_specs",
			      "files_replicated_sockets",
			      "files_replicated_fifos",
			      "files_replicated_hard_links",
			      "files_deleted_src",
			      "files_deleted_dst",
			      "files_skipped_up_to_date",
			      "files_skipped_user_conflict",
			      "files_skipped_error_io",
			      "files_skipped_error_net",
			      "files_skipped_error_checksum",
			      "bytes_transferred",
			      "bytes_data_total",
			      "bytes_data_file",
			      "bytes_data_sparse",
			      "bytes_data_unchanged",
			      "bytes_network_total",
			      "bytes_network_to_target",
			      "bytes_network_to_source",
			      "twlin",
			      "filelin"},
	sizeof(struct old_pworker_tw_stat_msg)},

	{OLD_CLUSTER_MSG2, "OLD_CLUSTER_MSG2", "ssUb",
	 {"name", "sync_id", "run_id", "devs"},
	    sizeof(struct old_cluster_msg2)},
	{DELETE_LIN_MSG, "DELETE_LIN_MSG", "U", {"slin"},
	    sizeof(struct delete_lin_msg)},
	{LINK_MSG, "LINK_MSG", "UUsuusuu", {"dirlin",
				"d_lin",
				"d_name",
				"d_enc",
				"d_type",
				"symlink",
				"senc",
				"new_singly_linked_file"},
	    sizeof(struct link_msg)},
	{UNLINK_MSG, "UNLINK_MSG", "UUsuu", {"dirlin",
				"d_lin",
				"d_name",
				"d_enc",
				"d_type"},
	    sizeof(struct unlink_msg)},

	{OLD_TARGET_INIT_MSG, "OLD_TARGET_INIT_MSG", "sssUsssssuuu", {
		"restricted_target_name",
		"policy_id",
		"policy_name",
		"run_id",
		"target_dir",
		"src_root_dir",
		"src_cluster_name",
		"src_cluster_id",
		"last_target_cluster_id",
		"option",
		"loglevel",
		"flags"},
	 sizeof(struct old_target_init_msg)},

	{TARGET_CANCEL_MSG, "TARGET_CANCEL_MSG", "s", {
		"policy_id"},
	 sizeof(struct target_cancel_msg)},

	{GENERIC_ACK_MSG, "GENERIC_ACK_MSG", "sus", {"id", "code", "msg"},
	 sizeof(struct generic_ack_msg)},

	{CLEANUP_SYNC_RUN_MSG, "CLEANUP_SYNC_RUN_MSG", "s", {"policy_id"},
	 sizeof(struct cleanup_sync_run_msg)},

	{CLEANUP_TARGET_TMP_FILES_MSG, "CLEANUP_TARGET_TMP_FILES_MSG", "ss",
		{"policy_id",
		 "tgt_path"},
	 sizeof(struct cleanup_target_tmp_files_msg)},

	{TARGET_MONITOR_SHUTDOWN_MSG, "TARGET_MONITOR_SHUTDOWN_MSG", "s",
	 {"msg"}, sizeof(struct target_monitor_shutdown_msg)},

	{OLD_LIN_UPDATE_MSG, "OLD_LIN_UPDATE_MSG", "uUuuuuUUUUUsusuuuUsu",
	 {"flag",
	  "slin",
	  "file_type",
	  "mode",
	  "uid",
	  "gid",
	  "atime_sec",
	  "atime_nsec",
	  "mtime_sec",
	  "mtime_nsec",
	  "size",
	  "acl",
	  "di_flags",
	  "symlink",
	  "senc",
	  "full_transfer",
	  "skip_bb_hash",
	  "parent_lin",
	  "d_name",
	  "d_enc"}, sizeof(struct old_lin_update_msg)},
	{LIN_UPDATE_MSG, "LIN_UPDATE_MSG", "uUuuuuUUUUUsusuuuUsuuu",
	 {"flag",
	  "slin",
	  "file_type",
	  "mode",
	  "uid",
	  "gid",
	  "atime_sec",
	  "atime_nsec",
	  "mtime_sec",
	  "mtime_nsec",
	  "size",
	  "acl",
	  "di_flags",
	  "symlink",
	  "senc",
	  "full_transfer",
	  "skip_bb_hash",
	  "parent_lin",
	  "d_name",
	  "d_enc",
	  "worm_committed",
	  "worm_retention_date"}, sizeof(struct lin_update_msg)},
	{LIN_COMMIT_MSG, "LIN_COMMIT_MSG", "Uus",
	  {"slin",
	  "size_changed",
	  "hash_str"}, sizeof(struct lin_commit_msg)},
	{JOB_STATUS_MSG, "JOB_STATUS_MSG", "u", {"job_status"},
	 sizeof(struct job_status_msg)},

	{OLD_TARGET_RESP_MSG, "OLD_TARGET_RESP_MSG", "sssb", {
		"error",
		"target_cluster_name",
		"target_cluster_id",
		"ips"},
	 sizeof(struct old_target_resp_msg)},
	 
	 {OLD_TARGET_RESP_MSG2, "OLD_TARGET_RESP_MSG2", "sssbUu", {
		"error",
		"target_cluster_name",
		"target_cluster_id",
		"ips",
		"domain_id",
		"domain_generation"},
	 sizeof(struct old_target_resp_msg2)},
	
	{TARGET_POLICY_DELETE_MSG, "TARGET_POLICY_DELETE_MSG", "s", {
		"policy_id"},
	 sizeof(struct target_policy_delete_msg)},

	{LIN_MAP_MSG, "LIN_MAP_MSG", "suUUU", {
		"name", "enc", "src_lin", "cur_cookie", "prev_cookie"},
	 sizeof(struct lin_map_msg)},
	
	{DIR_UPGRADE_MSG, "DIR_UPGRADE_MSG", "UU", {"slice_begin", "slice_max"}, 
	 sizeof(struct dir_upgrade_msg)},
	
	{UPGRADE_COMPLETE_MSG, "UPGRADE_COMPLETE_MSG", "Uu", {
		"domain_id", "domain_generation"}, 
	 sizeof(struct upgrade_complete_msg)},

	{TARGET_POL_STF_DOWNGRADE_MSG, "TARGET_POL_STF_DOWNGRADE_MSG",
	        "s", {"policy_id"},
	 sizeof(struct target_pol_stf_downgrade_msg)},

	{OLD_PWORKER_STF_STAT_MSG, "PWORKER_STF_STAT_MSG",
		"uuuuuuuuuuuuuuuuuuuuUUUUUUUuuuuuuuuuuuuuu", {
		"dirs_created_src",
		"dirs_deleted_src",
		"dirs_linked_src",
		"dirs_unlinked_src",
		"dirs_replicated",
		"files_total",
		"files_replicated_new_files",
		"files_replicated_updated_files",
		"files_replicated_regular",
		"files_replicated_with_ads",
		"files_replicated_ads_streams",
		"files_replicated_symlinks",
		"files_replicated_block_specs",
		"files_replicated_char_specs",
		"files_replicated_sockets",
		"files_replicated_fifos",
		"files_replicated_hard_links",
		"files_deleted_src",
		"files_linked_src",
		"files_unlinked_src",
		"bytes_data_total",
		"bytes_data_file",
		"bytes_data_sparse",
		"bytes_data_unchanged",
		"bytes_network_total",
		"bytes_network_to_target",
		"bytes_network_to_source",
		"cc_lins_total",
		"cc_dirs_new",
		"cc_dirs_deleted",
		"cc_dirs_moved",
		"cc_dirs_changed",
		"cc_files_new",
		"cc_files_linked",
		"cc_files_unlinked",
		"cc_files_changed",
		"ct_hash_exceptions_found",
		"ct_hash_exceptions_fixed",
		"ct_flipped_lins",
		"ct_corrected_lins",
		"ct_resynced_lins"},
	 sizeof(struct old_pworker_stf_stat_msg)},
	
	{OLD_SWORKER_STF_STAT_MSG, "SWORKER_STF_STAT_MSG", "uuuuuuu", {
		"dirs_created_dst",
		"dirs_deleted_dst",
		"dirs_linked_dst",
		"dirs_unlinked_dst",
		"files_deleted_dst",
		"files_linked_dst",
		"files_unlinked_dst"},
	 sizeof(struct old_sworker_stf_stat_msg)},

	{USER_ATTR_MSG, "USER_ATTR_MSG", "uuubb", {
		"namespc",
		"encoding",
		"is_dir",
		"key",
		"val"},
	 sizeof(struct user_attr_msg)},
	{STALE_DIR_MSG, "STALE_DIR_MSG", "UUUUUu", {
		"dir_slin",
		"atime_sec",
		"atime_nsec",
		"mtime_sec",
		"mtime_nsec",
		"is_start"},
	 sizeof(struct stale_dir_msg)},
	{STF_CHKPT_MSG, "STF_CHKPT_MSG", "Uu", {
		"slin",
		"oper_count"},
	 sizeof(struct stf_chkpt_msg)},
	{LIN_PREFETCH_MSG, "LIN_PREFETCH_MSG", "ub", {
		"type",
		"lins"},
	 sizeof(struct lin_prefetch_msg)},
	{CPU_THROTTLE_INIT_MSG, "CPU_THROTTLE_INIT_MSG", "u", {
		"worker_type"},
	 sizeof(struct cpu_throttle_init_msg)},
	{CPU_THROTTLE_STAT_MSG, "CPU_THROTTLE_STAT_MSG", "UU", {
		"time",
		"cpu_time"},
	 sizeof(struct cpu_throttle_stat_msg)},
	{CPU_THROTTLE_MSG, "CPU_THROTTLE_MSG", "u", {"ration"},
	 sizeof(struct cpu_throttle_msg)},
	{EXTRA_FILE_MSG, "EXTRA_FILE_MSG", "uUuuuuUUUUUsususu",
	 {"flag",
	  "slin",
	  "file_type",
	  "mode",
	  "uid",
	  "gid",
	  "atime_sec",
	  "atime_nsec",
	  "mtime_sec",
	  "mtime_nsec",
	  "size",
	  "acl",
	  "di_flags",
	  "symlink",
	  "senc",
	  "d_name",
	  "d_enc"}, sizeof(struct extra_file_msg)},
	{EXTRA_FILE_COMMIT_MSG, "EXTRA_FILE_COMMIT_MSG", "",
	  { }, sizeof(struct extra_file_commit_msg)},
	{LIN_MAP_BLK_MSG, "LIN_MAP_BLK_MSG", "u",
	  {"result"}, sizeof(struct lin_map_blk_msg)},
	{DATABASE_UPDT_MSG, "DATABASE_UPDT_MSG", "ub", {
		"flag",
		"updt"},
	sizeof(struct database_updt_msg)},
	{COMMON_VER_MSG, "COMMON_VER_MSG", "u", {
		"version"},
	sizeof(struct common_ver_msg)},
	{VERIFY_FAILOVER_MSG, "VERIFY_FAILOVER_MSG", "",
	  {}, 0}, 	// Stub entry for indexing purposes. No message content.
	{VERIFY_FAILOVER_RESP_MSG, "VERIFY_FAILOVER_RESP_MSG", "u", 
	  { "failover_done" }, sizeof(struct verify_failover_resp_msg)},
	{OLD_FAILBACK_PREP_FINALIZE_MSG, "OLD_FAILBACK_PREP_FINALIZE_MSG", "us",
	  { "create_fbpol",
	    "fbpol_dst_path" }, sizeof(struct old_failback_prep_finalize_msg)},
	{OLD_FAILBACK_PREP_FINALIZE_RESP_MSG,
	  "OLD_FAILBACK_PREP_FINALIZE_RESP_MSG", "sssss",
	  { "error",
	    "fbpol_id",
	    "fbpol_name",
	    "fbpol_src_cluster_name",
	    "fbpol_src_cluster_id" },
	  sizeof(struct old_failback_prep_finalize_resp_msg)},
	{CLEANUP_DATABASE_MIRROR_MSG, "CLEANUP_DATABASE_MIRROR_MSG", "us",
		{"flags",
		 "policy_id"},
	 sizeof(struct cleanup_database_mirror_msg)},
	{WORKER_PID_MSG, "WORKER_PID_MSG", "uu", {"worker_id", "process_id"}, 
	    sizeof(struct worker_pid_msg)},
	{BURST_SOCK_MSG, "BURST_SOCK_MSG", "uuu",
		{"flags", "port", "max_buf_size"}, 
	    sizeof(struct burst_sock_msg)},
	{WORM_STATE_MSG, "WORM_STATE_MSG", "uU",
	  { "committed",
	    "retention_date" },
	 sizeof(struct worm_state_msg)},
	{GENERAL_SIGNAL_MSG, "GENERAL_SIGNAL_MSG", "u", { "signal" },
	 sizeof(struct general_signal_msg)},
	{OLD_CLOUDPOOLS_BEGIN_MSG, "OLD_CLOUDPOOLS_BEGIN_MSG", "",
	  {}, sizeof(struct old_cloudpools_begin_msg)},
	{OLD_CLOUDPOOLS_STUB_DATA_MSG, "OLD_CLOUDPOOLS_STUB_DATA_MSG", "ub",
	  { "type", "data" }, sizeof(struct old_cloudpools_stub_data_msg)},
	{OLD_CLOUDPOOLS_DONE_MSG, "OLD_CLOUDPOOLS_DONE_MSG", "",
	  {}, sizeof(struct old_cloudpools_done_msg)},
};

static struct riptmesgtab ript_msgs[] = {
	{WORKER_INIT_MSG, "WORKER_INIT_MSG"},
	{WORKER_MSG, "WORKER_MSG"},
	{WORKER_STAT_MSG, "WORKER_STAT_MSG"},
	{CPU_USAGE_INIT_MSG, "CPU_USAGE_INIT_MSG"},
	{CPU_NODE_USAGE_MSG, "CPU_NODE_USAGE_MSG"},
	{TARGET_INIT_MSG, "TARGET_INIT_MSG"},
	{TARGET_RESP_MSG, "TARGET_RESP_MSG"},
	{WORK_INIT_MSG, "WORK_INIT_MSG"},
	{CLOUDPOOLS_SETUP_MSG, "CLOUDPOOLS_SETUP_MSG"},
	{CLOUDPOOLS_STUB_DATA_MSG, "CLOUDPOOLS_STUB_DATA_MSG"},
	{FAILBACK_PREP_FINALIZE_MSG, "FAILBACK_PREP_FINALIZE_MSG"},
	{FAILBACK_PREP_FINALIZE_RESP_MSG, "FAILBACK_PREP_FINALIZE_RESP_MSG"},
	{WORKER_NODE_UPDATE_MSG, "WORKER_NODE_UPDATE_MSG"},
	{PWORKER_TW_STAT_MSG, "PWORKER_TW_STAT_MSG"},
	{SWORKER_TW_STAT_MSG, "SWORKER_TW_STAT_MSG"},
	{PWORKER_STF_STAT_MSG, "PWORKER_STF_STAT_MSG"},
	{SWORKER_STF_STAT_MSG, "SWORKER_STF_STAT_MSG"},
	{COMP_COMMIT_BEGIN_MSG, "COMP_COMMIT_BEGIN_MSG"},
	{COMP_COMMIT_END_MSG, "COMP_COMMIT_END_MSG"},
	{COMP_TARGET_WORK_RANGE_MSG, "COMP_TARGET_WORK_RANGE_MSG"},
	{COMP_TARGET_CHECKPOINT_MSG, "COMP_TARGET_CHECKPOINT_MSG"},
	{COMP_MAP_BEGIN_MSG, "COMP_MAP_BEGIN_MSG"},
	{COMP_MAP_MSG, "COMP_MAP_MSG"},
	{TARGET_WORK_LOCK_MSG, "TARGET_WORK_LOCK_MSG"},
	{COMP_DIR_DELS_BEGIN_MSG, "COMP_COMMIT_DIR_DELS_BEGIN_MSG"},
};

/* Max length of name is define in isirep.h as MAX_FIELD_NAME_LEN */
static struct ript_message_fields_tab ript_msg_fields[] = {
	{RMF_POLICY_NAME, "policy_name"},
	{RMF_NODE_MASK, "node_mask"},
	{RMF_MAX_ALLOWED_WORKERS, "max_allowed_workers"},
	{RMF_ALLOWED_NODES, "allowed_nodes"},
	{RMF_WORKERS_USED, "workers_used"},
	{RMF_CPU_INIT_NAME, "name"},
	{RMF_CPU_NODE_USAGE_PCT, "usage_pct"},
	{RMF_CPU_NODE_USAGE_NAME, "name"},
	{RMF_TYPE, "type"},
	{RMF_SIZE, "size"},
	{RMF_DATA, "data"},
	{RMF_RESTRICTED_TARGET_NAME, "restricted_target_name"},
	{RMF_POLICY_ID, "policy_id"},
	{RMF_RUN_ID, "run_id"},
	{RMF_TARGET_DIR, "target_dir"},
	{RMF_SRC_ROOT_DIR, "src_root_dir"},
	{RMF_SRC_CLUSTER_NAME, "src_cluster_name"},
	{RMF_SRC_CLUSTER_ID, "src_cluster_id"},
	{RMF_LAST_TARGET_CLUSTER_ID, "last_target_cluster_id"},
	{RMF_OPTION, "option"},
	{RMF_LOGLEVEL, "loglevel"},
	{RMF_FLAGS, "flags"},
	{RMF_ERROR, "error"},
	{RMF_TARGET_CLUSTER_NAME, "target_cluster_name"},
	{RMF_TARGET_CLUSTER_ID, "target_cluster_id"},
	{RMF_IPS, "ips"},
	{RMF_DOMAIN_ID, "domain_id"},
	{RMF_DOMAIN_GENERATION, "domain_generation"},
	{RMF_CBM_FILE_VERSIONS, "cbm_file_versions"},
	{RMF_TARGET, "target"},
	{RMF_PEER, "peer"},
	{RMF_SOURCE_BASE, "source_base"},
	{RMF_TARGET_BASE, "target_base"},
	{RMF_BWHOST, "bwhost"},
	{RMF_PREDICATE, "predicate"},
	{RMF_SYNC_ID, "sync_id"},
	{RMF_LASTRUN_TIME, "lastrun_time"},
	{RMF_PASS, "pass"},
	{RMF_JOB_NAME, "job_name"},
	{RMF_ACTION, "action"},
	{RMF_OLD_TIME, "old_time"},
	{RMF_NEW_TIME, "new_time"},
	{RMF_RESTRICT_BY, "restrict_by"},
	{RMF_HASH_TYPE, "hash_type"},
	{RMF_MIN_HASH_PIECE_LEN, "min_hash_piece_len"},
	{RMF_HASH_PIECES_PER_FILE, "hash_pieces_per_file"},
	{RMF_RECOVERY_DIR, "recovery_dir"},
	{RMF_SKIP_BB_HASH, "skip_bb_hash"},
	{RMF_WORKER_ID, "worker_id"},
	{RMF_PREV_SNAPID, "prev_snapid"},
	{RMF_CUR_SNAPID, "cur_snapid"},
	{RMF_DEEP_COPY, "deep_copy"},
	{RMF_FBPOL_DST_PATH, "fbpol_dst_path"},
	{RMF_FBPOL_ID, "fbpol_id"},
	{RMF_FBPOL_NAME, "fbpol_name"},
	{RMF_FBPOL_SRC_CLUSTER_NAME, "fbpol_src_cluster_name"},
	{RMF_FBPOL_SRC_CLUSTER_ID, "fbpol_src_cluster_id"},
	{RMF_CPOOLS_SYNC_TYPE, "cpools_sync_type"},
	{RMF_COMPLIANCE_DOM_VER, "RMF_COMPLIANCE_DOM_VER"},
	{RMF_DIRS_VISITED, "dirs_visited"},
	{RMF_DIRS_DELETED_DST, "dirs_deleted_dst"},
	{RMF_FILES_TOTAL, "files_total"},
	{RMF_FILES_REPLICATED_SELECTED, "files_replicated_selected"},
	{RMF_FILES_REPLICATED_TRANSFERRED, "files_replicated_transferred"},
	{RMF_FILES_REPLICATED_NEW_FILES, "files_replicated_new_files"},
	{RMF_FILES_REPLICATED_UPDATED_FILES, "files_replicated_updated_files"},
	{RMF_FILES_REPLICATED_WITH_ADS, "files_replicated_with_ads"},
	{RMF_FILES_REPLICATED_ADS_STREAMS, "files_replicated_ads_streams"},
	{RMF_FILES_REPLICATED_SYMLINKS, "files_replicated_symlinks"},
	{RMF_FILES_REPLICATED_BLOCK_SPECS, "files_replicated_block_specs"},
	{RMF_FILES_REPLICATED_CHAR_SPECS, "files_replicated_char_specs"},
	{RMF_FILES_REPLICATED_SOCKETS, "files_replicated_sockets"},
	{RMF_FILES_REPLICATED_FIFOS, "files_replicated_fifos"},
	{RMF_FILES_REPLICATED_HARD_LINKS, "files_replicated_hard_links"},
	{RMF_FILES_DELETED_SRC, "files_deleted_src"},
	{RMF_FILES_DELETED_DST, "files_deleted_dst"},
	{RMF_FILES_SKIPPED_UP_TO_DATE, "files_skipped_up_to_date"},
	{RMF_FILES_SKIPPED_USER_CONFLICT, "files_skipped_user_conflict"},
	{RMF_FILES_SKIPPED_ERROR_IO, "files_skipped_error_io"},
	{RMF_FILES_SKIPPED_ERROR_NET, "files_skipped_error_net"},
	{RMF_FILES_SKIPPED_ERROR_CHECKSUM, "files_skipped_error_checksum"},
	{RMF_BYTES_TRANSFERRED, "bytes_transferred"},
	{RMF_BYTES_DATA_TOTAL, "bytes_data_total"},
	{RMF_BYTES_DATA_FILE, "bytes_data_file"},
	{RMF_BYTES_DATA_SPARSE, "bytes_data_sparse"},
	{RMF_BYTES_DATA_UNCHANGED, "bytes_data_unchanged"},
	{RMF_BYTES_NETWORK_TOTAL, "bytes_network_total"},
	{RMF_BYTES_NETWORK_TO_TARGET, "bytes_network_to_target"},
	{RMF_BYTES_NETWORK_TO_SOURCE, "bytes_network_to_source"},
	{RMF_TWLIN, "twlin"},
	{RMF_FILELIN, "filelin"},
	{RMF_BYTES_RECOVERABLE, "bytes_recoverable"},
	{RMF_BYTES_RECOVERED_SRC, "bytes_recovered_src"},
	{RMF_BYTES_RECOVERED_DST, "bytes_recovered_dst"},
	{RMF_DIRS_CREATED_SRC, "dirs_created_src"},
	{RMF_DIRS_DELETED_SRC, "dirs_deleted_src"},
	{RMF_DIRS_LINKED_SRC, "dirs_linked_src"},
	{RMF_DIRS_UNLINKED_SRC, "dirs_unlinked_src"},
	{RMF_DIRS_REPLICATED, "dirs_replicated"},
	{RMF_FILES_REPLICATED_REGULAR, "files_replicated_regular"},
	{RMF_FILES_LINKED_SRC, "files_linked_src"},
	{RMF_FILES_UNLINKED_SRC, "files_unlinked_src"},
	{RMF_CC_LINS_TOTAL, "cc_lins_total"},
	{RMF_CC_DIRS_NEW, "cc_dirs_new"},
	{RMF_CC_DIRS_DELETED, "cc_dirs_deleted"},
	{RMF_CC_DIRS_MOVED, "cc_dirs_moved"},
	{RMF_CC_DIRS_CHANGED, "cc_dirs_changed"},
	{RMF_CC_FILES_NEW, "cc_files_new"},
	{RMF_CC_FILES_LINKED, "cc_files_linked"},
	{RMF_CC_FILES_UNLINKED, "cc_files_unlinked"},
	{RMF_CC_FILES_CHANGED, "cc_files_changed"},
	{RMF_CT_HASH_EXCEPTIONS_FOUND, "ct_hash_exceptions_found"},
	{RMF_CT_HASH_EXCEPTIONS_FIXED, "ct_hash_exceptions_fixed"},
	{RMF_CT_FLIPPED_LINS, "ct_flipped_lins"},
	{RMF_CT_CORRECTED_LINS, "ct_corrected_lins"},
	{RMF_CT_RESYNCED_LINS, "ct_resynced_lins"},
	{RMF_DIRS_CREATED_DST, "dirs_created_dst"},
	{RMF_DIRS_LINKED_DST, "dirs_linked_dst"},
	{RMF_DIRS_UNLINKED_DST, "dirs_unlinked_dst"},
	{RMF_FILES_LINKED_DST, "files_linked_dst"},
	{RMF_FILES_UNLINKED_DST, "files_unlinked_dst"},
	{RMF_RESYNC_COMPLIANCE_DIRS_NEW, "resync_compliance_dirs_new"},
	{RMF_RESYNC_COMPLIANCE_DIRS_LINKED, "resync_compliance_dirs_linked"},
	{RMF_RESYNC_CONFLICT_FILES_NEW, "resync_conflict_files_new"},
	{RMF_RESYNC_CONFLICT_FILES_LINKED, "resync_conflict_files_linked"},
	{RMF_WORK_TYPE, "work_type"},
	{RMF_COMP_TARGET_MIN_LIN, "comp_target_min_lin"},
	{RMF_COMP_TARGET_MAX_LIN, "comp_target_max_lin"},
	{RMF_COMP_TARGET_CURR_LIN, "comp_target_curr_lin"},
	{RMF_COMP_TARGET_SPLIT_LIN, "comp_target_split_lin"},
	{RMF_COMP_TARGET_DONE, "comp_target_done"},
	{RMF_COMP_MAP_BUF, "comp_map_buf"},
	{RMF_CHKPT_LIN, "chkpt_lin"},
	{RMF_COMPLIANCE_CONFLICTS, "compliance_conflicts"},
	{RMF_RUN_NUMBER, "run_number"},
	{RMF_COMP_COMMITTED_FILES, "comp_committed_files"},
	{RMF_PEER_GUID, "peer_guid"},
	{RMF_WI_LIN, "wi_lin"},
	{RMF_LOCK_WORK_ITEM, "lock_work_item"},
	{RMF_WI_LOCK_MOD, "wi_lock_mod"},
	{RMF_TARGET_WORK_LOCKING_PATCH, "target_work_locking_patch"},
	{RMF_COMP_LATE_STAGE_UNLINK_PATCH, "comp_late_stage_unlink_patch"},
};

/* List of RMs and RMFs that are backported and need to be whitelisted in
 * previous releases.*/
static enum ript_messages rm_whitelist[] = {
	// Riptide -> Pipeline.
	TARGET_WORK_LOCK_MSG,
	COMP_DIR_DELS_BEGIN_MSG,
};

static enum ript_message_fields rmf_whitelist[] = {
	// Riptide -> Pipeline.
	RMF_WI_LIN,
	RMF_LOCK_WORK_ITEM,
	RMF_WI_LOCK_MOD,
	RMF_TARGET_WORK_LOCKING_PATCH,
	RMF_COMP_LATE_STAGE_UNLINK_PATCH,
	RMF_REQUESTED_WORKERS,
};

static int msgs_map[MAX_MSG];
static int ript_msgs_map[WHITELIST_MAX_MSG];
static bool msgs_inited = false;

static char generic_send_buf[BUFSIZE];

/* internal prototypes */
static int uset(void *p, unsigned u);
static int uget(void *p, unsigned *u);
static int pack_msg(unsigned type, void *buf, unsigned buflen,
		void *msg);
static int unpack_msg(unsigned type, void *msg, void *buf,
		int len);
static int msg_out_size(unsigned type, unsigned char *msg);
static int rep_msg_unpack(struct rep_msg *msg, char *buf, int buflen,
		int *needed);

static void handle_pipe(int sig);
static int mark_fd_closed(int fd);
static int process_timeouts(struct timeval *tv);
static int timeouts_pending(void);
static int timeval_greater(struct timeval *reference, struct timeval *cmp);
static int next_timeout_diff(struct timeval *t, struct timeval *tv);
static int add_kevent(void);
static struct fd_info *find_fd_info(int fd);

static int
uset(void *p, unsigned u)
{
	*((unsigned *)p) = htonl(u);
	return sizeof(unsigned);
}

static int
uget(void *p, unsigned *u)
{
	*u = ntohl(*((unsigned *)p));
	return sizeof(unsigned);
}

static bool
rm_in_whitelist(enum ript_messages msg)
{
	int i;

	for (i = 0; i < ARRAY_SIZE(rm_whitelist); i++) {
		if (msg == rm_whitelist[i])
			return true;
	}

	return false;
}

static bool
rmf_in_whitelist(enum ript_message_fields field)
{
	int i;

	for (i = 0; i < ARRAY_SIZE(rmf_whitelist); i++) {
		if (field == rmf_whitelist[i])
			return true;
	}

	return false;
}

uint32_t
build_ript_msg_type(uint32_t type)
{
	ASSERT(type < (1 << 24));
	return ((type << 8) | RIPT_MSG_MAGIC);
}

uint32_t
unbuild_ript_msg_type(uint32_t type)
{
	ASSERT(is_ript_msg_type(type));
	return (type >> 8);
}

/*
 * Ensure:
 * 1) The length of the packing matches the number of members listed
 *
 * 2) The calculated size of the packing matches the size of structure
 *    associated with the message
 *         
*/
void
msgs_sanity(void)
{
	int m_count; /* Number of member names for each message type */
	int p_count; /* Number of member packings for each message type */
	size_t s_size;  /* Size of unpacked message structure */
	int i, j, k, align_size;

	for (k = 0; k < MAX_MSG; k++) {
		i = msgs_map[k];
		if (i == -1)
			continue;
		p_count = 0;
		s_size = 0;
		m_count = 0;
		for (j = 0, align_size = 0; msgs[i].pack && j < strlen(msgs[i].pack); j++) {
			p_count++;
			switch (msgs[i].pack[j])
			{
			case 'b':
			case 'd':
				s_size += sizeof(unsigned) + sizeof(char *);
				ALIGN_PTR(s_size);
				align_size = 1;
				break;
			case 'u':
				s_size += sizeof(unsigned);
				break;
			case 'U':
				ALIGN_PTR(s_size);
				s_size += sizeof(uint64_t);
				align_size = 1;
				break;
			case 's':
				ALIGN_PTR(s_size);
				s_size += sizeof(char *);
				align_size = 1;
				break;
			default:
				log(ERROR, "Invalid msg packing '%c' for %s",
				    msgs[i].pack[j], msgs[i].name);
				ASSERT(false);
			}
		}
		if (align_size)
			ALIGN_PTR(s_size);
		if (s_size != msgs[i].size) {
			log(ERROR, "packing size (%zu) doesn't match "
			    "struct size (%d) for %s",
			    s_size, 
			    msgs[i].size, msgs[i].name);
			ASSERT(0);
		}

		for (j = 0; j < MAX_MSG_MEMBERS; j++) {
			if (msgs[i].members[j])
				m_count++;
			else
				break;
		}
		if (m_count != p_count) {
			log(ERROR, "msg err: pack len(%d) doesn't "
			    "match members len(%d) for type %s(%d)",
			    p_count, m_count, msgs[i].name, msgs[i].msg_id);
			ASSERT(0);
		}
	}
	
}

static void
init_msgs(void)
{
	int i;

	for (i = 0; i < MAX_MSG; i++)
		msgs_map[i] = -1;
	for (i = 0; i < sizeof(msgs) / sizeof(struct mesgtab); i ++)
		msgs_map[msgs[i].msg_id] = i;
	for (i = 0; i < WHITELIST_MAX_MSG; i++)
		ript_msgs_map[i] = -1;
	for (i = 0; i < sizeof(ript_msgs) / sizeof(struct riptmesgtab); i++)
		ript_msgs_map[ript_msgs[i].msg_id] = i;

	msgs_sanity();
}

static char *
get_message_packing(unsigned type)
{
	int index;

	ASSERT(type < MAX_MSG);
	if (!msgs_inited) {
		init_msgs();
		msgs_inited = true;
	}
	index = msgs_map[type];
	ASSERT(index != -1);
	return msgs[index].pack;
}

struct mesgtab *msg_get_mesg_tab(enum messages msg_id)
{
	if (!msgs_inited) {
		init_msgs();
		msgs_inited = true;
	}
	if (msg_id >= MAX_MSG || msgs_map[msg_id] == -1)
		return NULL;
	else
		return &(msgs[msgs_map[msg_id]]);
}

bool
is_ript_msg_type(const uint32_t type)
{
	return (type & 0xFF) == RIPT_MSG_MAGIC;
}

int
ript_msg_out_size(struct siq_ript_msg *msg)
{
	struct siq_ript_msg_field_hash_entry *fhe = NULL;
	struct siq_ript_msg_field *field = NULL;
	int res = 0;

	ISI_HASH_FOREACH(fhe, &msg->field_hash, hentry) {
		ISI_HASH_FOREACH(field, &fhe->vhash, hentry) {
			res += sizeof(fhe->fieldid); // Field id.
			res += sizeof(uint8_t);      // Type.
			res += sizeof(field->ver);   // Version.
			switch (field->type) {
				case SIQFT_INT8:
				case SIQFT_UINT8:
					res += 1;
					break;

				case SIQFT_INT16:
				case SIQFT_UINT16:
					res += 2;
					break;

				case SIQFT_INT32:
				case SIQFT_UINT32:
					res += 4;
					break;

				case SIQFT_INT64:
				case SIQFT_UINT64:
				case SIQFT_FLOAT64:
					res += 8;
					break;

				case SIQFT_STRING:
					// Add trailing NULL.
					res += sizeof(uint32_t);
					res += (field->value.strval == NULL) ?
					    0 : strlen(field->value.strval) +
					    sizeof(char);
					break;

				case SIQFT_BYTESTREAM:
					res += sizeof(uint32_t); // len
					res += field->value.bytestreamval.len;
					break;

				default:
					ASSERT(!"Invalid field type!");
					break;
			}
		}
	}

	return res;
}

void
ript_msg_init(struct siq_ript_msg *msg)
{
	ISI_HASH_INIT(&msg->field_hash, siq_ript_msg_field_hash_entry,
	    fieldid, hentry, H_SIQ_RIPT_MSG_FIELDS);
}

static struct siq_ript_msg_field_hash_entry *
_ript_msg_fhe_new(uint16_t fieldid)
{
	struct siq_ript_msg_field_hash_entry *fhe = NULL;
	fhe = calloc(sizeof(struct siq_ript_msg_field_hash_entry), 1);
	ASSERT(fhe);

	fhe->fieldid = fieldid;
	ISI_HASH_INIT(&fhe->vhash, siq_ript_msg_field, ver, hentry,
	    H_SIQ_RIPT_MSG_FIELD_VERS);
	return fhe;
}

static int
_pack_ript_msg_field(uint16_t fieldid, struct siq_ript_msg_field *field,
    void *buf_)
{
	uint8_t *buf = buf_;
	uint64_t uval;
	uint32_t len = 0;

	*((uint16_t *)buf) = htons(fieldid);
	buf += sizeof(uint16_t); // Field id.

	*buf = (uint8_t)field->type;
	buf += sizeof(uint8_t);

	*buf = (uint8_t)field->ver;
	buf += sizeof(uint8_t);

	switch (field->type) {
		case SIQFT_INT8:
			*((int8_t *)buf) = field->value.i8val;
			buf += sizeof(field->value.i8val);
			break;

		case SIQFT_UINT8:
			*((uint8_t *)buf) = field->value.u8val;
			buf += sizeof(field->value.u8val);
			break;

		case SIQFT_INT16:
			*((int16_t *)buf) = htons(field->value.i16val);
			buf += sizeof(field->value.i16val);
			break;

		case SIQFT_UINT16:
			*((uint16_t *)buf) = htons(field->value.u16val);
			buf += sizeof(field->value.u16val);
			break;

		case SIQFT_INT32:
			*((int32_t *)buf) = htonl(field->value.i32val);
			buf += sizeof(field->value.i32val);
			break;

		case SIQFT_UINT32:
			*((uint32_t *)buf) = htonl(field->value.u32val);
			buf += sizeof(field->value.u32val);
			break;

		case SIQFT_INT64:
			uval = *(uint64_t *)&field->value.i64val;

			*((uint32_t *)buf) = (uint32_t)htonl(uval >> 32);
			buf += sizeof(uint32_t);

			*((uint32_t *)buf) =
			    (uint32_t)htonl(uval & 0xFFFFFFFF);
			buf += sizeof(uint32_t);
			break;

		case SIQFT_UINT64:
			uval = *(uint64_t *)&field->value.u64val;

			*((uint32_t *)buf) = (uint32_t)htonl(uval >> 32);
			buf += sizeof(uint32_t);

			*((uint32_t *)buf) =
			    (uint32_t)htonl(uval & 0xFFFFFFFF);
			buf += sizeof(uint32_t);
			break;

		case SIQFT_FLOAT64:
			uval = *(uint64_t *)&field->value.f64val;

			*((uint32_t *)buf) = (uint32_t)htonl(uval >> 32);
			buf += sizeof(uint32_t);

			*((uint32_t *)buf) =
			    (uint32_t)htonl(uval & 0xFFFFFFFF);
			buf += sizeof(uint32_t);
			break;

		case SIQFT_STRING:
			// Include the trailing NULL.
			len = (field->value.strval == NULL) ?
			    0 : strlen(field->value.strval) + sizeof(char);
			*((uint32_t *)buf) = htonl(len);
			buf += sizeof(uint32_t);

			if (len > 0) {
				memcpy((char *)buf, field->value.strval, len);
				buf += len;
			}
			break;

		case SIQFT_BYTESTREAM:
			len = field->value.bytestreamval.len;
			*((uint32_t *)buf) = htonl(len);
			buf += sizeof(uint32_t); // len

			if (len > 0) {
				memcpy((char *)buf,
				    field->value.bytestreamval.data, len);
				buf += len;
			}
			break;

		default:
			ASSERT(!"Invalid field type!");
			break;
	}
	return (buf - (uint8_t *)buf_);
}

int
pack_ript_msg(struct siq_ript_msg *msg, void *buf_)
{
	struct siq_ript_msg_field_hash_entry *fhe = NULL;
	struct siq_ript_msg_field *field = NULL;
	uint8_t *buf = buf_;

	ISI_HASH_FOREACH(fhe, &msg->field_hash, hentry) {
		ISI_HASH_FOREACH(field, &fhe->vhash, hentry) {
			buf += _pack_ript_msg_field(fhe->fieldid, field, buf);
		}
	}

	return buf - (uint8_t *)buf_;
}

void
_free_ript_msg(struct siq_ript_msg *msg)
{
	struct siq_ript_msg_field_hash_entry *fhe = NULL;
	struct siq_ript_msg_field *field = NULL;

	if (!msg)
		return;

	ISI_HASH_FOREACH(fhe, &msg->field_hash, hentry) {
		ISI_HASH_FOREACH(field, &fhe->vhash, hentry) {
			isi_hash_remove(&fhe->vhash, &field->hentry);
			free(field);
		}

		isi_hash_remove(&msg->field_hash, &fhe->hentry);
		isi_hash_destroy(&fhe->vhash);
		free(fhe);
	}
	isi_hash_destroy(&msg->field_hash);
	memset(&msg->field_hash, 0, sizeof(msg->field_hash));
}

void
free_generic_msg(struct generic_msg *m)
{
	if (!m)
		return;

	if (!is_ript_msg_type(m->head.type))
		return;

	_free_ript_msg(&m->body.ript);
}

int
_unpack_ript_msg_field(struct siq_ript_msg *msg, unsigned char *buf_, int len)
{
	uint16_t fieldid = 0;
	uint8_t type = 0;
	uint8_t ver = 0;

	uint64_t u64;
	uint32_t uh, ul;
	uint32_t fieldlen;

	unsigned char *buf = buf_;

	struct siq_ript_msg_field_hash_entry *fhe = NULL;
	struct isi_hash_entry *fihe = NULL; // Field hash entry.

	struct siq_ript_msg_field *field = NULL;

	ASSERT((buf - buf_) + sizeof(uint16_t) < len);
	fieldid = ntohs(*((uint16_t *)buf));

	buf += sizeof(uint16_t);

	fihe = isi_hash_get(&msg->field_hash, &fieldid);
	if (!fihe) {
		fhe = _ript_msg_fhe_new(fieldid);
		isi_hash_add(&msg->field_hash, &fhe->hentry);
	} else {
		fhe = container_of(fihe, struct siq_ript_msg_field_hash_entry,
		    hentry);
	}

	ASSERT(fhe);

	ASSERT((buf - buf_) + sizeof(uint8_t) < len);
	type = *((uint8_t *)buf);
	ASSERT(type <= SIQFT_MAX);
	buf += sizeof(uint8_t);

	ASSERT((buf - buf_) + sizeof(uint8_t) < len);
	ver = *((uint8_t *)buf);
	ASSERT(ver > 0);
	buf += sizeof(uint8_t);

	if (do_msg_dump) {
		log(DEBUG, "Field - id: %d type: %d ver: %d\n", fieldid, type,
		    ver);
	}

	ASSERT(!isi_hash_get(&fhe->vhash, &ver));

	field = calloc(sizeof(struct siq_ript_msg_field), 1);
	ASSERT(field);

	field->type = type;
	field->ver = ver;

	switch (type) {
	case SIQFT_INT8:
		ASSERT((buf - buf_) + sizeof(int8_t) <= len);
		field->value.i8val = *((int8_t *)buf);
		buf += sizeof(int8_t);
		break;

	case SIQFT_UINT8:
		ASSERT((buf - buf_) + sizeof(uint8_t) <= len);
		field->value.u8val = *((uint8_t *)buf);
		buf += sizeof(uint8_t);
		break;

	case SIQFT_INT16:
		ASSERT((buf - buf_) + sizeof(int16_t) <= len);
		field->value.i16val = ntohs(*((int16_t *)buf));
		buf += sizeof(int16_t);
		break;

	case SIQFT_UINT16:
		ASSERT((buf - buf_) + sizeof(uint16_t) <= len);
		field->value.u16val = ntohs(*((uint16_t *)buf));
		buf += sizeof(uint16_t);
		break;

	case SIQFT_INT32:
		ASSERT((buf - buf_) + sizeof(int32_t) <= len);
		field->value.i32val = ntohl(*((int32_t *)buf));
		buf += sizeof(int32_t);
		break;

	case SIQFT_UINT32:
		ASSERT((buf - buf_) + sizeof(uint32_t) <= len);
		field->value.u32val = ntohl(*((uint32_t *)buf));
		buf += sizeof(uint32_t);
		break;

	case SIQFT_INT64:
		ASSERT((buf - buf_) + sizeof(int64_t) <= len);

		uh = ntohl(*((uint32_t *)buf));
		buf += sizeof(uint32_t);
		ul = ntohl(*((uint32_t *)buf));
		buf += sizeof(uint32_t);

		u64 = (uint64_t)uh << 32 | ul;
		field->value.i64val = (int64_t)u64;
		break;

	case SIQFT_UINT64:
		ASSERT((buf - buf_) + sizeof(uint64_t) <= len);

		uh = ntohl(*((uint32_t *)buf));
		buf += sizeof(uint32_t);
		ul = ntohl(*((uint32_t *)buf));
		buf += sizeof(uint32_t);

		u64 = (uint64_t)uh << 32 | ul;
		field->value.u64val = u64;
		break;

	case SIQFT_FLOAT64:
		ASSERT((buf - buf_) + sizeof(uint64_t) <= len);

		uh = ntohl(*((uint32_t *)buf));
		buf += sizeof(uint32_t);
		ul = ntohl(*((uint32_t *)buf));
		buf += sizeof(uint32_t);

		u64 = (uint64_t)uh << 32 | ul;
		field->value.f64val = *(double *)&u64;

		break;

	case SIQFT_STRING:
		ASSERT((buf - buf_) + sizeof(uint32_t) <= len);
		fieldlen = ntohl(*((uint32_t *)buf));
		buf += sizeof(uint32_t);

		ASSERT((buf - buf_) + fieldlen <= len);
		field->value.strval = (fieldlen > 0) ? (char *)buf : NULL;
		buf += fieldlen;
		break;

	case SIQFT_BYTESTREAM:
		ASSERT((buf - buf_) + sizeof(uint32_t) <= len);
		fieldlen = ntohl(*((uint32_t *)buf));
		buf += sizeof(uint32_t);

		ASSERT((buf - buf_) + fieldlen <= len);
		field->value.bytestreamval.len = fieldlen;
		field->value.bytestreamval.data =
		    (fieldlen > 0) ? buf : NULL;
		buf += fieldlen;
		break;

	default:
		ASSERT(!"Invalid message field type!");
		break;
	}

	isi_hash_add(&fhe->vhash, &field->hentry);

	return (buf - buf_);
}

int
unpack_ript_msg(struct siq_ript_msg *m, void *buf_, int len)
{
	unsigned char *buf = buf_;

	ISI_HASH_INIT(&m->field_hash, siq_ript_msg_field_hash_entry,
	    fieldid, hentry, H_SIQ_RIPT_MSG_FIELDS);

	while ((buf - (unsigned char *)buf_) < len) {
		buf += _unpack_ript_msg_field(m, buf,
		    len - (buf - (unsigned char *)buf_));
	}

	return (buf - (unsigned char *)buf_);
}

static int
ript_msg_get_field(struct siq_ript_msg *msg, uint16_t fieldid, uint8_t ver,
    bool create, struct siq_ript_msg_field **field_out)
{
	struct isi_hash_entry *fihe = NULL;
	struct isi_hash_entry *vihe = NULL;
	struct siq_ript_msg_field_hash_entry *fhe = NULL;

	int tmp_errno = 0;

	int ret = -1;

	if (!field_out || ver < 1 ||
	    (fieldid >= RMF_MAX && !rmf_in_whitelist(fieldid))) {
		tmp_errno = EINVAL;
		goto out;
	}

	fihe = isi_hash_get(&msg->field_hash, &fieldid);
	if (!fihe) {
		if (!create) {
			tmp_errno = ENOENT;
			goto out;
		}

		fhe = _ript_msg_fhe_new(fieldid);
		isi_hash_add(&msg->field_hash, &fhe->hentry);
	} else {
		fhe = container_of(fihe, struct siq_ript_msg_field_hash_entry,
		    hentry);
	}

	vihe = isi_hash_get(&fhe->vhash, &ver);
	if (!vihe) {
		if (!create) {
			tmp_errno = ENOENT;
			goto out;
		}

		*field_out = calloc(sizeof(struct siq_ript_msg_field), 1);
		ASSERT(*field_out);
		(*field_out)->ver = ver;
		isi_hash_add(&fhe->vhash, &(*field_out)->hentry);
	} else {
		*field_out = container_of(vihe, struct siq_ript_msg_field,
		    hentry);
	}
	ASSERT(*field_out);

	ret = 0;

out:
	if (tmp_errno)
		errno = tmp_errno;

	return ret;
}

#define DEF_RIPT_MSG_ACCESSOR(TYPE, FTYPE, UFIELD)			\
void									\
ript_msg_get_field_##TYPE(struct siq_ript_msg *msg, uint16_t fieldid,	\
    uint8_t ver, TYPE##_t *val, struct isi_error **error_out)		\
{									\
	struct siq_ript_msg_field *field = NULL;			\
	int ret = -1, tmp_errno = -1;					\
	char field_name[MAX_FIELD_NAME_LEN];				\
	struct isi_error *error = NULL;					\
									\
	if (!val) {							\
		errno = EINVAL;						\
		goto out;						\
	}								\
									\
	ret = ript_msg_get_field(msg, fieldid, ver, false, &field);	\
	if (ret)							\
		goto out;						\
									\
	ASSERT(field);							\
									\
	if (field->type != FTYPE) {					\
		ret = -1;						\
		errno = EINVAL;						\
		goto out;						\
	}								\
									\
	*val = field->value.UFIELD;					\
									\
	ret = 0;							\
									\
out:									\
	if (ret != 0) {							\
		tmp_errno = errno;					\
		ript_msg_field_str(field_name, sizeof(field_name),	\
		    fieldid);						\
		error = isi_system_error_new(tmp_errno,			\
		    "Error parsing %s: %s", field_name,			\
		    strerror(tmp_errno));				\
	}								\
	isi_error_handle(error, error_out);				\
}

DEF_RIPT_MSG_ACCESSOR(int8, SIQFT_INT8, i8val);
DEF_RIPT_MSG_ACCESSOR(uint8, SIQFT_UINT8, u8val);
DEF_RIPT_MSG_ACCESSOR(int16, SIQFT_INT16, i16val);
DEF_RIPT_MSG_ACCESSOR(uint16, SIQFT_UINT16, u16val);
DEF_RIPT_MSG_ACCESSOR(int32, SIQFT_INT32, i32val);
DEF_RIPT_MSG_ACCESSOR(uint32, SIQFT_UINT32, u32val);
DEF_RIPT_MSG_ACCESSOR(int64, SIQFT_INT64, i64val);
DEF_RIPT_MSG_ACCESSOR(uint64, SIQFT_UINT64, u64val);

void
ript_msg_get_field_float64(struct siq_ript_msg *msg, uint16_t fieldid,
    uint8_t ver, double *val, struct isi_error **error_out)
{
	struct siq_ript_msg_field *field = NULL;
	int ret = -1, tmp_errno = -1;
	char field_name[MAX_FIELD_NAME_LEN];
	struct isi_error *error = NULL;

	if (!val) {
		errno = EINVAL;
		goto out;
	}

	ret = ript_msg_get_field(msg, fieldid, ver, false, &field);
	if (ret)
		goto out;

	ASSERT(field);

	if (field->type != SIQFT_FLOAT64) {
		ret = -1;
		errno = EINVAL;
		goto out;
	}

	*val = field->value.f64val;

	ret = 0;

out:
	if (ret != 0) {
		tmp_errno = errno;
		ript_msg_field_str(field_name, sizeof(field_name),
		    fieldid);
		error = isi_system_error_new(tmp_errno,
		    "Error parsing %s: %s", field_name, strerror(tmp_errno));
	}
	isi_error_handle(error, error_out);
}

void
ript_msg_get_field_str(struct siq_ript_msg *msg, uint16_t fieldid,
    uint8_t ver, char **val, struct isi_error **error_out)
{
	struct siq_ript_msg_field *field = NULL;
	int ret = -1, tmp_errno = -1;
	char field_name[MAX_FIELD_NAME_LEN];
	struct isi_error *error = NULL;

	if (!val) {
		errno = EINVAL;
		goto out;
	}

	ret = ript_msg_get_field(msg, fieldid, ver, false, &field);
	if (ret)
		goto out;

	ASSERT(field);

	if (field->type != SIQFT_STRING) {
		ret = -1;
		errno = EINVAL;
		goto out;
	}

	*val = field->value.strval;

	ret = 0;

out:
	if (ret != 0) {
		tmp_errno = errno;
		ript_msg_field_str(field_name, sizeof(field_name),
		    fieldid);
		error = isi_system_error_new(tmp_errno,
		    "Error parsing %s: %s", field_name, strerror(tmp_errno));
	}
	isi_error_handle(error, error_out);
}

void
ript_msg_get_field_bytestream(struct siq_ript_msg *msg, uint16_t fieldid,
    uint8_t ver, uint8_t **val, uint32_t *len, struct isi_error **error_out)
{
	struct siq_ript_msg_field *field = NULL;
	int ret = -1, tmp_errno = -1;
	char field_name[MAX_FIELD_NAME_LEN];
	struct isi_error *error = NULL;

	if (!val || !len) {
		errno = EINVAL;
		goto out;
	}

	ret = ript_msg_get_field(msg, fieldid, ver, false, &field);
	if (ret)
		goto out;

	ASSERT(field);

	if (field->type != SIQFT_BYTESTREAM) {
		ret = -1;
		errno = EINVAL;
		goto out;
	}

	*val = field->value.bytestreamval.data;
	*len = field->value.bytestreamval.len;

	ret = 0;

out:
	if (ret != 0) {
		tmp_errno = errno;
		ript_msg_field_str(field_name, sizeof(field_name),
		    fieldid);
		error = isi_system_error_new(tmp_errno,
		    "Error parsing %s: %s", field_name, strerror(tmp_errno));
	}
	isi_error_handle(error, error_out);
}

/* For now, we will assert that setting a field succeeds. If
 * setting a field fails, it's probably that we failed to
 * allocate memory. */
#define DEF_RIPT_MSG_MUTATOR(TYPE, FTYPE, UFIELD) \
int									\
ript_msg_set_field_##TYPE(struct siq_ript_msg *msg, uint16_t fieldid,	\
    uint8_t ver, TYPE##_t val)						\
{									\
	struct siq_ript_msg_field *field = NULL;			\
									\
	int tmp_errno = 0;						\
	int ret = -1;							\
									\
	ret = ript_msg_get_field(msg, fieldid, ver, true, &field);	\
	if (ret) {							\
		tmp_errno = errno;					\
		goto out;						\
	}								\
									\
	field->type = FTYPE;						\
	field->value.UFIELD = val;					\
									\
	ret = 0;							\
out:									\
	if (tmp_errno)							\
		errno = tmp_errno;					\
									\
	ASSERT(ret == 0, "Failed to set message field: %u: %s", fieldid,\
	    strerror(tmp_errno));					\
	return ret;							\
}									\

DEF_RIPT_MSG_MUTATOR(int8, SIQFT_INT8, i8val);
DEF_RIPT_MSG_MUTATOR(uint8, SIQFT_UINT8, u8val);
DEF_RIPT_MSG_MUTATOR(int16, SIQFT_INT16, i16val);
DEF_RIPT_MSG_MUTATOR(uint16, SIQFT_UINT16, u16val);
DEF_RIPT_MSG_MUTATOR(int32, SIQFT_INT32, i32val);
DEF_RIPT_MSG_MUTATOR(uint32, SIQFT_UINT32, u32val);
DEF_RIPT_MSG_MUTATOR(int64, SIQFT_INT64, i64val);
DEF_RIPT_MSG_MUTATOR(uint64, SIQFT_UINT64, u64val);

int
ript_msg_set_field_float64(struct siq_ript_msg *msg, uint16_t fieldid,
    uint8_t ver, double val)
{
	struct siq_ript_msg_field *field = NULL;

	int tmp_errno = 0;
	int ret = -1;

	ret = ript_msg_get_field(msg, fieldid, ver, true, &field);
	if (ret) {
		tmp_errno = errno;
		goto out;
	}

	field->type = SIQFT_FLOAT64;
	field->value.f64val = val;

	ret = 0;
out:
	if (tmp_errno)
		errno = tmp_errno;

	ASSERT(ret == 0, "Failed to set message field: %u", fieldid);
	return ret;
}

int
ript_msg_set_field_str(struct siq_ript_msg *msg, uint16_t fieldid,
    uint8_t ver, char *val)
{
	struct siq_ript_msg_field *field = NULL;

	int tmp_errno = 0;
	int ret = -1;

	ret = ript_msg_get_field(msg, fieldid, ver, true, &field);
	if (ret) {
		tmp_errno = errno;
		goto out;
	}

	field->type = SIQFT_STRING;
	field->value.strval = val;

	ret = 0;
out:
	if (tmp_errno)
		errno = tmp_errno;

	ASSERT(ret == 0, "Failed to set message field: %u", fieldid);
	return ret;
}

int
ript_msg_set_field_bytestream(struct siq_ript_msg *msg, uint16_t fieldid,
    uint8_t ver, uint8_t *val, uint32_t len)
{
	struct siq_ript_msg_field *field = NULL;

	int tmp_errno = 0;
	int ret = -1;

	ret = ript_msg_get_field(msg, fieldid, ver, true, &field);
	if (ret) {
		tmp_errno = errno;
		goto out;
	}

	field->type = SIQFT_BYTESTREAM;
	field->value.bytestreamval.data = val;
	field->value.bytestreamval.len = len;

	ret = 0;
out:
	if (tmp_errno)
		errno = tmp_errno;

	ASSERT(ret == 0, "Failed to set message field: %u", fieldid);
	return ret;
}

static int
pack_msg(unsigned type, void *buf_, unsigned buflen,
    void *msg_)
{
	unsigned char *buf = buf_, *msg = msg_;
	const char *at;
	const unsigned char *start = buf;
	unsigned sized = -1;
	int i, j = 0;

	if (is_ript_msg_type(type))
		return pack_ript_msg((struct siq_ript_msg *)msg_, buf_);

	at = get_message_packing(type);//////根据这条消息的类型,获得这条消息里面的数据类型,从而知道pack,unpack时的指针如何移动
	i = msgs_map[type];
	//////* s=string, u=unsigned, U=u_int64_t, d=data */
	while (at && *at) {
		if (*at == 'u') { ////unsigned数据
			if (do_msg_dump)
				log(DEBUG, "    %s:\t%u", msgs[i].members[j], *((unsigned *)msg));
			buf += uset(buf, *((unsigned *)msg));
			msg += sizeof(unsigned);
		} else if (*at == 'U') {
			if (do_msg_dump)
				log(DEBUG, "    %s:\t%llu", msgs[i].members[j], *((uint64_t *)msg));
			ALIGN_PTR(msg);
			*((unsigned *)buf) = htonl(*(u_int64_t *)msg >> 32);
			buf += sizeof(unsigned);
			*((unsigned *)buf) = htonl(*(u_int64_t *)msg);
			buf += sizeof(unsigned);
			msg += sizeof(u_int64_t);
		} else if (*at == 's') {///string
			if (do_msg_dump)
				log(DEBUG, "    %s:\t\"%s\"", msgs[i].members[j], *(char **)msg);
			ALIGN_PTR(msg);
			sized = *((char **)msg) ? strlen(*(char **)msg) + 1 : 0;
			buf += uset(buf, sized);////先把这个msg的长度保存在buf上,buf指针后移
			memcpy(buf, *((char **)msg), sized);///再把这个msg的内容保存在buf上
			buf += sized;////buf指针后移
			msg += sizeof(unsigned char *);
		} else if (*at == 'b') {
			if (do_msg_dump)
				log(DEBUG, "    %s:\tbinary len %u", msgs[i].members[j], *((unsigned *)msg));
			sized = *((unsigned *)msg);
			msg += sizeof(unsigned);
			buf += uset(buf, sized);///先拷贝数据长度
			ALIGN_PTR(msg);
			memcpy(buf, *((char **)msg), sized);////再拷贝数据本身
			buf += sized;
			msg += sizeof(unsigned char *);
		} else if (*at == 'd') {
			if (do_msg_dump)
				log(DEBUG, "    %s:\tdata len %u", msgs[i].members[j], *((unsigned *)msg));
			sized = *((unsigned *)msg);
			buf += sized;
		} else
			log(FATAL, "Unknown pack format: %s", at);
		at++;
		j++;
	}

	return buf - start;
}

static int
unpack_msg(unsigned type, void *msg_, void *buf_, int len)
{
	unsigned char *msg = msg_, *buf = buf_;
	const char *at;
	unsigned sized = -1, uh, ul;
	const unsigned char *start = buf;
	uint64_t u64;
	int i, j = 0;
	unsigned char *orig_msg;

	struct siq_ript_msg *m = msg_;

	if (is_ript_msg_type(type))
		return unpack_ript_msg(m, buf_, len);
	//////* s=string, u=unsigned, U=u_int64_t, d=data */
	at = get_message_packing(type);
	i = msgs_map[type];
	log(TRACE, "Received %s from %s",
	    msg_type_str(type), last_msg_from);
	while (at && *at) {
		orig_msg = msg;
		if (*at == 'u') {
			buf += uget(buf, (unsigned *)msg); ///把buf中的字节拷贝出来给msg,拷贝后buf指针后移
			if (do_msg_dump)
				log(DEBUG, "    %s:\t%u", msgs[i].members[j], *((unsigned *)orig_msg));
			msg += sizeof(unsigned); ///msg指针后移
		} else if (*at == 'U') {
			ALIGN_PTR(msg);
			buf += uget(buf, &uh);
			buf += uget(buf, &ul);
			u64 = uh;
			*(u_int64_t *)msg = u64 << 32 | ul;
			if (do_msg_dump)
				log(DEBUG, "    %s:\t%llu", msgs[i].members[j], *((uint64_t *)orig_msg));
			msg += sizeof(u_int64_t);
		} else if (*at == 's') {
			ALIGN_PTR(msg);
			buf += uget(buf, &sized);
			*((unsigned char **)msg) = sized ? buf : 0;///字符串有'\0',会自动截断
			buf += sized;
			if (do_msg_dump)
				log(DEBUG, "    %s:\t\"%s\"", msgs[i].members[j], *(char **)orig_msg);
			msg += sizeof(unsigned char *);
		} else if (*at == 'b') {
			buf += uget(buf, &sized);
			*((unsigned *)msg) = sized;
			msg += sizeof(unsigned);
			ALIGN_PTR(msg);
			*((unsigned char **)msg) = buf;
			buf += sized;
			if (do_msg_dump)
				log(DEBUG, "    %s:\tbinary len %u", msgs[i].members[j], *((unsigned *)orig_msg));
			msg += sizeof(unsigned char *);
		} else if (*at == 'd') {
			*((unsigned *)msg) = len - (buf - start);
			msg += sizeof(unsigned);
			ALIGN_PTR(msg);
			*((unsigned char **)msg) = buf;
			if (do_msg_dump)
				log(DEBUG, "    %s:\tdata len %u", msgs[i].members[j], *((unsigned *)orig_msg));
			/* don't know how to move msg here.  Check that
			 * this is last pack chr. */
		} else
			log(FATAL, "Unknown pack format: %s", at);
		at++;
		j++;
	}

	return buf - start;
}

static int
msg_out_size(unsigned type, unsigned char *msg)
{
	const char *at = NULL;
	unsigned size = 0;
	unsigned sized = 0;

	struct siq_ript_msg *m = (struct siq_ript_msg *)msg;
	if (is_ript_msg_type(type))
		return ript_msg_out_size(m);

	at = get_message_packing(type);
	while (at && *at) {
		if (*at == 'u') {
			size += sizeof(unsigned);
			msg += sizeof(unsigned);
		} else if (*at == 'U') {
			ALIGN_PTR(msg);
			size += sizeof(u_int64_t);
			msg += sizeof(u_int64_t);
		} else if (*at == 's') {
			ALIGN_PTR(msg);
			sized = *(char **)msg ? strlen(*(char **)msg) + 1 : 0;
			size += sized;
			size += sizeof(unsigned);
			msg += sizeof(unsigned char *);
		} else if (*at == 'd') {
			sized = *((unsigned *)msg);
			size += sized;
		} else if (*at == 'b') {
			sized = *((unsigned *)msg);
			msg += sizeof(unsigned);
			ALIGN_PTR(msg);
			size += sizeof(unsigned) + sized;
			msg += sizeof(unsigned char *);
		} else
			log(FATAL, "Unknown pack str: %s", at);

		at++;
	}
    return size;
}

/*
 * This checks that the actual message size is matched, not
 * just to rep_msg.  This means that the other messages can just
 * use this func to check the whole message size.
 */
static int
rep_msg_unpack(struct rep_msg *msg, char *buf, int buflen, int *needed)
{
	char *p;

	if (buflen < sizeof(struct rep_msg)) {
		*needed = sizeof(struct rep_msg);
		return -1;
	}

	p = buf;
	p += uget(p, &msg->len);
	p += uget(p, &msg->type);

	if (buflen < msg->len) {
		*needed = msg->len;
		return -1;
	}

	return sizeof(struct rep_msg);
}

/*
 * timeouts
 */

#define USECINSEC 1000000

struct timeout
{
	struct timeout *next;
	void *ctx;
	struct timeval tv;
	int (*cb)(void *);
};

static int
timeval_greater(struct timeval *reference, struct timeval *cmp)
{
	if (cmp->tv_sec > reference->tv_sec)
		return 1;

	if (cmp->tv_sec < reference->tv_sec)
		return 0;

	if (cmp->tv_usec >= reference->tv_usec)
		return 1;

	return 0;
}

/* relative time non translated raw added */
static struct timeout *pre_proc_timeouts = NULL;

/* translated to real time ready to fire */
static struct timeout *timeouts = NULL;
static struct timeout sigtimer = {0};

void *
migr_register_timeout(struct timeval *tv, int (*cb)(void *), void *ctx)
{
	struct timeout *tout;

	if (ctx != TIMER_SIGNAL)
		tout = malloc(sizeof(struct timeout));
	else if (!sigtimer.ctx)
		tout = &sigtimer;
	else /* This is a signal, but no timeout is available. */
		return 0;
	ASSERT(tout);
	tout->tv = *tv;
	tout->cb = cb;
	tout->ctx = ctx;
	tout->next = pre_proc_timeouts;
	pre_proc_timeouts = tout;
	return tout;
}

void
migr_cancel_timeout(void *timer)
{
	struct timeout **t;

	for (t = &pre_proc_timeouts; *t; t = &(*t)->next) {
		if (*t == timer) {
			*t = (*t)->next;
			if ((void *)timer != &sigtimer)
				free((void *)timer);
			return;
		}
	}

	for (t = &timeouts; *t; t = &(*t)->next) {
		if (*t == (void *)timer) {
			*t = (*t)->next;
			if ((void *)timer != &sigtimer)
				free((void *)timer);
			break;
		}
	}
}

void
migr_cancel_all_timeouts(void)
{
	struct timeout *t, *n;

	t = pre_proc_timeouts;
	while (t) {
		n = t->next;
		if ((void *)t != &sigtimer)
			free(t);
		t = n;
	}
	pre_proc_timeouts = NULL;

	t = timeouts;
	while (t) {
		n = t->next;
		if ((void *)t != &sigtimer)
			free(t);
		t = n;
	}
	timeouts = NULL;
}

static int
timeouts_pending(void)
{
	return pre_proc_timeouts || timeouts;
}

static int
pre_process_timeouts(struct timeval *n)
{
	struct timeout *t;
	struct timeout *tmp;
	struct timeout *next;

	/* set up the preprocs relative to now */
	t = pre_proc_timeouts;
	pre_proc_timeouts = NULL;

	while (t) {
		next = t->next;
		t->tv.tv_sec += n->tv_sec;
		t->tv.tv_usec += n->tv_usec;
		while (t->tv.tv_usec >= USECINSEC) {
			t->tv.tv_sec++;
			t->tv.tv_usec -= USECINSEC;
		}
		if (!timeouts || timeval_greater(&t->tv, &timeouts->tv)) {
			t->next = timeouts;
			timeouts = t;
		} else {
			tmp = timeouts;
			while (tmp->next) {
				if (timeval_greater(&t->tv, &tmp->next->tv)) {
					t->next = tmp->next;
					tmp->next = t;
					break;
				}
				tmp = tmp->next;
			}
			if (!tmp->next) {
				t->next = 0;
				tmp->next = t;
			}
		}
		t = next;
	}
	return 0;
}

static int
process_timeouts(struct timeval *n)
{
	struct timeout *t;

	pre_process_timeouts(n);

	t = timeouts;
	while (t && timeval_greater(&t->tv, n)) {
		timeouts = timeouts->next;
		t->cb(t->ctx);
		if (t->ctx != TIMER_SIGNAL)
			free(t);
		else
			t->ctx = 0;
		t = timeouts;
	}

	pre_process_timeouts(n);
	return 0;
}

static int
next_timeout_diff(struct timeval *t, struct timeval *tv)
{
	/* no timeouts */
	if (!timeouts) {
		return -1;
	}

	/* timeouts post due */
	if (!timeval_greater(t, &timeouts->tv)) {
		tv->tv_sec = 0;
		tv->tv_usec = 0;
		return 0;
	}

	/* timeouts in the future */
	tv->tv_sec = timeouts->tv.tv_sec - t->tv_sec;
	if (timeouts->tv.tv_usec >= t->tv_usec) {
		tv->tv_usec = timeouts->tv.tv_usec - t->tv_usec;
	} else {
		tv->tv_sec --;
		tv->tv_usec = USECINSEC - t->tv_usec + timeouts->tv.tv_usec;
	}

	return 0;
}


/*
 * Time interval to send next NOOP MSG to pworker.
 * On network error, typically retry is seen for 30 sec
 * before the stack timeout's. 
 */
#define DEFAULT_NOOP_INTERVAL 20 /* In Sec */

/*
 * Callbacks
 */
#define NUM_FDS 2000

struct fd_info {
	int fd;
	int used;
	char *buf;
	int bufsize;
	int parsed;
	int inbuf;
	void *ctx;
	int discon_pending;
	time_t lastaction;
	migr_callback_t cbs[MAX_MSG + WHITELIST_MAX_MSG];
	migr_timeout_t group_cb;
	migr_timeout_t flexnet_cb;
	char *fd_name;
	bool skip_noop;
	time_t last_noop_time; /* Last noop msg send time */
	size_t noop_left; /* Total bytes left due to partial send */
	enum socket_type sock_type;
	burst_fd_t burst_fd;
	char *burst_send_buf;
	int burst_buf_used;
	int burst_buf_size;
};

static struct fd_info fds[NUM_FDS];
static int fd_count = 0;
static int fd_next = 0;
static int fd_max = 0;
static int ipc_fd = -1;
static int kqueue_fd = -1;
static int flexnet_conf_fd = -1;
static struct kevent kev;
static struct timespec ts = {0, 0};

static int parse_and_dispatch(struct fd_info *f);

/*
 * This is only for messages which are all data
 */
unsigned char *
get_data_offset(int fd, int *buflen)
{
	struct fd_info *info;

	info = find_fd_info(fd);
	*buflen = IDEAL_READ_SIZE + CHECKSUM_SIZE;
	if (!info || info->sock_type == SOCK_TCP) {
	/*
	 * This works as long as rep_msg is smaller than 64
	 * (see definition of BUFSIZE and IDEAL_READ_SIZE above)
	 */
		return ((unsigned char *)generic_send_buf +
		    sizeof(struct rep_msg));
	} else {
		return ((unsigned char *)info->burst_send_buf +
		    info->burst_buf_used + sizeof(struct rep_msg));
	}

}

static struct fd_info *
find_fd_info(int fd)
{
	int i;

	for (i = 0; i < fd_count; i++) {
		if (!fds[i].used)
			continue;
		if (fds[i].fd == fd)
			return &fds[i];
		else if (fd_max < fds[i].fd)
			fd_max = fds[i].fd;
	}

	return NULL;
}

enum socket_type
sock_type_from_fd_info(int fd)
{
	struct fd_info *info = NULL;

	info = find_fd_info(fd);
	if (!info)
		return SOCK_TCP;
	else
		return info->sock_type;
}

burst_fd_t
burst_fd_from_fd_info(int fd)
{
	struct fd_info *info = NULL;

	info = find_fd_info(fd);
	ASSERT(info && info->sock_type == SOCK_BURST);
	return info->burst_fd;
}

static void
send_noop(int sock, unsigned noop_bytes, struct fd_info *info,
    struct isi_error **error_out)
{
	int msg_len, rv, kept_errno;
	char buf[16];
	struct timeval timeout;
	struct generic_msg m;
	struct isi_error *error = NULL;

	/* Set the socket to non-blocking */
	timeout.tv_sec = 0;
	timeout.tv_usec = 10;
	if ((rv = setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout,
	    sizeof(struct timeval))) < 0) {
		kept_errno = errno;
		log(ERROR, "setsockopt failed on socket conn with"
		    "pworker : %s", strerror(errno));
		error = isi_system_error_new(errno);
		goto out;
	}

	m.head.type = NOOP_MSG;
	msg_len = generic_msg_pack(&m, buf, 16);
	ASSERT(msg_len < 16); /* NOOP_MSG takes 8 bytes */

	if (!info->noop_left) {
		info->noop_left = msg_len;
	}

	if (!noop_bytes) {
		noop_bytes = info->noop_left;
	} else {
		noop_bytes = MIN(noop_bytes, info->noop_left);
	}

	rv = send(sock, buf + (msg_len - info->noop_left), noop_bytes,
	    MSG_NOSIGNAL);
	if (rv == -1 && errno != EWOULDBLOCK) {
		kept_errno = errno;
		log(ERROR, "Error while sending heart beat NOOP_MSG"
		    " : %s", strerror(errno));
		error = isi_system_error_new(kept_errno);
		goto out;
	}

	if (rv > 0) {
		log(TRACE, "sent %d bytes of NOOP", rv);
		info->noop_left -= rv;
	}

	info->last_noop_time = time(0);

	/* Reset the socket back to blocking mode */
	timeout.tv_sec = 0;
	timeout.tv_usec = 0;
	if ((rv = setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout,
	    sizeof(struct timeval))) < 0) {
		kept_errno = errno;
		log(ERROR, "setsockopt failed on socket conn with"
		    "pworker : %s", strerror(errno));
		error = isi_system_error_new(errno);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

/**
 * Check connection with primary
 * force could be used to always send NOOP bytes to peer.
 * noop_bytes if set  indicates number of bytes (of NOOP_MSG)
 * to be sent in the call.
 * NOTE !!! We should never access socket if this function 
   returns errors
 */
void
check_connection_with_peer(int sock, bool force, unsigned noop_bytes,
    struct isi_error **error_out)
{
	char c;
	int rv;
	int kept_errno;
	struct timeval timeout;
	fd_set  rset, wset;
	struct isi_error *error = NULL;
	struct fd_info *info = NULL;

	log(TRACE, "check_connection_with_peer");

	if (sock == SOCK_LOCAL_OPERATION)
		goto out;

	ASSERT(sock != 0);

	info = find_fd_info(sock);
	if (!info) {
		log(ERROR, "Invalid socket fd");
		error = isi_system_error_new(EINVAL);
		goto out;
	}

	FD_ZERO(&rset);
	FD_ZERO(&wset);
	FD_SET(sock, &rset);
	FD_SET(sock, &wset);

	/* First check socket status in non-blocking way*/	
	timeout.tv_sec = 0;
	timeout.tv_usec = 0;
	rv = select(sock + 1, &rset, &wset, NULL, &timeout);
	if (rv == -1) {
		kept_errno = errno;
		log(ERROR, "Select call failed with pworker socket: %s",
		    strerror(errno));
		error = isi_system_error_new(errno);
		goto out;
	}

	if (FD_ISSET(sock, &wset) && (force ||
	    (time(0) - info->last_noop_time) > DEFAULT_NOOP_INTERVAL)) {
		/*
		 * This means that send socket buffer is not full.
		 * But we are not sure if there is data in send buffer
		 * or not. So send a NOOP_MSG in non-blocking mode
		 * only after SEND_INTERVAL (see above )
		 */
		send_noop(sock, noop_bytes, info, &error);
		if (error)
			goto out;

		/*
		 * If this is a forced check, it means it's critical code. In
		 * order to be absolutely sure the connection is still open, we
		 * have to send noop twice without errors. Sockets are weird -
		 * if the socket is closed, the first send_noop may still
		 * succeed and simply set a flag (RST flag) so that the next
		 * send fails.
		 */
		if (force) {
			send_noop(sock, noop_bytes, info, &error);
			if (error)
				goto out;
		}
	}

	if (FD_ISSET(sock, &rset)) {
		/*
		 * We do read with MSG_PEEK. This could be triggered
		 * either due to data in recv buffer or connection close.
		 */
		rv = recv(sock, &c, 1, MSG_PEEK);
		if (rv == -1) {
			kept_errno = errno;
			if (errno == ECONNRESET) {
				log(ERROR, "Connection reset by peer pworker");
			} else if (errno == ETIMEDOUT) {
				log(ERROR, "Connection timeout due to network"
				    "error");
			} else {
				log(ERROR, "recv error on pworker socket : %s",
				    strerror(errno));
			}
			error = isi_system_error_new(kept_errno);
			goto out;
		}
	}

out:
	isi_error_handle(error, error_out);
}

static int
add_kevent(void)
{
	/* When the flexnet conf file changes, this fd becomes invalid
	 * Solution: close and reopen it
	 */
	if (flexnet_conf_fd >= 0) {
		close(flexnet_conf_fd);
		flexnet_conf_fd = -1;
	}
	flexnet_conf_fd = open( "/ifs/.ifsvar/modules/flexnet/flx_config.xml",
	    O_RDONLY | O_NONBLOCK);
	if (flexnet_conf_fd < 0) {
		log(ERROR, "Failed to open Flexnet conf.");
		return 1;
	}
	memset(&kev, 0, sizeof(struct kevent));
	EV_SET(&kev, flexnet_conf_fd, EVFILT_VNODE, EV_ADD | 
		EV_ENABLE | EV_ONESHOT, NOTE_DELETE | NOTE_WRITE | 
		NOTE_EXTEND | NOTE_RENAME | NOTE_REVOKE, 0, 0);

	if (kevent(kqueue_fd, &kev, 1, NULL, 0, &ts) < 0) {
		log(ERROR, "Failed to add kevent.");
		return 1;
	}

	return 0;
}

void
migr_register_callback(int fd, unsigned type, migr_callback_t cb)
{
	struct fd_info *info;
	int ript_msg_type = -1;

	if (is_ript_msg_type(type)) {
		ript_msg_type = unbuild_ript_msg_type(type);
		ASSERT(ript_msg_type >= 0 &&
		(ript_msg_type < RM_MAX_MSG || rm_in_whitelist(ript_msg_type)));
	} else
		ASSERT(type > REP_MSG && type < MAX_MSG);

	info = find_fd_info(fd);

	ASSERT(info != NULL, "type = %u, fd = %d", type, fd);

	info->cbs[(ript_msg_type == -1) ? type :
	    (ript_msg_type + MAX_MSG)] = cb;
}

migr_callback_t
migr_get_registered_callback(int fd, unsigned type)
{
	struct fd_info *info;
	int ript_msg_type = -1;

	ASSERT((type > REP_MSG && type < MAX_MSG) ||
	    (is_ript_msg_type(type) &&
	    (ript_msg_type = unbuild_ript_msg_type(type)) >= 0 &&
	    (ript_msg_type < RM_MAX_MSG || rm_in_whitelist(ript_msg_type))));

	info = find_fd_info(fd);
	ASSERT(info != NULL, "type = %u, fd = %d", type, fd);

	return info->cbs[(ript_msg_type == -1) ? type :
	    (ript_msg_type + MAX_MSG)];
}

void
migr_unregister_all_callbacks(int fd)
{
	struct fd_info *info;

	info = find_fd_info(fd);
	if (!info)
		return;
	for (int i = 0; i < MAX_MSG + WHITELIST_MAX_MSG; i++)
		info->cbs[i] = NULL;
}

int
migr_register_group_callback(migr_timeout_t cb, void *ctx)
{
	struct fd_info *info;
	int devid;
	
	if (ipc_fd != -1) {
		migr_rm_fd(ipc_fd);
		ipc_fd = -1;
	}

	for (; (ipc_fd = ipc_connect(&devid)) == -1; siq_nanosleep(5, 0));
	migr_add_fd(ipc_fd, ctx, "group");

	info = find_fd_info(ipc_fd);
	info->group_cb = cb;
	info->skip_noop = true;

	return devid;
}

void
migr_register_flexnet_callback(migr_timeout_t cb, void *ctx)
{
	struct fd_info *info;

	if (kqueue_fd < 0) {
		kqueue_fd = kqueue();
		ASSERT(kqueue_fd != -1);
	}

	if (add_kevent() != 0) {
		log(ERROR, "Failed to add_kevent().");
		return;
	}

	/* Only need to add_fd once */
	if (migr_add_fd(kqueue_fd, ctx, "flexnet") != 0) {
		log(ERROR, "Failed to migr_add_fd().");
		return;
	}

	info = find_fd_info(kqueue_fd);
	ASSERT(info != NULL);
	info->flexnet_cb = cb;
	info->skip_noop = true;
}


static void __attribute__((constructor))
sigpipe_init(void)
{
	signal(SIGPIPE, SIG_IGN);
}

static int
_migr_add_fd(int fd, void *ctx, char *name, bool skip_noop)
{
	ASSERT(fd != -1);

	fds[fd_next].fd = fd;
	fds[fd_next].fd_name = strdup(name);
	fds[fd_next].used = 1;
	fds[fd_next].buf = (char *)malloc(IBUFSIZE);
	fds[fd_next].bufsize = IBUFSIZE;
	fds[fd_next].parsed = 0;
	fds[fd_next].inbuf = 0;
	fds[fd_next].ctx = ctx;
	fds[fd_next].discon_pending = 0;
	fds[fd_next].lastaction = time(0);
	fds[fd_next].skip_noop = skip_noop;
	fds[fd_next].noop_left = 0;
	fds[fd_next].burst_send_buf = NULL;
	fds[fd_next].burst_buf_size = 0;
	fds[fd_next].burst_buf_used = 0;
	memset(fds[fd_next].cbs, 0, sizeof(fds[fd_next].cbs));

	if (fd_count == fd_next)
		fd_count++;

	for (; fd_next < NUM_FDS && fds[fd_next].used; fd_next++);

	if (fd_next == NUM_FDS) {
		log(ERROR, "add_fd: Exceeded NUM_FDS");
		return 1;
	}

	if (fd_max < fd)
		fd_max = fd;

	return 0;
}

int
migr_add_listen_fd(int fd, void *ctx, char *name)
{
	return _migr_add_fd(fd, ctx, name, true);
}

int
migr_add_fd(int fd, void *ctx, char *name)
{
	return _migr_add_fd(fd, ctx, name, false);
}

int
migr_set_fd(int fd, void *ctx)
{
	struct fd_info *info;

	info = find_fd_info(fd);
	if (!info)
		return 1;

	info->ctx = ctx;
	return 0;
}

int
migr_rm_fd(int fd)
{
	int i, found = -1, max = 0;

	for (i = 0; i < fd_count; i++) {
		if (!fds[i].used)
			continue;
		if (fds[i].fd == fd)
			found = i;
		else if (max < fds[i].fd)
			max = fds[i].fd;
	}
	if (found < 0) {
		log(ERROR, "rm_fd: fd not found");
		return 1;
	}
	free(fds[found].buf);
	fds[found].used = 0;
	fds[found].fd = -1;
	free(fds[found].fd_name);
	fds[found].fd_name = NULL;
	if (found < fd_next)
		fd_next = found;
	if (fd_count == found - 1)
		fd_count--;
	fd_max = max;
	return 0;
}

void
migr_rm_and_close_all_fds()
{
	int i;

	for (i = 0; i < fd_count; i++) {
		if (!fds[i].used)
			continue;
		free(fds[i].buf);
		fds[i].used = 0;
		close(fds[i].fd);
		fds[i].fd = -1;
	}
	fd_next = 0;
	fd_count = 0;
	fd_max = 0;
	ipc_fd = -1;
	kqueue_fd = -1;
	flexnet_conf_fd = -1;
}

static int
mark_fd_closed(int fd)
{
	int i;

	for (i = 0; i < fd_count; i++) {
		if (fds[i].used && fds[i].fd == fd) {
			fds[i].discon_pending = 1;
			return 0;
		}
	}
	return -1;
}

void
migr_fd_set_burst_recv_buf(int fd, int buf_size)
{
	struct fd_info *info;

	info = find_fd_info(fd);
	ASSERT(info != NULL);

	if (buf_size < info->bufsize)
		return;

	info->buf = realloc(info->buf, buf_size);
	info->bufsize = buf_size;
}

void
migr_fd_set_burst_sock(int fd, burst_fd_t burst_fd, int buf_size)
{
	struct fd_info *info;

	info = find_fd_info(fd);
	ASSERT(info != NULL);

	info->sock_type = SOCK_BURST;
	info->burst_fd = burst_fd;
	info->skip_noop = true;

	/* We would track this fd for stats and bwt */	
	set_tracked_fd(fd);

	if (buf_size > 0) {
		info->burst_send_buf = malloc(buf_size);
		info->burst_buf_size = buf_size;
		info->burst_buf_used = 0;
	}
}

static int
parse_and_dispatch(struct fd_info *f)
{
	int ret;
	int needed;
	int index;
	struct isp_op_delta track_op;

	/* Bug 150379 - break if there was a write failure */
	if (f->discon_pending) {
		return -1;
	}

	ISP_OP_INIT(&track_op);

	/*
	 * ---------------------------------------------------------
	 * |xxxxxxxx|XXXXXXXXXXXXXXXXXXXXX|                        |
	 * ---------------------------------------------------------
	 *       parsed                 inbuf                   bufsize
	 *
	 * x = already processed data
	 * X = to process data
	 */

	while (1) {
		struct generic_msg GEN_MSG_INIT_CLEAN(msg);

		ret = generic_msg_unpack(&msg, f->buf + f->parsed,
					f->inbuf - f->parsed, &needed);
		if (ret <= 0) {
			/* If to fill this message we need more
			   that space in the buffer, the move it back */
			if (needed > f->bufsize) {
				ASSERT(needed <= BUFSIZE,
				    "Invalid message len %d", needed);
				f->buf = realloc(f->buf, BUFSIZE);
				f->bufsize = BUFSIZE;
			}
			if (needed > f->bufsize - f->inbuf) {
				memmove(f->buf, f->buf + f->parsed,
						f->inbuf - f->parsed);
				f->inbuf -= f->parsed;
				f->parsed = 0;
			}
			break;
		}
#ifdef MSG_DEBUG_PRINT
		print_msg(&msg);
#endif
		RECV_RPC_MSG_LOG(msg);
		f->lastaction = time(0);

		ASSERT(msg.head.type < MAX_MSG ||
		    is_ript_msg_type(msg.head.type));
		ISP_OP_BEG(&track_op, ISP_PROTO_SIQ,
		    is_ript_msg_type(msg.head.type) ? ISP_SIQ_RIPT_OFFSET +
		    unbuild_ript_msg_type(msg.head.type) : msg.head.type);
		track_op.out_bytes = 0;
		track_op.in_bytes = msg.head.len;

		index = is_ript_msg_type(msg.head.type) ?
		    unbuild_ript_msg_type(msg.head.type) + MAX_MSG :
		    msg.head.type;

		if (msg.head.type != NOOP_MSG) {
			if ((msg.head.type >= build_ript_msg_type(RM_MAX_MSG) &&
			    !rm_in_whitelist(unbuild_ript_msg_type(msg.head.type))) ||
			    (msg.head.type >= MAX_MSG && msg.head.type <
			    build_ript_msg_type(0))) {
				log(INFO, "Received msg of unknown type: %x",
				    msg.head.type);
			} else if (f->cbs[index]) {
				f->cbs[index](&msg, f->ctx);
			} else {
				log(INFO, "Received msg of unknown type: %x",
				    msg.head.type);
			}
		}
		ISP_OP_END(&track_op);
		f->parsed += ret;
		if (!f->used) {
			return 0; /* until it's too late, my dear :) */
		} else if (f->discon_pending) {
			/* Bug 150379 - break if there was a write failure */
			return -1;
		}

	}
	if (f->discon_pending) {
		return -1;
	} else {
		return 0;
	}
}

/*
 * Returns 0 on success,
 * -1 on successful disconnect (no errors),
 * and 1 on error.
 */
static int
read_fd(struct fd_info *f)
{
	int ret = 0;
	int got;

	got = recv(f->fd, f->buf + f->inbuf,
	    f->bufsize - f->inbuf, 0);

	if (got > 0) {
		add_recv_netstat(f->fd, got);
		f->inbuf += got;
		if (-1 == parse_and_dispatch(f)) {
			ret = 1;
		}
	} else if (got == 0) {
		/* Disconnect without errors. */
		ret = -1;
	} else {
		ret = 1;
	}

	return ret;
}

static int
read_burst_fd(struct fd_info *f)
{
	int ret = 0;
	int got;
	int msg_size = 0;
	burst_pollfd_t pollfd;
	burst_fd_t r;

	r = f->burst_fd;
	ASSERT(r >= 0);

	/*
	 * Without waiting for any data check if the 
	 * data is already available in the socket.
	 * We have already used select in outer call
	 * to check if the data is available.
	 */
	pollfd._fd = r;
	pollfd._events = BURST_POLLIN;
	pollfd._revents = 0;
	burst_poll(&pollfd, 1, 0);

	if (pollfd._revents & BURST_POLLIN) {
		/* First get the msg_size */
		msg_size = burst_peek_msg_size(r);
		if (msg_size < 0 || (msg_size > f->bufsize)) {
			ret = 1;
			goto out;
		}
		if (msg_size == 0)
			goto out;
		
		/* If there is not much space left , then make some room */
		if ((f->bufsize - f->inbuf) < msg_size) {
			memmove(f->buf, f->buf + f->parsed,
			    f->inbuf - f->parsed);
			f->inbuf -= f->parsed;
			f->parsed = 0;
			/* Now we should have more space */
			if ((f->bufsize - f->inbuf) < msg_size) {
				ret = 1;
				goto out;
			}
		}

		got = burst_recv(r, f->buf + f->inbuf,
		    f->bufsize - f->inbuf);
		if (got == -1)
		{
			log(ERROR, "Connection error");
			ret = 1;
			goto out;
		}

		log(TRACE, "Received %d bytes from burst", got);

		add_recv_netstat(f->fd, got);
		f->inbuf += got;
		if (-1 == parse_and_dispatch(f)) {
			ret = 1;
		}
	}
out:
	return ret;
}

/*
 * Returns 0 on success,
 * -1 on successful disconnect (no errors),
 * and 1 on error.
 */
static int
read_sock_fd(struct fd_info *f)
{
	int ret = 0;

	last_msg_from = f->fd_name;
	switch(f->sock_type) {
		case SOCK_TCP:
			ret = read_fd(f);
			break;
		case SOCK_BURST:
			ret = read_burst_fd(f);
			break;
		default:
			ASSERT(0);
	}
	return ret;
}

static int
read_len(int fd, char *buf, int len)
{
	int to_read, n_read, offset = 0;

	to_read = len;
	while (to_read > 0) {
		n_read = recv(fd, buf + offset, to_read, 0);
		if (n_read < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		if (n_read == 0)
			break;
		to_read -= n_read;
		offset += n_read;
	}
	return len - to_read;
}


/*
 * Do a synchronous read from a socket.
 */
enum read_msg_resp
read_msg_from_fd(int fd, struct generic_msg *msg, bool wait_for_it,
    bool registered_fd, char **ret_buf)
{
	struct fd_info *info = NULL;
	struct fd_info local_info = {};
	fd_set set;
	struct timeval timeout = {}, *t = NULL;
	struct rep_msg *hdr;
	int rc, n, junk;
	unsigned len;

	if (registered_fd) {	
		info = find_fd_info(fd);
		if (!info) {
			log(ERROR, "%s: Attempt to read unregistered fd",
			    __func__);
			return READ_MSG_RESP_ERROR;
		}
	} else {
		info = &local_info;
	}

	/* Just do a poll */
	if (!wait_for_it)
		t = &timeout;

	FD_ZERO(&set);
	FD_SET(fd, &set);
	if ((rc = select(fd + 1, &set, 0, 0, t)) != 1) {
		/* Timeout on poll */
		if (rc == 0)
			return READ_MSG_RESP_TIMEOUT;
		else {
			log(ERROR, "Failed on select: %s", strerror(errno));
			return READ_MSG_RESP_ERROR;
		}
	}

	last_msg_from = info->fd_name;

	if (!info->buf) {
		info->bufsize = IBUFSIZE;
		info->buf = calloc(1, info->bufsize);
		ASSERT(info->buf);
	}
	
	n = read_len(fd, info->buf, sizeof(struct rep_msg));
	if (n < 0) {
		log(ERROR, "Problem reading from socket of %s: %s",
		    info->fd_name, strerror(errno));
		return READ_MSG_RESP_ERROR;
	}
	if (n == 0) {
		log(DEBUG, "Read EOF from socket of %s", info->fd_name);
		return READ_MSG_RESP_EOF;
	}
	hdr = (struct rep_msg *) info->buf;
	len = ntohl(hdr->len);
	if (len > info->bufsize) {
		info->bufsize = len;
		info->buf = realloc(info->buf, len);
		ASSERT(info->buf);
	}

	/* For messages with more than a header */
	if (len > sizeof(struct rep_msg)) {
		n = read_len(fd, info->buf + sizeof(struct rep_msg),
		    len - sizeof(struct rep_msg));
		if (n <= 0) {
			log(ERROR, "Problem reading from socket. %s",
			    strerror(errno));
			return READ_MSG_RESP_ERROR;
		}
	}
	generic_msg_unpack(msg, info->buf, len, &junk);
	if (!registered_fd) {
		ASSERT(ret_buf != NULL);
		*ret_buf = info->buf;
	}
	return READ_MSG_RESP_OK;
}

void
migr_force_process_stop(void)
{
	log(DEBUG, "%s", __func__);
	force_process_stop = true;
}

int
migr_process(void)
{
	struct fd_set set;
	int i, s, selret, readret, ret = 0;
	struct timeval tv, timeout = {KEEPALIVE / 5, 0};
	struct generic_msg msg;
	char err[256];

	while ((fd_max || timeouts_pending()) && !force_process_stop) {

		gettimeofday(&now, NULL);
		process_timeouts(&now);
		if (!timeouts_pending() && !fd_max)
			continue;

		FD_ZERO(&set);
		for (i = 0; i < fd_count; i++) {
			if (!fds[i].used)
				continue;
			FD_SET(fds[i].fd, &set);
			if (fds[i].lastaction + KEEPALIVE < now.tv_sec &&
			    !fds[i].skip_noop) {
				msg.head.type = NOOP_MSG;
				msg_send(fds[i].fd, &msg);
			}
		}

		/* Catch force_proces_stop right before a select that will
		 * likely wait for the complete timeout if force_process_stop
		 * is set */
		if (force_process_stop)
			break;
		selret = select(fd_max + 1, &set, 0, 0,
		    next_timeout_diff(&now, &tv) == -1 ? &timeout : &tv);
		if (selret < 0 && errno != EINTR) {
			ret = -1;
			log(ERROR, "select() failed");
			goto exit;
		} else if (selret <= 0) /* Timeout or EINTR. */
			continue;

		for (i = 0; i < fd_count; i++) {
			if (!fds[i].used || !FD_ISSET(fds[i].fd, &set))
				continue;
			if (fds[i].fd == ipc_fd) {
				ipc_readgroup(ipc_fd);
				fds[i].group_cb(fds[i].ctx);
			} else if (fds[i].fd == kqueue_fd) {
				/* Dequeue an event from the kqueue */
				log(DEBUG, "Flexnet change received");
				ts.tv_sec = 1;
				if ((kevent(kqueue_fd, NULL, 0, &kev, 1, &ts) <
				    0) && errno != EINTR) {
					log(ERROR, "Failed to get kevent");
					continue;
				}

				log(TRACE, "Completed kevent");
				fds[i].flexnet_cb(fds[i].ctx);

				log(TRACE, "Completed callback");
				/* Readd the event back to the kqueue */
				if (add_kevent() != 0) {
					log(ERROR, "Failed to add_kevent().");
					continue;
				}
				log(TRACE, "Completed add_kevent");
			} else if (!fds[i].ctx) {
				s = accept(fds[i].fd, 0, 0);
				if (fds[i].cbs[CONN_MSG])
					fds[i].cbs[CONN_MSG](0, &s);
				else
					log(ERROR, "No callback for CONN");
			} else if ((readret = read_sock_fd(&fds[i]))) {
				/* readret < 0 means to to disconnect without
				 * errors. */
				if (readret > 0 && errno &&
				    !strerror_r(errno, err, sizeof(err)))
					log(INFO, "Read socket error: %s",
					    err);
				if (fds[i].cbs[DISCON_MSG]) {
					msg.head.type = DISCON_MSG;
					msg.body.discon.fd = fds[i].fd;
					msg.body.discon.fd_name = fds[i].fd_name;
					fds[i].cbs[DISCON_MSG](&msg, fds[i].ctx);
				} else
					log(ERROR, "No callback for DISCON");
			}
		}
	}
exit:
	log(DEBUG, "exiting from process function (%d, %d, %d)",
		fd_max, timeouts_pending(), force_process_stop);
	force_process_stop = false;
	return ret;
}

/*
 * generic_msg
 */
/*
struct generic_msg
{
	struct rep_msg  head;/////消息头
	union
	{
		struct cpu_throttle_init_msg cpu_throttle_init;
		struct cpu_throttle_stat_msg cpu_throttle_stat;
		struct cpu_throttle_msg cpu_throttle;
	}body;/////消息体
};
*/

int   //////把一个需要发送的消息打包: pack header, pack body
generic_msg_pack(struct generic_msg *msg, void *buf/*2*1024*1024 打包后的内容放进buf中*/, int buflen/*需要打包的字节数*/)
{
	int hdrlen;
	int bodylen;
	int totallen;

	/* set size */  /////消息头+消息体. 根据head.type来判断是哪条消息,从而计算出消息体大小
	totallen = sizeof(struct rep_msg/*response message作为header*/) + msg_out_size(msg->head.type,
			(unsigned char *)&msg->body);

	ASSERT(buflen >= totallen, "Send buffer too small (%d/%d)",
	    buflen, totallen);

	msg->head.len = totallen; //////总长度:消息头长度+消息体长度  保存在struct generic_msg的头部中的len字段

	/* pack header */
	hdrlen = pack_msg(REP_MSG, buf, buflen, &msg->head);/////打包的字节放在buf中

	/* pack body */
	bodylen = pack_msg(msg->head.type, (unsigned char *)buf + hdrlen,/////body字节放在header之后
	    buflen - hdrlen, &msg->body);

	if (totallen != hdrlen + bodylen)
		log(FATAL, "lengths don't match (%d v. %d)", totallen,
		    hdrlen + bodylen);
	return hdrlen + bodylen;
}

int
generic_msg_unpack(struct generic_msg *msg, void *buf, int len, int *need)
{
	int used;

	if ((used = rep_msg_unpack((struct rep_msg *)msg, buf, len, need)) < 0)  ////unpack 消息header
		return -1;
	unpack_msg(msg->head.type, &msg->body, (unsigned char *)buf + used, /////unpack 消息body
	    msg->head.len - sizeof(struct rep_msg));

	return msg->head.len;
}

static int
tcp_msg_send(int s, struct generic_msg *m)
{
	int len = -1, i;
	int rv = 0;
	volatile int err;
	bool found = false;
	struct isp_op_delta track_op;
	struct generic_msg tm;
	int msg_len;
	char buf[16];
	
	ASSERT(s != 0);
	for (i = 0; i < fd_count; i++)
		if (fds[i].used && fds[i].fd == s) {
			fds[i].lastaction = time(0);
			log(TRACE, "Sending %s to %s",
			    msg_type_str(m->head.type), fds[i].fd_name);
			found = true;
			break;
		}
	if (!found)
		log(DEBUG, "Sending %s on socket %d",
			    msg_type_str(m->head.type), s);

	if ((len = generic_msg_pack(m, generic_send_buf, BUFSIZE)) <= 0)
		return len;
	ASSERT(m->head.len == len, "head.len/len (%d/%d)", m->head.len, len);
	siq_track_op_beg(SIQ_OP_RPC_SEND);
	/* 
	 * SyncIQ will never purposely use fd 0 
	 * Only in case its local operation for ex ASSESS run,
	 * the socket descriptor will be set to special value
	 */
	if (s == SOCK_LOCAL_OPERATION)
		return -1;

	ISP_OP_INIT(&track_op);
	ISP_OP_BEG(&track_op, ISP_PROTO_SIQ, is_ript_msg_type(m->head.type) ?
	    ISP_SIQ_RIPT_OFFSET + ISP_SIQ_SND_OFFSET +
	    unbuild_ript_msg_type(m->head.type) : ISP_SIQ_SND_OFFSET +
	    m->head.type);
	track_op.out_bytes = len;
	track_op.in_bytes = 0;

	/* If there is NOOP bytes left, then send it */
	if (fds[i].noop_left) {
		log(TRACE, "%zd bytes left to be sent for NOOP_MSG",
		    fds[i].noop_left);
		tm.head.type = NOOP_MSG;
		msg_len = generic_msg_pack(&tm, buf, 16);
		ASSERT(msg_len < 16); /* NOOP_MSG takes 8 bytes */
		ASSERT(fds[i].noop_left <= msg_len);
		rv = full_send(s, buf + (msg_len - fds[i].noop_left),
		    fds[i].noop_left);
		if (rv == 0) {
			fds[i].noop_left = 0;
		}
	}

	if (!rv) {
		rv = full_send(s, generic_send_buf, len);
	}
	if (rv == -1) {
		err = errno;
		char *name = "";
		if (found && fds[i].fd_name)
			name = fds[i].fd_name;
		switch (errno) {
			case EPIPE:
			case ECONNABORTED:
			case ECONNRESET:
			case ETIMEDOUT:
			case ENOTCONN:
			case ENODEV:
				if (fds[i].discon_pending)
					break;
				log(NOTICE, "Closing %s (%d): %s", 
				    name, s, strerror(errno));
				mark_fd_closed(s);
				break;
			case EBADF:
			case EACCES:
			case ENOTSOCK:
			case EFAULT:
			case EMSGSIZE:
			case EAGAIN: //This should be handled by full_send
			case ENOBUFS:
			case EHOSTUNREACH:
			case EISCONN:
			case ECONNREFUSED:
			case EHOSTDOWN:
			case ENETDOWN:
			case EPERM:
				ASSERT(err == 0, "Bad send msg (%u, %u) "
				    "errno %d on conn %d name %s %d",
				    m->head.type, m->head.len, err, i, name,
				    s);
				break;
			default:
				ASSERT(err == 0, "Unexpected bad send msg"
				    " (%u, %u) errno %d on conn %d " 
				    "name %s %d", m->head.type, m->head.len, 
				    err, i, name, s);
				break;
				
		}
	}

	ISP_OP_END(&track_op);
	siq_track_op_end(SIQ_OP_RPC_SEND);
	/* Keep track of last few RPC msg types */
	SEND_RPC_MSG_LOG(m);
	if (rv == -1) {
		errno = err;
	}

	return rv;
}

static int
burst_msg_send(int s, struct generic_msg *m)
{
	int res = -1;
	int len;
	int unused_buf = 0;
	struct fd_info *info;

	info = find_fd_info(s);
	ASSERT(info && info->sock_type == SOCK_BURST);
	/* Burst should never have NOOP bits used */
	ASSERT(info->noop_left == 0);

	/*
	 * If there is enough room to coalesce the msg
	 * into burst_send_buffer then just add it to the end.
	 * Else flush the contents of burst_sedn_buffer and
	 * then add the msg to the buffer.
	 */
	unused_buf = info->burst_buf_size - info->burst_buf_used;
	log(TRACE, "burst_msg_send: msg: %d unused_buf: %d / %d", m->head.type, 
	    unused_buf, BUFSIZE);
	if (unused_buf <= BUFSIZE) {
		res = full_send(s, info->burst_send_buf, info->burst_buf_used);
		if (res < 0) {
			ASSERT(0);
		}
		info->burst_buf_used = 0;
	}

	// XXX TBD , Do we need to add RPC stats here XXX
	len =  generic_msg_pack(m, info->burst_send_buf + info->burst_buf_used,
	    BUFSIZE);
	ASSERT(len > 0 && len < BUFSIZE);
	info->burst_buf_used += len;
	return res;
}

void
flush_burst_send_buf(int s)
{
	int res;
	struct fd_info *info;

	info = find_fd_info(s);
	if (!info || info->sock_type != SOCK_BURST)
		return;
	
	log(TRACE, "Flushing burst buffer: %d", info->burst_buf_used);

	if (info->burst_buf_used <= 0)
		return;

	res = full_send(s, info->burst_send_buf, info->burst_buf_used);
	if (res < 0)
		ASSERT(0);

	info->burst_buf_used = 0;
}


int
msg_send(int s, struct generic_msg *m)
{
	int res;
	struct fd_info *info;
	info = find_fd_info(s);

	if (!info || info->sock_type == SOCK_TCP) {
		res = tcp_msg_send(s, m);
	} else {
		res = burst_msg_send(s, m);
	}
	return res;
}

int
general_signal_send(int s, enum general_signal sig)
{
	struct generic_msg m = {};
	
	m.head.type = GENERAL_SIGNAL_MSG;
	m.body.general_signal.signal = sig;

	return msg_send(s, &m);
}


#define HANDSHAKE_MAGIC 0xf3ab41a9



struct handshake {
	unsigned magic;
	unsigned version;
};

int
msg_handshake(int s, unsigned max_ver, unsigned *prot_ver,
    enum handshake_policy policy)
{
	/*
	 * prot_ver set to SHAKE_FAIL on socket error
	 * prot_ver set to SHAKE_MAGIC_REJ for HANDSHAKE_MAGIC mismatch
	 * prot_ver set to SHAKE_LOCAL_REJ if local version > remote version
	 * prot_ver set to SHAKE_PEER_REJ if local verion < remote version
	 * prot_ver set to max_ver on success
	 */
	struct handshake our, their;
	struct timeval to;
	struct fd_set fds;
	int i;

	unsigned null_prot_ver;

	if (policy == POL_NO_SHAKE)
		return 0;

	if (prot_ver == NULL)
		prot_ver = &null_prot_ver;
	*prot_ver = SHAKE_FAIL;

	/* Start by sending our handshake info. */
	our.magic = htonl(HANDSHAKE_MAGIC);
	our.version = htonl(max_ver);
	if (send(s, &our, sizeof our, 0) != sizeof our)
		return -1;

	/* Wait for handshake from other side. */
	FD_ZERO(&fds);
	FD_SET(s, &fds);
	to.tv_sec = 30;
	to.tv_usec = 0;

	/* Reject if not recv'd. */
	if (select(s + 1, &fds, 0, 0, &to) != 1)    
		return -1;

	/* Reject if bad size.*/
	if (recv(s, &their, sizeof their, 0) != sizeof their)
		return -1;

	/* Reject if bad magic. */
	if (ntohl(their.magic) != HANDSHAKE_MAGIC) {
		*prot_ver = SHAKE_MAGIC_REJ;
		return -1;
	}

	/* Reject if exact match required but not met. */
	if (policy == POL_MATCH_OR_FAIL && our.version != their.version) {
		*prot_ver = (ntohl(our.version) > ntohl(their.version) ?
		    SHAKE_LOCAL_REJ : SHAKE_PEER_REJ);
		return -1;
	}
	if (our.version == their.version) {          
		*prot_ver = ntohl(our.version);
		return 0;
	}

	/* If our version is higher than the peer's, and the handshake policy
	 * allows fallback, and we support the peer version, then fallback. If
	 * the policy doesn't allow fallback, or we can't support the peer's
	 * lower version, then fail. If the peer's version is higher, let it
	 * decide what to do. */
	if (ntohl(our.version) > ntohl(their.version)) {
		if (policy == POL_ALLOW_FALLBACK && 
		    ntohl(their.version) >= MIN_MSG_VERSION) {
			*prot_ver = ntohl(their.version);
			i = 0;
		} else if (policy == POL_DENY_FALLBACK &&
		    ntohl(their.version) >= get_deny_fallback_min_ver()) {
			*prot_ver = ntohl(their.version);
			i = 0;
		} else {
			log(ERROR, "Local cluster refused connection due to "
			    "SyncIQ version incompatibility: "
			    "local=%x, remote=%x",
			    ntohl(our.version), ntohl(their.version));
			*prot_ver = SHAKE_LOCAL_REJ;
			i = -1;
		}

		if (send(s, &i, sizeof i, 0) != sizeof i || i) {
			*prot_ver = SHAKE_FAIL;
			return -1;
		}
	} else {
		to.tv_sec = 5;
		to.tv_usec = 0;

		/* Reject if not recv'd. */
		if (select(s + 1, &fds, 0, 0, &to) != 1) {
			*prot_ver = SHAKE_FAIL;
			return -1;
		}

		/* Reject if bad size.*/
		if (recv(s, &i, sizeof i, 0) != sizeof i) {
			*prot_ver = SHAKE_FAIL;
			return -1;
		}

		/*
		 * We should have received yes/no answer about whether the other
		 * side can emulate our version.  If the answer is yes, set
		 * prot_ver.
		 */
		if (i == 0)
			*prot_ver = ntohl(our.version);
		else {
			log(ERROR, "Remote cluster refused connection due to "
			    "SyncIQ version incompatibility: "
			    "local=%x, remote=%x",
			    ntohl(our.version), ntohl(their.version));
			*prot_ver = SHAKE_PEER_REJ;
		}
	}
	return i ? -1 : 0;
}


char *
msg_type_str(int msg_type)
{
	int i, ript_msg_type;
	static char buf[30];

	if (!msgs_inited) {
		init_msgs();
		msgs_inited = true;
	}

	if (msg_type >= 0 && msg_type < MAX_MSG) {
		i = msgs_map[msg_type];
		if (i >= 0)
			return msgs[i].name;
	} else if (is_ript_msg_type(msg_type) &&
	    ((ript_msg_type = unbuild_ript_msg_type(msg_type)) >= 0 &&
	    (ript_msg_type < RM_MAX_MSG || rm_in_whitelist(ript_msg_type)))) {
		i = ript_msgs_map[ript_msg_type];
		if (i >= 0)
			return ript_msgs[i].name;
	}

	snprintf(buf, sizeof(buf), "Unknown msg type %d", msg_type);
	return buf;
}

void
ript_msg_field_str(char *buf, size_t n, enum ript_message_fields field)
{
	int i;
	char *name = NULL;

	if (field < RMF_MAX && ript_msg_fields[field].field == field) {
		name = ript_msg_fields[field].name;
	} else if (rmf_in_whitelist(field)) {
		for (i = ARRAY_SIZE(ript_msg_fields) - 1; i >= 0; i--) {
			if (field == ript_msg_fields[i].field) {
				name = ript_msg_fields[i].name;
				break;
			}
		}
	}

	snprintf(buf, n, "%s (%d)", name ? name : "Unknown msg field", field);
}

void
set_msg_dump(bool setting)
{
	do_msg_dump = setting;
}

/****   CHECKSUM   ******/
void checksumInitReal(checksum_context* ctx)
{
	*((u_int32_t*)ctx->result) = crc32(0, Z_NULL, 0);
}

void checksumUpdateReal(checksum_context* ctx, const char* buf, unsigned len)
{
	*((u_int32_t*)ctx->result) = crc32(*((u_int32_t*)ctx->result),
		(unsigned char *)buf, len);
}

/* Debugging output */

static void
print_fd_info(FILE *output)
{
	int i;
	
	fprintf(output, "ipc_fd: %d \n", ipc_fd);
	fprintf(output, "kqueue_fd: %d \n", kqueue_fd);
	fprintf(output, "flexnet_conf_fd: %d \n", flexnet_conf_fd);
	fprintf(output, "fd_count: %d\n", fd_count);
	fprintf(output, "fds:\n");
	fprintf(output, "  ---\n");
	for (i = 0; i < fd_count; i++) {
		fprintf(output, "  name: %s \n", fds[i].fd_name);
		fprintf(output, "  fd: %d \n", fds[i].fd);
		fprintf(output, "  bufsize: %d \n", fds[i].bufsize);
		fprintf(output, "  parsed: %d \n", fds[i].parsed);
		fprintf(output, "  inbuf: %d \n", fds[i].inbuf);
		fprintf(output, "  lastaction: %.24s (%lld) \n", 
		    ctime(&fds[i].lastaction), fds[i].lastaction);
		fprintf(output, "  ---\n");
	}
}

static void
print_msg_log(FILE *output, struct msglog *mlog)
{
	int i;
	char *name;
	struct msglog *current = NULL;
	
	for (i = 0; i < MSG_LOG_SIZE; i++) {
		current = &mlog[i];
		name = msg_type_str(current->type);
		fprintf(output, "%ld: Type: %d (%s) Length: %d\n",
		    current->cnt, current->type, name, current->len);
	}
}

void
print_net_state(FILE *output)
{
	fprintf(output, "\n****Network Library State****\n");
	fprintf(output, "Total Messages: %ld\n", totmsgcnt);
	fprintf(output, "Total Messages Sent: %ld\n", sendmsgcnt);
	fprintf(output, "Total Messages Received: %ld\n", recvmsgcnt);
	fprintf(output, "\n****FD State****\n");
	print_fd_info(output);
	fprintf(output, "\n****Send Message Log****\n");
	print_msg_log(output, sendmsglog);
	fprintf(output, "\n****Receive Message Log****\n");
	print_msg_log(output, recvmsglog);
	fprintf(output, "****End Network Library State****\n");
}

