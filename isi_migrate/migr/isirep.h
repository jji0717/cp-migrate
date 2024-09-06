#ifndef _MIGR_ISIREP_H
#define _MIGR_ISIREP_H

#include <sys/isi_acl.h>
#include <sys/time.h>

#include <dirent.h>
#include <stdarg.h>
#include <zlib.h>

#include <ifs/ifs_syscalls.h>
#include <isi_util/syscalls.h>
#include <ifs/ifs_procoptions.h>

#include <err.h>
#include "adt.h"

#include <isi_celog/isi_celog.h>
#include <isi_util/enc.h>
#include <isi_acl/sd_acl.h>

#include "isi_migrate/sched/siq_jobctl.h"
#include "isi_migrate/parser/siq_pparser.h"
#include "isi_migrate/config/siq_util.h"
#include "isi_migrate/sched/siq_log.h"
#include "isi_migrate/siq_error.h"
#include "isi_burst_sdk/include/burst.h"

#include <isi_licensing/licensing.h>

/* 
 * TSM 1.0 ships in triple license mode as described below.
 * License check shall be applied to user policies only.
 * 
 * TSM 1.0 will finally ship in dual licensing mode, but since 
 * a request for triple mode is anticipated, DIQ_LIC is retained.
 */
#define ILM(m) (isi_licensing_module_status(ISI_LICENSING_##m) == ISI_LICENSING_LICENSED)
#define SIQ_LICOK ( ILM(SYNCIQ_v_2_0) )
#define MIQ_LICOK SIQ_LICOK

#define SIQ_LIC_OK(act) ( SIQ_LICOK )

#define SIQ_IFS_DIR "/ifs/.ifsvar/modules/tsm/"
#define SIQ_IFS_CONFIG_DIR SIQ_IFS_DIR "config/"
#define SIQ_CONFLICT_LOG_DIR SIQ_IFS_DIR "conflict_logs"
#define SIQ_CONFLICT_LOG "compliance_conflicts.log"
#define SIQ_SOURCE_RECORDS_DIR SIQ_IFS_CONFIG_DIR "source_records"
#define SIQ_SOURCE_RECORDS_LOCK_DIR SIQ_IFS_CONFIG_DIR "source_record_locks"
#define SIQ_IFS_IDMAP_DIR "/ifs/.ifsvar/modules/tsm/samba-bak"
#define SIQ_WORKER_POOL_DIR "/ifs/.ifsvar/modules/tsm/config/worker_pool"
#define SIQ_WORKER_POOL_TOTAL SIQ_WORKER_POOL_DIR "/total_rationed_workers"
#define SIQ_SCHED_EVENT_FILE SIQ_IFS_CONFIG_DIR "sched_event"
#define SIQ_COMP_V1_V2_PATCH_FILE "/ifs/.ifsvar/modules/tsm/config/comp_v1_v2_patch"
#define SIQ_COMP_LATE_STAGE_UNLINK_PATCH_FILE SIQ_IFS_CONFIG_DIR "comp_late_stage_unlink_patch"
#define SIQ_WORK_LOCK_DIR SIQ_IFS_DIR "work_locks"
#define SIQ_TARGET_WORK_LOCK_DIR SIQ_IFS_DIR "target_work_locks"
#define SIQ_TARGET_WORK_LOCKING_PATCH_FILE SIQ_IFS_CONFIG_DIR "target_work_locking_patch"
#define SIQ_FP_DIR SIQ_IFS_DIR "failpoint_files"
#define SIQ_SNAPSHOT_LOCK_DIR SIQ_IFS_CONFIG_DIR "snapshot_locks"

/* Define new failpoint files here */
#define CREATE_LIN_STALL_FP "create_lin_stall"

/* With x of type (struct in_addr_t *) */
#define ip2text(x)  inet_ntoa(*(struct in_addr *)x)

/* Pre-M4 */
#undef GLOBAL_CONFIG_EMULATE
#undef CONFIG_EMULATE

/* Special value set to socket in case job is local operation */
#define SOCK_LOCAL_OPERATION -2

/* dirent lookup options */
#define FASTPATH_ONLY 1

/* file_type descriptions */
#define FT_REG "regular"
#define FT_SYM "symlink"
#define FT_CHAR "char device"
#define FT_BLOCK "block device"
#define FT_SOCK "socket"
#define FT_DIR "directory"
#define FT_FIFO "fifo"
#define FT_UNKNOWN "unknown"

/* tmp working directory used to store lins during stf sync */
#define TMP_WORKING_DIR ".tmp-working-dir"

#define FOFB_SNAP_DEL "DELETE_FOFB_SNAPSHOT"

/* Component identities for logging purposes */
#define SIQ_SCHEDULER	"scheduler"
#define SIQ_COORDINATOR	"coordinator"
#define SIQ_TMONITOR	"tmonitor"
#define SIQ_PWORKER	"pworker"
#define SIQ_SWORKER	"sworker"
#define SIQ_PHASHER	"phasher"
#define SIQ_SHASHER	"shasher"
#define SIQ_BANDWIDTH	"bandwidth"

#define MAX_FIELD_NAME_LEN 64

struct stat;
struct sockaddr_in;
struct timeval;
struct generic_msg;
struct inx_addr;
struct siq_ript_msg;

extern struct inx_addr g_src_addr;
extern struct inx_addr g_dst_addr;

extern struct timeval now;

#define USECS_PER_SEC 1000000

static __inline int64_t
tv_usec_diff(struct timeval *t_new, struct timeval *t_old)
{
	/* Must use int64_t to avoid possible overflow */
	return ((int64_t)t_new->tv_sec - t_old->tv_sec) * USECS_PER_SEC +
	    t_new->tv_usec - t_old->tv_usec;
}

static __inline char *
ends_with(char *name, const char *has) {
    int name_len = strlen(name);
    int has_len = strlen(has);

    if (has_len > name_len)
	    return NULL;

    if (!strncasecmp(name + name_len - has_len, has, has_len))
	    return name + name_len - has_len;
    return NULL;
} 

/*************	SIQ OP TRACKING	**************/
enum siq_ops {
	SIQ_OP_FILE_DATA,
	SIQ_OP_FILE_ACKS,
	SIQ_OP_DIR_DEL,
	SIQ_OP_DIR_FINISH,
	SIQ_OP_RPC_SEND,
	SIQ_OP_TOTAL_OPS
};

struct siq_track_op {
	/* flag used for code correctness */
	bool started;
	/* Total count of operation */
	uint64_t total;
	/* Total time spent on operation in micro sec */
	uint64_t time_total;
	struct timeval time_delta;
};

void siq_track_op_beg(int op);
void siq_track_op_end(int op);
void siq_track_op_end_nocount(int op);
void siq_track_op_bump_count(int op);
void log_siq_ops(void);

/*********   END OF SIQ OP TRACKING    ***********/

enum socket_type {
	SOCK_TCP,
	SOCK_BURST
};

enum handshake_policy {
	POL_NO_SHAKE,
	POL_ALLOW_FALLBACK,
	POL_DENY_FALLBACK,
	POL_MATCH_OR_FAIL
};

enum origin {PWORKER,
	     SWORKER,
	     PWORKER_AND_SWORKER};

enum hash_error {HASH_ERR_FILE_SYSTEM,
		 HASH_ERR_MISSING_DIR,
		 HASH_ERR_UNEXPECTED_MSG};

enum hash_status {HASH_PIECE,
		  HASH_EOF,
		  HASH_SYNC,
		  HASH_NO_SYNC};

enum hash_type {HASH_NONE,
		HASH_MD4,
		HASH_MD5,
		HASH_TYPE_MAX};

enum sync_state {UNDECIDED,
		 NEEDS_SYNC,
		 NO_SYNC};

enum hash_stop {HASH_STOP_ALL,
		HASH_STOP_FILE,
		HASH_STOP_FROM};
		
enum file_type {SIQ_FT_REG,
		SIQ_FT_SYM,
		SIQ_FT_CHAR,
		SIQ_FT_BLOCK,
		SIQ_FT_SOCK,
		SIQ_FT_DIR,
		SIQ_FT_FIFO,
		SIQ_FT_UNKNOWN};

enum read_msg_resp {READ_MSG_RESP_OK,
		    READ_MSG_RESP_ERROR,
		    READ_MSG_RESP_EOF,
		    READ_MSG_RESP_TIMEOUT};

enum user_attr_namespace {NS_NONE,
			  NS_USER,
			  NS_SYSTEM};

enum worker_type {SIQ_WT_UNKNOWN,
		  SIQ_WT_PWORKER,
		  SIQ_WT_SWORKER,
		  SIQ_WT_P_HASHER,
		  SIQ_WT_S_HASHER};

enum general_signal {SIQ_GENSIG_EXIT,
		     SIQ_GENSIG_BUSY_EXIT,
		     SIQ_COMP_V1_V2_PATCH,
		     SIQ_GENSIG_INVALID};

/* See migr/nodes.c to see how options are used. */
enum target_init_option {
	OPTION_INVALID = -1,
	OPTION_INIT,
	OPTION_IPS_ONLY,
	OPTION_RESTART,
};

/* Number of bytes to read from a file at time for creating a hash */
static const int OPTIMAL_FILE_READ_SIZE = 4096 * 1024;

struct filelist {
	char *list;
	int used;
	int size;
	int elems;
	unsigned range_begin; 		/* inclusive */
	unsigned range_end;		/* inclusive */
};

/********
 * LIST *    
 ********/
/*
 * reset list and add 1-st element - dir name (for deleted-files list)
 * dir should be in UTF-8, list size shoul be enough
 */
extern void
init_list_deleted(struct filelist *list, char* dir);

extern void
init_list_deleted_dst(struct filelist *list, char* dir, char* clu_name);

/*
 * reset list but keep the initial directory header
 */
extern void
reset_list_head(struct filelist *list);

extern void
dump_listmsg_to_file(int fd,  char* data, unsigned size);

/*
 * mode constants
 */
/* Name of . directory isn't part of list */
/* list p->s for sync operation */
#define LIST_TO_SYNC 1
/* Name of . directory is the first part of list*/
/* list p->c for log deleted files */
#define LIST_TO_LOG  2
/* list sworker->p->c file for dump deleted files on destination */
#define LIST_TO_SLOG 3

/*
 * reset list for future use
 */
void
listreset(struct filelist *list);

/*
 * add name of dirent to list, if list are full, function sends it.
 * dir is mandatory for LIST_TO_LOG
 */
void
listadd(struct filelist *list, const char *name, enc_t enc,
    u_int64_t cookie, int mode, int fd, char* dir);

/*
 * send whatever is left in the list to "fd" with mode "mode"
 */
void
listflush(struct filelist *list, int mode, int fd);

/* 
 * only use this for testing. it changes the max allowed size for a list
 * message
 */
void
listoverridesize(int size);

/* shall never exceed message buffer size */
#define MAXLISTSIZE 0x40000

enum file_data_msg_type {
	MSG_DTYPE_SPARSE,
	MSG_DTYPE_DATA,
	MSG_DTYPE_UNCHANGED
};

// Riptide message format.

#define RIPT_MSG_MAGIC 0xB9

enum siq_msg_field_type {
	SIQFT_INT8 = 0,
	SIQFT_UINT8,
	SIQFT_INT16,
	SIQFT_UINT16,
	SIQFT_INT32,
	SIQFT_UINT32,
	SIQFT_INT64,
	SIQFT_UINT64,
	SIQFT_FLOAT64,
	SIQFT_STRING,
	SIQFT_BYTESTREAM,
	SIQFT_MAX = 11
};

struct siq_ript_msg_field_hash_entry {
	uint16_t fieldid;
	struct isi_hash vhash; // Hashes version to field entry.
	struct isi_hash_entry hentry;
};

struct siq_ript_msg_field {
	enum siq_msg_field_type type;
	uint8_t ver;
	union {
		int8_t		i8val;
		uint8_t		u8val;
		int16_t		i16val;
		uint16_t	u16val;
		int32_t		i32val;
		uint32_t	u32val;
		int64_t		i64val;
		uint64_t	u64val;
		double		f64val;
		char *		strval;
		struct {
			uint32_t	len;
			uint8_t		*data;
		} bytestreamval;
	} value;
	struct isi_hash_entry hentry;
};

#define DECL_RIPT_MSG_ACCESSOR(TYPE) \
void ript_msg_get_field_##TYPE(struct siq_ript_msg *msg, uint16_t fieldid, \
    uint8_t ver, TYPE##_t *val, struct isi_error **error_out);

DECL_RIPT_MSG_ACCESSOR(int8);
DECL_RIPT_MSG_ACCESSOR(uint8);
DECL_RIPT_MSG_ACCESSOR(int16);
DECL_RIPT_MSG_ACCESSOR(uint16);
DECL_RIPT_MSG_ACCESSOR(int32);
DECL_RIPT_MSG_ACCESSOR(uint32);
DECL_RIPT_MSG_ACCESSOR(int64);
DECL_RIPT_MSG_ACCESSOR(uint64);

void ript_msg_get_field_float64(struct siq_ript_msg *msg, uint16_t fieldid,
    uint8_t ver, double *val, struct isi_error **error_out);
void ript_msg_get_field_str(struct siq_ript_msg *msg, uint16_t fieldid,
    uint8_t ver, char **val, struct isi_error **error_out);
void ript_msg_get_field_bytestream(struct siq_ript_msg *msg, uint16_t fieldid,
    uint8_t ver, uint8_t **val, uint32_t *len, struct isi_error **error_out);

/* These macros are not to be used with bytestreams
 * Note: ERR is a "struct isi_error **error" */
#define RIPT_MSG_GET_FIELD(TYPE, MSG, FIELDID, VER, VAL, ERR, LABEL)	\
ript_msg_get_field_##TYPE(MSG, FIELDID, VER, VAL, ERR);			\
if (*ERR != NULL)							\
	goto LABEL;							\

#define RIPT_MSG_GET_FIELD_ENOENT(TYPE, MSG, FIELDID, VER, VAL, ERR, LABEL)\
ript_msg_get_field_##TYPE(MSG, FIELDID, VER, VAL, ERR);			\
if (*ERR != NULL) {							\
	if (!isi_system_error_is_a(*ERR, ENOENT))			\
		goto LABEL;						\
	isi_error_free(*ERR);						\
	*ERR = NULL;							\
}									\


#define DECL_RIPT_MSG_MUTATOR(TYPE) \
    int ript_msg_set_field_##TYPE(struct siq_ript_msg *msg, uint16_t fieldid,\
	uint8_t ver, TYPE##_t val);

DECL_RIPT_MSG_MUTATOR(int8);
DECL_RIPT_MSG_MUTATOR(uint8);
DECL_RIPT_MSG_MUTATOR(int16);
DECL_RIPT_MSG_MUTATOR(uint16);
DECL_RIPT_MSG_MUTATOR(int32);
DECL_RIPT_MSG_MUTATOR(uint32);
DECL_RIPT_MSG_MUTATOR(int64);
DECL_RIPT_MSG_MUTATOR(uint64);

int ript_msg_set_field_float64(struct siq_ript_msg *msg, uint16_t fieldid,
    uint8_t ver, double val);
int ript_msg_set_field_str(struct siq_ript_msg *msg, uint16_t fieldid,
    uint8_t ver, char *val);
int ript_msg_set_field_bytestream(struct siq_ript_msg *msg, uint16_t fieldid,
    uint8_t ver, uint8_t *val, uint32_t len);

void ript_msg_init(struct siq_ript_msg *msg);
int pack_ript_msg(struct siq_ript_msg *msg, void *buf_);
int _unpack_ript_msg_field(struct siq_ript_msg *msg, unsigned char *buf_,
    int len);
int unpack_ript_msg(struct siq_ript_msg *msg_, void *buf_, int len);
void _free_ript_msg(struct siq_ript_msg *msg);

bool is_ript_msg_type(uint32_t type);
int ript_msg_out_size(struct siq_ript_msg *msg);
void free_generic_msg(struct generic_msg *m);

#define GEN_MSG_INIT_CLEAN(__ptr) __ptr __cleanup(free_generic_msg) = {};

/* net.c */
int connect_to(char hostname[], unsigned short port, enum handshake_policy pol,
    unsigned max_ver, unsigned *prot_ver, const struct inx_addr *forced_addr,
    bool bw_track, struct isi_error **error_out);
int listen_on(unsigned short port, bool block);
int lock_file(char *path, int lock_type);
int read_total_workers(int *total_workers);
int write_lnn_file(char *path, int max_workers);
int read_lnn_file(char *path, int *to_return);
int write_total_workers(int total_workers);
int fork_accept_from(unsigned short port, unsigned *prot_ver, int debug,
    enum handshake_policy policy, int max_num_children, int lnn);
int full_send(int fd, char *buf, int len);
void sigchld(int);
int get_child_count(void);
enum socket_type sock_type_from_fd_info(int fd);
burst_fd_t burst_fd_from_fd_info(int fd);
void flush_burst_send_buf(int s);
uint32_t ript_build_msg_type(uint32_t type);

int get_bandwidth(void);
void set_bandwidth(int);

int ipc_connect(int *);
int ipc_readgroup(int);
int group_has_quorum(void);

void add_recv_netstat(int fd, uint64_t got);
void set_tracked_fd(int fd);
uint64_t migr_get_recv_netstat(void);
uint64_t migr_get_send_netstat(void);

/* msg.c */
#define TIMER_SIGNAL (void *)signal /* Sentinel value. */

enum ript_message_fields;

typedef int (*migr_callback_t)(struct generic_msg *, void *);
typedef int (*migr_timeout_t)(void *);

void migr_register_callback(int, unsigned, migr_callback_t);
migr_callback_t migr_get_registered_callback(int, unsigned);
void migr_unregister_all_callbacks(int fd);
int migr_register_group_callback(migr_timeout_t, void *);
void migr_register_flexnet_callback(migr_timeout_t, void *);
void *migr_register_timeout(struct timeval *, migr_timeout_t, void *);
void migr_cancel_timeout(void *);
void migr_cancel_all_timeouts(void);
int migr_add_fd(int fd, void *ctx, char *name);
int migr_add_listen_fd(int fd, void *ctx, char *name);
int migr_set_fd(int fd, void *ctx);
int migr_rm_fd(int fd);
void migr_rm_and_close_all_fds(void);
void migr_fd_set_burst_sock(int fd, burst_fd_t burst_fd, int buf_size);
void migr_fd_set_burst_recv_buf(int fd, int buf_size);

void migr_force_process_stop(void);
int migr_process(void);
void msgs_sanity(void);

enum read_msg_resp
read_msg_from_fd(int fd, struct generic_msg *msg, bool wait_for_it,
    bool registered_fd, char **buf);

unsigned char *get_data_offset(int fd, int *buflen);
int generic_msg_unpack(struct generic_msg *, void *, int, int *);
int generic_msg_pack(struct generic_msg *, void *, int);
int msg_send(int sock, struct generic_msg *m);
int general_signal_send(int sock, enum general_signal wsig);
void override_deny_fallback_min_ver(unsigned ver);
void override_connect_timeout(int timeout);
int msg_handshake(int sock, unsigned max_ver, unsigned *prot_ver,
    enum handshake_policy policy);
char *msg_type_str(int msg_type);
void ript_msg_field_str(char *buf, size_t n, enum ript_message_fields field);
void set_msg_dump(bool setting);

int delete_acl_fd(int fd);
void clear_ads_entries(int fd, struct stat *st, struct isi_error **error_out);

bool ip_lists_match(int, char **, int, char **);
void find_domain_by_lin(ifs_lin_t lin, enum domain_type type, bool ready_only,
    ifs_domainid_t *dom_id, uint32_t *dom_gen, struct isi_error **error_out);
void find_domain(const char *root, enum domain_type type, bool ready_only, 
    ifs_domainid_t *dom_id, uint32_t *dom_gen, struct isi_error **error_out);
void get_failover_snap_name(char *pol_name, char *ssname);
void get_changelist_snap_name(char *pol_name, char *ssname);
struct isi_str *get_unique_snap_name(char *base_name,
    struct isi_error **error_out);
bool get_worm_state(ifs_lin_t, ifs_snapid_t, struct worm_state *,
    bool *, struct isi_error **);
bool
is_worm_committed(ifs_lin_t lin, ifs_snapid_t snapid,
    struct isi_error **error_out);
bool
is_conflict_res_needed(ifs_lin_t lin, ifs_snapid_t snap1, ifs_snapid_t snap2,
    struct isi_error **error_out);
bool
comp_skip_ads(ifs_lin_t lin, ifs_snapid_t snap1, ifs_snapid_t snap2,
    struct isi_error **error_out);
bool is_or_has_compliance_dir(const char *path, struct isi_error **error_out);
uint32_t build_ript_msg_type(uint32_t type);
uint32_t unbuild_ript_msg_type(uint32_t type);
void create_fp_file(char *fp_file, struct isi_error **error_out);
void wait_for_deleted_fp_file(char *fp_file);

/*size of checksum in bytes. (checksum in the begining of the data)*/
#define CHECKSUM_SIZE 4

/* This must be less than or equal to MAX_MSG_FDM_DATA */
#define IDEAL_READ_SIZE (1024 * 2048)
/*Start trying with 4 Mb buff size*/
#define SOCK_BUFF_SIZE (4*1024*1024)
/*Minimum buff size we are working with is 128 Kb*/
#define MIN_SOCK_BUFF_SIZE (16*1024)

typedef struct _checksum_context {
	char result[CHECKSUM_SIZE];
} checksum_context;

void checksumInitReal(checksum_context * ctx);
/*size of buf <= msg size -> len could be unsigned to avoid overflow*/
void checksumUpdateReal(checksum_context* ctx, const char* buf, unsigned len);

static __inline void
checksumInit(checksum_context * ctx, bool no_int)
{
	if(!no_int)
		checksumInitReal(ctx);
}
/*size of buf <= msg size -> len could be unsigned to avoid overflow*/
static __inline void
checksumUpdate(checksum_context* ctx, const char* buf, unsigned len,
    bool no_int)
{
	if(!no_int)
		checksumUpdateReal(ctx, buf, len);
}
/*
 * These are the bounds of the protocol version that we are prepared
 * to handle.
 */
#define MIN_MSG_VERSION		0x00010006
#define MSG_VERSION_DAISY	0x00010006
#define MSG_VERSION_JAMAICAN	0x00010007
#define MSG_VERSION_SBONNET	0x00010008
#define MSG_VERSION_HABANERO	0x00010009
#define MSG_VERSION_CHOPU	0x0001000a
#define MSG_VERSION_CHOPUVILLE  0x0001000b
#define MSG_VERSION_RIPTIDE	0x0001000c
#define MSG_VERSION_HALFPIPE	0x0001000d

#define MSG_VERSION		MSG_VERSION_HALFPIPE

/* Errors that can be returned from msg_handshake via the prot_ver argument */
#define SHAKE_FAIL	0x10000000
#define SHAKE_LOCAL_REJ	0x10000001
#define SHAKE_PEER_REJ	0x10000002
#define SHAKE_MAGIC_REJ	0x10000003

/* nodes.c */

struct nodex {
	int devid;
	int lnn;
	struct inx_addr addr;
};

/* same info as nodex, just packed for the wire, and less the devid field */
struct lnn_ipx_pair {
	uint32_t lnn;
	struct inx_addr addr;
} __packed;

/* struct to contain both identifiers of a node */
struct node_ids {
	int devid;
	int lnn;
};

/*
 * CHOPU (6.5) and lesser versions did not have field called zid
 * in inx addr struct. This breaks our sync jobs (between different versions)
 * where we send raw bits of inx addr struct list.
 * The fix is create inx addr structure without the zid field and use
 * is to parse messages from 6.5 cluster. We should be careful
 * about future version (post Mavericks) since there is hard dependency
 * of inx addr structure format which requires we either keep conversion
 * routines or do not change the structure at all.
 */
struct inx_addr_v_3 {
	sa_family_t inx_family; /* AF_INET or AF_INET6 only! */
	union {
		uint8_t inx_blob[sizeof(struct in6_addr)]; /* byte blob */
		struct in_addr inx_v4;
		struct in6_addr inx_v6;
	} uinx_addr;
	uint8_t inx_nm;
};

struct lnn_ipx_pair_v_3 {
	uint32_t lnn;
	struct inx_addr_v_3 addr;
} __packed;

void
construct_inx_addr_list(struct nodex *nodes, int node_count, unsigned curr_ver,
    char **pairs_out, unsigned *len_out);
void
copy_or_upgrade_inx_addr_list(char *pairs_in, unsigned ipslen,
    unsigned curr_ver, struct lnn_ipx_pair **pairs_out, int *count_out);

/* legacy_send_lnn_ip_pairs sends a array of packed struct node_lnn_ip_pairs
 * (limited to IPv4 addrs) */
#pragma pack(1)
struct node_lnn_ip_pairs {
	u_int32_t lnn;
	u_int32_t ip;
};
#pragma pack()

void get_up_devids(struct int_set *devids);
void get_up_node_ids(struct ptr_vec *nodes_out, struct isi_error **error_out);
void get_not_dead_storage_nodes(struct int_set *set);
int get_local_lnn(struct isi_error **error_out);

const struct inx_addr *get_local_node_pool_addr(const char *subnet_pool,
    struct isi_error **error_out);

char **allocate_ip_list(int n);
void free_ip_list(char **);

int
get_local_cluster_info(
	struct nodex **nodes_out,
	char ***ip_list,
	int **lnn_list,
	size_t *lnn_list_len,
	struct inx_addr *target_addr,
	char *restrict_by_name,
	bool use_internal,
	bool v4_only,
	char **msg_out,
	struct isi_error **error_out);

bool max_down_nodes(int max_down);
int lowest_node(void);
int get_num_nodes(void);
int get_num_up_nodes(void);

void get_ip_address(char **ip, int devid, bool get_internal,
    struct isi_error **error_out);

/* utils.c */
struct path {
	char *path;
	char *utf8path;
	int pathlen;
	int utf8pathlen;
	int used;
	int utf8used;
	struct enc_vec	evec;
};

int get_concurrent_snapshot_lock(int max_concurrent,
    struct isi_error **error_out);

ifs_snapid_t takesnap(const char *, char *, time_t, char *,
    struct isi_error **);
time_t get_snap_ctime(ifs_snapid_t snapid);
struct path *relpath(const struct path *, const struct path *);
char *inx_addr_to_str(const struct inx_addr *addr);

struct path *path_changebeg(const struct path *all_path,
    const struct path *_old, const struct path *_new);

int fd_rel_to_ifs(int ifs_fd, struct path *path);

void snap_path_to_normal_path(struct path *path);

struct path *path_drop_ifs(const struct path *);

int schdir(struct path *);
int smkchdir(struct path *);

int schdirfd(struct path *, int fd_in, int *fd_out);
int smkchdirfd(struct path *, int fd_in, int *fd_out);
int smkchdirfd_upgrade(struct path *, int fd_in, uint64_t domain_id, 
    int *fd_out);

#define ensurelen(str, need) {					\
	int __need = (need);					\
	if (__need > str##len)					\
		str = realloc(str, str##len = __need + 256);	\
}

void  path_snap_init(struct path *);

/* old path init (without utf8) */
void  _path_init(struct path *, const char *, const struct enc_vec *);
void  path_init(struct path *, const char *, const struct enc_vec *);
void  path_copy(struct path *, const struct path *);
/* old path add (without utf8) */
void  _path_add(struct path *, const char *, int, enc_t);
void  path_add(struct path *, const char *, int, enc_t);
void  path_rem(struct path *, int newlen);
void  path_truncate(struct path *, int len);
/* Pretty print along with enc tags. */
const char *path_fmt(const struct path *); 
/* 
 * Translate names to utf-8,
 * used in updated path functions. 
 */
char *path_utf8(struct path *, int *utf8pathlen, int *utf8used);
void  path_clean(struct path *);

extern int
siq_select(siq_plan_t *plan, char *name, enc_t enc, struct stat *st, time_t t,
    char *utf8path, enum file_type, struct isi_error **error_out);

#define SYNCRUN_FILE_DIR "/ifs/.ifsvar/modules/tsm/running/"
void create_syncrun_file(const char *sync_id, uint64_t run_id,
    struct isi_error **error_out);
void remove_syncrun_file(char *sync_id);

void create_wi_locking_dir(char *pol_id, int mod, struct isi_error **error_out);
void delete_wi_locking_dir(char *pol_id, struct isi_error **error_out);

bool
remove_dirent_entry( int dirfd, char *d_name, unsigned d_enc,
    unsigned d_type, bool skip_cstore, struct isi_error **error_out);

void
recreate_links_from_snap_version(ifs_lin_t source_lin, ifs_snapid_t snapid,
    ifs_lin_t dest_lin, bool is_compliance, struct isi_error **error_out);

int
remove_all_entries_fullscan( uint64_t dlin, bool skip_cstore,
    struct isi_error **error_out);

bool
dirent_lookup( int dirfd, uint64_t resume_cookie, uint64_t direntlin,
    struct dirent *entryp, struct isi_error **error_out);

bool
dirent_lookup_by_lin(ifs_snapid_t snap, uint64_t direntlin, int flag,
    uint64_t *p_linp, struct dirent *entryp, struct isi_error **error_out);

int
remove_all_parent_dirents( int fd, struct stat *st, bool skip_cstore,
     struct isi_error **error_out);

int
try_force_delete_entry(int dirfd, char *d_name, enc_t enc);

enum file_type
stat_mode_to_file_type(mode_t mode);

enum file_type
dirent_type_to_file_type(unsigned char d_type);
int
dtype_to_ifstype(unsigned char d_type);

bool
supported_special_file(struct stat *st);

unsigned char
file_type_to_dirent_type(enum file_type);

const char *
file_type_to_name(enum file_type);

void
dump_exitcode_ring(int error);

void
init_node_mask(char *mask, char initial, int size);

void
merge_and_flip_node_mask(char *tgt, char *src, size_t tgt_size);

/* hasher.c */
int fork_hasher(enum origin origin, void *ctx, int snap_fd,
    enum hash_type hash_type, uint64_t min_hash_piece_len, int pieces_per_file,
    int log_level, uint16_t cpu_port);
char *hash_status_str(enum hash_status status);
char *hash_error_str(enum hash_error err);
char *origin_str(int origin);
char *sync_state_str(enum sync_state state);
char *hash_stop_str(enum hash_stop stop);
void
check_connection_with_peer(int fd, bool force, unsigned noop_bytes,
    struct isi_error **error_out);

void
check_in_snap(ifs_lin_t lin, ifs_snapid_t snap, bool *found, bool missing_ok,
    struct isi_error **error_out);
bool get_dir_parent(ifs_lin_t lin, ifs_snapid_t snap, ifs_lin_t *lin_out,
    bool missing_ok, struct isi_error **error_out);
void
set_filter_path_error_logging(bool filter);
void
dir_get_dirent_path(ifs_lin_t root_lin, ifs_lin_t lin, ifs_snapid_t snap,
    ifs_lin_t parent_lin, char **path_dirents_out, int *path_len_out,
    struct isi_error **error_out);
void
dir_get_utf8_str_path(char *prefix, ifs_lin_t root_lin, ifs_lin_t lin,
    ifs_snapid_t snap, ifs_lin_t parent_lin, char **utf8_str_out,
    struct isi_error **error_out);
void
dir_get_utf8_str_path_2(char *prefix, ifs_lin_t root_lin, ifs_lin_t lin,
    ifs_snapid_t snap, ifs_lin_t parent_lin, char **utf8_str_out,
    uint32_t *path_depth, struct isi_error **error_out);
bool
is_compliance_store(int fd, struct isi_error **error_out);
void
log_conflict_path(int dir_fd, char *d_name, unsigned d_enc,
    struct isi_error **error_out);
bool
is_compliance_store_lin(ifs_lin_t lin, ifs_snapid_t snapid,
    struct isi_error **error_out);
/* version.c */

struct version_status {
	uint32_t committed_version;
	bool local_node_upgraded;
};

void
get_version_status(bool use_bw_failpoints, struct version_status *ver_stat);

void
log_version_ufp(uint32_t version, const char *component, const char *context);

void
log_job_version_ufp(struct siq_job_version *job_version, const char *component,
    const char *context);

/*
 * Messaging Protocol. Do not leave gaps between message numbers.
 * Any deprecated message types should be left in the enum and renamed with
 * the "DEPRECATED" prefix.
 */

enum messages {
	REP_MSG = 0,

	/* special */
	NOOP_MSG = 1,
	CONN_MSG = 2,
	DISCON_MSG = 3,
	ERROR_MSG = 4,
	GROUP_MSG = 5,
	OLD_CLUSTER_MSG = 6,
	KILL_MSG = 7,

	/* p <-> s */
	AUTH_MSG = 8,
	DEPRECATED_DIR_MSG = 9,
	DEPRECATED_FILE_MSG = 10,
	DEPRECATED_SYMLINK_MSG = 11,
	OLD_DATA_MSG = 12,
	DONE_MSG = 13,

	/* c <-> p */
	OLD_WORK_INIT_MSG = 14,
	WORK_REQ_MSG = 15,
	DEPRECATED_WORK_RESP_MSG = 16,

	/* bw <-> p */
	DEPRECATED_BAND_INIT_MSG = 17,
	DEPRECATED_RESERVE_MSG = 18,
	DEPRECATED_BANDWIDTH_MSG = 19,

	/* new p <-> s messages for for Poblano: acls, encs, ads */
	DEPRECATED_FILE_MSG2 = 20,
	DEPRECATED_DIR_MSG2 = 21,
	DEPRECATED_SYMLINK_MSG2 = 22,
	ADS_MSG = 23,

	/* new p <-> s messages for Thai: st_flags */
	DEPRECATED_FILE_MSG3 = 24,
	OLD_DIR_MSG3 = 25,
	DEPRECATED_SYMLINK_MSG3 = 26,

	/*new TSM message p<-s for end processing the file*/
	DEPRECATED_ACK_MSG = 27,
	OLD_SWORKER_TW_STAT_MSG = 28,
	/* new TSM job control (c->p) msg */
	JOBCTL_MSG = 29,
	/* list of files, p->s, s->p->c  */
	LIST_MSG = 30,

	/* snapshot replication support */
	SNAP_MSG = 31,
	SNAP_RESP_MSG = 32,

	DEPRECATED_WORK_INIT_MSG2 = 33,
	DEPRECATED_WORK_RESP_MSG2 = 34,
	OLD_FILE_MSG4 = 35,
	OLD_SYMLINK_MSG4 = 36,
	OLD_ACK_MSG2 = 37,
	DEPRECATED_SIQ_STAT_MSG2 = 38,
	/*New Daisy msgs*/
	POSITION_MSG = 39,
	DEPRECATED_WORK_INIT_MSG3 = 40,

	DIR_MSG4 = 41,
	ACK_DIR_MSG = 42,
	SHORT_HASH_RESP_MSG = 43,
	FULL_HASH_DIR_REQ_MSG = 44,
	FULL_HASH_FILE_REQ_MSG = 45,
	FULL_HASH_FILE_RESP_MSG = 46,
	HASH_STOP_MSG = 47,
	HASH_QUIT_MSG = 48,
	HASH_ERROR_MSG = 49,

	/* new v2.5 msgs supporting worker balancing */
	WORK_RESP_MSG3 = 50,
	SPLIT_REQ_MSG = 51,
	SPLIT_RESP_MSG = 52,
	FILE_DATA_MSG = 53,
	OLD_FILE_BEGIN_MSG = 54,
	FILE_DONE_MSG = 55,
	OLD_WORK_INIT_MSG4 = 56,
	ACK_MSG3 = 57,
	SIQ_ERROR_MSG = 58,
	
	DEPRECATED_SIQ_STAT_MSG3 = 59,
	OLD_CLUSTER_MSG2 = 60,

	/* coord -> bwt */
	BANDWIDTH_INIT_MSG = 61,
	THROTTLE_INIT_MSG = 62,
	BANDWIDTH_STAT_MSG = 63,
	THROTTLE_STAT_MSG = 64,

	/* bwt -> coord and coord -> worker */
	BANDWIDTH_MSG = 65,
	THROTTLE_MSG = 66,

	/* v3.0 */
	OLD_TARGET_INIT_MSG = 67,
	TARGET_CANCEL_MSG = 68,
	GENERIC_ACK_MSG = 69,
	CLEANUP_SYNC_RUN_MSG = 70,
	TARGET_MONITOR_SHUTDOWN_MSG = 71,
	JOB_STATUS_MSG = 72,
	OLD_TARGET_RESP_MSG = 73,
	TARGET_POLICY_DELETE_MSG = 74,
	OLD_WORK_INIT_MSG5 = 75,

	/* STF-based messages */
	DELETE_LIN_MSG = 80,
	LINK_MSG = 81,
	UNLINK_MSG = 82,
	OLD_LIN_UPDATE_MSG = 83,
	LIN_COMMIT_MSG = 84,
	CLEANUP_TARGET_TMP_FILES_MSG = 85,

	/* file lists confined to directory hash ranges */
	LIST_MSG2 = 86,

	OLD_PWORKER_TW_STAT_MSG = 87,
	OLD_TARGET_RESP_MSG2 = 88,
	OLD_WORK_INIT_MSG6 = 89,
	LIN_MAP_MSG = 90,
	OLD_FILE_BEGIN_MSG2 = 91,
	DIR_UPGRADE_MSG = 92,
	UPGRADE_COMPLETE_MSG = 93,
	LIN_ACK_MSG = 94,
	TARGET_POL_STF_DOWNGRADE_MSG = 95,

	OLD_PWORKER_STF_STAT_MSG = 96,
	OLD_SWORKER_STF_STAT_MSG = 97,

	USER_ATTR_MSG = 98,

	STALE_DIR_MSG = 99,
	STF_CHKPT_MSG = 100,
	LIN_PREFETCH_MSG = 101,

	EXTRA_FILE_MSG = 102,
	EXTRA_FILE_COMMIT_MSG = 103,

	FILE_BEGIN_MSG = 104,
	LIN_UPDATE_MSG = 105,

	CPU_THROTTLE_INIT_MSG = 106,
	CPU_THROTTLE_STAT_MSG = 107,
	CPU_THROTTLE_MSG = 108,
	LIN_MAP_BLK_MSG = 109,
	DATABASE_UPDT_MSG = 110,
	COMMON_VER_MSG = 111,

	VERIFY_FAILOVER_MSG = 112,
	VERIFY_FAILOVER_RESP_MSG = 113,
	OLD_FAILBACK_PREP_FINALIZE_MSG = 114,
	OLD_FAILBACK_PREP_FINALIZE_RESP_MSG = 115,
	CLEANUP_DATABASE_MIRROR_MSG = 116,
	WORKER_PID_MSG = 117,
	BURST_SOCK_MSG = 118,
	WORM_STATE_MSG = 119,

	GENERAL_SIGNAL_MSG = 120,
	OLD_CLOUDPOOLS_BEGIN_MSG = 121,
	OLD_CLOUDPOOLS_STUB_DATA_MSG = 122,
	OLD_CLOUDPOOLS_DONE_MSG = 123,

	MAX_MSG
};

/* Note: When adding new messages update the corresponding arrays in msg.c
 * and src/sys/sys/isi_stats_siq.h. */
enum ript_messages {
	WORKER_INIT_MSG = 0,
	WORKER_MSG = 1,
	WORKER_STAT_MSG = 2,
	CPU_USAGE_INIT_MSG = 3,
	CPU_NODE_USAGE_MSG = 4,
	TARGET_INIT_MSG = 5,
	TARGET_RESP_MSG = 6,
	WORK_INIT_MSG = 7,
	CLOUDPOOLS_SETUP_MSG = 8,
	CLOUDPOOLS_STUB_DATA_MSG = 9,
	FAILBACK_PREP_FINALIZE_MSG = 10,
	FAILBACK_PREP_FINALIZE_RESP_MSG = 11,
	WORKER_NODE_UPDATE_MSG = 12,
	PWORKER_TW_STAT_MSG = 13,
	SWORKER_TW_STAT_MSG = 14,
	PWORKER_STF_STAT_MSG = 15,
	SWORKER_STF_STAT_MSG = 16,
	COMP_COMMIT_BEGIN_MSG = 17,
	COMP_COMMIT_END_MSG = 18,
	COMP_TARGET_WORK_RANGE_MSG = 19,
	COMP_TARGET_CHECKPOINT_MSG = 20,
	COMP_MAP_BEGIN_MSG = 21,
	COMP_MAP_MSG = 22,
	RM_MAX_MSG,

	/* Messages that are backported from future
	 * releases should be added here. This is
	 * so that the message ID's will be consistent
	 * between all releases. These messages will
	 * also need to be added to the rm_whitelist
	 * array in the isilon/lib/isi_migrate/migr/msg.c
	 * file. */

	/* Pipeline Backported Messages */
	TARGET_WORK_LOCK_MSG = 28,
	COMP_DIR_DELS_BEGIN_MSG = 33,
	WHITELIST_MAX_MSG
};

/* Riptide message fields */
enum ript_message_fields {
	RMF_POLICY_NAME = 0,
	RMF_NODE_MASK = 1,
	RMF_MAX_ALLOWED_WORKERS = 2,
	RMF_ALLOWED_NODES = 3,
	RMF_WORKERS_USED = 4,
	RMF_CPU_INIT_NAME = 5,
	RMF_CPU_NODE_USAGE_PCT = 6,
	RMF_CPU_NODE_USAGE_NAME = 7,

	RMF_TYPE = 8,
	RMF_SIZE = 9,
	RMF_DATA = 10,

	RMF_RESTRICTED_TARGET_NAME = 11,
	RMF_POLICY_ID = 12,
	RMF_RUN_ID = 13,
	RMF_TARGET_DIR = 14,
	RMF_SRC_ROOT_DIR = 15,
	RMF_SRC_CLUSTER_NAME = 16,
	RMF_SRC_CLUSTER_ID = 17,
	RMF_LAST_TARGET_CLUSTER_ID = 18,
	RMF_OPTION = 19,
	RMF_LOGLEVEL = 20,
	RMF_FLAGS = 21,

	RMF_ERROR = 22,
	RMF_TARGET_CLUSTER_NAME = 23,
	RMF_TARGET_CLUSTER_ID = 24,
	RMF_IPS = 25,
	RMF_DOMAIN_ID = 26,
	RMF_DOMAIN_GENERATION = 27,

	RMF_CBM_FILE_VERSIONS = 28,

	RMF_TARGET = 29,
	RMF_PEER = 30,
	RMF_SOURCE_BASE = 31,
	RMF_TARGET_BASE = 32,
	RMF_BWHOST = 33,
	RMF_PREDICATE = 34,
	RMF_SYNC_ID = 35,
	RMF_LASTRUN_TIME = 36,
	RMF_PASS = 37,
	RMF_JOB_NAME = 38,
	RMF_ACTION = 39,
	RMF_OLD_TIME = 40,
	RMF_NEW_TIME = 41,
	RMF_RESTRICT_BY = 42,
	RMF_HASH_TYPE = 43,
	RMF_MIN_HASH_PIECE_LEN = 44,
	RMF_HASH_PIECES_PER_FILE = 45,
	RMF_RECOVERY_DIR = 46,
	RMF_SKIP_BB_HASH = 47,
	RMF_WORKER_ID = 48,
	RMF_PREV_SNAPID = 49,
	RMF_CUR_SNAPID = 50,
	RMF_DEEP_COPY = 51,

	RMF_FBPOL_DST_PATH = 52,

	RMF_FBPOL_ID = 53,
	RMF_FBPOL_NAME = 54,
	RMF_FBPOL_SRC_CLUSTER_NAME = 55,
	RMF_FBPOL_SRC_CLUSTER_ID = 56,
	RMF_CPOOLS_SYNC_TYPE = 57,

	RMF_COMPLIANCE_DOM_VER = 58,

	RMF_DIRS_VISITED = 59,
	RMF_DIRS_DELETED_DST = 60,
	RMF_FILES_TOTAL = 61,
	RMF_FILES_REPLICATED_SELECTED = 62,
	RMF_FILES_REPLICATED_TRANSFERRED = 63,
	RMF_FILES_REPLICATED_NEW_FILES = 64,
	RMF_FILES_REPLICATED_UPDATED_FILES = 65,
	RMF_FILES_REPLICATED_WITH_ADS = 66,
	RMF_FILES_REPLICATED_ADS_STREAMS = 67,
	RMF_FILES_REPLICATED_SYMLINKS = 68,
	RMF_FILES_REPLICATED_BLOCK_SPECS = 69,
	RMF_FILES_REPLICATED_CHAR_SPECS = 70,
	RMF_FILES_REPLICATED_SOCKETS = 71,
	RMF_FILES_REPLICATED_FIFOS = 72,
	RMF_FILES_REPLICATED_HARD_LINKS = 73,
	RMF_FILES_DELETED_SRC = 74,
	RMF_FILES_DELETED_DST = 75,
	RMF_FILES_SKIPPED_UP_TO_DATE = 76,
	RMF_FILES_SKIPPED_USER_CONFLICT = 77,
	RMF_FILES_SKIPPED_ERROR_IO = 78,
	RMF_FILES_SKIPPED_ERROR_NET = 79,
	RMF_FILES_SKIPPED_ERROR_CHECKSUM = 80,
	RMF_BYTES_TRANSFERRED = 81,
	RMF_BYTES_DATA_TOTAL = 82,
	RMF_BYTES_DATA_FILE = 83,
	RMF_BYTES_DATA_SPARSE = 84,
	RMF_BYTES_DATA_UNCHANGED = 85,
	RMF_BYTES_NETWORK_TOTAL = 86,
	RMF_BYTES_NETWORK_TO_TARGET = 87,
	RMF_BYTES_NETWORK_TO_SOURCE = 88,
	RMF_TWLIN = 89,
	RMF_FILELIN = 90,

	RMF_BYTES_RECOVERABLE = 91,
	RMF_BYTES_RECOVERED_SRC = 92,
	RMF_BYTES_RECOVERED_DST = 93,

	RMF_DIRS_CREATED_SRC = 94,
	RMF_DIRS_DELETED_SRC = 95,
	RMF_DIRS_LINKED_SRC = 96,
	RMF_DIRS_UNLINKED_SRC = 97,
	RMF_DIRS_REPLICATED = 98,
	RMF_FILES_REPLICATED_REGULAR = 99,
	RMF_FILES_LINKED_SRC = 100,
	RMF_FILES_UNLINKED_SRC = 101,
	RMF_CC_LINS_TOTAL = 102,
	RMF_CC_DIRS_NEW = 103,
	RMF_CC_DIRS_DELETED = 104,
	RMF_CC_DIRS_MOVED = 105,
	RMF_CC_DIRS_CHANGED = 106,
	RMF_CC_FILES_NEW = 107,
	RMF_CC_FILES_LINKED = 108,
	RMF_CC_FILES_UNLINKED = 109,
	RMF_CC_FILES_CHANGED = 110,
	RMF_CT_HASH_EXCEPTIONS_FOUND = 111,
	RMF_CT_HASH_EXCEPTIONS_FIXED = 112,
	RMF_CT_FLIPPED_LINS = 113,
	RMF_CT_CORRECTED_LINS = 114,
	RMF_CT_RESYNCED_LINS = 115,

	RMF_DIRS_CREATED_DST = 116,
	RMF_DIRS_LINKED_DST = 117,
	RMF_DIRS_UNLINKED_DST = 118,
	RMF_FILES_LINKED_DST = 119,
	RMF_FILES_UNLINKED_DST = 120,

	RMF_RESYNC_COMPLIANCE_DIRS_NEW = 121,
	RMF_RESYNC_COMPLIANCE_DIRS_LINKED = 122,
	RMF_RESYNC_CONFLICT_FILES_NEW = 123,
	RMF_RESYNC_CONFLICT_FILES_LINKED = 124,

	RMF_WORK_TYPE = 125,
	RMF_COMP_TARGET_MIN_LIN = 126,
	RMF_COMP_TARGET_MAX_LIN = 127,
	RMF_COMP_TARGET_CURR_LIN = 128,
	RMF_COMP_TARGET_SPLIT_LIN = 129,
	RMF_COMP_TARGET_DONE = 130,

	RMF_COMP_MAP_BUF = 131,
	RMF_CHKPT_LIN = 132,

	RMF_COMPLIANCE_CONFLICTS = 133,
	RMF_RUN_NUMBER = 134,

	RMF_COMP_COMMITTED_FILES = 135,

	RMF_PEER_GUID = 136,

	RMF_MAX,

	/* RMFs that are backported from future
	 * releases should be added here. This is
	 * so that the RMF ID's will be consistent
	 * between all releases. These messages will
	 * also need to be added to the rmf_whitelist
	 * array in the isilon/lib/isi_migrate/migr/msg.c
	 * file. */

	/* Pipeline Backported Fields */
	RMF_WI_LIN = 151,
	RMF_LOCK_WORK_ITEM = 161,
	RMF_WI_LOCK_MOD = 162,
	RMF_TARGET_WORK_LOCKING_PATCH = 163,
	RMF_COMP_LATE_STAGE_UNLINK_PATCH = 203,
	RMF_REQUESTED_WORKERS = 199,
};

struct rep_msg {
	unsigned len;
	unsigned type;
};

struct discon_msg {
	int fd;
	char *fd_name;
};

struct error_msg {
	unsigned error;
	unsigned io;
	char *str;
};

struct siq_err_msg {
	unsigned	siqerr;
	char		*errstr;
	char 		*nodename;
	/* source or destination */
	unsigned	errloc;
	//FS related errors 

	/* local unix error */
	unsigned	error;
	/*file */
	char		*file;
	unsigned	 enc;
	u_int64_t	 filelin;
	/*dirrectory path*/
	char		*dir;
	u_int64_t	dirlin;
	unsigned	eveclen;
	enc_t 		*evec;
};

struct old_cluster_msg {
	char *name;
	unsigned devslen;
	char *devs;
};

struct old_cluster_msg2 {
	char *name;
	char *sync_id;
	uint64_t run_id;
	unsigned devslen;
	char *devs;
};

struct old_target_init_msg
{
	/* If not using target_restrict, this is not set. If using
	 * target_restrict, this is the target's SmartConnect zone name the
	 * policy uses to refer to the target. */
	char *restricted_target_name;

	/* The unique policy ID. */
	char *policy_id;

	/* The policy name. */
	char *policy_name;

	/* A unique ID for this run (right now it's the time the coordinator
	 * starts. */
	uint64_t run_id;

	char *target_dir;
	char *src_root_dir;
	char *src_cluster_name;
	char *src_cluster_id;

	/* If this policy has been run before, this was the target cluster's
	 * ID. */
	char *last_target_cluster_id;

	uint32_t option; /* Really enum target_init_option */
	uint32_t loglevel;

	uint32_t flags;
};

struct old_target_resp_msg {
	char *error;
	char *target_cluster_name;
	char *target_cluster_id;
	unsigned ipslen;
	char *ips;
};

struct old_target_resp_msg2 {
	char *error;
	char *target_cluster_name;
	char *target_cluster_id;
	unsigned ipslen;
	char *ips;
	uint64_t domain_id;
	uint32_t domain_generation;
};

struct target_cancel_msg {
	/* The policy id to cancel a sync job for */
	char *policy_id;
};

#define ACK_OK	0
#define ACK_ERR	1
#define ACK_WARN 2

struct generic_ack_msg {
	/* If acknowledging a request with a unique id, the request's unique
	 * id */
	char *id;

	/* Any acknowledgment code previously set up. */
	uint32_t code;

	/* Any message to go with the acknowledgment */
	char *msg;
};

struct target_monitor_shutdown_msg {
	char *msg;
};

struct job_status_msg {
	uint32_t job_status;
};

struct target_policy_delete_msg {
	char *policy_id;
};

struct target_pol_stf_downgrade_msg {
	char *policy_id;
};

struct cleanup_sync_run_msg {
	char *policy_id;
};

struct cleanup_target_tmp_files_msg {
	char *policy_id;
	char *tgt_path;
};

#define CLEANUP_REP	0x01
#define CLEANUP_LMAP	0x02
struct cleanup_database_mirror_msg {
	unsigned flags;
	char *policy_id;
};

struct kill_msg {
	char *config;
	char *set;
};

struct auth_msg {
	unsigned salt;
	char *pass;
};

struct old_dir_msg3 {
	char *dir;
	unsigned mode;
	unsigned uid;
	unsigned gid;
	unsigned atime;
	unsigned mtime;
	unsigned reap;
	char *acl;
	unsigned eveclen;
	enc_t *evec;
	unsigned di_flags;
};

/* The only difference between old_dir_msg3 and dir_msg4 is the addition of
 * dir_lin in dir_msg4. We need to keep old_dir_msg3 around for backward
 * compatibility (old pworker to new sworker)  */
struct dir_msg4 {
	char *dir;
	unsigned mode;
	unsigned uid;
	unsigned gid;
	unsigned atime;
	unsigned mtime;
	unsigned reap;
	char *acl;
	unsigned eveclen;
	enc_t *evec;
	unsigned di_flags;
	u_int64_t dir_lin;
	unsigned need_ack_dir;
};

struct old_file_msg4 {
	unsigned mode;
	u_int64_t size;
	unsigned uid;
	unsigned gid;
	unsigned atime;
	unsigned mtime;
	char *file;
	unsigned enc;
	char *acl;
	unsigned di_flags;
	/*Additional members for TSM*/
	u_int64_t rev;
	u_int64_t dir_lin;
	u_int64_t filelin;
};

#define MAXHASHLEN  32

#pragma pack(1)
struct short_hash_req_info {
	u_int64_t hash_offset;
	u_int64_t hash_len;
	char hash_str[MAXHASHLEN + 1];
};
#pragma pack()
	
struct old_file_begin_msg {
	u_int64_t src_lin;
	u_int64_t src_dlin;
	u_int64_t src_dkey;
	u_int64_t size;
	unsigned file_type;
	unsigned di_flags;
	unsigned mode;
	unsigned uid;
	unsigned gid;
	unsigned atime;
	unsigned mtime;
	char *acl;
	unsigned full_transfer;
	char *fname;
	unsigned fenc;
	char *symlink;
	unsigned senc;
	unsigned sync_state;
	/* packed array of struct short_hash_req_info */
	unsigned short_hashes_len;
	char *short_hashes;
};

struct old_file_begin_msg2 {
	u_int64_t src_lin;
	u_int64_t src_dlin;
	u_int64_t src_dkey;
	u_int64_t size;
	unsigned file_type;
	unsigned di_flags;
	unsigned mode;
	unsigned uid;
	unsigned gid;
	uint64_t atime_sec;
	uint64_t atime_nsec;
	uint64_t mtime_sec;
	uint64_t mtime_nsec;
	char *acl;
	unsigned full_transfer;
	char *fname;
	unsigned fenc;
	char *symlink;
	unsigned senc;
	unsigned sync_state;
	/* packed array of struct short_hash_req_info */
	unsigned short_hashes_len;
	char *short_hashes;
	/* Used for upgrades of copy policies */
	u_int64_t prev_cookie;
	unsigned skip_bb_hash;
};

struct file_begin_msg {
	u_int64_t src_lin;
	u_int64_t src_dlin;
	u_int64_t src_dkey;
	u_int64_t size;
	unsigned file_type;
	unsigned di_flags;
	unsigned mode;
	unsigned uid;
	unsigned gid;
	uint64_t atime_sec;
	uint64_t atime_nsec;
	uint64_t mtime_sec;
	uint64_t mtime_nsec;
	char *acl;
	unsigned full_transfer;
	char *fname;
	unsigned fenc;
	char *symlink;
	unsigned senc;
	unsigned sync_state;
	/* packed array of struct short_hash_req_info */
	unsigned short_hashes_len;
	char *short_hashes;
	/* Used for upgrades of copy policies */
	u_int64_t prev_cookie;
	unsigned skip_bb_hash;

	//XXXDPL Consider making a more generic extattr section here.
	// So we don't have to rev the msg every time we add a new inode
	// attribute.
	unsigned worm_committed; // boolean
	unsigned worm_retention_date; 
};

#define LIN_CREATE 1

struct old_lin_update_msg {
	unsigned flag; /* New Lin/Update Lin */
	/* Lin Information */
	uint64_t slin;
	unsigned file_type;
	unsigned mode;
	unsigned uid;
	unsigned gid;
	uint64_t atime_sec;
	uint64_t atime_nsec;
	uint64_t mtime_sec;
	uint64_t mtime_nsec;
	uint64_t size;
	char *acl;
	unsigned di_flags;
	char *symlink;
	unsigned senc;
	unsigned full_transfer;
	unsigned skip_bb_hash;
	/* Optional Dirent Information */
	uint64_t parent_lin;
	char *d_name;
	unsigned d_enc;
};

struct lin_update_msg {
	unsigned flag; /* New Lin/Update Lin */
	/* Lin Information */
	uint64_t slin;
	unsigned file_type;
	unsigned mode;
	unsigned uid;
	unsigned gid;
	uint64_t atime_sec;
	uint64_t atime_nsec;
	uint64_t mtime_sec;
	uint64_t mtime_nsec;
	uint64_t size;
	char *acl;
	unsigned di_flags;
	char *symlink;
	unsigned senc;
	unsigned full_transfer;
	unsigned skip_bb_hash;
	/* Optional Dirent Information */
	uint64_t parent_lin;
	char *d_name;
	unsigned d_enc;
	// WORM state.
	unsigned worm_committed; // bool
	unsigned worm_retention_date;
};

struct lin_commit_msg {
	uint64_t slin;
	unsigned size_changed;
	char *hash_str;
};

struct extra_file_commit_msg {
};

struct file_done_msg {
	unsigned success;
	u_int64_t src_lin;
	u_int64_t src_dlin;
	char *fname;
	unsigned fenc;
	unsigned sync_state;
	unsigned size_changed;
	char *hash_str;
};

struct old_symlink_msg4 {
	unsigned mode;
	unsigned uid;
	unsigned gid;
	unsigned atime;
	unsigned mtime;
	char *name;
	char *target;
	unsigned nenc;
	unsigned tenc;
	unsigned di_flags;
	/*Additional members for TSM*/
	u_int64_t rev;
	u_int64_t dir_lin;
	u_int64_t filelin;
};

struct old_data_msg {
	unsigned size;
	char *data;
};

struct file_data_msg {
	u_int64_t src_lin;
	u_int64_t src_dlin;
	char *fname;
	unsigned fenc;
	u_int64_t offset;
	unsigned data_type;
	unsigned logical_size;
	unsigned checksum;
	unsigned data_size;
	char *data;
};

struct ads_msg {
	unsigned start;
};

struct done_msg {
	unsigned count;
};

struct delete_lin_msg {
	uint64_t slin;
};

struct link_msg {
	uint64_t dirlin;
	uint64_t d_lin;
	char *d_name;
	unsigned d_enc;
	unsigned d_type;
	char *symlink;
	unsigned senc;
	unsigned flags; /* Variable renamed in Halfpipe. */
};

struct unlink_msg {
	uint64_t dirlin;
	uint64_t d_lin;
	char *d_name;
	unsigned  d_enc;
	unsigned d_type;
};

struct extra_file_msg {
	unsigned flag;
	uint64_t slin;
	unsigned file_type;
	unsigned mode;
	unsigned uid;
	unsigned gid;
	uint64_t atime_sec;
	uint64_t atime_nsec;
	uint64_t mtime_sec;
	uint64_t mtime_nsec;
	uint64_t size;
	char *acl;
	unsigned di_flags;
	char *symlink;
	unsigned senc;
	char *d_name;
	unsigned d_enc;
};

#define FLAG_REAP		0x1
#define FLAG_ASSESS		0x2
#define FLAG_CLEAR		0x4
#define FLAG_NO_INTEGRITY	0x8
#define FLAG_MKLINKS		0x10
#define FLAG_LOGDELETES		0x20
#define FLAG_SYSTEM		0x40
#define FLAG_SNAPSHOT_PRI	0x80
#define FLAG_SNAPSHOT_SEC	0x100
#define FLAG_BURST_SOCK		0x200
#define FLAG_DIFF_SYNC		0x400
#define FLAG_STF_SYNC		0x800
#define FLAG_HAS_TARGET_AWARE	0x1000
#define FLAG_STF_UPGRADE_SYNC	0x2000
#define FLAG_STF_LINMAP_DELETE	0x4000
#define FLAG_LIN_MAP_BLK	0x8000
#define FLAG_RESTORE		0x10000
#define FLAG_VER_3_5		0x20000
#define FLAG_FAILBACK_PREP	0x40000
#define FLAG_COMPLIANCE		0x80000
#define FLAG_EXPECTED_DATALOSS	0x100000
#define FLAG_FILE_SPLIT		0x200000
#define FLAG_VER_CLOUDPOOLS	0x400000
#define FLAG_DOMAIN_MARK	0x800000
#define FLAG_COMPLIANCE_V2	0x1000000
#define FLAG_LOG_CONFLICTS	0x2000000
#define FLAG_COMP_V1_V2_PATCH	0x4000000
#define FLAG_ENABLE_HASH_TMPDIR 0x80000000

#define LINK_MSG_NEW_SINGLY_LINKED	0x1
#define LINK_MSG_CONFLICT_RES		0x2
#define LINK_MSG_SKIP_LIN_CHKPT		0x4
#define LINK_MSG_CHKPT_CHILD_LIN	0x8

#define LIN_UPDT_MSG_SKIP_ADS		0x1

/* Legacy */
struct old_work_init_msg {
	char *target; /* FQDN/public IP */
	char *peer;
	char *path;
	char *src;
	char *bwhost;
	char *predicate;
	unsigned lastrun_time;
	unsigned delete_secs;

	/* As defined aove */
	unsigned flags;
	char *pass;

	/*
	 * job_name is needed in workers to print info msg on start, and, 
	 * possibly to segregate workers from different jobs in BW/T
	 */
	char *job_name;
	unsigned loglevel;
	/*new action enum Replication or Migr or Sync or Local Del*/
	unsigned action;
};

/* Deprecated */
struct old_work_init_msg4 {
	char *target; /* FQDN/public IP */
	char *peer;
	char *path;
	char *src;
	char *bwhost;
	char *predicate;
	char *sync_id;
	uint64_t run_id;
	unsigned lastrun_time;
	unsigned delete_secs;

	/* As defined above */
	unsigned flags;
	char *pass;

	/*
	 * job_name is needed in workers to print info msg on start, and, 
	 * possibly to segregate workers from different jobs in BW/T
	 */
	char *job_name;
	unsigned loglevel;
	/*new action enum Replication or Migr or Sync or Local Del*/
	unsigned action;
	char *src2;
	char *basesrc;
	unsigned old_time;
	unsigned new_time;
	char *restrict_by;
	unsigned hash_type; /* really enum hash_type. Not HASH_NONE indicates
			     * diff sync */
	u_int64_t min_hash_piece_len;
	unsigned hash_pieces_per_file;
	char *recovery_dir;
	unsigned skip_bb_hash;
	unsigned worker_id;
};

/* Used coord -> pworker and pworker -> sworker */
struct old_work_init_msg5 {
	char *target; /* FQDN/public IP */
	char *peer;
	char *source_base;	/* sync base path on source */
	char *target_base;	/* sync base path on target */
	char *bwhost;
	char *predicate;
	char *sync_id;
	uint64_t run_id;
	unsigned lastrun_time;

	/* As defined above */
	unsigned flags;
	char *pass;

	/*
	 * job_name is needed in workers to print info msg on start, and, 
	 * possibly to segregate workers from different jobs in BW/T
	 */
	char *job_name;
	unsigned loglevel;
	/*new action enum Replication or Migr or Sync or Local Del*/
	unsigned action;
	unsigned old_time;
	unsigned new_time;
	char *restrict_by;
	unsigned hash_type; /* really enum hash_type. Not HASH_NONE indicates
			     * diff sync */
	uint64_t min_hash_piece_len;
	unsigned hash_pieces_per_file;
	char *recovery_dir;
	unsigned skip_bb_hash;
	unsigned worker_id;

	uint64_t prev_snapid;	/* previous sync snapshot id */
	uint64_t cur_snapid;	/* current sync snapshot id */
};

struct old_work_init_msg6 {
	char *target; /* FQDN/public IP */
	char *peer;
	char *source_base;	/* sync base path on source */
	char *target_base;	/* sync base path on target */
	char *bwhost;
	char *predicate;
	char *sync_id;
	uint64_t run_id;
	unsigned lastrun_time;

	/* As defined above */
	unsigned flags;
	char *pass;

	/*
	 * job_name is needed in workers to print info msg on start, and, 
	 * possibly to segregate workers from different jobs in BW/T
	 */
	char *job_name;
	unsigned loglevel;
	/*new action enum Replication or Migr or Sync or Local Del*/
	unsigned action;
	unsigned old_time;
	unsigned new_time;
	char *restrict_by;
	unsigned hash_type; /* really enum hash_type. Not HASH_NONE indicates
			     * diff sync */
	uint64_t min_hash_piece_len;
	unsigned hash_pieces_per_file;
	char *recovery_dir;
	unsigned skip_bb_hash;
	unsigned worker_id;

	uint64_t prev_snapid;	/* previous sync snapshot id */
	uint64_t cur_snapid;	/* current sync snapshot id */
	
	uint64_t domain_id;
	uint32_t domain_generation;
};

struct work_req_msg {
	unsigned errors;
	unsigned dirs;
	unsigned files;
	unsigned transfered;
	u_int64_t net;
	unsigned deleted;
	unsigned purged;
	unsigned initial;
};

#define WORK_RESP_RECURSE 1
#if 0
#define WORK_RESP_RENAME  2
#endif /*rename 0*/

struct work_resp_msg {
	unsigned flags;
	char *work;
	unsigned sellen;
	char *sel;
	unsigned eveclen;
	enc_t *evec;
};

struct work_resp_msg2 {
	unsigned flags;
	char *work;
	unsigned sellen;
	char *sel;
	unsigned eveclen;
	enc_t *evec;
	u_int64_t twlin;
	u_int64_t filelin;
};

struct work_resp_msg3 {
	u_int64_t wi_lin;
};

struct split_req_msg {
	uint64_t split_wi_lin;
};

struct split_resp_msg {
	unsigned succeeded;
};

/* master throttle host -> coord and coord -> pworker */
struct bandwidth_msg {
	unsigned ration;
};

struct throttle_msg {
	unsigned ration;
};

/* pworker/sworker -> local throttle host */
struct cpu_throttle_init_msg {
	unsigned worker_type;
};

struct cpu_throttle_stat_msg {
	uint64_t time;
	uint64_t cpu_time;
};

/* local throttle host -> pworker/sworker */
struct cpu_throttle_msg {
	unsigned ration;
};

/* coord -> master throttle host */
struct bandwidth_init_msg {
	char *policy_name;
	unsigned num_workers;
	unsigned priority;
};

struct throttle_init_msg {
	char *policy_name;
	unsigned num_workers;
	unsigned priority;
};

struct bandwidth_stat_msg {
	char *name;
	unsigned bytes_sent;
};

struct throttle_stat_msg {
	char *name;
	unsigned files_sent;
};

#define ACK_RES_OK 0
#define ACK_RES_ERROR 1

struct old_ack_msg2 {
	char *file;
	unsigned enc;
	u_int64_t rev;
	u_int64_t filesize;
	u_int64_t dir_lin;

	unsigned result; /*type is bool (0 - Ok, 1 - Error)*/
	unsigned io_errors; /*Number of io*/
	unsigned checksum_errors;
	unsigned transfered;
	u_int64_t filelin;
};

struct ack_msg3 {
	uint64_t src_lin;
	uint64_t src_dlin;
	uint64_t src_dkey;
	char *file;
	unsigned enc;
};

struct lin_ack_msg {
	uint64_t src_lin;
	uint64_t dst_lin;
	unsigned oper_count;
	unsigned result; /* Boolean, (0 - Ok, 1 - Error)*/
	unsigned sys_err; /* Unix error */
	unsigned siq_err; /* SynqIQ error */
};

struct old_pworker_tw_stat_msg {
	unsigned dirs_visited;
	unsigned dirs_dst_deleted;
	unsigned files_total;
	unsigned files_replicated_selected;
	unsigned files_replicated_transferred;
	unsigned files_replicated_new_files;
	unsigned files_replicated_updated_files;
	unsigned files_replicated_files_with_ads;
	unsigned files_replicated_ads_streams;
	unsigned files_replicated_symlinks;
	unsigned files_replicated_block_specs;
	unsigned files_replicated_char_specs;
	unsigned files_replicated_sockets;
	unsigned files_replicated_fifos;
	unsigned files_replicated_hard_links;
	unsigned files_deleted_src;
	unsigned files_deleted_dst;
	unsigned files_skipped_up_to_date;
	unsigned files_skipped_user_conflict;
	unsigned files_skipped_error_io;
	unsigned files_skipped_error_net;
	unsigned files_skipped_error_checksum;
	u_int64_t bytes_transferred;
	u_int64_t bytes_data_total;
	u_int64_t bytes_data_file;
	u_int64_t bytes_data_sparse;
	u_int64_t bytes_data_unchanged;
	u_int64_t bytes_network_total;
	u_int64_t bytes_network_to_target;
	u_int64_t bytes_network_to_source;
	u_int64_t twlin;
	u_int64_t filelin;
};

struct old_sworker_tw_stat_msg {
	unsigned dirs_visited;
	unsigned dirs_dst_deleted;
	unsigned files_total;
	unsigned files_replicated_selected;
	unsigned files_replicated_transferred;
	unsigned files_deleted_src;
	unsigned files_deleted_dst;
	unsigned files_skipped_up_to_date;
	unsigned files_skipped_user_conflict;
	unsigned files_skipped_error_io;
	unsigned files_skipped_error_net;
	unsigned files_skipped_error_checksum;
	u_int64_t bytes_transferred;
	u_int64_t bytes_recoverable;
	u_int64_t bytes_recovered_src;
	u_int64_t bytes_recovered_dst;
};

struct old_pworker_stf_stat_msg {
	unsigned dirs_created_src;
	unsigned dirs_deleted_src;
	unsigned dirs_linked_src;
	unsigned dirs_unlinked_src;
	unsigned dirs_replicated;
	unsigned files_total;
	unsigned files_replicated_new_files;
	unsigned files_replicated_updated_files;
	unsigned files_replicated_regular;
	unsigned files_replicated_with_ads;
	unsigned files_replicated_ads_streams;
	unsigned files_replicated_symlinks;
	unsigned files_replicated_block_specs;
	unsigned files_replicated_char_specs;
	unsigned files_replicated_sockets;
	unsigned files_replicated_fifos;
	unsigned files_replicated_hard_links;
	unsigned files_deleted_src;
	unsigned files_linked_src;
	unsigned files_unlinked_src;
	u_int64_t bytes_data_total;
	u_int64_t bytes_data_file;
	u_int64_t bytes_data_sparse;
	u_int64_t bytes_data_unchanged;
	u_int64_t bytes_network_total;
	u_int64_t bytes_network_to_target;
	u_int64_t bytes_network_to_source;
	unsigned cc_lins_total;
	unsigned cc_dirs_new;
	unsigned cc_dirs_deleted;
	unsigned cc_dirs_moved;
	unsigned cc_dirs_changed;
	unsigned cc_files_new;
	unsigned cc_files_linked;
	unsigned cc_files_unlinked;
	unsigned cc_files_changed;
	unsigned ct_hash_exceptions_found;
	unsigned ct_hash_exceptions_fixed;
	unsigned ct_flipped_lins;
	unsigned ct_corrected_lins;
	unsigned ct_resynced_lins;
};

struct old_sworker_stf_stat_msg {
	unsigned dirs_created_dst;
	unsigned dirs_deleted_dst;
	unsigned dirs_linked_dst;
	unsigned dirs_unlinked_dst;
	unsigned files_deleted_dst;
	unsigned files_linked_dst;
	unsigned files_unlinked_dst;
};

struct jobctl_msg {
	unsigned state;
};

struct list_msg {
	unsigned last;
	unsigned used;
	char	*list;
};

struct list_msg2 {
	unsigned range_begin;
	unsigned range_end;
	unsigned continued;
	unsigned last;
	unsigned used;
	char *list;
};

/*snapshot msgs*/
struct snap_msg {
	char *name;
	char *alias;
	char *path;
	unsigned expiration;
};

#define SNAP_MSG_RESULT_OK 		0
#define SNAP_MSG_RESULT_ERROR 		1
#define SNAP_MSG_RESULT_SKIP 		2
#define FAILOVER_SNAP_MSG_RESULT_ERROR	3

struct snap_resp_msg {
	unsigned result;
	char *msg;
};

#define POSITION_NONE		0x0
#define POSITION_ERROR		0x1
#define POSITION_LOG_DELETED_S	0x2
struct position_msg {
	unsigned	 reason;
	char		*comment;
	/*file*/
	char		*file;
	unsigned	 enc;
	u_int64_t	 filelin;
	/*parent directory*/
	char		*dir;
	unsigned	 eveclen;
	enc_t		*evec;
	u_int64_t	 dirlin;
};

struct ack_dir_msg
{
	char *dir;
	unsigned eveclen;
	enc_t *evec;
	unsigned is_new_dir;
};

struct short_hash_resp_msg {
	u_int64_t src_dkey;
	unsigned sync_state; /* Really enum sync_state */
};

struct full_hash_dir_req_msg
{
	char *dir;
	unsigned eveclen;
	enc_t *evec;
	uint64_t src_dlin;
};

struct full_hash_file_req_msg
{
	char *file;
	enc_t enc;
	u_int64_t size;
	u_int64_t src_dlin;
	u_int64_t src_dkey;
};

struct full_hash_file_resp_msg
{
	char *file;
	enc_t enc;
	uint64_t src_dlin;
	u_int64_t src_dkey;
	u_int64_t offset;
	u_int64_t len;
	char *hash_str;
	unsigned hash_type;
	unsigned status; // actually enum hash_status
	unsigned origin; // actually enum origin
};

struct hash_stop_msg
{
	unsigned stop_type; // actually enum hash_stop
	char *file;
	enc_t enc;
	uint64_t src_dlin;
	u_int64_t src_dkey;
};

struct hash_error_msg {
	unsigned hash_error;
};

struct lin_map_msg {
	char *name;
	enc_t enc;
	uint64_t src_lin;
	/* Used for upgrades of copy policies */
	u_int64_t cur_cookie;
	u_int64_t prev_cookie;
};

struct dir_upgrade_msg {
	u_int64_t slice_begin;
	u_int64_t slice_max;
};

struct upgrade_complete_msg {
	u_int64_t domain_id;
	int domain_generation;
};

struct user_attr_msg {
	unsigned namespc;
	unsigned encoding;
	unsigned is_dir;
	unsigned keylen;
	char *key;
	unsigned vallen;
	char *val;
};

struct stale_dir_msg {
	uint64_t src_dlin;
	uint64_t atime_sec;
	uint64_t atime_nsec;
	uint64_t mtime_sec;
	uint64_t mtime_nsec;
	unsigned is_start;
};

#define STF_ACK_INTERVAL       10000
struct stf_chkpt_msg {
	uint64_t slin;
	unsigned oper_count;
};


#define TOTAL_STF_LINS 10
struct lin_prefetch_msg {
	unsigned type;
	unsigned linslen;
	char *lins;
};

struct lin_map_blk_msg {
	unsigned lmap_updates;
};

#define REP_IS_DIR		(1 << 0)
#define REP_NON_HASHED		(1 << 1)
#define REP_NOT_ON_TARGET	(1 << 2)
// Reserved for cloudpools sync (1 << 3)
// type (stub/deep copy)	(1 << 4)

#pragma pack(1)
struct rep_update {
	uint64_t slin;
	unsigned lcount;
	unsigned flag;
};

struct lmap_update {
	uint64_t slin;
	uint64_t dlin;
};
#pragma pack()

#define REPSTATE_UPDT	0x01
#define LMAP_UPDT	0x02
#define FLIP_REGULAR	0x04
#define REVERT_ENTRIES	0x08
#define RESYNC_ENTRIES	0x10
#define MIRROR_ENTRIES	0x20

struct database_updt_msg {
	unsigned flag;
	unsigned updtlen;
	char *updt;
};

struct common_ver_msg {
	unsigned version;
};

// Unneeded since empty: struct failover_verify_msg {}; 

struct verify_failover_resp_msg {
	unsigned failover_done;
};

struct old_failback_prep_finalize_msg {
	unsigned create_fbpol;
	char *fbpol_dst_path;
};

struct old_failback_prep_finalize_resp_msg {
	char *error;
	char *fbpol_id;
	char *fbpol_name;
	char *fbpol_src_cluster_name;
	char *fbpol_src_cluster_id;
};

struct worker_pid_msg {
	unsigned worker_id;
	unsigned process_id;
};

struct burst_sock_msg {
	unsigned flags;
	unsigned port;
	unsigned max_buf_size;
};

struct worm_state_msg {
	unsigned 	committed;
	u_int64_t	retention_date;
};

struct general_signal_msg {
	unsigned signal;
};

struct old_cloudpools_begin_msg {
};

struct old_cloudpools_stub_data_msg {
	unsigned type;
	unsigned size;
	char *data;
};

struct old_cloudpools_done_msg {
};

struct siq_ript_msg {
	struct isi_hash field_hash; // Hash field id to version hash table.
};

struct generic_msg
{
	struct rep_msg head;
	union
	{
		struct discon_msg discon;
		struct error_msg error;
		struct old_cluster_msg old_cluster;
		struct old_cluster_msg2 old_cluster2;
		struct old_target_init_msg old_target_init;
		struct old_target_resp_msg old_target_resp;
		struct old_target_resp_msg2 old_target_resp2;
		struct target_cancel_msg target_cancel;
		struct generic_ack_msg generic_ack;
		struct target_monitor_shutdown_msg target_monitor_shutdown;
		struct job_status_msg job_status;
		struct target_policy_delete_msg target_policy_delete;
		struct cleanup_sync_run_msg cleanup_sync_run;
		struct cleanup_target_tmp_files_msg cleanup_target_tmp_files;
		struct kill_msg kill;

		struct auth_msg auth;
		struct old_dir_msg3 old_dir3;
		struct dir_msg4 dir4;
		struct old_file_msg4 old_file4;
		struct old_symlink_msg4 old_symlink4;
		struct old_data_msg old_data;
		struct ads_msg ads;
		struct done_msg done;
		struct old_work_init_msg old_work_init;
		struct old_work_init_msg4 old_work_init4;
		struct old_work_init_msg5 work_init5;
		struct old_work_init_msg6 old_work_init6;
		struct work_req_msg work_req;
		struct work_resp_msg work_resp;
		struct work_resp_msg2 work_resp2;
		struct work_resp_msg3 work_resp3;
		struct split_req_msg split_req;
		struct split_resp_msg split_resp;

		struct old_ack_msg2 old_ack2;
		struct ack_msg3 ack3;
		struct lin_ack_msg lin_ack;
		struct old_sworker_tw_stat_msg old_sworker_tw_stat;
		struct jobctl_msg job_ctl;
		struct list_msg list;
		struct list_msg2 list2;

		struct snap_msg snap;
		struct snap_resp_msg snap_resp;

		struct position_msg position;

		struct ack_dir_msg ack_dir;
		struct short_hash_resp_msg short_hash_resp;
		struct full_hash_dir_req_msg full_hash_dir_req;
		struct full_hash_file_req_msg full_hash_file_req;
		struct full_hash_file_resp_msg full_hash_file_resp;
		struct hash_stop_msg hash_stop;
		struct hash_error_msg hash_error;
		struct file_data_msg file_data;
		struct old_file_begin_msg2 old_file_begin2;
		struct old_file_begin_msg old_file_begin_msg;
		struct file_begin_msg file_begin_msg;
		struct file_done_msg file_done_msg;
		struct siq_err_msg  siq_err;
		struct old_pworker_tw_stat_msg old_pworker_tw_stat;
		
		struct bandwidth_init_msg bandwidth_init;
		struct throttle_init_msg throttle_init;
		struct bandwidth_msg bandwidth;
		struct throttle_msg throttle;
		struct bandwidth_stat_msg bandwidth_stat;
		struct throttle_stat_msg throttle_stat;
		struct delete_lin_msg delete_lin;
		struct link_msg link;
		struct unlink_msg unlink;
		struct old_lin_update_msg old_lin_update;
		struct lin_update_msg lin_update;
		struct lin_commit_msg lin_commit;
		struct lin_map_msg lin_map;
		struct dir_upgrade_msg dir_upgrade;
		struct upgrade_complete_msg upgrade_complete;
		struct target_pol_stf_downgrade_msg target_pol_stf_downgrade;
		struct old_pworker_stf_stat_msg old_pworker_stf_stat;
		struct old_sworker_stf_stat_msg old_sworker_stf_stat;
		struct user_attr_msg user_attr;
		struct stale_dir_msg stale_dir;
		struct stf_chkpt_msg stf_chkpt;
		struct lin_prefetch_msg lin_prefetch;
		struct extra_file_msg extra_file; 
		struct extra_file_commit_msg extra_file_commit;
		struct cpu_throttle_init_msg cpu_throttle_init;
		struct cpu_throttle_stat_msg cpu_throttle_stat;
		struct cpu_throttle_msg cpu_throttle;
		struct lin_map_blk_msg	lin_map_blk;
		struct database_updt_msg database_updt;
		struct common_ver_msg	common_ver;
		//struct failover_verify_msg failover_verify;
		struct verify_failover_resp_msg verify_failover_resp;
		struct old_failback_prep_finalize_msg
		    old_failback_prep_finalize;
		struct old_failback_prep_finalize_resp_msg
		    old_failback_prep_finalize_resp;
		struct cleanup_database_mirror_msg cleanup_database_mirror;
		struct worker_pid_msg worker_pid;
		struct burst_sock_msg burst_sock;
		struct worm_state_msg worm_state;
		struct general_signal_msg general_signal;
		struct old_cloudpools_begin_msg old_cloudpools_begin;
		struct old_cloudpools_stub_data_msg old_cloudpools_stub_data;
		struct old_cloudpools_done_msg old_cloudpools_done_msg;
		struct siq_ript_msg ript;
	} body;
};

/* Riptide message structs */
struct worker_init_msg {
	char *policy_name;
	char *node_mask;
};

struct worker_msg {
	unsigned max_num_workers;
	char *allowed_nodes;
	unsigned requested_workers;
};

struct worker_stat_msg {
	unsigned workers_used;
};


/* local throttle host -> master throttle host
 * name is the ip of the sender's node */
struct cpu_usage_init_msg {
	char *name;
};

struct cpu_node_usage_msg {
	uint32_t usage_pct;
	char *name;
};

struct target_init_msg
{
	/* If not using target_restrict, this is not set. If using
	 * target_restrict, this is the target's SmartConnect zone name the
	 * policy uses to refer to the target. */
	char *restricted_target_name;

	/* The unique policy ID. */
	char *policy_id;

	/* The policy name. */
	char *policy_name;

	/* A unique ID for this run (right now it's the time the coordinator
	 * starts. */
	uint64_t run_id;

	char *target_dir;
	char *src_root_dir;
	char *src_cluster_name;
	char *src_cluster_id;

	/* If this policy has been run before, this was the target cluster's
	 * ID. */
	char *last_target_cluster_id;

	uint32_t option; /* Really enum target_init_option */
	uint32_t loglevel;

	uint32_t flags;

	uint64_t wi_lock_mod;

	/* Indicates if source supports late stage unlink of directories in
	 * compliance mode
	 */
	uint8_t comp_late_stage_unlink_patch;
};

struct target_resp_msg {
	char *error;
	char *target_cluster_name;
	char *target_cluster_id;
	uint32_t ipslen;
	uint8_t *ips;
	uint64_t domain_id;
	uint32_t domain_generation;

	/* Cloudpools common version information */
	uint8_t *tgt_cbm_vers_buf;
	uint32_t tgt_cbm_vers_buf_len;

	/* Compliance Domain version of the target dir */
	uint8_t compliance_dom_ver;

	uint8_t target_work_locking_patch;

	/* Indicates if target supports late stage unlink of directories in
	 * compliance mode
	 */
	uint8_t comp_late_stage_unlink_patch;
};

struct cloudpools_begin_msg {
};

struct cloudpools_stub_data_msg {
	unsigned type;
	unsigned size;
	char *data;
};

struct cloudpools_done_msg {
};


struct work_init_msg {
	char *target; /* FQDN/public IP */
	char *peer;
	char *source_base;	/* sync base path on source */
	char *target_base;	/* sync base path on target */
	char *bwhost;
	char *predicate;
	char *sync_id;
	uint64_t run_id;
	uint64_t run_number;
	uint32_t lastrun_time;

	/* As defined above */
	uint32_t flags;
	char *pass;

	/*
	 * job_name is needed in workers to print info msg on start, and,
	 * possibly to segregate workers from different jobs in BW/T
	 */
	char *job_name;
	uint32_t loglevel;
	/*new action enum Replication or Migr or Sync or Local Del*/
	uint32_t action;
	uint32_t old_time;
	uint32_t new_time;
	char *restrict_by;
	uint32_t hash_type; /* really enum hash_type. Not HASH_NONE indicates
			     * diff sync */
	uint64_t min_hash_piece_len;
	uint32_t hash_pieces_per_file;
	char *recovery_dir;
	uint32_t skip_bb_hash;
	uint32_t worker_id;

	uint64_t prev_snapid;	/* previous sync snapshot id */
	uint64_t cur_snapid;	/* current sync snapshot id */

	uint64_t domain_id;
	uint32_t domain_generation;

	/* Cloudpools common version information */
	uint8_t *common_cbm_vers_buf;
	uint32_t common_cbm_vers_buf_len;
	uint32_t deep_copy;
	char *peer_guid;

	uint64_t wi_lock_mod;
	uint8_t target_work_locking_patch;
};

struct failback_prep_finalize_msg {
	uint32_t create_fbpol;
	char *fbpol_dst_path;
	uint32_t deep_copy;
};

struct failback_prep_finalize_resp_msg {
	char *error;
	char *fbpol_id;
	char *fbpol_name;
	char *fbpol_src_cluster_name;
	char *fbpol_src_cluster_id;
};

struct worker_node_update_msg {
	char *node_mask;
};

struct pworker_tw_stat_msg {
        uint32_t dirs_visited;
        uint32_t dirs_dst_deleted;
        uint32_t files_total;
        uint32_t files_replicated_selected;
        uint32_t files_replicated_transferred;
        uint32_t files_replicated_new_files;
        uint32_t files_replicated_updated_files;
        uint32_t files_replicated_with_ads;
        uint32_t files_replicated_ads_streams;
        uint32_t files_replicated_symlinks;
        uint32_t files_replicated_block_specs;
        uint32_t files_replicated_char_specs;
        uint32_t files_replicated_sockets;
        uint32_t files_replicated_fifos;
        uint32_t files_replicated_hard_links;
        uint32_t files_deleted_src;
        uint32_t files_deleted_dst;
        uint32_t files_skipped_up_to_date;
        uint32_t files_skipped_user_conflict;
        uint32_t files_skipped_error_io;
        uint32_t files_skipped_error_net;
        uint32_t files_skipped_error_checksum;
        uint64_t bytes_transferred;
        uint64_t bytes_data_total;
        uint64_t bytes_data_file;
        uint64_t bytes_data_sparse;
        uint64_t bytes_data_unchanged;
        uint64_t bytes_network_total;
        uint64_t bytes_network_to_target;
        uint64_t bytes_network_to_source;
        uint64_t twlin;
        uint64_t filelin;
};

struct sworker_tw_stat_msg {
        uint32_t dirs_visited;
        uint32_t dirs_dst_deleted;
        uint32_t files_total;
        uint32_t files_replicated_selected;
        uint32_t files_replicated_transferred;
        uint32_t files_deleted_src;
        uint32_t files_deleted_dst;
        uint32_t files_skipped_up_to_date;
        uint32_t files_skipped_user_conflict;
        uint32_t files_skipped_error_io;
        uint32_t files_skipped_error_net;
        uint32_t files_skipped_error_checksum;
        uint64_t bytes_transferred;
        uint64_t bytes_recoverable;
        uint64_t bytes_recovered_src;
        uint64_t bytes_recovered_dst;
        uint32_t committed_files;
};

struct pworker_stf_stat_msg {
        uint32_t dirs_created_src;
        uint32_t dirs_deleted_src;
        uint32_t dirs_linked_src;
        uint32_t dirs_unlinked_src;
        uint32_t dirs_replicated;
        uint32_t files_total;
        uint32_t files_replicated_new_files;
        uint32_t files_replicated_updated_files;
        uint32_t files_replicated_regular;
        uint32_t files_replicated_with_ads;
        uint32_t files_replicated_ads_streams;
        uint32_t files_replicated_symlinks;
        uint32_t files_replicated_block_specs;
        uint32_t files_replicated_char_specs;
        uint32_t files_replicated_sockets;
        uint32_t files_replicated_fifos;
        uint32_t files_replicated_hard_links;
        uint32_t files_deleted_src;
        uint32_t files_linked_src;
        uint32_t files_unlinked_src;
        uint64_t bytes_data_total;
        uint64_t bytes_data_file;
        uint64_t bytes_data_sparse;
        uint64_t bytes_data_unchanged;
        uint64_t bytes_network_total;
        uint64_t bytes_network_to_target;
        uint64_t bytes_network_to_source;
        uint32_t cc_lins_total;
        uint32_t cc_dirs_new;
        uint32_t cc_dirs_deleted;
        uint32_t cc_dirs_moved;
        uint32_t cc_dirs_changed;
        uint32_t cc_files_new;
        uint32_t cc_files_linked;
        uint32_t cc_files_unlinked;
        uint32_t cc_files_changed;
        uint32_t ct_hash_exceptions_found;
        uint32_t ct_hash_exceptions_fixed;
        uint32_t ct_flipped_lins;
        uint32_t ct_corrected_lins;
        uint32_t ct_resynced_lins;
	uint32_t resync_compliance_dirs_new;
	uint32_t resync_compliance_dirs_linked;
	uint32_t resync_conflict_files_new;
	uint32_t resync_conflict_files_linked;
};

struct sworker_stf_stat_msg {
        uint32_t dirs_created_dst;
        uint32_t dirs_deleted_dst;
        uint32_t dirs_linked_dst;
        uint32_t dirs_unlinked_dst;
        uint32_t files_deleted_dst;
        uint32_t files_linked_dst;
        uint32_t files_unlinked_dst;
	uint32_t compliance_conflicts;
        uint32_t committed_files;
};

struct comp_commit_begin_msg {
};

struct comp_commit_end_msg {
};

struct comp_dir_dels_begin_msg {
};

struct comp_target_work_range_msg {
	uint32_t work_type;
	uint64_t comp_target_min_lin;
	uint64_t comp_target_max_lin;
};

struct comp_target_checkpoint_msg {
	uint32_t work_type;
	uint64_t comp_target_curr_lin;
	uint8_t done;
	uint64_t comp_target_split_lin;
};

struct comp_map_begin_msg {
};

struct comp_map_end_msg {
};

struct comp_map_msg {
	uint32_t size;
	uint32_t comp_map_buf_len;
	uint8_t *comp_map_buf;
	uint64_t chkpt_lin;
};

struct target_work_lock_msg {
	uint64_t wi_lin;
	uint8_t lock_work_item;
};

/**
 ** Job Control Shortcut
 */

static inline enum siq_job_state get_job_state(const char *dir)
{
	enum siq_job_state state;

	if (SIQ_JS_NONE == (state = siq_job_current_state(dir))) {
		log(FATAL, "Failed to query job state from %s",
		    dir ? dir : "unknown");
	}

	return state;
}

#ifdef DEBUG_PERFORMANCE_STATS
void file_data_start(void);
void file_data_end(void);
#endif

void print_net_state(FILE *output);

#endif /* _MIGR_ISIREP_H */
