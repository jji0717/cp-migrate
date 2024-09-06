ISI_TOP=	../../..

LIB=		isi_migrate
SHLIB_MAJOR=	2
SHLIB_MINOR=	0
MAN=
.include "${ISI_TOP}/bin/isi_migrate/local/Makefile.tsm"

YFLAGS+=	-p getdate_yy
CFLAGS+=        -Wno-pointer-sign
CFLAGS+=	-fPIC
CFLAGS+=	-I${.CURDIR} -I${.OBJDIR}

.PATH: $(.CURDIR)/../parser $(.CURDIR)/../sched $(.CURDIR)/../migr

CLEANFILES+=	getdate.c y.tab.h

LDFLAGS+=	${LOCAL_LDFLAGS}

LIBADD+=	ifs
LIBADD+=	isi_burst_sdk
LIBADD+=	isi_celog
LIBADD+=	isi_config
LIBADD+=	isi_date
LIBADD+=	isi_domain
LIBADD+=	isi_event
LIBADD+=	isi_flexnet
LIBADD+=	isi_gconfig
LIBADD+=	isi_gmp
LIBADD+=	isi_ilog
LIBADD+=	isi_inx_addr
LIBADD+=	isi_ilog
LIBADD+=	isi_net
LIBADD+=	isi_sbtree
LIBADD+=	isi_snapshot
LIBADD+=	isi_ufp
LIBADD+=	isi_upgrade_api
LIBADD+=	isi_util
LIBADD+=	isi_xml

LDADD+=		-lsqlite3
LDADD+=		-lmd
LDADD+=		-lz

# config
SRCS+=		migrate_adt.c
SRCS+=		bwt_conf.c
SRCS+=		siq_conf.c
SRCS+=		siq_gconfig_gcfg.c
SRCS+=		siq_job.c
SRCS+=		siq_policy.c
SRCS+=		siq_report.c
SRCS+=		siq_util.c
SRCS+=		siq_error.c
SRCS+=		siq_source.c
SRCS+=		siq_target.c
SRCS+=		siq_sqlite.c
SRCS+=		siq_btree_public.c
SRCS+=		siq_alert.c

# parser
SRCS+=		unparser.c
SRCS+=		getdate.c
SRCS+=		function.c
SRCS+=		pparser.c
SRCS+=		operator.c
SRCS+=		siq_ui_pred.c
SRCS+=		option.c

CHECK_SRCS=	check_pred.c
CHECK_SRCS+=	check_select.c

# sched
SRCS+=		sched_utils.c
SRCS+=		start_policy.c
SRCS+=		siq_jobctl_getnextrun.c
SRCS+=		siq_atomic_primitives.c
SRCS+=		siq_jobctl.c
SRCS+=		siq_log.c
SRCS+=		siq_glob_param_load.c
SRCS+=		sched_reports.c
SRCS+=		sched_snap_locks.c

CHECK_SRCS+=	check_job.c
CHECK_SRCS+=	check_policy.c
CHECK_SRCS+=	check_source.c
CHECK_SRCS+=	check_target.c
CHECK_SRCS+=	check_alert.c

# migr
SRCS+=		adt.c
SRCS+=		alg.c
SRCS+=		hasher.c
SRCS+=		listfuncs.c
SRCS+=		msg.c
SRCS+=		net.c
SRCS+=		nodes.c
SRCS+=		utils.c
SRCS+=		treewalk.c
SRCS+=		siq_workers_utils.c
SRCS+=		siq_error_handling.c
SRCS+=		restrict.c
SRCS+=		selectors.c
SRCS+=		linmap.c
SRCS+=		repstate.c
SRCS+=		worklist.c
SRCS+=		summ_stf.c
SRCS+=		work_item.c
SRCS+=		siq_btree.c
SRCS+=		cpu_throttle.c
SRCS+=		cpools_sync_state.c
SRCS+=		lin_list.c
SRCS+=		compliance_map.c
SRCS+=		version.c
SRCS+=		comp_conflict_log.c

CHECK_SRCS=	check_msg.c
CHECK_SRCS+=	check_dir.c
CHECK_SRCS+=	check_treewalk.c
CHECK_SRCS+=	check_chdir.c
CHECK_SRCS+=	check_snap_path_handling.c
CHECK_SRCS+=	check_repstate.c
CHECK_SRCS+=	check_cpu.c
CHECK_SRCS+=	check_utils.c
CHECK_SRCS+=	check_cpstate.c
CHECK_SRCS+=	check_get_local_cluster_info.c
CHECK_SRCS+=	check_lin_list.c
CHECK_SRCS+=	check_compliance_map.c
CHECK_SRCS+=	check_checkpoints.c
CHECK_SRCS+=	check_linmap.c

CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_gmp -lisi_gmp -lisi_celog
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_testutil -lisi_testutil

# gconfig auto-generation

GCFG_SRCS+=	siq_gconfig.gcfg

GCFG_DEPEND_ON_TOOL_UPDATES=1

NO_WERROR=	bug 149946

UPGRADE_POST_COMMIT_HOOKS += comp_v1_v2_patch
UPGRADE_POST_COMMIT_HOOKS += comp_late_stage_unlink_patch
UPGRADE_POST_COMMIT_HOOKS += target_work_locking_patch

.include "${ISI_TOP}/isi.gcfg.pre.mk"
.include "${ISI_TOP}/isi.lib.mk"
.include "${ISI_TOP}/isi.gcfg.post.mk"
.include "${ISI_TOP}/isi.upgradehooks.mk"
