
# PROVIDE: isi_cpool_d_common
# REQUIRE: isi_cpool_config
# REQUIRE: isi_config
# REQUIRE: isi_daemon
# REQUIRE: isi_hw
# REQUIRE: isi_ilog
# REQUIRE: isi_sbtree
# REQUIRE: isi_util
# REQUIRE: isi_flexnet
# REQUIRE: isi_job
# REQUIRE: isi_daemon
# REQUIRE: isi_pools_fsa_gcfg
# REQUIRE: isi_licensing
# REQUIRE: isi_upgrade_api

UPGRADE_POST_COMMIT_HOOKS += upgrade_post_commit_archive_task_v2
UPGRADE_PRE_UPGRADE_MANDATORY_HOOKS += isi_upgrade_cloud_archive_task_version_checker

ISI_TOP=	../..

LIB_CXX=	isi_cpool_d_common
SHLIB_MAJOR=	1
SHLIB_MINOR=	0
MAN=
CFLAGS+=  -o0
SRCS=
SRCS+=		cpool_d_common.cpp
SRCS+=		cpool_d_debug.cpp
SRCS+=		cpool_fd_store.cpp
SRCS+=		cpool_test_fail_point.cpp
SRCS+=		daemon_job_configuration.cpp
SRCS+=		daemon_job_manager.cpp
SRCS+=		daemon_job_member.cpp
SRCS+=		daemon_job_record.cpp
SRCS+=		daemon_job_request.cpp
SRCS+=		daemon_job_stats.cpp
SRCS+=		daemon_job_types.cpp
SRCS+=		hsbt.c
SRCS+=		isi_cloud_object_id.cpp
SRCS+=		operation_type.cpp
SRCS+=		scoped_object_list.cpp
SRCS+=		task.cpp
SRCS+=		ckpt.cpp
SRCS+=		task_key.cpp
SRCS+=		task_map.cpp
SRCS+=		ufp_init.c
SRCS+=		isi_cpool_revoked_fd.cpp
SRCS+=		resume_key.cpp
SRCS+=		daemon_job_reader.cpp
SRCS+=		sbt_reader.cpp

LIBADD+=	ifs
LIBADD+=	isi_cloud_common
LIBADD+=	isi_cpool_config
LIBADD+=	isi_config
LIBADD+=	isi_daemon
LIBADD+=	isi_flexnet
LIBADD+=	isi_ilog
LIBADD+=	isi_job
LIBADD+=	isi_pools_fsa_gcfg
LIBADD+=	isi_sbtree
LIBADD+=	isi_ufp
LIBADD+=	isi_upgrade_api
LIBADD+=	isi_util
LIBADD+=	isi_util_cpp
LIBADD+=	procstat

CHECK_SRCS+=	check_checkpointing.cpp
CHECK_SRCS+=	check_common.c
CHECK_SRCS+=	check_cpool_fd_store.cpp
CHECK_SRCS+=	check_cpool_flock.cpp
CHECK_SRCS+=	check_daemon_job_configuration.cpp
CHECK_SRCS+=	check_daemon_job_manager.cpp
CHECK_SRCS+=	check_daemon_job_member.cpp
CHECK_SRCS+=	check_daemon_job_record.cpp
CHECK_SRCS+=	check_daemon_job_reader.cpp
CHECK_SRCS+=	check_daemon_job_stats.cpp
CHECK_SRCS+=	check_hsbt.c
CHECK_SRCS+=	check_isi_cloud_object_id.cpp
CHECK_SRCS+=	check_operation_type.cpp
CHECK_SRCS+=	check_scoped_object_list.cpp
CHECK_SRCS+=	check_task.cpp
CHECK_SRCS+=	check_task_key.cpp
CHECK_SRCS+=	check_task_map.cpp
CHECK_SRCS+=	unit_test_helpers.cpp

CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_daemon -lisi_daemon
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_pools_fsa_gcfg -lisi_pools_fsa_gcfg
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_licensing -lisi_licensing
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_ecs -lisi_ecs
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_event -lisi_event
CHECK_LIBS+=	-L${ISI_OBJ}/lib/util -lutil
CHECK_LIBS+=	-L${ISI_OBJ}/lib/procstat -lprocstat
CHECK_LIBS+=	-lpthread

.include "${ISI_TOP}/isi.lib.mk"
.include "${ISI_TOP}/isi.upgradehooks.mk"
